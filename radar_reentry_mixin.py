#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
递进雷达闸门 + 智能限价再入场（混入 PositionSupervisorDeepcoin）。
终极版：5m/3m 极值优于 TV 挂限价；休眠至激活线；硬止损不重入。
Deepcoin v1.0 · 与 Binance 逻辑完全对齐。
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

from reentry_profiles import (
    ACTIVATION_TP1_FRAC,
    STERILE_MAX_RETRY,
    activation_frac_for_attempt,
    apply_tier_to_breath_profile,
    arm_stop_price,
    get_reentry_profile,
    make_reentry_client_order_id,
    reentry_enabled,
    tier_label,
)
from smart_reentry_engine import (
    REENTRY_STATE_KEYS,
    blank_reentry_state,
    bump_after_reentry_fill,
    evaluate_flat_for_reentry,
    init_cycle_on_open,
    max_unfilled_refreshes,
    open_reentry_window,
    plan_reentry_limit,
)

logger = logging.getLogger(__name__)


def _client(self):
    """Exchange-agnostic client accessor."""
    return getattr(self, "binance_client", None) or getattr(self, "deepcoin_client", None)


class RadarReentryMixin:
    """递进激活 + 限价再入场。依赖宿主的 deepcoin_client / dingtalk / breath 方法。"""

    def _init_reentry_runtime(self):
        blank = blank_reentry_state()
        for k, v in blank.items():
            setattr(self, k, v)
        self._reentry_open_snap = None
        self._reentry_cycle_aborted = False
        self._base_breath_profile = dict(getattr(self, "breath_profile", None) or {})

    def _reentry_state_dict(self) -> Dict[str, Any]:
        return {
            "reentry_attempt": int(getattr(self, "reentry_attempt", 0) or 0),
            "radar_tier": int(getattr(self, "radar_tier", 0) or 0),
            "adx_tier": int(getattr(self, "adx_tier", 1) or 1),
            "reentry_window_deadline_ts": float(
                getattr(self, "reentry_window_deadline_ts", 0) or 0
            ),
            "radar_activation_frac": float(
                getattr(self, "radar_activation_frac", ACTIVATION_TP1_FRAC) or ACTIVATION_TP1_FRAC
            ),
            "radar_activation_price": float(
                getattr(self, "radar_activation_price", 0) or 0
            ),
            "radar_activation_adx": float(
                getattr(self, "radar_activation_adx", 0) or 0
            ),
            "radar_activation_sticky": bool(
                getattr(self, "radar_activation_sticky", False)
            ),
            "cycle_tv_price": float(getattr(self, "cycle_tv_price", 0) or 0),
            "cycle_tv_side": getattr(self, "cycle_tv_side", None),
            "cycle_open_atr": float(getattr(self, "cycle_open_atr", 0) or 0),
            "cycle_entry": float(getattr(self, "cycle_entry", 0) or 0),
            "reentry_active": bool(getattr(self, "reentry_active", False)),
            "reentry_limit_order_id": getattr(self, "reentry_limit_order_id", None),
            "reentry_limit_px": float(getattr(self, "reentry_limit_px", 0) or 0),
            "reentry_limit_deadline_ts": float(
                getattr(self, "reentry_limit_deadline_ts", 0) or 0
            ),
            "reentry_unfilled_refreshes": int(
                getattr(self, "reentry_unfilled_refreshes", 0) or 0
            ),
            "reentry_order_tag": getattr(self, "reentry_order_tag", None),
            "reentry_sterile_fail_count": int(
                getattr(self, "reentry_sterile_fail_count", 0) or 0
            ),
            "last_exit_source": str(getattr(self, "last_exit_source", "") or ""),
            "last_exit_px": float(getattr(self, "last_exit_px", 0) or 0),
            "radar_pending_arm": bool(getattr(self, "radar_pending_arm", True)),
        }

    def _load_reentry_state_from_dict(self, s: Dict[str, Any]):
        if not isinstance(s, dict):
            return
        blank = blank_reentry_state()
        for k, default in blank.items():
            if k not in s:
                continue
            val = s.get(k, default)
            if k in (
                "reentry_attempt", "radar_tier", "adx_tier", "reentry_unfilled_refreshes",
                "reentry_sterile_fail_count",
            ):
                setattr(self, k, int(val or 0))
            elif k in (
                "radar_activation_frac", "reentry_window_deadline_ts", "cycle_tv_price", "cycle_open_atr",
                "cycle_entry", "reentry_limit_px", "reentry_limit_deadline_ts",
                "last_exit_px",
            ):
                setattr(self, k, float(val or 0))
            elif k in ("reentry_active", "radar_pending_arm", "radar_activation_sticky"):
                setattr(self, k, bool(val))
            elif k == "reentry_order_tag":
                setattr(self, k, str(val) if val else None)
            else:
                setattr(self, k, val)

    def _clear_reentry_cycle(self, source=""):
        """新 TV / 硬止损 / 周期结束：清再入场与周期字段。"""
        try:
            self._cancel_reentry_limit(reason=source or "清周期")
        except Exception:
            pass
        blank = blank_reentry_state()
        for k, v in blank.items():
            setattr(self, k, v)
        self._reentry_open_snap = None
        self._reentry_cycle_aborted = False
        base = getattr(self, "_base_breath_profile", None)
        if isinstance(base, dict) and base:
            self.breath_profile = dict(base)
        if source:
            logger.info(f"🧹 [{self.symbol}] 再入场周期已清零 | {source}")

    def _apply_tier_breath_overlay(self):
        """§5.2/§5.3 档位分级呼吸叠加。"""
        base = getattr(self, "_base_breath_profile", None) or getattr(
            self, "breath_profile", None
        ) or {}
        if not getattr(self, "_base_breath_profile", None) and base:
            self._base_breath_profile = dict(base)
        attempt = int(
            getattr(self, "radar_tier", 0)
            or getattr(self, "adx_tier", 1)
            or 1
        )
        self.breath_profile = apply_tier_to_breath_profile(
            dict(self._base_breath_profile or base),
            attempt,
            get_reentry_profile(self.symbol),
        )

    def _begin_open_radar_dormant(self, *, side, entry, tv_price, open_atr,
                                  reentry_attempt=None, adx_tier=None, radar_tier=None,
                                  adx=None, activation_ratio=None):
        """开仓后：硬+TP 已挂；雷达休眠至 ADX 启动线。"""
        attempt = int(
            reentry_attempt if reentry_attempt is not None
            else getattr(self, "reentry_attempt", 0) or 0
        )
        at = int(
            adx_tier if adx_tier is not None else getattr(self, "adx_tier", 1) or 1
        )
        rt = int(
            radar_tier if radar_tier is not None
            else getattr(self, "radar_tier", at) or at
        )
        adx_v = float(
            adx if adx is not None
            else getattr(self, "last_adx", 0)
            or getattr(self, "radar_activation_adx", 0)
            or 25.0
        )
        tps = list(getattr(self, "tv_tps", None) or [])
        tp1_v = float(tps[0] or 0) if tps else 0.0
        st = init_cycle_on_open(
            side=side,
            tv_price=tv_price,
            entry=entry,
            open_atr=open_atr,
            reentry_attempt=attempt,
            symbol=self.symbol,
            adx_tier=at,
            radar_tier=rt,
            adx=adx_v,
            activation_ratio=activation_ratio,
            tp1=tp1_v,
        )
        for k, v in st.items():
            setattr(self, k, v)
        self.radar_activated = False
        self._radar_handoff_done = False
        self._radar_armed_after_tp1 = False
        self._radar_activation_notified = False
        self._radar_notify_pending = False
        frac = float(st.get("radar_activation_frac") or 0)
        # 规格 v1.0：绝对价格锚定（首次=(TP1+TP2)/2 · 重入=TP2）
        from reentry_profiles import radar_gate_price_from_tps
        tps = list(getattr(self, "tv_tps", None) or [])
        tp1_px = float(tps[0] or 0) if len(tps) > 0 else 0.0
        tp2_px = float(tps[1] or 0) if len(tps) > 1 else 0.0
        gate_px = float(st.get("radar_activation_price") or 0)
        if tp1_px > 0 and tp2_px > 0 and gate_px <= 0:
            gate_px = radar_gate_price_from_tps(tp1_px, tp2_px, attempt)
        gate_lab = f"绝对价格锚定 {'(重入=TP2)' if attempt >= 1 else '(首次=(TP1+TP2)/2)'}"
        self._radar_trigger_gate = f"被动雷达·{gate_lab}"
        self._apply_tier_breath_overlay()
        logger.info(
            f"⏳ [{self.symbol}] 雷达休眠至激活 "
            f"mode={gate_lab} attempt={attempt} "
            f"ratio={frac:.0%} gate≈{gate_px:.4f}"
        )

    def _radar_is_dormant(self) -> bool:
        """未激活一律休眠。"""
        return not bool(getattr(self, "radar_activated", False))

    def _activation_reached_for_arm(self, curr_px: float) -> bool:
        """§5.0 现价触雷达激活线（首次=(TP1+TP2)/2 · 重入=TP2）。"""
        gate = float(getattr(self, "radar_activation_price", 0) or 0)
        if gate <= 0:
            return False
        side = str(getattr(self, "current_side", "") or "").upper()
        if side == "LONG":
            return float(curr_px or 0) >= gate
        elif side == "SHORT":
            return float(curr_px or 0) <= gate
        return False

    def _maybe_arm_radar_on_activation(self, live_qty, curr_px, source=""):
        """价触激活线（或本周期 sticky）：挂雷达 STOP@保本位，开始马拉松跟随。"""
        if bool(getattr(self, "radar_activated", False)):
            return True
        force = "强制" in str(source or "") or "force" in str(source or "").lower()
        if not force and not self._activation_reached_for_arm(curr_px):
            consumed = set(getattr(self, "tp_levels_consumed", []) or [])
            if not (1 in consumed and 2 in consumed):
                return False
            force = True
            source = f"{source or 'arm'}·TP12已成交"
        live_qty = float(live_qty or getattr(self, "watched_qty", 0) or 0)
        if live_qty <= 0:
            return False
        entry = float(getattr(self, "watched_entry", 0) or 0)
        side = str(getattr(self, "current_side", "") or "").strip().upper()
        atr = float(
            getattr(self, "open_atr", 0)
            or getattr(self, "cycle_open_atr", 0)
            or getattr(self, "current_atr", 0)
            or 0
        )
        if atr <= 0 and hasattr(self, "_get_locked_initial_atr"):
            try:
                atr = float(self._get_locked_initial_atr() or 0)
            except Exception:
                atr = 0.0
        profile = getattr(self, "breath_profile", None) or {}
        tick = float(profile.get("tick_size") or 0.01)
        fee_pct = profile.get("fee_cover_pct")
        init = float(
            arm_stop_price(
                side, entry, atr,
                tick_size=tick,
                fee_cover_pct=fee_pct,
            ) or 0
        )
        if init <= 0:
            init = float(getattr(self, "initial_stop", 0) or 0)
        if init <= 0:
            init = float(getattr(self, "current_sl", 0) or 0)
        if init <= 0:
            hard = float(getattr(self, "frozen_hard_sl_px", 0) or getattr(self, "tv_sl", 0) or 0)
            if hard > 0:
                init = hard
                logger.warning(
                    f"⚠️ [{self.symbol}] 武装用硬止损价兜底 initial_stop={init:.2f} | {source}"
                )
        if init <= 0:
            logger.warning(f"⚠️ [{self.symbol}] 达激活线但无 initial_stop | {source}")
            return False
        self.initial_stop = float(init)
        self.current_sl = float(init)
        self.tv_sl = float(init)
        self._apply_tier_breath_overlay()
        self._radar_arming = True
        try:
            ok = self._ensure_radar_sl(init, live_qty=live_qty, for_handoff=True)
        finally:
            self._radar_arming = False
        if not ok:
            logger.warning(
                f"⚠️ [{self.symbol}] 达激活线但雷达 STOP 未挂出 @{init:.2f} | "
                f"{source or '价触'} → 保持休眠重试"
            )
            return False
        self.radar_activated = True
        self.radar_pending_arm = False
        self._radar_handoff_done = True
        self._radar_armed_after_tp1 = True
        frac = float(getattr(self, "radar_activation_frac", 0.5) or 0.5)
        attempt = int(getattr(self, "reentry_attempt", 0) or 0)
        open_kind = "重入开仓" if attempt >= 1 else "首次开仓"
        self._radar_trigger_gate = (
            f"马拉松雷达已激活·保本起步·{open_kind}·绝对价格锚定 | "
            f"{source or '价触'}"
        )
        self._radar_stage_last = 1
        if not getattr(self, "_radar_arm_ding_sent", False):
            self._radar_notify_pending = True
            try:
                self._report_radar_first_activation(
                    live_qty, curr_px, init, sl_placed=True,
                    trigger_gate=self._radar_trigger_gate,
                )
            except Exception as e:
                logger.debug(f"雷达激活钉钉跳过: {e}")
        self._save_state()
        logger.info(
            f"📡 [{self.symbol}] 马拉松雷达已激活·保本起步 @{init:.2f} "
            f"(entry={entry:.2f}) | {self._radar_trigger_gate} | hung=True"
        )
        return True

    def _snapshot_cycle_for_reentry(self) -> Dict[str, Any]:
        consumed = list(getattr(self, "tp_levels_consumed", None) or [])
        tp1_ever = (
            1 in consumed
            or bool(getattr(self, "_tp1_filled_hint", False))
            or bool(getattr(self, "_ws_tp1_fill_hint", False))
        )
        return {
            "side": getattr(self, "current_side", None),
            "entry": float(getattr(self, "watched_entry", 0) or 0),
            "qty": float(
                getattr(self, "initial_qty", 0)
                or getattr(self, "watched_qty", 0)
                or 0
            ),
            "atr": float(
                getattr(self, "cycle_open_atr", 0)
                or getattr(self, "open_atr", 0)
                or getattr(self, "current_atr", 0)
                or 0
            ),
            "tv_price": float(
                getattr(self, "cycle_tv_price", 0)
                or getattr(self, "tv_price", 0)
                or 0
            ),
            "reentry_attempt": int(getattr(self, "reentry_attempt", 0) or 0),
            "radar_activation_frac": float(
                getattr(self, "radar_activation_frac", 0) or 0.78
            ),
            "tv_tps": list(getattr(self, "tv_tps", None) or [0, 0, 0]),
            "frozen_hard_sl_px": float(getattr(self, "frozen_hard_sl_px", 0) or 0),
            "initial_stop": float(getattr(self, "initial_stop", 0) or 0),
            "current_sl": float(getattr(self, "current_sl", 0) or 0),
            "radar_activated": bool(getattr(self, "radar_activated", False)),
            "tp_levels_consumed": consumed,
            "tp1_ever_filled": bool(tp1_ever),
            "adx_tier": int(getattr(self, "adx_tier", 1) or 1),
            "payload": dict(
                (getattr(self, "last_tv_signal", None) or {}).get("payload")
                or getattr(self, "last_tv_signal", None)
                or {}
            ) if isinstance(getattr(self, "last_tv_signal", None), dict) else {},
        }

    def _exit_px_near_hard(self, exit_px: float) -> bool:
        hard = float(getattr(self, "frozen_hard_sl_px", 0) or 0)
        px = float(exit_px or 0)
        if hard <= 0 or px <= 0:
            return False
        if abs(px - hard) <= max(2.5, px * 0.002):
            return True
        hint = getattr(self, "_ws_hard_sl_fill_hint", None) or {}
        try:
            ts = float(hint.get("ts") or 0)
            hpx = float(hint.get("px") or hint.get("stop") or 0)
        except (TypeError, ValueError):
            ts, hpx = 0.0, 0.0
        if ts > 0 and (time.time() - ts) <= 600.0:
            if hpx > 0 and abs(hpx - hard) <= max(3.0, hard * 0.003):
                return True
            if abs(px - hard) <= max(8.0, hard * 0.005):
                return True
        return False

    def _latch_ws_hard_sl_fill(self, fill_px=0.0, stop_px=0.0, source=""):
        """WS STOP 成交闩锁（供空仓归因硬止损）。"""
        hard = float(getattr(self, "frozen_hard_sl_px", 0) or 0)
        px = float(fill_px or stop_px or 0)
        sp = float(stop_px or 0)
        self._ws_hard_sl_fill_hint = {
            "ts": time.time(),
            "px": px or sp or hard,
            "stop": sp or hard,
            "source": str(source or "ws_stop"),
        }
        logger.info(
            f"📌 [{self.symbol}] WS硬止损成交闩锁 "
            f"px={float(self._ws_hard_sl_fill_hint['px']):.2f} "
            f"stop={float(self._ws_hard_sl_fill_hint['stop']):.2f} | {source}"
        )

    def _fetch_reentry_klines(self):
        """拉取 5m / 3m K 线（≥3 根，供 parse_kline_extreme 取最近已收盘根）。"""
        c = _client(self)
        if not c:
            return None, None
        k5, k3 = None, None
        try:
            k5 = c.fetch_klines(self.symbol, interval="5m", limit=3)
        except Exception as e:
            logger.warning(f"[{self.symbol}] 拉5m K线失败: {e}")
        try:
            k3 = c.fetch_klines(self.symbol, interval="3m", limit=3)
        except Exception as e:
            logger.debug(f"[{self.symbol}] 拉3m K线失败: {e}")
        return k5, k3

    def _clear_reentry_order_tag(self, reason=""):
        """仅在成交 / 确认撤销 / TTL 刷新前调用：释放本地标签后才允许新挂。"""
        old = getattr(self, "reentry_order_tag", None)
        self.reentry_order_tag = None
        if old:
            logger.info(
                f"🏷️ [{self.symbol}] 再入订单标签已释放 tag={old} | {reason}"
            )

    def _ensure_sterile_for_reentry(self, reason="再入前清场") -> bool:
        """
        仓位归零后挂再入限价前：必须 qty=0 且挂单列表为空。
        最多 STERILE_MAX_RETRY 轮；失败 → 钉钉 + 暂停该品种。
        """
        c = _client(self)
        max_n = int(STERILE_MAX_RETRY)
        last_detail = ""
        for i in range(1, max_n + 1):
            try:
                self._purge_all_defense_orders_on_flat(
                    f"{reason}·第{i}轮", max_rounds=6,
                )
            except Exception as e:
                logger.warning(f"[{self.symbol}] 再入清场撤单异常: {e}")
            if c:
                try:
                    book = c.get_open_orders(self.symbol)
                    if book == "QUERY_FAILED" or book is None:
                        last_detail = "挂单=QUERY_FAILED"
                        logger.error(
                            f"🚫 [{self.symbol}] {reason} 查单失败 → 拒挂（fail-closed）"
                        )
                        time.sleep(0.6 * i)
                        continue
                    for o in (book or []):
                        if not isinstance(o, dict):
                            continue
                        oid = o.get("orderId") or o.get("algoId")
                        if oid:
                            try:
                                c.cancel_order(self.symbol, order_id=oid)
                            except Exception:
                                try:
                                    c.cancel_order(self.symbol, order=o)
                                except Exception:
                                    pass
                except Exception as e:
                    last_detail = f"撤单异常:{e}"
                    time.sleep(0.6 * i)
                    continue

            if hasattr(self, "_verify_sterile_flat"):
                ok = bool(self._verify_sterile_flat())
            else:
                pos = self._get_active_position(prefer_ws=False)
                ok = pos != "QUERY_FAILED" and not (
                    pos and float(pos.get("size") or 0) > 0
                )
            if ok:
                self.reentry_sterile_fail_count = 0
                logger.info(
                    f"🧹 [{self.symbol}] {reason} 无菌通过 | 第{i}/{max_n}轮"
                )
                return True
            last_detail = str(
                getattr(self, "_last_sterile_flat_fail_detail", "") or "无菌未过"
            )
            logger.warning(
                f"⚠️ [{self.symbol}] {reason} 第{i}/{max_n}轮未过 | {last_detail}"
            )
            time.sleep(0.8 * i)

        self.reentry_sterile_fail_count = int(
            getattr(self, "reentry_sterile_fail_count", 0) or 0
        ) + 1
        try:
            self.trading_paused = True
        except Exception:
            pass
        try:
            import dingtalk
            self._call_dingtalk(
                dingtalk.report_system_alert,
                title=f"再入前无菌失败·已暂停 [{self.symbol}]",
                detail=(
                    f"{reason} 连续{max_n}轮未净场 | {last_detail} | "
                    f"已 trading_paused=True，禁止再挂限价（防叠单击穿）"
                ),
                level="紧急",
                suggestion="APP 手动全部撤单确认净场后 /admin/resume",
            )
        except Exception:
            pass
        logger.error(
            f"🚨 [{self.symbol}] {reason} 失败超限 → 暂停交易 | {last_detail}"
        )
        return False

    def _maybe_start_smart_limit_reentry(self, snap: Dict[str, Any], meta: Dict[str, Any]):
        """仓位归零且微赚/保本后挂限价再入；硬止损/亏损/超次不挂。§8"""
        if not reentry_enabled(self.symbol):
            logger.info(f"⏸ [{self.symbol}] 智能再入已关闭(enabled=False)")
            return False
        if getattr(self, "_reentry_cycle_aborted", False):
            return False
        if bool(getattr(self, "reentry_active", False)):
            return False
        if getattr(self, "reentry_order_tag", None):
            logger.error(
                f"🚫 [{self.symbol}] 本地再入标签仍在 "
                f"tag={self.reentry_order_tag} → 拒启动（防狂挂）"
            )
            return False
        if self.monitoring or float(getattr(self, "watched_qty", 0) or 0) > 0:
            return False
        pos = self._get_active_position(prefer_ws=False)
        if pos == "QUERY_FAILED":
            return False
        if pos and float(pos.get("size") or 0) > 0:
            return False

        snap = snap or {}
        meta = meta or {}
        exit_src = str(meta.get("exit_source") or "")
        side = str(snap.get("side") or meta.get("side") or "").upper()
        entry = float(snap.get("entry") or meta.get("entry_px") or 0)
        exit_px = float(
            meta.get("live_exit_px")
            or getattr(self, "last_exit_px", 0)
            or 0
        )
        atr = float(snap.get("atr") or 0)
        attempt = int(snap.get("reentry_attempt") or 0)

        # §8 硬止损出局 → 禁止再入
        if self._exit_px_near_hard(exit_px) or exit_src in ("vps_hard_sl", "hard_sl"):
            self._clear_reentry_cycle(source="硬止损出局·禁止再入")
            return False

        window_ts = float(open_reentry_window(self.symbol))
        self.reentry_window_deadline_ts = window_ts
        ok, why = evaluate_flat_for_reentry(
            exit_source=exit_src,
            side=side,
            entry=entry,
            exit_px=exit_px,
            atr=atr,
            reentry_attempt=attempt,
            symbol=self.symbol,
            window_deadline_ts=window_ts,
            tp1_ever_filled=bool(snap.get("tp1_ever_filled")),
            adx_tier=int(snap.get("adx_tier") if snap.get("adx_tier") is not None else 1),
        )
        if not ok:
            logger.info(
                f"🚫 [{self.symbol}] 不启动再入场: {why} | "
                f"src={exit_src} exit={exit_px:.2f} attempt={attempt} "
                f"tp1_filled={bool(snap.get('tp1_ever_filled'))} "
                f"tier={snap.get('adx_tier')}"
            )
            if why in (
                "hard_sl_no_reentry", "max_reentries", "tv_close_no_reentry",
                "tp1_already_filled", "tier_not_strong",
            ):
                self._clear_reentry_cycle(source=why)
            return False

        # §8 闭环第一步：无菌确认（仓+单皆零）后才允许挂再入限价
        if not self._ensure_sterile_for_reentry(reason="智能再入·仓位归零清场"):
            return False

        self.cycle_tv_side = side
        self.cycle_tv_price = float(snap.get("tv_price") or 0)
        self.cycle_open_atr = atr
        self.cycle_entry = entry
        self.reentry_attempt = attempt
        self.radar_tier = attempt
        self.radar_activation_frac = float(
            snap.get("radar_activation_frac")
            or activation_frac_for_attempt(attempt, get_reentry_profile(self.symbol))
        )
        self.last_exit_source = exit_src
        self.last_exit_px = exit_px
        self.reentry_unfilled_refreshes = 0
        self._reentry_open_snap = dict(snap)
        self._reentry_open_snap["exit_source"] = exit_src
        self._reentry_open_snap["exit_px"] = exit_px

        placed = self._place_reentry_limit(side=side, reason="雷达保本·智能再入")
        if not placed:
            logger.warning(f"⚠️ [{self.symbol}] 再入限价挂出失败")
            return False
        try:
            import dingtalk
            self._call_dingtalk(
                dingtalk.report_system_alert,
                title=f"智能再入场限价已挂 [{self.symbol}]",
                detail=(
                    f"{side} ADX档{tier_label(int(getattr(self, 'adx_tier', 1) or 1))} "
                    f"attempt={attempt}/{int(get_reentry_profile(self.symbol).get('max_reentries') or 1)} | "
                    f"limit@{float(self.reentry_limit_px):.2f} | "
                    f"tag={getattr(self, 'reentry_order_tag', None)} | "
                    f"TV@{float(self.cycle_tv_price):.2f} | "
                    f"exit={exit_src}@{exit_px:.2f} | 无菌已确认"
                ),
                level="提示",
            )
        except Exception:
            pass
        return True

    def _place_reentry_limit(self, side=None, reason="", *, is_refresh=False):
        c = _client(self)
        side = str(side or getattr(self, "cycle_tv_side", "") or "").upper()
        if side not in ("LONG", "SHORT"):
            return False

        pending_tag = getattr(self, "reentry_order_tag", None)
        if pending_tag and not is_refresh:
            logger.error(
                f"🚫 [{self.symbol}] 本地订单标签未释放 tag={pending_tag} "
                f"→ 拒挂第二笔（防查不到单狂挂）| {reason}"
            )
            return False

        if is_refresh:
            n = int(getattr(self, "reentry_unfilled_refreshes", 0) or 0) + 1
            self.reentry_unfilled_refreshes = n
            cap = max_unfilled_refreshes(self.symbol)
            if n > cap:
                logger.warning(
                    f"🚫 [{self.symbol}] 再入限价连续未成交刷新 {n}>{cap} → 终止周期"
                )
                self._cancel_reentry_limit(reason="未成交超限")
                self.reentry_active = False
                self._clear_reentry_cycle(source="unfilled_refresh_cap")
                return False
            self._cancel_reentry_limit(reason="TTL刷新·先撤旧标签")
        elif getattr(self, "reentry_limit_order_id", None):
            logger.error(
                f"🚫 [{self.symbol}] 已有再入限价 id={self.reentry_limit_order_id} "
                f"→ 拒挂 | {reason}"
            )
            return False

        pos = self._get_active_position(prefer_ws=False)
        if pos == "QUERY_FAILED":
            return False
        if pos and float(pos.get("size") or 0) > 0:
            logger.warning(f"🚫 [{self.symbol}] 再入挂单前仍有仓 → 中止")
            return False
        if not is_refresh:
            if hasattr(self, "_verify_sterile_flat") and not self._verify_sterile_flat():
                if not self._ensure_sterile_for_reentry(reason="再入挂单前复检"):
                    return False

        tv = float(getattr(self, "cycle_tv_price", 0) or 0)
        k5, k3 = self._fetch_reentry_klines()
        plan, why = plan_reentry_limit(
            side=side, tv_price=tv, symbol=self.symbol,
            klines_5m=k5, klines_3m=k3,
        )
        if not plan:
            logger.warning(
                f"🚫 [{self.symbol}] 再入限价中止: {why} | TV@{tv:.2f}"
            )
            if why == "not_better_than_tv":
                self._cancel_reentry_limit(reason="无法优于TV")
                self.reentry_active = False
                self._clear_reentry_cycle(source="not_better_than_tv")
            return False

        qty = float((getattr(self, "_reentry_open_snap", None) or {}).get("qty") or 0)
        if qty <= 0:
            qty = float(getattr(self, "base_qty", 0) or 0)
        if qty <= 0:
            logger.error(f"🚨 [{self.symbol}] 再入限价无数量")
            return False

        lim = float(plan["limit_px"])
        open_side = "BUY" if side == "LONG" else "SELL"
        tag = make_reentry_client_order_id(self.symbol, side, lim, time.time())
        self.reentry_order_tag = tag
        try:
            self._save_state()
        except Exception:
            pass

        if c:
            try:
                book = c.get_open_orders(self.symbol)
                if book == "QUERY_FAILED" or book is None:
                    logger.error(
                        f"🚫 [{self.symbol}] 挂单前查单失败 → 释放标签并拒挂 tag={tag}"
                    )
                    self._clear_reentry_order_tag(reason="查单失败拒挂")
                    return False
                for o in (book or []):
                    if not isinstance(o, dict):
                        continue
                    if str(o.get("type") or "").upper() != "LIMIT":
                        continue
                    if str(o.get("side") or "").upper() != open_side:
                        continue
                    try:
                        opx = float(o.get("price") or 0)
                    except (TypeError, ValueError):
                        continue
                    if abs(opx - lim) <= max(lim * 1e-8, 1e-6):
                        oid = o.get("orderId")
                        self.reentry_active = True
                        self.reentry_limit_order_id = oid
                        self.reentry_limit_px = lim
                        self.reentry_limit_deadline_ts = float(plan["deadline_ts"])
                        coid = str(o.get("clientOrderId") or "") or tag
                        self.reentry_order_tag = coid
                        self._save_state()
                        logger.warning(
                            f"♻️ [{self.symbol}] 复用已有同价再入限价 id={oid} "
                            f"@{lim:.2f} tag={coid}"
                        )
                        return True
            except Exception as e:
                logger.error(f"🚫 [{self.symbol}] 挂单前查单异常 → 拒挂: {e}")
                self._clear_reentry_order_tag(reason="查单异常拒挂")
                return False

            order = c.place_limit_order(
                open_side, qty, lim, symbol=self.symbol, reduce_only=False,
                cl_ord_id=tag,
            )
            if not order:
                self._clear_reentry_order_tag(reason="下单失败释放")
                return False
            oid = order.get("orderId") or order.get("algoId")
        else:
            oid = f"local-{int(time.time())}"

        self.reentry_active = True
        self.reentry_limit_order_id = oid
        self.reentry_limit_px = lim
        self.reentry_limit_deadline_ts = float(plan["deadline_ts"])
        self._save_state()
        logger.info(
            f"📥 [{self.symbol}] 再入限价已挂 {side} {qty} @{lim:.2f} "
            f"src={plan.get('source')} id={oid} tag={tag} | {reason} | "
            f"refresh={int(getattr(self, 'reentry_unfilled_refreshes', 0) or 0)}"
        )
        return True

    def _cancel_reentry_limit(self, reason=""):
        c = _client(self)
        oid = getattr(self, "reentry_limit_order_id", None)
        tag = getattr(self, "reentry_order_tag", None)
        if oid and c:
            try:
                c.cancel_order(self.symbol, order_id=oid)
                logger.info(
                    f"🗑️ [{self.symbol}] 撤再入限价 id={oid} tag={tag} | {reason}"
                )
            except Exception as e:
                try:
                    c.cancel_order(self.symbol, order={"orderId": oid})
                except Exception as e2:
                    logger.debug(f"撤再入限价跳过: {e}/{e2}")
        self.reentry_limit_order_id = None
        self.reentry_limit_px = 0.0
        self.reentry_limit_deadline_ts = 0.0
        self._clear_reentry_order_tag(reason=reason or "撤单释放标签")

    def _reentry_tick(self):
        """空仓时：TTL 刷新 / 成交检测 / 终止条件。"""
        if not bool(getattr(self, "reentry_active", False)):
            return False
        if self.monitoring or float(getattr(self, "watched_qty", 0) or 0) > 0:
            return False

        side = str(getattr(self, "cycle_tv_side", "") or "").upper()
        pos = self._get_active_position(prefer_ws=True)
        if pos == "QUERY_FAILED":
            return False
        if pos and float(pos.get("size") or 0) > 0:
            if str(pos.get("side") or "").upper() == side:
                return self._on_reentry_limit_filled(pos)
            logger.warning(f"⚠️ [{self.symbol}] 再入期间出现反向仓 → 中止周期")
            self._clear_reentry_cycle(source="再入期反向仓")
            return False

        now = time.time()
        deadline = float(getattr(self, "reentry_limit_deadline_ts", 0) or 0)
        if deadline > 0 and now >= deadline:
            logger.info(f"⏰ [{self.symbol}] 再入限价 TTL 到期 → 按最新5m极值重挂")
            return bool(
                self._place_reentry_limit(
                    side=side, reason="TTL刷新", is_refresh=True,
                )
            )
        return True

    def _on_reentry_limit_filled(self, pos: Dict[str, Any]) -> bool:
        """再入限价成交 → attempt+1，按新成交价挂 hard+TP12，雷达休眠候命。§8"""
        side = str(pos.get("side") or getattr(self, "cycle_tv_side", "") or "").upper()
        entry = float(pos.get("entry_price") or 0)
        qty = float(pos.get("size") or 0)
        if side not in ("LONG", "SHORT") or entry <= 0 or qty <= 0:
            return False
        prev = int(getattr(self, "reentry_attempt", 0) or 0)
        prev_frac = float(getattr(self, "radar_activation_frac", 0.5) or 0.5)
        bumped = bump_after_reentry_fill(
            prev, prev_frac, self.symbol,
            adx_tier=int(getattr(self, "adx_tier", 1) or 1),
            adx=float(
                getattr(self, "radar_activation_adx", 0)
                or getattr(self, "last_adx", 0)
                or 25.0
            ),
            entry=entry,
            open_atr=float(
                getattr(self, "cycle_open_atr", 0)
                or getattr(self, "open_atr", 0)
                or 0
            ),
            side=side,
            tp1=float((list(getattr(self, "tv_tps", None) or [0])[0]) or 0),
        )
        self.reentry_limit_order_id = None
        self.reentry_limit_px = 0.0
        self.reentry_limit_deadline_ts = 0.0
        self.reentry_active = False
        self._clear_reentry_order_tag(reason="再入成交释放")
        for k, v in bumped.items():
            if k == "tier_coeffs":
                continue
            setattr(self, k, v)

        snap = dict(getattr(self, "_reentry_open_snap", None) or {})
        tv_tps = list(snap.get("tv_tps") or [0, 0, 0])
        atr = float(
            getattr(self, "cycle_open_atr", 0) or snap.get("atr") or 0
        )
        tv_price = float(
            getattr(self, "cycle_tv_price", 0) or snap.get("tv_price") or entry
        )

        self.current_side = side
        self.watched_entry = entry
        self.watched_qty = qty
        self.initial_qty = qty
        self.tv_price = tv_price
        if hasattr(self, "_sanitize_tp_prices"):
            self.tv_tps = self._sanitize_tp_prices(tv_tps)
        else:
            self.tv_tps = tv_tps
        self.open_atr = atr
        self._tv_signal_atr = atr
        self.monitoring = True
        self._apply_tier_breath_overlay()

        try:
            if hasattr(self, "_ensure_tp123_prices_from_tv"):
                if not self._tp_prices_valid_for_side(side, entry):
                    self.tv_tps = [0.0, 0.0, 0.0]
                self._ensure_tp123_prices_from_tv(entry)
        except Exception as e:
            logger.warning(f"[{self.symbol}] 再入成交 TP 重算跳过: {e}")

        self._begin_open_radar_dormant(
            side=side, entry=entry, tv_price=tv_price, open_atr=atr,
            reentry_attempt=int(bumped["reentry_attempt"]),
        )
        radar_init = 0.0
        try:
            from breath_stop import initial_stop_price
            init = initial_stop_price(
                side, entry, atr, profile=getattr(self, "breath_profile", None),
            )
            if init > 0:
                radar_init = float(init)
                self.initial_stop = radar_init
                self.current_sl = radar_init
                self.tv_sl = radar_init
        except Exception:
            pass

        self._save_state()
        if hasattr(self, "_ensure_price_ws"):
            self._ensure_price_ws()
        if hasattr(self, "_ensure_sentinel_running"):
            self._ensure_sentinel_running()
        hard_px = 0.0
        arm_ok = False
        try:
            arm_ok = bool(self._arm_temp_stop_and_tp12(
                qty, entry, side,
                source=f"再入成交·attempt={self.reentry_attempt}",
            ))
            hard_px = float(getattr(self, "frozen_hard_sl_px", 0) or 0)
            if hasattr(self, "_resolve_atr_scenario_after_open"):
                self._resolve_atr_scenario_after_open(entry, side, qty)
            if radar_init > 0:
                self.initial_stop = radar_init
                self.current_sl = radar_init
                self.tv_sl = radar_init
            if self._radar_is_dormant():
                self._strip_radar_stop_keep_hard(reason="再入后雷达仍休眠")
        except Exception as e:
            logger.error(f"[{self.symbol}] 再入后防线失败: {e}")

        hard_hung = hard_px > 0 and arm_ok
        tp_note = ""
        try:
            tps = list(getattr(self, "tv_tps", None) or [])
            tp_note = (
                f"TP1={float(tps[0] or 0):.2f} TP2={float(tps[1] or 0):.2f}"
                if len(tps) >= 2 else "TP=?"
            )
        except Exception:
            tp_note = "TP=?"
        slip = abs(entry - tv_price) if tv_price > 0 else 0.0
        try:
            import dingtalk
            self._call_dingtalk(
                dingtalk.report_system_alert,
                title=f"智能再入已成交·防线核实 [{self.symbol}]",
                detail=(
                    f"{side} {qty} @ fill={entry:.2f} (TV@{tv_price:.2f} 滑点={slip:.2f}) | "
                    f"档位{tier_label(int(self.reentry_attempt))} "
                    f"attempt={self.reentry_attempt} "
                    f"frac={float(self.radar_activation_frac):.0%} | "
                    f"硬止损@{hard_px:.2f} hung={1 if hard_hung else 0} | "
                    f"{tp_note} | 雷达休眠候命 | "
                    f"标签已释放 tag=None"
                ),
                level="紧急" if not hard_hung else "提示",
                suggestion=(
                    "硬止损未确认挂出：立即核对 STOP；"
                    if not hard_hung else
                    "核对 hard+TP12 与雷达休眠状态"
                ),
            )
        except Exception:
            pass
        logger.info(
            f"✅ [{self.symbol}] 再入成交 {side} {qty}@{entry:.2f} "
            f"attempt={self.reentry_attempt} hard@{hard_px:.2f} "
            f"dormant=1 hung={1 if hard_hung else 0}"
        )
        return True

    def _strip_radar_stop_keep_hard(self, reason=""):
        """休眠窗：盘口只留 closePosition 永久硬止损，撤掉其余 STOP（禁双挂）。"""
        c = _client(self)
        try:
            ids = dict(getattr(self, "_defense_order_ids", {}) or {})
            hard_id = str(ids.get("hard_stop") or "").strip()
            rid = str(ids.get("radar_stop") or ids.get("stop") or "").strip()
            cancelled = []
            if rid and rid != hard_id and c:
                try:
                    c.cancel_order(self.symbol, order_id=rid)
                    cancelled.append(rid)
                except Exception as e:
                    logger.debug(f"撤雷达id失败 {rid}: {e}")
            if c:
                try:
                    orders = c.get_open_orders(self.symbol) or []
                except Exception:
                    orders = []
                for o in orders:
                    t = str(o.get("type") or o.get("orderType") or "").upper()
                    if "STOP" not in t:
                        continue
                    cp = str(o.get("closePosition") or "").lower() in ("true", "1")
                    if cp:
                        continue
                    oid = str(o.get("orderId") or o.get("algoId") or "")
                    if not oid or oid == hard_id:
                        continue
                    try:
                        c.cancel_order(self.symbol, order_id=oid)
                        cancelled.append(oid)
                    except Exception as e:
                        logger.debug(f"撤休眠STOP失败 {oid}: {e}")
            if cancelled:
                ids["radar_stop"] = ""
                ids["stop"] = ""
                self._defense_order_ids = ids
                logger.info(
                    f"🗑️ [{self.symbol}] 已撤休眠期雷达/多余STOP "
                    f"{cancelled} | {reason}"
                )
            if hasattr(self, "_clear_pending_tags_for_kind"):
                try:
                    self._clear_pending_tags_for_kind("RADAR", save=False)
                except Exception:
                    pass
        except Exception as e:
            logger.debug(f"撤休眠雷达跳过: {e}")
        self.radar_activated = False
        self.radar_pending_arm = True
