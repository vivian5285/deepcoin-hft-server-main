#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# position_supervisor_deepcoin.py — 与币安 VPS 逻辑完全对齐（深币张数/15x 适配）
import logging
import time
import threading
import os
import json
import queue
from datetime import datetime
from logging.handlers import RotatingFileHandler
from deepcoin_client import deepcoin_client, CLIENT_VERSION
import dingtalk

if not os.path.exists('logs'):
    os.makedirs('logs')
handler = RotatingFileHandler('logs/deepcoin_brain.log', maxBytes=5 * 1024 * 1024, backupCount=3)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] Brain: %(message)s',
    handlers=[handler, logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

DEEPCOIN_SUPERVISOR_VERSION = "v13.8.4-recover-guard"
SENTINEL_POLL_NORMAL = 6
SENTINEL_POLL_ARMING = 3
SENTINEL_POLL_RADAR = 2
DUST_ORPHAN_CONTRACTS = 1
TP_COMPLETE_RESIDUAL_RATIO = 0.12
OPEN_OVERSIZE_RATIO = 1.10
SIGNAL_DEDUP_SEC = 45
DEFENSE_ALIGN_COOLDOWN_SEC = 60
SENTINEL_GRACE_AFTER_RECOVER_SEC = 45
RECOVER_LOCK_FILE = "logs/.recover_singleton.lock"
RECOVER_LOCK_TTL_SEC = 180
REGIME_CAP_COOLDOWN_SEC = 90
REGIME_CAP_TOLERANCE_CONTRACTS = 0
CAP_MIN_RETAIN_RATIO = 0.25
CAP_TRIM_MAX_ROUNDS = 4
QTY_DRIFT_TOLERANCE_PCT = 0.015  # 微漂 ≤1.5%：仅同步账本
QTY_ALIGN_MIN_PCT = 0.10         # 偏离 ≥10% 才离谱，触发对齐/档位裁减
SHIELD_HARD_STOP_PCT = 0.10  # 开仓价 ±10% 硬止损全平（开单即挂，雷达激活后撤）
SHIELD_TIER_PCTS = (SHIELD_HARD_STOP_PCT,)
SHIELD_TIER_RATIOS = (1.0,)
SHIELD_STOP_TOLERANCE = 2.0
SHIELD_MAINTAIN_COOLDOWN_SEC = 60
SHIELD_FAIL_BACKOFF_BASE_SEC = 45
SHIELD_FAIL_BACKOFF_MAX_SEC = 300
SHIELD_QTY_TOLERANCE_PCT = 0.04
SHIELD_MAX_TIER_ORDERS = 1
RADAR_DINGTALK_COOLDOWN_SEC = 120
# 同向 TV 智能筛选：① ATR 变化 → 先平后开；② 价差低于该百分比 → 不重复开仓，仅刷新 TP123
SAME_DIR_MIN_SPREAD_PCT = 0.15
SAME_DIR_DEDUP_SEC = 300
ATR_SIMILAR_RATIO = 0.03  # 持仓 ATR 与 TV ATR 偏差 ≤3% 视为未变
TV_JOURNAL = "logs/deepcoin_tv_journal.jsonl"
OPEN_JOURNAL = "logs/deepcoin_open_journal.jsonl"


class PositionSupervisor:
    def __init__(self):
        self.symbol = "ETH-USDT-SWAP"
        self.monitoring = False
        self._lock = threading.Lock()

        # 与币安完全一致的四档矩阵：activation=启动雷达的 TP1 距离比例，trail_offset=ATR 追踪倍数
        self.regime_settings = {
            1: {"margin": 0.15, "ratios": [0.25, 0.35, 0.40], "activation": 0.40, "trail_offset": 0.40},
            2: {"margin": 0.25, "ratios": [0.20, 0.35, 0.45], "activation": 0.50, "trail_offset": 0.60},
            3: {"margin": 0.35, "ratios": [0.18, 0.32, 0.50], "activation": 0.60, "trail_offset": 0.90},
            4: {"margin": 0.50, "ratios": [0.05, 0.20, 0.75], "activation": 0.70, "trail_offset": 1.30},
        }
        self.leverage = 15
        self.face_value = 0.1

        self.regime = 3
        self.current_atr = 30.0
        self.best_price = 0.0
        self.current_sl = 0.0
        self.tv_price = 0.0
        self.tv_tps = [0.0, 0.0, 0.0]

        self.initial_qty = 0
        self.watched_qty = 0
        self.watched_entry = 0.0
        self.current_side = None
        self.last_tv_side = None
        self.last_tv_signal = None
        self._scan_ticks = 0
        self._signal_queue = queue.Queue()
        self._signal_worker_started = False
        self._sentinel_active = False
        self.open_regime = 3
        self.open_atr = 30.0
        self._last_entry_signal = None
        self._recover_in_progress = False
        self._recover_tp_unconfirmed = False
        self._open_in_progress = False
        self._open_tp_unconfirmed = False
        self._last_signal_fp = None
        self._last_signal_fp_ts = 0.0
        self._defense_align_in_progress = False
        self._last_defense_align_ok_ts = 0.0
        self._guardian_bad_streak = 0
        self._sentinel_grace_until = 0.0
        self._last_regime_cap_ts = 0.0
        self.shield_active = False
        self.shield_tiers_consumed = []
        self._last_shield_maintain_ts = 0.0
        self._shield_fail_streak = 0
        self._last_shield_fail_ts = 0.0
        self._shield_arm_notified = False
        self.shield_sized_qty = 0.0
        self._last_radar_report_ts = 0.0
        self._last_radar_report_sl = 0.0
        self.sizing_principal = 0.0

        self.state_file = 'deepcoin_vps_state.json'
        logger.info(
            f"🧠 深币 VPS [{DEEPCOIN_SUPERVISOR_VERSION}/{CLIENT_VERSION}] "
            f"军师托管版已加载：双轨智慧雷达 · {self.leverage}x 杠杆"
        )
        self._start_signal_worker()
        self._start_idle_flat_patrol()

    def _start_idle_flat_patrol(self):
        """空仓待命时后台巡检：发现孤立残张 → 自动扫尾 + 钉钉"""
        def loop():
            while True:
                time.sleep(30)
                if self.monitoring:
                    continue
                if not self._lock.acquire(timeout=2.0):
                    continue
                try:
                    if self.monitoring:
                        continue
                    pos = self._get_active_position()
                    if not pos or self._safe_qty(pos.get("size")) <= 0:
                        continue
                    real_amt = self._safe_qty(pos["size"])
                    if not self._is_dust_qty(real_amt) and not self._should_finalize_tp_victory(real_amt):
                        continue
                    if not self.current_side:
                        self.current_side = "LONG" if pos.get("posSide") == "long" else "SHORT"
                    logger.warning(
                        f"🐜 [空闲巡检] 发现残量 {self.current_side} {real_amt}张 → 扫尾"
                    )
                    self._sweep_dust_and_finalize("重启扫描：盘口蚂蚁仓自动扫平")
                except Exception as e:
                    logger.error(f"空闲巡检异常: {e}")
                finally:
                    self._lock.release()

        threading.Thread(target=loop, daemon=True, name="idle-flat-patrol").start()

    @staticmethod
    def _call_dingtalk(fn, **kwargs):
        """兼容 VPS 旧版 dingtalk.py（缺少 verified / swept_dust 等新参数）"""
        try:
            fn(**kwargs)
        except TypeError as exc:
            if "unexpected keyword argument" not in str(exc):
                raise
            legacy = {
                k: v for k, v in kwargs.items()
                if k not in ("verified", "swept_dust", "radar_sl_ok", "action_type")
            }
            logger.warning(f"钉钉旧版降级播报 {getattr(fn, '__name__', 'dingtalk')}: {exc}")
            fn(**legacy)

    def _start_signal_worker(self):
        if self._signal_worker_started:
            return
        self._signal_worker_started = True
        threading.Thread(target=self._signal_worker_loop, daemon=True, name="tv-signal-worker").start()

    def _signal_worker_loop(self):
        while True:
            payload = self._signal_queue.get()
            try:
                self._process_signal(payload)
            except Exception as e:
                logger.error(f"❌ 信号处理异常: {e}", exc_info=True)
            finally:
                self._signal_queue.task_done()

    def _signal_fingerprint(self, payload):
        return (
            str(payload.get("action", "")).strip().upper(),
            self._safe_int(payload.get("regime"), 3),
            round(self._safe_float(payload.get("price"), 0), 2),
            round(self._safe_float(payload.get("atr"), 0), 2),
        )

    def enqueue_signal(self, payload):
        fp = self._signal_fingerprint(payload)
        action = fp[0] or "?"
        now = time.time()
        if (
            fp == self._last_signal_fp
            and now - self._last_signal_fp_ts < SIGNAL_DEDUP_SEC
        ):
            logger.warning(
                f"📬 TV信号去重忽略: {action} | {SIGNAL_DEDUP_SEC}s 内重复推送"
            )
            return
        if self._open_in_progress and action in ("LONG", "SHORT"):
            logger.warning(f"📬 开仓进行中，忽略重复建仓信号 {action}")
            return
        self._last_signal_fp = fp
        self._last_signal_fp_ts = now
        depth = self._signal_queue.qsize()
        self._signal_queue.put(payload)
        logger.info(f"📬 TV信号入队: {action} | 队列深度 {depth + 1}")

    def signal_queue_depth(self):
        return self._signal_queue.qsize()

    def _append_journal(self, path, record):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        record = dict(record)
        record["ts"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    def _load_last_journal_entry(self, path):
        if not os.path.exists(path):
            return None
        last = None
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        last = json.loads(line)
                    except json.JSONDecodeError:
                        continue
        return last

    def _record_tv_signal(self, payload, raw_action):
        entry = {
            "action": raw_action,
            "regime": self.regime,
            "atr": self.current_atr,
            "price": self.tv_price,
            "tv_tps": self.tv_tps,
            "reason": payload.get("reason", ""),
        }
        self.last_tv_signal = entry
        self._append_journal(TV_JOURNAL, entry)
        logger.info(
            f"📡 TV日志: {raw_action} R{self.regime} @ {self.tv_price:.2f} "
            f"TP={self.tv_tps}"
        )

    def _record_open_log(self, side, qty, entry, source="open"):
        self._append_journal(OPEN_JOURNAL, {
            "source": source,
            "side": side,
            "qty": qty,
            "entry": entry,
            "regime": self.regime,
            "tv_tps": self.tv_tps,
            "tv_price": self.tv_price,
            "last_tv_side": self.last_tv_side,
        })

    def _load_last_tv_open_signal(self):
        """TV 日志中最近一条 LONG/SHORT（CLOSE 之后仍可用于方向对账）"""
        if not os.path.exists(TV_JOURNAL):
            return None
        last_open = None
        with open(TV_JOURNAL, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                action = (entry.get("action") or "").upper()
                if action in ("LONG", "SHORT"):
                    last_open = entry
        return last_open

    def _reconcile_context_on_recover(self, pos):
        """重启对账：实盘头寸 vs 账本 vs 最新 TV / 开仓日志"""
        notes = []
        reconcile = {
            "notes": notes,
            "tv_close": False,
            "direction_mismatch": False,
            "qty_manual_change": None,
        }
        side = "LONG" if pos.get("posSide") == "long" else "SHORT"
        real_amt = self._safe_qty(pos.get("size"))
        saved_watched = self._safe_qty(self.watched_qty)
        saved_initial = self._safe_qty(self.initial_qty)

        last_tv = self._load_last_journal_entry(TV_JOURNAL)
        last_open = self._load_last_journal_entry(OPEN_JOURNAL)
        last_open_tv = self._load_last_tv_open_signal()

        if last_tv:
            self.last_tv_signal = last_tv
            tv_action = (last_tv.get("action") or "").upper()
            tv_tps_saved = self._sanitize_tp_prices(last_tv.get("tv_tps", []))
            tv_tp_count = sum(1 for t in tv_tps_saved if t > 0)

            if last_tv.get("regime"):
                self.regime = int(last_tv["regime"])
            if last_tv.get("atr"):
                self.current_atr = float(last_tv["atr"])
            if self.tv_price <= 0 and float(last_tv.get("price", 0) or 0) > 0:
                self.tv_price = float(last_tv["price"])

            if tv_action in ("LONG", "SHORT"):
                self.last_tv_side = tv_action
                if tv_tp_count > 0:
                    self.tv_tps = tv_tps_saved
                    notes.append(f"TV日志同步止盈价 {self.tv_tps}")
                if side != tv_action:
                    reconcile["direction_mismatch"] = True
                    notes.append(
                        f"方向背离: 实盘{side} vs TV最新{tv_action} ({last_tv.get('ts', '')})"
                    )
            elif tv_action.startswith("CLOSE"):
                reconcile["tv_close"] = True
                notes.append(
                    f"TV最新为{tv_action} ({last_tv.get('ts', '')})，实盘仍有仓 → 应清场"
                )
                if last_open_tv:
                    self.last_tv_side = (last_open_tv.get("action") or "").upper()
                    open_tps = self._sanitize_tp_prices(last_open_tv.get("tv_tps", []))
                    if sum(1 for t in open_tps if t > 0) > 0:
                        self.tv_tps = open_tps

        if not self.last_tv_side and last_open_tv:
            self.last_tv_side = (last_open_tv.get("action") or "").upper()

        if last_open:
            open_side = last_open.get("side")
            if open_side and side != open_side:
                notes.append(f"开仓日志方向 {open_side} ≠ 实盘 {side}")
            open_entry = float(last_open.get("entry", 0) or 0)
            entry = float(pos.get("entry_price", 0) or 0)
            if open_entry > 0 and abs(entry - open_entry) > 3.0:
                notes.append(f"入场偏差: 开仓日志 {open_entry:.2f} vs 实盘 {entry:.2f}")

        if saved_watched > 0 and self._is_material_qty_change(saved_watched, real_amt):
            action_msg = (
                "手动加仓" if real_amt > saved_watched
                else "部分止盈吃单 / 手动减仓"
            )
            reconcile["qty_manual_change"] = (saved_watched, real_amt, action_msg)
            notes.append(f"人工异动(重启): {saved_watched}张 → {real_amt}张 ({action_msg})")

        if not self.last_tv_side:
            self.last_tv_side = side
        elif side != self.last_tv_side and not reconcile["tv_close"]:
            reconcile["direction_mismatch"] = True
            if not any("方向背离" in n for n in notes):
                notes.append(f"方向背离: 实盘{side} vs TV指令{self.last_tv_side}")

        if saved_initial <= 0 and real_amt > 0:
            self.initial_qty = real_amt

        for n in notes:
            logger.warning(f"🔎 重启对账: {n}")
        return reconcile

    def _qty_change_ratio(self, old_qty, new_qty):
        old = float(old_qty or 0)
        new = float(new_qty or 0)
        if old <= 0 and new <= 0:
            return 0.0
        return abs(new - old) / max(old, new, 1e-9)

    def _is_material_qty_change(self, old_qty, new_qty):
        """离谱级异动：偏离 ≥10% 才触发对齐；微漂仅同步账本"""
        old = self._safe_qty(old_qty)
        new = self._safe_qty(new_qty)
        delta = abs(new - old)
        if delta <= REGIME_CAP_TOLERANCE_CONTRACTS:
            return False
        ratio = self._qty_change_ratio(old, new)
        return ratio >= QTY_ALIGN_MIN_PCT

    @staticmethod
    def _sanitize_tp_prices(tp_list):
        """TV/状态文件里的浮点价统一规整到 2 位小数，避免 1517.4 触发 PriceNotOnTick"""
        out = []
        for t in tp_list:
            try:
                out.append(round(float(t), 2) if float(t) > 0 else 0.0)
            except (TypeError, ValueError):
                out.append(0.0)
        return out

    def _save_state(self):
        try:
            with open(self.state_file, 'w') as f:
                json.dump({
                    "last_tv_side": self.last_tv_side,
                    "current_side": self.current_side,
                    "watched_qty": self.watched_qty,
                    "watched_entry": self.watched_entry,
                    "current_sl": self.current_sl,
                    "monitoring": self.monitoring,
                    "regime": self.regime,
                    "current_atr": self.current_atr,
                    "tv_tps": self.tv_tps,
                    "tv_price": self.tv_price,
                    "best_price": self.best_price,
                    "initial_qty": self.initial_qty,
                    "last_tv_signal": self.last_tv_signal,
                    "open_regime": self.open_regime,
                    "open_atr": self.open_atr,
                    "shield_active": getattr(self, "shield_active", False),
                    "shield_tiers_consumed": list(getattr(self, "shield_tiers_consumed", []) or []),
                    "shield_sized_qty": float(getattr(self, "shield_sized_qty", 0) or 0),
                    "sizing_principal": float(getattr(self, "sizing_principal", 0) or 0),
                }, f)
        except Exception as e:
            logger.error(f"保存状态失败: {e}")

    @staticmethod
    def _safe_qty(val, default=0):
        """Deepcoin API 常返回 '1.000000' 字符串，须先 float 再 int"""
        if val is None or val == "":
            return default
        try:
            return int(float(val))
        except (TypeError, ValueError):
            return default

    def _get_active_position(self):
        res = deepcoin_client.get_position_info(self.symbol)
        if res and 'data' in res:
            for p in res['data']:
                if self._safe_qty(p.get("pos")) > 0:
                    return {
                        "size": self._safe_qty(p.get("pos")),
                        "entry_price": round(float(p.get("avgPx", p.get("price", 0)) or 0), 2),
                        "posSide": p.get("posSide", "long").lower(),
                    }
        return None

    def _verify_flat(self):
        pos = self._get_active_position()
        return pos is None or self._safe_qty(pos.get("size")) == 0

    def _ensure_flat_before_open(self, reason_tag="开仓前"):
        if self._wait_verify(self._verify_flat, retries=4, delay=0.4):
            return True
        logger.warning(f"⚠️ {reason_tag}：检测到残留持仓，启动强制平仓")
        if self._close_all(f"{reason_tag} · 强制清场", reset_state=True):
            return self._wait_verify(self._verify_flat, retries=6, delay=0.5)
        return False

    def _snapshot_sizing_principal(self, reason=""):
        """全平/开仓前：锁定 USDT 合约本金余额，供本周期开仓与超标核查共用"""
        principal = deepcoin_client.get_principal_wallet_balance()
        if principal > 0:
            self.sizing_principal = principal
            self._save_state()
            logger.info(f"📸 本金快照 {principal:.2f} USDT ({reason})")
            if reason and ("全平" in reason or "开仓前" in reason):
                target_qty = None
                margin_pct = None
                if self.regime in self.regime_settings and "开仓前" in reason:
                    margin_pct = self.regime_settings[self.regime]["margin"]
                    if self.tv_price > 0:
                        t, _, _, _, _ = self._regime_cap_target_qty(self.tv_price, self.regime)
                        target_qty = t
                try:
                    dingtalk.report_principal_snapshot(
                        reason=reason,
                        principal=principal,
                        regime=self.regime if "开仓前" in reason else None,
                        margin_pct=margin_pct,
                        target_qty=target_qty,
                        leverage=self.leverage,
                    )
                except Exception as e:
                    logger.warning(f"本金快照钉钉跳过: {e}")
        return principal

    def _resolve_cap_sizing_base(self, wallet_balance=None):
        """
        档位额度唯一基数：sizing_principal 快照 × TV 档位%。
        亏损导致 wallet 低于快照时，用实时 wallet 下限（不放大浮盈）。
        """
        wallet = float(
            wallet_balance if wallet_balance is not None
            else deepcoin_client.get_principal_wallet_balance()
        )
        principal = float(getattr(self, "sizing_principal", 0) or 0)
        if principal > 0:
            if wallet > 0 and wallet < principal:
                return wallet
            return principal
        return wallet

    def _regime_cap_target_qty(self, curr_px, regime=None):
        """按 TV 档位：本金快照 × margin% × 杠杆 → 仓位上限张数"""
        regime = int(regime if regime is not None else self.regime)
        if regime not in self.regime_settings:
            regime = 3
        wallet = deepcoin_client.get_principal_wallet_balance()
        balance = self._resolve_cap_sizing_base(wallet)
        margin_pct = self.regime_settings[regime]["margin"]
        margin_usdt = balance * margin_pct
        if curr_px <= 0:
            return 0, balance, margin_usdt, margin_pct, regime
        qty = max(int((margin_usdt * self.leverage) / (curr_px * self.face_value)), 1)
        return qty, balance, margin_usdt, margin_pct, regime

    def _validate_cap_trim_plan(self, live_qty, target_qty, trim_qty):
        live = self._safe_qty(live_qty)
        target = int(target_qty or 0)
        trim = int(trim_qty or 0)
        if live <= 0 or target <= 0:
            return "数量无效，无法裁减"
        if trim <= 0:
            return None
        retain = target / live if live > 0 else 0
        if retain < CAP_MIN_RETAIN_RATIO and live > target * 2:
            return (
                f"目标仅相当于实盘的 {retain:.1%}，疑似误用「可用保证金」而非「本金快照」"
                f"（目标 {target} 张 vs 实盘 {live} 张）"
            )
        if trim > live * 0.85 and target < live * 0.15:
            return (
                f"裁减幅度过大：将平掉 {trim} 张，仅保留 {target} 张，疑似额度基数算错"
            )
        expected = live - target
        if abs(trim - expected) > max(int(live * 0.05), 1):
            return f"裁减量不符：计划 {trim} 张，应为 {expected} 张"
        return None

    def _calc_target_open_qty(self, curr_px):
        qty, balance, margin_usdt, margin_pct, _ = self._regime_cap_target_qty(curr_px, self.regime)
        return qty, balance, margin_usdt, margin_pct

    def _regime_cap_tolerance(self, target_qty):
        """档位裁减容忍：离谱才管 — 超标 ≤10% 不裁"""
        target = int(target_qty or 0)
        if target <= 0:
            return REGIME_CAP_TOLERANCE_CONTRACTS
        pct_tol = max(1, int(round(target * QTY_ALIGN_MIN_PCT)))
        return max(REGIME_CAP_TOLERANCE_CONTRACTS, pct_tol)

    def _is_oversize_for_regime(self, live_qty, curr_px, regime=None):
        target, _, _, margin_pct, reg = self._regime_cap_target_qty(curr_px, regime)
        live_qty = self._safe_qty(live_qty)
        if target <= 0 or live_qty <= 0:
            return False, target, margin_pct, reg
        tol = self._regime_cap_tolerance(target)
        excess = live_qty - int(target)
        if excess > REGIME_CAP_TOLERANCE_CONTRACTS and excess <= tol:
            logger.info(
                f"📎 [档位限额] 微超 {live_qty} > {target} 张 "
                f"(+{excess}, {excess / max(target, 1):.2%} ≤ {QTY_ALIGN_MIN_PCT:.0%} 容忍)，跳过裁减"
            )
        return live_qty > int(target) + tol, target, margin_pct, reg

    def _trim_position_to_target(self, target_qty, action, reason_tag="叠仓Remediation"):
        """叠仓Remediation：仅裁减 excess=实盘-目标，带安全校验"""
        target_qty = int(target_qty or 0)
        pos = self._get_active_position()
        real = self._safe_qty(pos.get("size")) if pos else 0
        if not pos or target_qty <= 0:
            return real
        cap_tol = self._regime_cap_tolerance(target_qty)
        if real <= target_qty + cap_tol:
            return real
        trim_qty = real - target_qty
        plan_err = self._validate_cap_trim_plan(real, target_qty, trim_qty)
        if plan_err:
            logger.error(f"✂️ {reason_tag} 中止: {plan_err} | live={real} target={target_qty}")
            dingtalk.report_system_alert(
                "档位裁减已中止（安全保护）",
                f"场景：{reason_tag}\n"
                f"实盘：**{real}** 张 → 目标：**{target_qty}** 张\n"
                f"原因：{plan_err}",
                suggestion="请核对本金快照与 TV 档位是否一致；勿手动干预，待下一 TV 信号或人工核查后重试",
            )
            return real
        close_side = "sell" if action == "LONG" else "buy"
        pos_side = "long" if action == "LONG" else "short"
        logger.warning(
            f"✂️ {reason_tag}: 裁减 {trim_qty} 张 "
            f"(实盘 {real} → 目标 {target_qty})"
        )
        deepcoin_client.cancel_all_open_orders(self.symbol)
        time.sleep(0.5)
        self._cancel_all_tp_limit_orders(max_rounds=3)
        time.sleep(0.3)
        new_sz = real
        for _ in range(CAP_TRIM_MAX_ROUNDS):
            pos = self._get_active_position()
            if not pos:
                break
            cur = self._safe_qty(pos.get("size"))
            if cur <= target_qty + cap_tol:
                new_sz = cur
                break
            slice_trim = cur - target_qty
            if slice_trim <= 0:
                new_sz = cur
                break
            deepcoin_client.place_market_order(
                self.symbol, close_side, pos_side, slice_trim, reduce_only=True,
            )
            time.sleep(1.0)
            verified = self._wait_verify(
                lambda: self._get_active_position(),
                retries=6,
                delay=0.5,
            )
            new_sz = self._safe_qty(verified.get("size")) if verified else cur
            if new_sz <= target_qty + cap_tol:
                break
        if new_sz < target_qty * 0.5 and real > target_qty * 1.5:
            dingtalk.report_system_alert(
                "档位裁减过度",
                f"目标 **{target_qty}** 张，裁减后仅 **{new_sz}** 张（原 **{real}** 张）",
                suggestion="疑似额度基数错误，请核对本金快照与 TV 档位，必要时人工恢复仓位",
            )
        elif new_sz > target_qty * OPEN_OVERSIZE_RATIO:
            dingtalk.report_system_alert(
                "叠仓裁减未达标",
                f"目标 **{target_qty}** 张，裁减后仍 **{new_sz}** 张",
                suggestion="请人工核查 Deepcoin 盘口与挂单，雷达将继续尝试纠偏",
            )
        return new_sz

    def _radar_enforce_regime_cap(self, live_qty, curr_px, force=False):
        """
        雷达最高权限：实盘超过 TV 档位保证金上限 → reduceOnly 裁减 → 重挂 TP123。
        雷达移动止损位不变，仅补挂缺失 STOP。
        """
        live_qty = self._safe_qty(live_qty)
        if live_qty <= 0 or not self.current_side:
            return None
        if not force and (
            getattr(self, "_open_in_progress", False)
            or getattr(self, "_recover_in_progress", False)
        ):
            return None

        oversize, target, margin_pct, regime = self._is_oversize_for_regime(
            live_qty, curr_px, self.regime,
        )
        if not oversize:
            return None

        now = time.time()
        severe = live_qty > target * 1.35
        if (
            not severe
            and now - getattr(self, "_last_regime_cap_ts", 0) < REGIME_CAP_COOLDOWN_SEC
        ):
            logger.info(
                f"📡 [雷达档位限额] 超标 {live_qty}>{target} 张 但冷却中 "
                f"(R{regime} {margin_pct:.0%})"
            )
            return None

        _, balance, margin_usdt, margin_pct, regime = self._regime_cap_target_qty(curr_px, regime)
        old_qty = live_qty
        logger.warning(
            f"📡 [雷达档位限额] R{regime} 上限 {target} 张 "
            f"(本金 {balance:.0f}U×{margin_pct:.0%}×{self.leverage}x) | "
            f"实盘 {live_qty} 张 超标 → 强制裁减"
        )

        new_qty = self._trim_position_to_target(
            target, self.current_side, reason_tag=f"雷达R{regime}档位限额",
        )
        pos = self._get_active_position()
        entry = float(pos["entry_price"]) if pos else self.watched_entry
        self.watched_qty = new_qty
        self.initial_qty = new_qty
        if pos:
            self.watched_entry = entry
        self._save_state()

        sl = self._radar_sl_to_pass()
        result = self._enforce_defense_alignment(
            new_qty, entry, dynamic_sl=sl,
            reason=f"雷达档位限额 R{regime} 裁减后 TP 对齐", rounds=3,
        )
        if sl and not self._has_trigger_sl_near(sl):
            self._ensure_radar_sl(new_qty, sl)

        self._last_regime_cap_ts = now
        verify_note = (
            f"本金 {balance:.2f}U × R{regime} {margin_pct:.0%} × {self.leverage}x "
            f"= 保证金 {margin_usdt:.0f}U → 上限 {target} 张 | "
            f"裁减 {old_qty} → {new_qty} 张 | "
            f"TP {result['matched']}/{result['expected']} | "
            f"{self._format_audit_summary(result['audit'])} | "
            f"雷达SL={'已保留/已补' if sl else '待命'}"
        )
        self._call_dingtalk(
            dingtalk.report_radar_regime_cap_trim,
            side=self.current_side,
            old_qty=old_qty,
            new_qty=new_qty,
            target_qty=target,
            regime=regime,
            margin_pct=margin_pct,
            tp_audit=result["audit"],
            verify_note=verify_note,
            principal_balance=balance,
            margin_usdt=margin_usdt,
            leverage=self.leverage,
            trim_qty=old_qty - new_qty,
        )
        return {"new_qty": new_qty, "target": target, "result": result}

    def _is_dust_qty(self, qty):
        """深币最小 1 张；无主仓账本时的孤立 1 张视为蚂蚁仓"""
        q = self._safe_qty(qty)
        if q <= 0:
            return False
        ref = self._safe_qty(self.initial_qty) + self._safe_qty(self.watched_qty)
        return q == DUST_ORPHAN_CONTRACTS and ref == 0

    def _should_finalize_tp_victory(self, real_amt):
        """止盈网格已吃完、盘口无 TP 限价单，但可能残留张数 → 扫尾收网"""
        real_amt = self._safe_qty(real_amt)
        if real_amt <= 0:
            return False
        if self._is_dust_qty(real_amt):
            return True
        if self._collect_limit_tp_prices():
            return False
        ref = self._safe_qty(self.initial_qty or self.watched_qty)
        if ref > 0:
            threshold = max(DUST_ORPHAN_CONTRACTS, int(ref * TP_COMPLETE_RESIDUAL_RATIO))
            if real_amt <= threshold:
                return True
        return False

    def _verify_position_qty(self, expected_qty, expected_side=None):
        pos = self._verify_position(expected_side)
        if not pos or self._safe_qty(pos.get("size")) != expected_qty:
            return None
        return pos

    def _report_flat_close(self, reason, swept_dust=False):
        """平仓/止盈收网钉钉：REST 核查重试"""
        flat = self._wait_verify(self._verify_flat, retries=6, delay=0.5)
        base_note = "盘口无持仓 | 挂单已清空 | 智慧大脑复位待命"
        if swept_dust:
            base_note = f"蚂蚁仓已市价扫尾 | {base_note}"
        if flat:
            verify_note = base_note
        else:
            pos = self._get_active_position()
            residual = self._safe_qty(pos["size"]) if pos else 0
            if residual > 0 and not self._is_dust_qty(residual):
                logger.warning(
                    f"平仓钉钉跳过：空仓核查未通过 | 残留 {residual}张 | reason={reason}"
                )
                return
            verify_note = f"{base_note} | REST 同步略延迟"
            logger.info(f"平仓钉钉：REST 延迟，仍推送收网播报 | reason={reason}")
        self._call_dingtalk(
            dingtalk.report_supervisor_close,
            reason=reason or "仓位归零 (人工全平 / 止盈吃满)",
            verify_note=verify_note,
            verified=flat,
            swept_dust=swept_dust,
        )

    def _sweep_dust_and_finalize(self, reason):
        """哨兵检测：止盈后蚂蚁仓/无 TP 残张 → 撤单 + reduceOnly 扫尾 + 收网钉钉"""
        logger.warning(f"🐜 止盈扫尾：检测到残量，启动蚂蚁仓强平 → {reason}")
        self.monitoring = False
        deepcoin_client.cancel_all_open_orders(self.symbol)
        time.sleep(0.4)
        for round_i in range(4):
            pos = self._get_active_position()
            if not pos or self._safe_qty(pos.get("size")) <= 0:
                break
            close_side = "sell" if pos["posSide"] == "long" else "buy"
            live_sz = self._safe_qty(pos["size"])
            logger.info(f"🐜 扫尾第 {round_i + 1}/4: {close_side} {live_sz}张 reduceOnly")
            deepcoin_client.place_market_order(
                self.symbol, close_side, pos["posSide"], live_sz, reduce_only=True,
            )
            time.sleep(1.0)
        self.watched_qty = 0
        self.initial_qty = 0
        self.current_side = None
        self._save_state()
        deepcoin_client.cancel_all_open_orders(self.symbol)
        self._report_flat_close(reason, swept_dust=True)

    def _apply_recover_live_alignment(self, side, reconcile):
        """重启以实盘为准：不回放 TV 平仓，不因日志方向差异核武全平（仅 live webhook 才平仓）"""
        extra_notes = []
        if reconcile.get("tv_close"):
            action = (self.last_tv_signal or {}).get("action", "CLOSE")
            msg = (
                f"TV日志末条为 {action}，重启不回放平仓 → 以实盘 {side} 继续闪电接管"
            )
            logger.warning(f"🔄 [重启] {msg}")
            extra_notes.append(msg)
            last_open_tv = self._load_last_tv_open_signal()
            if last_open_tv:
                self.last_tv_side = (last_open_tv.get("action") or side).upper()
                open_tps = self._sanitize_tp_prices(last_open_tv.get("tv_tps", []))
                if sum(1 for t in open_tps if t > 0) > 0:
                    self.tv_tps = open_tps
        if reconcile.get("direction_mismatch") or (
                self.last_tv_side and side != self.last_tv_side
        ):
            old_tv = self.last_tv_side
            self.last_tv_side = side
            msg = f"方向以实盘为准: {side} (TV日志={old_tv})"
            logger.warning(f"🔄 [重启] {msg}")
            extra_notes.append(msg)
        elif not self.last_tv_side:
            self.last_tv_side = side
        return extra_notes

    def _scan_and_sweep_dust_on_startup(self, was_monitoring=False):
        """重启首检：发现蚂蚁仓/止盈残张 → 扫尾收网，避免误接管为正常持仓"""
        pos = self._get_active_position()
        if not pos or self._safe_qty(pos.get("size")) <= 0:
            return False
        if not self.current_side:
            self.current_side = "LONG" if pos.get("posSide") == "long" else "SHORT"
        real_amt = self._safe_qty(pos["size"])
        ref = max(self._safe_qty(self.initial_qty), self._safe_qty(self.watched_qty))
        if was_monitoring and not self._is_dust_qty(real_amt):
            if ref <= 0 or real_amt > max(
                DUST_ORPHAN_CONTRACTS, int(ref * TP_COMPLETE_RESIDUAL_RATIO)
            ):
                logger.info(
                    f"🔄 [重启扫描] 活跃主仓 {real_amt}张 (ref={ref})，跳过蚂蚁扫尾"
                )
                return False
        if not self._is_dust_qty(real_amt) and not self._should_finalize_tp_victory(real_amt):
            return False
        if self._safe_qty(self.initial_qty) > 0 or self._safe_qty(self.watched_qty) > 0:
            reason = "仓位归零 (止盈吃单 / 人工全平 / TV 强制平仓)"
        else:
            reason = "重启扫描：盘口蚂蚁仓自动扫平"
        logger.warning(
            f"🐜 [重启扫描] {self.current_side} 残量 {real_amt}张 "
            f"(initial={self.initial_qty}, watched={self.watched_qty}) → 扫尾强平"
        )
        self._sweep_dust_and_finalize(reason)
        return True

    def _recover_missed_flat_on_startup(self, was_monitoring=False):
        """重启对账：服务宕机期间已全平，但账本仍有仓 → 补发收网钉钉"""
        pos = self._get_active_position()
        if pos and self._safe_qty(pos.get("size")) > 0:
            return False

        prev_watched = self._safe_qty(self.watched_qty)
        prev_initial = self._safe_qty(self.initial_qty)
        prev_side = self.current_side

        had_active_book = (
            prev_watched > 0
            or prev_initial > 0
            or prev_side in ("LONG", "SHORT")
            or was_monitoring
        )
        if not had_active_book:
            last_open = self._load_last_journal_entry(OPEN_JOURNAL)
            if last_open and last_open.get("source") in ("open", "recover"):
                had_active_book = True
                prev_watched = prev_watched or self._safe_qty(last_open.get("qty", 0))
                prev_side = prev_side or last_open.get("side")

        if not had_active_book:
            return False

        logger.warning(
            f"📭 [重启对账] 账本/日志曾有仓 (watched={prev_watched}, side={prev_side}, "
            f"monitoring={was_monitoring}) 但盘口已全平 → 补发收网播报"
        )
        deepcoin_client.cancel_all_open_orders(self.symbol)
        self.monitoring = False
        self.watched_qty = 0
        self.initial_qty = 0
        self.current_side = None
        self._save_state()

        verify_note = (
            f"重启对账补发 | 原账本 {prev_watched}张 {prev_side or ''} | "
            f"盘口无持仓 | 挂单已清空 | 智慧大脑复位待命"
        )
        self._call_dingtalk(
            dingtalk.report_supervisor_close,
            reason="仓位归零 (止盈吃单 / 人工全平 / TV 强制平仓)",
            verify_note=verify_note,
            verified=True,
            swept_dust=False,
        )
        return True

    def _verify_position(self, expected_side=None):
        pos = self._get_active_position()
        if not pos or self._safe_qty(pos.get("size")) <= 0:
            return None
        side = "LONG" if pos["posSide"] == "long" else "SHORT"
        if expected_side and side != expected_side:
            return None
        return pos

    def _is_tp_limit_order(self, o):
        if o.get("ordType") not in ("limit", "post_only", None):
            return False
        val = o.get("reduceOnly")
        if val is True or str(val).lower() in ("true", "1"):
            return True
        if not self.current_side:
            return False
        close_side = "sell" if self.current_side == "LONG" else "buy"
        return str(o.get("side", "")).lower() == close_side

    def _collect_limit_tp_prices(self):
        prices = []
        for o in deepcoin_client.get_pending_orders(self.symbol):
            if not self._is_tp_limit_order(o):
                continue
            px = float(o.get("px", 0) or 0)
            if px > 0:
                prices.append(round(px, 2))
        return sorted(prices)

    def _collect_tp_limit_orders(self):
        orders = []
        for o in deepcoin_client.get_pending_orders(self.symbol):
            if not self._is_tp_limit_order(o):
                continue
            px = float(o.get("px", 0) or 0)
            if px <= 0:
                continue
            orders.append({
                "orderId": o.get("ordId"),
                "price": round(px, 2),
                "qty": self._safe_qty(o.get("sz")),
            })
        return orders

    def _expected_tp_count(self, tp_pxs=None):
        tp_pxs = tp_pxs if tp_pxs is not None else self.tv_tps
        return sum(1 for t in tp_pxs if t > 0)

    def _tp_split_regime(self):
        if self.watched_qty and self._safe_qty(self.watched_qty) > 0:
            return int(getattr(self, "open_regime", self.regime) or self.regime)
        return int(self.regime)

    def _expected_tp_levels(self, live_qty):
        regime = self._tp_split_regime()
        ratios = self.regime_settings[regime]["ratios"]
        q1, q2, q3 = self._calculate_tp_quantities(live_qty, ratios)
        return [
            {"level": 1, "qty": q1, "price": self.tv_tps[0]},
            {"level": 2, "qty": q2, "price": self.tv_tps[1]},
            {"level": 3, "qty": q3, "price": self.tv_tps[2]},
        ]

    def _audit_tp_levels(self, live_qty, tolerance=1.0):
        """严格审计：每档价位唯一 + 张数符合 regime 比例 + 无孤儿单"""
        live_qty = self._resolve_live_qty(live_qty)
        orders = self._collect_tp_limit_orders()
        levels = []
        matched_full = 0
        issues = []

        for lv in self._expected_tp_levels(live_qty):
            if lv["qty"] <= 0 or lv["price"] <= 0:
                continue
            at_px = [o for o in orders if abs(o["price"] - lv["price"]) <= tolerance]
            status = "ok"
            actual_qty = 0
            if len(at_px) == 0:
                status = "missing"
                issues.append(f"TP{lv['level']} @{lv['price']:.2f} 缺失")
            elif len(at_px) > 1:
                status = "duplicate"
                actual_qty = sum(o["qty"] for o in at_px)
                issues.append(f"TP{lv['level']} @{lv['price']:.2f} 重复 {len(at_px)} 张")
            elif at_px[0]["qty"] != lv["qty"]:
                status = "qty_mismatch"
                actual_qty = at_px[0]["qty"]
                issues.append(
                    f"TP{lv['level']} {actual_qty}张 ≠ 期望 {lv['qty']}张 "
                    f"({self.regime_settings[self._tp_split_regime()]['ratios']})"
                )
            else:
                matched_full += 1
                actual_qty = at_px[0]["qty"]
            levels.append({**lv, "status": status, "actual_qty": actual_qty})

        expected_prices = [lv["price"] for lv in levels]
        orphans = [
            o for o in orders
            if not any(abs(o["price"] - p) <= tolerance for p in expected_prices)
        ]
        for o in orphans:
            issues.append(f"孤儿止盈 @{o['price']:.2f} {o['qty']}张")

        expected = self._expected_tp_count()
        pending_prices = sorted({o["price"] for o in orders})
        return {
            "matched_full": matched_full,
            "expected": expected,
            "levels": levels,
            "issues": issues,
            "orphans": orphans,
            "pending_prices": pending_prices,
            "live_qty": live_qty,
        }

    def _format_audit_summary(self, audit):
        parts = []
        for lv in audit.get("levels", []):
            if lv["price"] <= 0:
                continue
            icon = "✅" if lv["status"] == "ok" else "❌"
            line = f"{icon}TP{lv['level']} {lv['qty']}张@{lv['price']:.2f}"
            if lv["status"] != "ok":
                line += f"({lv['status']})"
            parts.append(line)
        if audit.get("issues"):
            parts.append("问题:" + "; ".join(audit["issues"][:3]))
        return " | ".join(parts) if parts else "无有效 TP"

    def _count_matched_tp_orders(self, tp_pxs, tolerance=1.0, live_qty=None):
        if live_qty is not None and live_qty > 0:
            audit = self._audit_tp_levels(live_qty, tolerance)
            return audit["matched_full"], audit["pending_prices"]
        pending_prices = self._collect_limit_tp_prices()
        matched = 0
        for tp in tp_pxs:
            if tp <= 0:
                continue
            if any(abs(p - tp) <= tolerance for p in pending_prices):
                matched += 1
        return matched, pending_prices

    def _has_duplicate_tp_orders(self, tolerance=1.0):
        orders = self._collect_tp_limit_orders()
        expected = self._expected_tp_count()
        if expected <= 0:
            return False
        if len(orders) > expected:
            return True
        for tp in self.tv_tps:
            if tp <= 0:
                continue
            at_px = [o for o in orders if abs(o["price"] - tp) <= tolerance]
            if len(at_px) > 1:
                return True
        return False

    def _defenses_fully_ok(self, live_qty, dynamic_sl=None, tolerance=1.0):
        tp_pxs = self.tv_tps
        expected = self._expected_tp_count(tp_pxs)
        if expected == 0:
            return dynamic_sl is None or self._has_trigger_sl_near(dynamic_sl, tolerance)

        audit = self._audit_tp_levels(live_qty, tolerance)
        if audit["matched_full"] < expected:
            return False
        if audit["orphans"]:
            return False
        if dynamic_sl and not self._has_trigger_sl_near(dynamic_sl, tolerance):
            return False
        return True

    def _patch_missing_tp_levels(self, live_qty, tolerance=1.0):
        live_qty = self._resolve_live_qty(live_qty)
        audit = self._audit_tp_levels(live_qty, tolerance)
        if self._defense_needs_immediate_fix(audit):
            logger.warning("补挂跳过：检测到重复/缺失/偏差，改走核武对齐")
            return 0
        close_side = "sell" if self.current_side == "LONG" else "buy"
        pos_side = "long" if self.current_side == "LONG" else "short"
        ratios = self.regime_settings[self._tp_split_regime()]["ratios"]
        qty1, qty2, qty3 = self._calculate_tp_quantities(live_qty, ratios)
        levels = [(qty1, self.tv_tps[0]), (qty2, self.tv_tps[1]), (qty3, self.tv_tps[2])]
        placed = 0

        for q, px in levels:
            if q <= 0 or px <= 0:
                continue
            orders = self._collect_tp_limit_orders()
            at_px = [o for o in orders if abs(o["price"] - px) <= tolerance]
            if len(at_px) == 1 and at_px[0]["qty"] == q:
                logger.info(f"  ✓ TP @ {px:.2f} 已存在 {at_px[0]['qty']}张，跳过")
                continue
            for o in at_px:
                if o.get("orderId"):
                    deepcoin_client.cancel_order(self.symbol, ord_id=o["orderId"])
                    time.sleep(0.25)
            logger.info(f"  + 补挂 TP @ {px:.2f} qty={q}张")
            res = deepcoin_client.place_limit_order(
                self.symbol, close_side, pos_side, px, q, reduce_only=True,
            )
            if res and deepcoin_client._is_success(res):
                placed += 1
            time.sleep(0.4)
        return placed

    def _cancel_orphan_tp_orders(self, live_qty, tolerance=1.0):
        audit = self._audit_tp_levels(live_qty, tolerance)
        cancelled = 0
        for o in audit["orphans"]:
            if o.get("orderId"):
                deepcoin_client.cancel_order(self.symbol, ord_id=o["orderId"])
                cancelled += 1
                time.sleep(0.2)
        if cancelled:
            logger.info(f"🧹 撤销 {cancelled} 张孤儿止盈单")
        return cancelled

    def _pick_best_tp_order(self, orders, target_qty):
        if not orders:
            return None
        return min(orders, key=lambda o: abs(o["qty"] - target_qty))

    def _surgical_repair_tp_defenses(self, live_qty, entry, tolerance=1.0):
        """重启智能修复：读实盘 → 去重 → 补缺/纠偏，避免核武毁掉正确盘口"""
        live_qty = self._resolve_live_qty(live_qty)
        if live_qty <= 0:
            return self._audit_tp_levels(live_qty), 0

        close_side = "sell" if self.current_side == "LONG" else "buy"
        pos_side = "long" if self.current_side == "LONG" else "short"
        actions = 0
        audit = self._audit_tp_levels(live_qty, tolerance)

        actions += self._cancel_orphan_tp_orders(live_qty, tolerance)
        if actions:
            time.sleep(0.4)
            audit = self._audit_tp_levels(live_qty, tolerance)

        for lv in self._expected_tp_levels(live_qty):
            price = lv["price"]
            target_q = lv["qty"]
            if price <= 0 or target_q <= 0:
                continue

            at_px = [
                o for o in self._collect_tp_limit_orders()
                if abs(o["price"] - price) <= tolerance
            ]

            if len(at_px) > 1:
                keep = self._pick_best_tp_order(at_px, target_q)
                for o in at_px:
                    if o["orderId"] == keep["orderId"]:
                        continue
                    deepcoin_client.cancel_order(self.symbol, ord_id=o["orderId"])
                    actions += 1
                    time.sleep(0.2)
                logger.info(
                    f"🔧 重启去重 TP{lv['level']} @{price:.2f}："
                    f"撤 {len(at_px) - 1} 留 {keep['qty']} 张"
                )
                time.sleep(0.35)
                at_px = [keep]

            if len(at_px) == 1:
                if at_px[0]["qty"] != target_q:
                    deepcoin_client.cancel_order(self.symbol, ord_id=at_px[0]["orderId"])
                    actions += 1
                    time.sleep(0.3)
                    res = deepcoin_client.place_limit_order(
                        self.symbol, close_side, pos_side, price, target_q,
                        reduce_only=True,
                    )
                    if res and deepcoin_client._is_success(res):
                        actions += 1
                        logger.info(
                            f"🔧 重启纠偏 TP{lv['level']} @{price:.2f} → {target_q} 张"
                        )
                    time.sleep(0.35)
                continue

            res = deepcoin_client.place_limit_order(
                self.symbol, close_side, pos_side, price, target_q, reduce_only=True,
            )
            if res and deepcoin_client._is_success(res):
                actions += 1
                logger.info(f"🔧 重启补挂 TP{lv['level']} @{price:.2f} qty={target_q} 张")
            time.sleep(0.35)

        final = self._audit_tp_levels(live_qty, tolerance)
        if actions:
            logger.info(
                f"🔧 重启智能修复完成 {actions} 步 | "
                f"{final['matched_full']}/{final['expected']} | "
                f"{self._format_audit_summary(final)}"
            )
        return final, actions

    def _cancel_stop_orders(self, scope="all"):
        cancelled = 0
        for t in deepcoin_client.get_trigger_orders_pending(self.symbol):
            if scope == "radar" and not self._is_radar_trigger_order(t):
                continue
            if scope == "shield" and not self._is_shield_trigger_order(t):
                continue
            oid = t.get("ordId")
            if oid:
                deepcoin_client.cancel_trigger_order(self.symbol, oid)
                cancelled += 1
                time.sleep(0.2)
        return cancelled

    @staticmethod
    def _trigger_order_price(t):
        for key in ("triggerPx", "slTriggerPrice", "triggerPrice"):
            val = t.get(key)
            if val is not None and str(val).strip() not in ("", "0"):
                try:
                    return round(float(val), 2)
                except (TypeError, ValueError):
                    pass
        return None

    def _shield_stop_price(self, entry=None):
        entry = float(entry or self.watched_entry or 0)
        if entry <= 0 or not self.current_side:
            return None
        if self.current_side == "LONG":
            return round(entry * (1 - SHIELD_HARD_STOP_PCT), 2)
        if self.current_side == "SHORT":
            return round(entry * (1 + SHIELD_HARD_STOP_PCT), 2)
        return None

    def _shield_tier_prices(self, entry=None):
        px = self._shield_stop_price(entry)
        return [px] if px else []

    def _is_shield_trigger_order(self, t, tier_prices=None):
        px = self._trigger_order_price(t)
        if px is None:
            return False
        tier_prices = tier_prices or self._shield_tier_prices()
        return any(abs(px - tp) <= SHIELD_STOP_TOLERANCE for tp in tier_prices)

    def _is_radar_trigger_order(self, t):
        if not self._is_radar_active():
            return False
        px = self._trigger_order_price(t)
        if px is None:
            return False
        return abs(px - round(float(self.current_sl), 2)) <= SHIELD_STOP_TOLERANCE

    def _adverse_move_pct(self, curr_px):
        entry = self.watched_entry
        if not entry or curr_px <= 0:
            return 0.0
        if self.current_side == "LONG":
            return max(0.0, (entry - curr_px) / entry)
        if self.current_side == "SHORT":
            return max(0.0, (curr_px - entry) / entry)
        return 0.0

    def _favorable_move_pct(self, curr_px):
        entry = self.watched_entry
        if not entry or curr_px <= 0:
            return 0.0
        if self.current_side == "LONG":
            return max(0.0, (curr_px - entry) / entry)
        if self.current_side == "SHORT":
            return max(0.0, (entry - curr_px) / entry)
        return 0.0

    def _resolve_defense_regime(self, curr_px):
        """FAVORABLE=雷达已/应激活 | SHIELD=维护10%硬止损"""
        if curr_px <= 0 or not self.watched_entry:
            return "SHIELD"
        if self._is_radar_active() or self._should_radar_trail(curr_px):
            return "FAVORABLE"
        return "SHIELD"

    def _should_disarm_shield_for_favorable(self, curr_px):
        """雷达达到 TP1 激活比例 → 撤 10% 硬止损，交棒移动保本"""
        stop_px = self._shield_stop_price()
        has_shield = bool(
            getattr(self, "shield_active", False)
            or (stop_px and self._has_shield_stop_at_price(stop_px))
        )
        if not has_shield:
            return False
        return self._is_radar_active() or self._should_radar_trail(curr_px)

    def _shield_needs_exchange_action(self, live_qty, audit):
        status = audit.get("status")
        if status == "duplicate":
            return True
        if status == "missing":
            return True
        if status == "qty_mismatch":
            sized = float(getattr(self, "shield_sized_qty", 0) or 0)
            if sized > 0 and self._qty_change_ratio(sized, live_qty) < QTY_ALIGN_MIN_PCT:
                return False
            return audit.get("max_drift_pct", 1.0) > SHIELD_QTY_TOLERANCE_PCT
        return False

    def _process_directional_defenses(self, real_amt, curr_px):
        """
        雷达激活 → 撤 10% 硬止损 + 移动保本；
        否则 → 维护开单时挂的 10% 全平硬止损（只挂一次，先查实盘）。
        """
        if self._resolve_defense_regime(curr_px) == "FAVORABLE":
            if self._should_disarm_shield_for_favorable(curr_px):
                progress = self._radar_activation_progress(curr_px)
                self._disarm_shield(
                    f"雷达激活(进度 {progress:.0%})，撤销 {SHIELD_HARD_STOP_PCT:.0%} 硬止损",
                    notify=True,
                )
            self._process_radar_trailing(real_amt, curr_px)
            return
        self._maintain_hard_shield(real_amt, curr_px)

    def _should_activate_shield(self, curr_px):
        """雷达未激活时始终应挂/维护 10% 硬止损"""
        if not self.watched_entry or not self.current_side:
            return False
        if self._is_radar_active() or self._should_radar_trail(curr_px):
            return False
        return True

    def _remaining_shield_tier_indices(self):
        consumed = set(getattr(self, "shield_tiers_consumed", []) or [])
        return [i for i, pct in enumerate(SHIELD_TIER_PCTS) if pct not in consumed]

    def _shield_quantities_for_remaining(self, live_qty):
        remaining = self._remaining_shield_tier_indices()
        live_qty = self._safe_qty(live_qty)
        if not remaining or live_qty <= 0:
            return {}
        if len(remaining) == 1:
            return {remaining[0]: live_qty}
        weights = [SHIELD_TIER_RATIOS[i] for i in remaining]
        wsum = sum(weights) or 1.0
        norm = [w / wsum for w in weights]
        qs = self._calculate_tp_quantities(live_qty, norm)
        return {remaining[i]: qs[i] for i in range(len(remaining))}

    def _has_shield_stop_at_price(self, tp, tier_prices=None):
        tier_prices = tier_prices or self._shield_tier_prices()
        for t in deepcoin_client.get_trigger_orders_pending(self.symbol):
            if not self._is_shield_trigger_order(t, tier_prices):
                continue
            px = self._trigger_order_price(t)
            if px is not None and abs(px - tp) <= SHIELD_STOP_TOLERANCE:
                return True
        return False

    def _shield_orders_at_tiers(self, tier_prices):
        buckets = {i: [] for i in range(len(tier_prices))}
        for t in deepcoin_client.get_trigger_orders_pending(self.symbol):
            px = self._trigger_order_price(t)
            if px is None:
                continue
            for i, tp in enumerate(tier_prices):
                if abs(px - tp) <= SHIELD_STOP_TOLERANCE:
                    tsz = self._safe_qty(t.get("sz", t.get("size", 0)))
                    buckets[i].append({"order": t, "qty": tsz})
                    break
        return buckets

    def _purge_shield_stop_orders(self, tier_prices=None):
        tier_prices = tier_prices or self._shield_tier_prices()
        if not tier_prices:
            return 0
        cancelled = 0
        for t in deepcoin_client.get_trigger_orders_pending(self.symbol):
            px = self._trigger_order_price(t)
            if px is None:
                continue
            if not any(abs(px - tp) <= SHIELD_STOP_TOLERANCE for tp in tier_prices):
                continue
            oid = t.get("ordId")
            if oid:
                deepcoin_client.cancel_trigger_order(self.symbol, oid)
                cancelled += 1
                time.sleep(0.15)
        return cancelled

    def _split_shield_quantities(self, total_qty):
        return self._calculate_tp_quantities(self._safe_qty(total_qty), list(SHIELD_TIER_RATIOS))

    def _can_maintain_shield_now(self, force=False):
        if force:
            return True
        now = time.time()
        if now < getattr(self, "_sentinel_grace_until", 0):
            return False
        if now - getattr(self, "_last_shield_maintain_ts", 0) < SHIELD_MAINTAIN_COOLDOWN_SEC:
            return False
        streak = getattr(self, "_shield_fail_streak", 0)
        if streak > 0:
            backoff = min(
                SHIELD_FAIL_BACKOFF_BASE_SEC * (2 ** (streak - 1)),
                SHIELD_FAIL_BACKOFF_MAX_SEC,
            )
            if now - getattr(self, "_last_shield_fail_ts", 0) < backoff:
                return False
        return True

    def _record_shield_maintain(self, success):
        self._last_shield_maintain_ts = time.time()
        if success:
            self._shield_fail_streak = 0
        else:
            self._shield_fail_streak = getattr(self, "_shield_fail_streak", 0) + 1
            self._last_shield_fail_ts = time.time()

    def _audit_shield_orders(self, live_qty, entry=None):
        tier_prices = self._shield_tier_prices(entry)
        live_qty = self._safe_qty(self._resolve_live_qty(live_qty))
        remaining = self._remaining_shield_tier_indices()
        result = {
            "status": "none",
            "live_qty": live_qty,
            "remaining": remaining,
            "tier_prices": tier_prices,
            "buckets": {},
            "qty_map": {},
            "max_drift_pct": 0.0,
            "issues": [],
        }
        if not remaining:
            result["status"] = "ok" if live_qty <= 0 else "none"
            return result
        if live_qty <= 0:
            result["status"] = "missing"
            result["issues"].append("no_position")
            return result

        qty_map = self._shield_quantities_for_remaining(live_qty)
        result["qty_map"] = qty_map
        buckets = self._shield_orders_at_tiers(tier_prices)
        result["buckets"] = buckets

        has_duplicate = False
        has_missing = False
        has_qty_mismatch = False
        max_drift_pct = 0.0

        for idx in remaining:
            q = qty_map.get(idx, 0)
            if q <= 0:
                continue
            orders = buckets.get(idx, [])
            if not orders:
                has_missing = True
                result["issues"].append(f"tier{idx + 1}_missing")
            elif len(orders) > SHIELD_MAX_TIER_ORDERS:
                has_duplicate = True
                result["issues"].append(f"tier{idx + 1}_dup:{len(orders)}")
            else:
                drift = abs(orders[0]["qty"] - q) / q if q > 0 else 1.0
                max_drift_pct = max(max_drift_pct, drift)
                if drift > SHIELD_QTY_TOLERANCE_PCT:
                    has_qty_mismatch = True
                    result["issues"].append(
                        f"tier{idx + 1}_qty:{orders[0]['qty']}vs{q}"
                    )

        for idx, orders in buckets.items():
            if idx not in remaining and orders:
                has_duplicate = True
                result["issues"].append(f"tier{idx + 1}_orphan:{len(orders)}")

        result["max_drift_pct"] = max_drift_pct
        if has_duplicate:
            result["status"] = "duplicate"
        elif has_missing:
            result["status"] = "missing"
        elif has_qty_mismatch:
            result["status"] = "qty_mismatch"
        else:
            result["status"] = "ok"
        return result

    def _shield_orders_adequate(self, audit):
        if audit["status"] == "ok":
            return True
        if audit["status"] == "qty_mismatch":
            return audit.get("max_drift_pct", 1.0) <= SHIELD_QTY_TOLERANCE_PCT
        return False

    def _shield_orders_ok(self, live_qty, entry=None):
        return self._shield_orders_adequate(self._audit_shield_orders(live_qty, entry))

    def _try_acquire_recover_singleton(self):
        """多 worker 导入时仅允许一个进程执行重启接管，避免双钉钉/双撤挂"""
        try:
            os.makedirs("logs", exist_ok=True)
            if os.path.exists(RECOVER_LOCK_FILE):
                age = time.time() - os.path.getmtime(RECOVER_LOCK_FILE)
                if age < RECOVER_LOCK_TTL_SEC:
                    try:
                        with open(RECOVER_LOCK_FILE, encoding="utf-8") as f:
                            info = f.read().strip()
                    except Exception:
                        info = "?"
                    logger.info(
                        f"🔄 跳过重复重启接管 (另一进程 {age:.0f}s 前已执行: {info})"
                    )
                    return False
            with open(RECOVER_LOCK_FILE, "w", encoding="utf-8") as f:
                f.write(f"pid={os.getpid()} ts={datetime.now().isoformat()}")
            return True
        except Exception as e:
            logger.warning(f"recover singleton lock: {e}")
            return True

    def _build_recover_health_report(self, pos, curr_px, tp_audit, shield_audit=None):
        """重启全域核查：实盘头寸 + TV + TP123 + 硬止损 + 浮盈/浮亏防线路由"""
        entry = float(pos.get("entry_price", self.watched_entry) or 0)
        curr_px = float(curr_px or 0)
        favorable = self._favorable_move_pct(curr_px) if curr_px > 0 else 0.0
        adverse = self._adverse_move_pct(curr_px) if curr_px > 0 else 0.0
        radar_progress = self._radar_activation_progress(curr_px) if curr_px > 0 else 0.0
        radar_active = self._is_radar_active()
        should_radar = self._should_radar_trail(curr_px) if curr_px > 0 else radar_active

        shield_audit = shield_audit or self._audit_shield_orders(pos["size"], entry)
        shield_ok = self._shield_orders_adequate(shield_audit)

        if should_radar or radar_active:
            pnl_label = f"浮盈·雷达区 (进度 {radar_progress:.0%})"
            defense_plan = "撤10%硬止损 → 雷达移动保本防回吐"
        elif adverse > 0.001:
            pnl_label = f"浮亏 {adverse:.1%}"
            defense_plan = "持有 TP123 + 10%硬止损全平"
        elif favorable > 0.001:
            pnl_label = f"微盈 {favorable:.1%}·未达雷达激活"
            defense_plan = "持有 TP123 + 10%硬止损 (朝TP1迈进中)"
        else:
            pnl_label = "保本附近"
            defense_plan = "持有 TP123 + 10%硬止损"

        stop_px = self._shield_stop_price(entry)
        if should_radar or radar_active:
            shield_status = "已撤销/不适用 (雷达接管)"
        elif shield_ok:
            shield_status = f"已挂 @ {stop_px:.2f}" if stop_px else "已核实"
        else:
            shield_status = (
                f"待补挂 @ {stop_px:.2f}" if stop_px
                else shield_audit.get("status", "missing")
            )

        tv_side = self.last_tv_side or "?"
        tv_match = (pos.get("side") == tv_side)
        qty_saved = self._safe_qty(self.watched_qty or 0)
        qty_match = qty_saved <= 0 or not self._is_material_qty_change(qty_saved, pos["size"])

        return {
            "pnl_label": pnl_label,
            "defense_plan": defense_plan,
            "favorable_pct": favorable,
            "adverse_pct": adverse,
            "radar_progress": radar_progress,
            "radar_active": radar_active,
            "should_radar": should_radar,
            "shield_ok": shield_ok,
            "shield_status": shield_status,
            "shield_audit": shield_audit,
            "tp_matched": tp_audit.get("matched_full", 0),
            "tp_expected": tp_audit.get("expected", 0),
            "tv_match": tv_match,
            "qty_match": qty_match,
        }

    def _apply_recover_defense_policy(self, real_amt, curr_px, health):
        """重启一次性防线：浮亏→核实/补挂10%硬止损；浮盈达TP1激活比→撤硬止损+挂雷达保本"""
        actions = []
        if health.get("should_radar") or health.get("radar_active"):
            stop_px = self._shield_stop_price()
            has_shield = bool(
                getattr(self, "shield_active", False)
                or (stop_px and self._has_shield_stop_at_price(stop_px))
            )
            if has_shield:
                n = self._cancel_stop_orders(scope="shield")
                self.shield_active = False
                self.shield_sized_qty = 0.0
                self._shield_arm_notified = False
                self._save_state()
                actions.append(f"撤硬止损{n}笔")
            if not self._is_radar_active():
                self._refresh_radar_state_on_recover(curr_px, self.watched_entry)
            sl = self.current_sl if self._is_radar_active() else None
            if sl and not self._has_trigger_sl_near(sl):
                if self._ensure_radar_sl(real_amt, sl):
                    actions.append(f"雷达止损@{sl:.2f}")
                else:
                    actions.append(f"雷达止损待补@{sl:.2f}")
            elif sl:
                actions.append(f"雷达止损已齐@{sl:.2f}")
        else:
            ok = self._maintain_hard_shield(real_amt, curr_px, force=True)
            stop_px = self._shield_stop_price()
            if ok:
                actions.append(
                    f"10%硬止损已齐@{stop_px:.2f}" if stop_px else "10%硬止损已齐"
                )
            else:
                actions.append(
                    f"10%硬止损待补@{stop_px:.2f}" if stop_px else "10%硬止损待补"
                )
        return actions

    def _reconcile_shield_on_recover(self, live_qty, curr_px):
        if live_qty <= 0 or not self.watched_entry:
            return
        if self._is_radar_active() or (curr_px > 0 and self._should_radar_trail(curr_px)):
            if getattr(self, "shield_active", False):
                self._disarm_shield("重启接管：雷达优先")
            return

        audit = self._audit_shield_orders(live_qty)
        if self._shield_orders_adequate(audit):
            self.shield_active = True
            self._shield_fail_streak = 0
            self.shield_sized_qty = live_qty
            self._shield_arm_notified = True
            logger.info(
                f"🛡️ 重启：盘口 10% 硬止损已齐，跳过重挂"
            )
            self._save_state()
            return

        if audit["status"] == "duplicate":
            purged = self._purge_shield_stop_orders(audit["tier_prices"])
            self._record_shield_maintain(success=False)
            logger.warning(
                f"🛡️ 重启：撤净防护盾叠单 {purged} 笔，宽限期后哨兵按实盘补挂"
            )
            self.shield_active = True
            self._save_state()
            return

        if curr_px > 0 and self._should_activate_shield(curr_px):
            self.shield_active = True
            logger.info(
                "🛡️ 重启：10% 硬止损待补挂（宽限期后哨兵按冷却处理）"
            )
            self._save_state()

    def _disarm_shield(self, reason="", notify=False):
        n = self._cancel_stop_orders(scope="shield")
        had = getattr(self, "shield_active", False) or bool(
            getattr(self, "shield_tiers_consumed", [])
        )
        live_qty = self._resolve_live_qty(self.watched_qty or 0)
        entry = self.watched_entry
        self.shield_active = False
        self.shield_tiers_consumed = []
        self.shield_sized_qty = 0.0
        self._shield_arm_notified = False
        self._save_state()
        if reason and (had or n):
            logger.info(f"🛡️ [硬止损解除] {reason} | 撤销 {n} 笔 {SHIELD_HARD_STOP_PCT:.0%} 止损")
        if notify and n > 0:
            progress = 0.0
            try:
                curr_px = deepcoin_client.get_current_price(self.symbol) or 0
                progress = self._radar_activation_progress(curr_px)
            except Exception:
                curr_px = 0
            self._call_dingtalk(
                dingtalk.report_shield_disarmed,
                side=self.current_side,
                live_qty=live_qty,
                entry=entry,
                cancelled_count=n,
                reason=reason,
                radar_progress=progress,
                verify_note=(
                    f"撤 {n} 笔 {SHIELD_HARD_STOP_PCT:.0%} 硬止损 | "
                    f"{'雷达已激活，专注移动保本' if progress >= 1.0 else f'雷达进度 {progress:.0%}，推升止损防回吐'}"
                ),
            )

    def _place_shield_stops(self, live_qty, entry=None, reason="", force=False):
        entry = float(entry or self.watched_entry or 0)
        live_qty = self._resolve_live_qty(live_qty)
        if live_qty <= 0 or entry <= 0 or not self.current_side:
            return False
        tier_prices = self._shield_tier_prices(entry)
        remaining = self._remaining_shield_tier_indices()
        if not remaining:
            self.shield_active = False
            self._save_state()
            return True

        audit = self._audit_shield_orders(live_qty, entry)
        if self._shield_orders_adequate(audit):
            self.shield_active = True
            self._shield_fail_streak = 0
            if not getattr(self, "shield_sized_qty", 0):
                self.shield_sized_qty = live_qty
            self._save_state()
            return True

        if not self._shield_needs_exchange_action(live_qty, audit) and not force:
            self.shield_active = True
            self.shield_sized_qty = live_qty
            self._save_state()
            return True

        if not self._can_maintain_shield_now(force=force):
            return getattr(self, "shield_active", False)

        if audit["status"] == "duplicate" and not force:
            purged = self._purge_shield_stop_orders(tier_prices)
            self._record_shield_maintain(success=False)
            logger.warning(
                f"🛡️ 防护盾叠单清理：撤 {purged} 笔，冷却后再按实盘 {live_qty} 张 补挂"
            )
            return False

        qty_map = self._shield_quantities_for_remaining(live_qty)
        total_shield_qty = sum(qty_map.get(i, 0) for i in remaining)
        purged = self._purge_shield_stop_orders(tier_prices)
        if purged:
            logger.warning(
                f"🛡️ 撤净旧硬止损 {purged} 笔 → 按实盘 {live_qty} 张 重挂 @ -{SHIELD_HARD_STOP_PCT:.0%}"
            )
            time.sleep(0.6)

        close_side = "sell" if self.current_side == "LONG" else "buy"
        pos_side = "long" if self.current_side == "LONG" else "short"
        placed = 0
        for idx in remaining:
            q = qty_map.get(idx, 0)
            tp = tier_prices[idx]
            if q <= 0:
                continue
            limit_px = tp * (0.9995 if close_side == "sell" else 1.0005)
            res = deepcoin_client.place_trigger_order(
                self.symbol, close_side, pos_side, q, tp,
                order_type="limit", price=limit_px,
                td_mode="cross", mrg_position="merge",
            )
            if res and str(res.get("code", "0")) in ("0", "00000", ""):
                placed += 1
                logger.info(
                    f"🛡️ 硬止损 -{SHIELD_HARD_STOP_PCT:.0%}: "
                    f"{q} 张 @ {tp:.2f} 全平 (实盘 {live_qty} 张)"
                )
            time.sleep(0.35)

        post_audit = self._audit_shield_orders(live_qty, entry)
        ok = self._shield_orders_adequate(post_audit)
        self._record_shield_maintain(success=ok)
        if ok:
            self.shield_active = True
            self.shield_sized_qty = live_qty
            self._save_state()
            stop_px = tier_prices[0] if tier_prices else entry
            logger.warning(
                f"🛡️ [10%硬止损] 已挂 | {live_qty} 张 @ {stop_px:.2f} | "
                f"新挂 {placed} 笔 | 雷达激活后自动撤销"
            )
            if not getattr(self, "_shield_arm_notified", False):
                self._shield_arm_notified = True
                self._call_dingtalk(
                    dingtalk.report_adverse_shield_armed,
                    side=self.current_side,
                    entry=entry,
                    live_qty=live_qty,
                    adverse_pct=SHIELD_HARD_STOP_PCT,
                    tier_prices=[stop_px],
                    tier_pcts=SHIELD_TIER_PCTS,
                    verify_note=(
                        (reason or f"开仓价 ±{SHIELD_HARD_STOP_PCT:.0%} 硬止损全平")
                        + f" | 实盘 {live_qty} 张 @ {stop_px:.2f} | 仅播报一次"
                    ),
                )
        elif placed > 0:
            dingtalk.report_system_alert(
                "10%硬止损未对齐",
                f"已撤旧单 {purged} 笔、新挂 {placed} 笔，但核实未通过 | "
                f"实盘 {live_qty} 张 | {', '.join(post_audit.get('issues', []))}",
                suggestion="系统已退避冷却，下轮自动重试；请勿手动重复挂",
            )
        return ok

    def _maintain_hard_shield(self, real_amt, curr_px=None, force=False):
        """雷达未激活：维护开单时的 10% 硬止损（先查实盘，只挂一次）"""
        if real_amt <= 0 or not self.watched_entry:
            return False
        if not force and (
            self._is_radar_active()
            or (curr_px and curr_px > 0 and self._should_radar_trail(curr_px))
        ):
            return False

        live_qty = self._resolve_live_qty(real_amt)
        audit = self._audit_shield_orders(live_qty)

        if self._shield_orders_adequate(audit):
            self.shield_active = True
            self._shield_fail_streak = 0
            if not getattr(self, "shield_sized_qty", 0):
                self.shield_sized_qty = live_qty
            self._save_state()
            return True

        if not force and not self._shield_needs_exchange_action(live_qty, audit):
            self.shield_active = True
            self.shield_sized_qty = live_qty
            self._save_state()
            return True

        if not self._can_maintain_shield_now(force=force):
            return getattr(self, "shield_active", False)

        if audit["status"] == "duplicate" and not force:
            purged = self._purge_shield_stop_orders(audit["tier_prices"])
            self._record_shield_maintain(success=False)
            logger.warning(
                f"🛡️ 硬止损叠单清理：撤 {purged} 笔，冷却后再按实盘 {live_qty} 张 补挂"
            )
            return False

        return self._place_shield_stops(
            live_qty,
            reason=f"维护 {SHIELD_HARD_STOP_PCT:.0%} 硬止损全平",
            force=force,
        )

    def _process_adverse_shield(self, real_amt, curr_px):
        """兼容旧调用 → 维护硬止损"""
        return self._maintain_hard_shield(real_amt, curr_px)

    def _is_radar_active(self):
        if not self.watched_entry or not self.current_sl:
            return False
        if self.current_side == "LONG":
            return self.current_sl > self.watched_entry
        if self.current_side == "SHORT":
            return self.current_sl < self.watched_entry
        return False

    def _radar_sl_to_pass(self):
        return self.current_sl if self._is_radar_active() else None

    def _audit_requires_nuclear(self, audit):
        expected = audit.get("expected", 0)
        if expected <= 0:
            return False
        if audit.get("matched_full", 0) >= expected and not audit.get("orphans"):
            return False
        orders = self._collect_tp_limit_orders()
        if len(orders) > expected:
            return True
        if audit.get("matched_full", 0) == 0 and audit.get("issues"):
            return True
        bad = [lv for lv in audit.get("levels", []) if lv.get("status") in ("duplicate", "qty_mismatch")]
        if bad:
            return True
        missing = sum(1 for lv in audit.get("levels", []) if lv.get("status") == "missing")
        if missing >= 1:
            return True
        if audit.get("orphans"):
            return True
        return False

    def _cancel_all_tp_limit_orders(self, max_rounds=4):
        total = 0
        for round_i in range(max_rounds):
            orders = [
                o for o in deepcoin_client.get_pending_orders(self.symbol)
                if self._is_tp_limit_order(o)
            ]
            if not orders:
                break
            for o in orders:
                oid = o.get("ordId")
                if oid:
                    deepcoin_client.cancel_order(self.symbol, ord_id=oid)
                    total += 1
                    time.sleep(0.12)
            logger.info(f"🧹 撤限价止盈 第{round_i + 1}轮: {len(orders)} 张")
            time.sleep(0.6)
        if total:
            logger.info(f"🧹 已撤销限价止盈合计 {total} 张")
        return total

    def _scorched_earth_cancel_for_recover(self):
        for attempt in range(6):
            deepcoin_client.cancel_all_open_orders(self.symbol)
            time.sleep(0.8)
            self._cancel_all_tp_limit_orders(max_rounds=4)
            time.sleep(0.6)
            remaining = self._collect_tp_limit_orders()
            if not remaining:
                logger.info(f"☢️ 重启撤单完成，限价止盈已清零 (第 {attempt + 1} 轮)")
                return True
            remain_txt = ", ".join(f"{o['qty']}@{o['price']}" for o in remaining[:4])
            logger.warning(
                f"⚠️ 撤单后仍剩 {len(remaining)} 张限价止盈 ({remain_txt}) "
                f"→ 重试 {attempt + 1}/6"
            )
        logger.error("❌ 重启撤单未净：重复 TP 可能残留，非权限问题时请 APP 手动全撤后重启")
        return False

    def _ensure_radar_sl(self, live_qty, sl_price):
        if not sl_price:
            return False
        if self._has_trigger_sl_near(sl_price):
            return True
        self._place_radar_sl(live_qty, sl_price)
        time.sleep(0.35)
        return self._has_trigger_sl_near(sl_price)

    def _refresh_radar_state_on_recover(self, curr_px, entry):
        """重启：按现价恢复 best_price / 雷达激活 / 追踪止损位"""
        if curr_px <= 0 or not entry:
            return
        fee_buffer = entry * 0.0015
        trail_offset = self.current_atr * self.regime_settings[self.regime]["trail_offset"]

        if self.best_price == 0.0:
            self.best_price = entry
        if self.current_side == "LONG":
            self.best_price = max(self.best_price, curr_px)
        else:
            self.best_price = min(self.best_price, curr_px)

        progress = self._radar_activation_progress(curr_px)
        if progress >= 1.0:
            if self.current_side == "LONG":
                breakeven_floor = entry + fee_buffer
                trail_sl = max(round(self.best_price - trail_offset, 2), breakeven_floor)
                if not self._is_radar_active() or trail_sl > self.current_sl:
                    self.current_sl = max(self.current_sl or entry, trail_sl)
            else:
                breakeven_floor = entry - fee_buffer
                trail_sl = min(round(self.best_price + trail_offset, 2), breakeven_floor)
                if not self._is_radar_active() or trail_sl < self.current_sl:
                    self.current_sl = min(self.current_sl or entry, trail_sl)
            logger.info(
                f"📡 重启雷达恢复: 进度 {progress:.0%} | best={self.best_price:.2f} | "
                f"SL={self.current_sl:.2f}"
            )
        elif self.current_sl == 0.0:
            self.current_sl = entry

    def _nuclear_realign_tp(self, live_qty, entry, dynamic_sl=None, rounds=3):
        sl_preserve = dynamic_sl is not None
        last_audit = self._audit_tp_levels(live_qty)
        for r in range(rounds):
            logger.warning(
                f"☢️ 核武级止盈清场重挂 {r + 1}/{rounds} | 持仓 {live_qty}张 | "
                f"当前 {last_audit['matched_full']}/{last_audit['expected']} | "
                f"{self._format_audit_summary(last_audit)}"
            )
            if sl_preserve:
                self._cancel_all_tp_limit_orders()
            else:
                deepcoin_client.cancel_all_open_orders(self.symbol)
            time.sleep(1.0)
            tp_sl = None if sl_preserve else dynamic_sl
            placed = self._rebuild_defenses(live_qty, entry, dynamic_sl=tp_sl)
            logger.info(f"☢️ 核武轮 {r + 1} 新挂 {placed} 笔限价止盈")
            if sl_preserve:
                time.sleep(0.6)
                self._ensure_radar_sl(live_qty, dynamic_sl)
            time.sleep(1.0)
            last_audit = self._audit_tp_levels(live_qty)
            if self._defenses_fully_ok(live_qty, dynamic_sl):
                logger.info(f"☢️ 核武重挂成功: {self._format_audit_summary(last_audit)}")
                return last_audit
            logger.warning(
                f"☢️ 核武轮 {r + 1} 仍未对齐: {self._format_audit_summary(last_audit)}"
            )
            time.sleep(1.5)
        return last_audit

    def _tp_audit_ok(self, audit):
        expected = audit.get("expected", 0)
        if expected <= 0:
            return True
        return (
            audit.get("matched_full", 0) >= expected
            and not audit.get("orphans")
            and not self._defense_needs_immediate_fix(audit)
        )

    def _mark_defense_align_ok(self):
        self._last_defense_align_ok_ts = time.time()
        self._guardian_bad_streak = 0

    def _defense_needs_immediate_fix(self, audit):
        if self._audit_requires_nuclear(audit):
            return True
        for lv in audit.get("levels", []):
            if lv.get("status") in ("duplicate", "missing", "qty_mismatch"):
                return True
        return bool(audit.get("issues") or audit.get("orphans"))

    def _enforce_defense_alignment(self, live_qty, entry, dynamic_sl=None, reason="", rounds=3,
                                   recover_mode=False):
        live_qty = self._resolve_live_qty(live_qty)
        if live_qty <= 0:
            audit = self._audit_tp_levels(live_qty)
            return {
                "matched": 0, "expected": audit.get("expected", 0),
                "pending_prices": [], "rebuilt": False, "audit": audit, "nuclear": False,
            }
        if reason:
            logger.info(f"🛡️ 防线对齐: {reason} | 持仓 {live_qty}张")

        self._defense_align_in_progress = True
        try:
            audit = self._audit_tp_levels(live_qty)

            if recover_mode and self._tp_audit_ok(audit):
                logger.info(
                    f"✅ 重启接管：盘口 TP 已齐，跳过核武撤挂 | "
                    f"{self._format_audit_summary(audit)}"
                )
                if dynamic_sl and not self._has_trigger_sl_near(dynamic_sl):
                    self._ensure_radar_sl(live_qty, dynamic_sl)
                self._mark_defense_align_ok()
                return {
                    "matched": audit["matched_full"],
                    "expected": audit["expected"],
                    "pending_prices": audit["pending_prices"],
                    "rebuilt": False,
                    "audit": audit,
                    "nuclear": False,
                }

            if recover_mode and self._defense_needs_immediate_fix(audit):
                repaired, n_actions = self._surgical_repair_tp_defenses(live_qty, entry)
                audit = repaired
                if self._tp_audit_ok(audit):
                    logger.info(
                        f"✅ 重启智能修复成功 ({n_actions} 步)，无需核武 | "
                        f"{self._format_audit_summary(audit)}"
                    )
                    if dynamic_sl and not self._has_trigger_sl_near(dynamic_sl):
                        self._ensure_radar_sl(live_qty, dynamic_sl)
                    self._mark_defense_align_ok()
                    return {
                        "matched": audit["matched_full"],
                        "expected": audit["expected"],
                        "pending_prices": audit["pending_prices"],
                        "rebuilt": n_actions > 0,
                        "audit": audit,
                        "nuclear": False,
                    }
                logger.warning(
                    f"⚠️ 重启智能修复后仍不齐 ({n_actions} 步) → 升级核武 | "
                    f"{self._format_audit_summary(audit)}"
                )

            if not recover_mode and self._tp_audit_ok(audit):
                logger.info(f"✅ TP 已齐，跳过撤单: {self._format_audit_summary(audit)}")
                if dynamic_sl and not self._has_trigger_sl_near(dynamic_sl):
                    self._ensure_radar_sl(live_qty, dynamic_sl)
                self._mark_defense_align_ok()
                return {
                    "matched": audit["matched_full"],
                    "expected": audit["expected"],
                    "pending_prices": audit["pending_prices"],
                    "rebuilt": False,
                    "audit": audit,
                    "nuclear": False,
                }

            if recover_mode:
                self._scorched_earth_cancel_for_recover()
            else:
                self._cancel_all_tp_limit_orders()
            time.sleep(0.45)
            audit = self._audit_tp_levels(live_qty)
            if self._tp_audit_ok(audit):
                logger.info(f"✅ 撤单后 TP 已齐: {self._format_audit_summary(audit)}")
                if dynamic_sl and not self._has_trigger_sl_near(dynamic_sl):
                    self._ensure_radar_sl(live_qty, dynamic_sl)
                self._mark_defense_align_ok()
                return {
                    "matched": audit["matched_full"],
                    "expected": audit["expected"],
                    "pending_prices": audit["pending_prices"],
                    "rebuilt": False,
                    "audit": audit,
                    "nuclear": False,
                }

            sl_preserve = dynamic_sl if (dynamic_sl and self._is_radar_active() and not recover_mode) else None
            audit = self._nuclear_realign_tp(
                live_qty, entry, dynamic_sl=sl_preserve, rounds=rounds,
            )
            if audit["matched_full"] < audit["expected"]:
                logger.warning("☢️ 首轮核武未齐，追加一轮重挂")
                if recover_mode:
                    self._scorched_earth_cancel_for_recover()
                else:
                    self._cancel_all_tp_limit_orders(max_rounds=4)
                time.sleep(0.6)
                audit = self._nuclear_realign_tp(
                    live_qty, entry, dynamic_sl=sl_preserve, rounds=max(2, rounds - 1),
                )
            if dynamic_sl and not recover_mode and not self._has_trigger_sl_near(dynamic_sl):
                self._ensure_radar_sl(live_qty, dynamic_sl)
            if self._tp_audit_ok(audit):
                self._mark_defense_align_ok()
            return {
                "matched": audit["matched_full"],
                "expected": audit["expected"],
                "pending_prices": audit["pending_prices"],
                "rebuilt": True,
                "audit": audit,
                "nuclear": True,
            }
        finally:
            self._defense_align_in_progress = False

    def _radar_guardian_audit(self, real_amt, curr_px):
        if real_amt <= 0 or not self.monitoring:
            return None
        if getattr(self, "_recover_in_progress", False):
            return None
        if getattr(self, "_open_in_progress", False):
            return None
        if getattr(self, "_defense_align_in_progress", False):
            return None

        cap = self._radar_enforce_regime_cap(real_amt, curr_px)
        if cap:
            real_amt = cap["new_qty"]
            if self._tp_audit_ok(cap["result"]["audit"]):
                return cap

        audit = self._audit_tp_levels(real_amt)
        sl = self._radar_sl_to_pass()

        if self._tp_audit_ok(audit):
            self._guardian_bad_streak = 0
            if sl and not self._has_trigger_sl_near(sl):
                self._ensure_radar_sl(real_amt, sl)
            return None

        self._guardian_bad_streak += 1
        now = time.time()
        severe = self._defense_needs_immediate_fix(audit)
        in_grace = now < getattr(self, "_sentinel_grace_until", 0)
        in_cooldown = (
            now - getattr(self, "_last_defense_align_ok_ts", 0)
            < DEFENSE_ALIGN_COOLDOWN_SEC
        )
        if (in_grace or in_cooldown) and not severe and self._guardian_bad_streak < 2:
            logger.info(
                f"📡 [雷达守护] TP 审计波动，暂不重挂 "
                f"({'重启宽限期' if in_grace else '冷却期'}) | "
                f"{self._format_audit_summary(audit)}"
            )
            return None

        logger.warning(
            f"📡 [雷达守护] TP 未对齐 → 撤单重算重挂 | "
            f"{self._format_audit_summary(audit)}"
        )
        sl_preserve = sl if self._is_radar_active() else None
        result = self._enforce_defense_alignment(
            real_amt, self.watched_entry, dynamic_sl=sl_preserve,
            reason="雷达守护实时纠偏", rounds=3,
        )
        new_audit = result["audit"]
        if new_audit["matched_full"] < new_audit["expected"]:
            self._call_dingtalk(
                dingtalk.report_system_alert,
                title="雷达守护：止盈仍未对齐",
                detail=(
                    f"{self.current_side} {real_amt}张 | "
                    f"{self._format_audit_summary(new_audit)} | 请人工核查 Deepcoin 挂单"
                ),
            )
        elif self._defense_needs_immediate_fix(audit):
            logger.info(
                f"📡 [雷达守护] 纠偏完成: "
                f"{new_audit['matched_full']}/{new_audit['expected']} | "
                f"{self._format_audit_summary(new_audit)}"
            )
            if getattr(self, "_recover_tp_unconfirmed", False):
                self._recover_tp_unconfirmed = False
                self._call_dingtalk(
                    dingtalk.report_radar_guardian_realigned,
                    side=self.current_side,
                    qty=real_amt,
                    tp_audit=new_audit,
                    verify_note=(
                        f"重启接管竞态后雷达已纠偏 | "
                        f"{new_audit['matched_full']}/{new_audit['expected']} | "
                        f"{self._format_audit_summary(new_audit)}"
                    ),
                )
            elif getattr(self, "_open_tp_unconfirmed", False):
                self._open_tp_unconfirmed = False
                self._call_dingtalk(
                    dingtalk.report_radar_guardian_realigned,
                    side=self.current_side,
                    qty=real_amt,
                    tp_audit=new_audit,
                    verify_note=(
                        f"开仓后雷达已纠偏 | "
                        f"{new_audit['matched_full']}/{new_audit['expected']} | "
                        f"{self._format_audit_summary(new_audit)}"
                    ),
                )
        return result

    def _full_rebuild_tp_loop(self, live_qty, entry, dynamic_sl=None):
        result = self._enforce_defense_alignment(
            live_qty, entry, dynamic_sl=dynamic_sl, reason="全量重建", rounds=3,
        )
        audit = result["audit"]
        return audit["matched_full"], audit["pending_prices"], audit["expected"]

    def _smart_realign_defenses(self, live_qty, entry, dynamic_sl=None, reason=""):
        return self._enforce_defense_alignment(
            live_qty, entry, dynamic_sl=dynamic_sl, reason=reason or "智能防线对齐", rounds=3,
        )

    def _place_radar_sl(self, live_qty, sl_price):
        close_side = "sell" if self.current_side == "LONG" else "buy"
        pos_side = "long" if self.current_side == "LONG" else "short"
        sl_qty = self._resolve_live_qty(live_qty)
        deepcoin_client.place_trigger_order(
            self.symbol, close_side, pos_side, sl_qty, sl_price,
            order_type="market", td_mode="cross", mrg_position="merge",
        )

    def _has_tp_limit_at_price(self, price, tolerance=1.0):
        if price <= 0:
            return False
        for o in self._collect_tp_limit_orders():
            if abs(o["price"] - price) <= tolerance:
                return True
        return False

    def _detect_tp_fills(self, old_qty, new_qty):
        """根据张数变化 + 盘口 TP 单消失推断成交档位"""
        if new_qty >= old_qty:
            return []
        ratios = self.regime_settings[self._tp_split_regime()]["ratios"]
        o1, o2, o3 = self._calculate_tp_quantities(old_qty, ratios)
        fills = []
        budget = old_qty - new_qty
        for level, tp_px, slice_qty in (
            (1, self.tv_tps[0], o1),
            (2, self.tv_tps[1], o2),
            (3, self.tv_tps[2], o3),
        ):
            if tp_px <= 0 or slice_qty <= 0 or budget <= 0:
                continue
            order_gone = not self._has_tp_limit_at_price(tp_px)
            if order_gone and budget >= max(1, slice_qty - 1):
                fill_qty = min(budget, slice_qty)
                fills.append({"level": level, "price": tp_px, "qty": fill_qty})
                budget -= fill_qty
        return fills

    def _detect_shield_fills(self, old_qty, new_qty, curr_px):
        if not getattr(self, "shield_active", False):
            return []
        if new_qty >= old_qty:
            return []
        stop_px = self._shield_stop_price()
        if not stop_px:
            return []
        if self._has_shield_stop_at_price(stop_px):
            return []
        fill_qty = old_qty - new_qty
        if fill_qty <= 0:
            return []
        return [{
            "tier": 1,
            "pct": SHIELD_HARD_STOP_PCT,
            "price": stop_px,
            "qty": fill_qty,
        }]

    def _classify_position_change(self, old_qty, new_qty, curr_px):
        if new_qty > old_qty:
            return {"kind": "add", "tp_fills": [], "shield_fills": []}
        if new_qty >= old_qty:
            return {"kind": "unchanged", "tp_fills": [], "shield_fills": []}
        tp_fills = self._detect_tp_fills(old_qty, new_qty)
        shield_fills = self._detect_shield_fills(old_qty, new_qty, curr_px)
        favorable = (
            self._is_radar_active()
            or (curr_px > 0 and self._should_radar_trail(curr_px))
        )
        if tp_fills and shield_fills and favorable:
            shield_fills = []
        if tp_fills:
            return {"kind": "tp_fill", "tp_fills": tp_fills, "shield_fills": []}
        if shield_fills:
            return {"kind": "shield_fill", "tp_fills": [], "shield_fills": shield_fills}
        return {"kind": "reduce_unknown", "tp_fills": [], "shield_fills": []}

    def _advance_radar_on_tp_fill(self, tp_fills, curr_px, live_qty):
        if not tp_fills:
            return None
        for f in tp_fills:
            px = f["price"]
            if self.current_side == "LONG":
                self.best_price = max(self.best_price, px, curr_px or 0)
            else:
                bp = curr_px if curr_px and curr_px > 0 else px
                self.best_price = min(self.best_price, px, bp)
        max_level = max(f["level"] for f in tp_fills)
        tp3 = self.tv_tps[2] if len(self.tv_tps) > 2 else 0.0
        new_sl = self._compute_radar_sl()
        if new_sl is not None:
            fee_buffer = self.watched_entry * 0.0015
            if self.current_side == "LONG":
                floor = self.watched_entry + fee_buffer
                self.current_sl = max(self.current_sl or floor, new_sl, floor)
            else:
                ceiling = self.watched_entry - fee_buffer
                self.current_sl = min(self.current_sl or ceiling, new_sl, ceiling)
        note = f"TP{max_level}成交"
        if max_level >= 2 and tp3 > 0:
            note += f" → 雷达止损向 TP3({tp3:.2f}) 动态收紧"
        logger.info(
            f"📈 [雷达推进] {note} | SL={self.current_sl:.2f} | best={self.best_price:.2f}"
        )
        self._save_state()
        return self.current_sl if self._is_radar_active() else None

    def _handle_smart_qty_change(self, old_qty, new_qty, curr_px):
        change = self._classify_position_change(old_qty, new_qty, curr_px)
        kind = change["kind"]
        result = None
        sl_to_pass = None

        if kind == "add":
            sl_to_pass = self._radar_sl_to_pass()
            result = self._smart_realign_defenses(
                new_qty, self.watched_entry, dynamic_sl=sl_to_pass,
                reason="加仓后防线对齐",
            )
            if self._should_activate_shield(curr_px):
                self._maintain_hard_shield(new_qty, curr_px, force=True)
        elif kind == "tp_fill":
            levels = ",".join(f"TP{f['level']}" for f in change["tp_fills"])
            self._disarm_shield(f"{levels} 成交，切换雷达追踪", notify=True)
            sl_to_pass = self._advance_radar_on_tp_fill(
                change["tp_fills"], curr_px, new_qty,
            )
            result = self._smart_realign_defenses(
                new_qty, self.watched_entry, dynamic_sl=sl_to_pass,
                reason=f"{levels} 成交智能对齐",
            )
            if sl_to_pass and not self._has_trigger_sl_near(sl_to_pass):
                self._ensure_radar_sl(new_qty, sl_to_pass)
        elif kind == "shield_fill":
            f = change["shield_fills"][0]
            logger.warning(
                f"🛡️ [智慧大脑] {SHIELD_HARD_STOP_PCT:.0%}硬止损成交 "
                f"{old_qty} ➔ {new_qty} @ {f['price']:.2f}"
            )
            self._disarm_shield("10%硬止损成交", notify=True)
            self.shield_tiers_consumed = []
            result = self._smart_realign_defenses(
                new_qty, self.watched_entry, dynamic_sl=None,
                reason="硬止损成交后 TP 重算",
            )
            self._call_dingtalk(
                dingtalk.report_shield_tier_fill,
                side=self.current_side,
                tier_pct=f["pct"],
                tier_price=f["price"],
                filled_qty=f["qty"],
                remain_qty=new_qty,
                entry_px=self.watched_entry,
                remaining_tiers=[],
                verify_note=(
                    f"硬止损 -{f['pct']:.0%} @ {f['price']:.2f} 成交 | "
                    f"剩余 {new_qty} 张"
                ),
            )
        else:
            self._bump_best_on_tp_fill(old_qty, new_qty, curr_px)
            self._sync_radar_sl_from_best(curr_px)
            sl_to_pass = self._radar_sl_to_pass()
            result = self._smart_realign_defenses(
                new_qty, self.watched_entry, dynamic_sl=sl_to_pass,
                reason="人工异动重对齐",
            )
            if self._should_disarm_shield_for_favorable(curr_px):
                self._disarm_shield("行情转有利，切换雷达保本", notify=True)
            elif self._should_activate_shield(curr_px) or getattr(self, "shield_active", False):
                self._maintain_hard_shield(new_qty, curr_px, force=True)

        self._save_state()
        return change, result

    def _report_qty_change_dingtalk(self, old_qty, new_qty, realign_result):
        """TP 成交 / 减仓：REST 重试核查后必达钉钉"""
        verified_pos = self._wait_verify(
            lambda: self._verify_position(self.current_side),
            retries=8,
            delay=0.5,
        )
        verified = (
            verified_pos is not None
            and self._safe_qty(verified_pos.get("size", 0)) == new_qty
        )
        entry_px = (
            float(verified_pos.get("entry_price", self.watched_entry))
            if verified_pos else self.watched_entry
        )
        verify_note = (
            f"核实 {new_qty}张 @ {entry_px:.2f} | "
            f"止盈 {realign_result['matched']}/{realign_result['expected']} 档 | "
            f"{self._format_audit_summary(realign_result['audit'])}"
        )
        if not verified:
            verify_note += f" | {dingtalk.VERIFY_DELAY_MARK}"

        fills = self._detect_tp_fills(old_qty, new_qty)
        if fills:
            for fill in fills:
                self._call_dingtalk(
                    dingtalk.report_tp_fill,
                    tp_level=fill["level"],
                    tp_price=fill["price"],
                    filled_qty=fill["qty"],
                    remain_qty=new_qty,
                    entry_px=entry_px,
                    side=self.current_side or "?",
                    regime=self.regime,
                    verify_note=verify_note,
                    verified=verified,
                )
                logger.info(
                    f"📣 TP{fill['level']} 成交钉钉已推送 @ {fill['price']:.2f} "
                    f"({fill['qty']}张)"
                )
        else:
            action_msg = (
                "手动加仓" if new_qty > old_qty else "部分止盈吃单 / 手动减仓"
            )
            self._call_dingtalk(
                dingtalk.report_manual_position_change,
                action_type=action_msg,
                old_qty=old_qty,
                new_qty=new_qty,
                new_entry_price=entry_px,
                verify_note=verify_note,
                tp_audit=realign_result["audit"],
                verified=verified,
            )

        if realign_result["expected"] > 0 and realign_result["matched"] < realign_result["expected"]:
            dingtalk.report_system_alert(
                "人工异动后止盈未对齐",
                f"{self._format_audit_summary(realign_result['audit'])}",
            )

    def _report_radar_intervention(self, real_amt, new_sl, action_msg, sl_placed=True):
        """雷达推止损：同价位冷却期内不重复播报"""
        now = time.time()
        if (
            abs(new_sl - getattr(self, "_last_radar_report_sl", 0)) < 2.0
            and now - getattr(self, "_last_radar_report_ts", 0) < RADAR_DINGTALK_COOLDOWN_SEC
        ):
            return
        verified = self._wait_verify(
            lambda: self._has_trigger_sl_near(new_sl),
            retries=8,
            delay=0.5,
        )
        base_note = (
            f"条件止损 @ {new_sl:.2f} | 持仓 {real_amt}张 | 轮询 {SENTINEL_POLL_RADAR}s"
        )
        if not sl_placed and not verified:
            logger.warning(f"雷达钉钉：止损 @ {new_sl:.2f} 提交失败且盘口未核查到")
            return
        if verified:
            verify_note = base_note
        else:
            verify_note = f"{base_note} | {dingtalk.VERIFY_DELAY_MARK}"
            logger.info(f"雷达钉钉：止损已挂 REST 延迟，仍推送 @{new_sl:.2f}")
        self._call_dingtalk(
            dingtalk.report_intervention,
            qty=real_amt,
            entry_px=self.watched_entry,
            new_sl=new_sl,
            action_msg=action_msg,
            verify_note=verify_note,
            verified=verified,
        )
        self._last_radar_report_ts = now
        self._last_radar_report_sl = new_sl

    def _realign_radar_defenses(self, live_qty, entry, new_sl):
        """雷达推升：TP 异常才核武；止损单独换"""
        self._cancel_stop_orders(scope="radar")
        time.sleep(0.35)
        audit = self._audit_tp_levels(live_qty)
        if self._defense_needs_immediate_fix(audit):
            self._enforce_defense_alignment(
                live_qty, entry, dynamic_sl=new_sl,
                reason="雷达推升前 TP 纠偏", rounds=2,
            )
        sl_placed = self._ensure_radar_sl(live_qty, new_sl)
        if not sl_placed:
            self._place_radar_sl(live_qty, new_sl)
            time.sleep(0.35)
            sl_placed = self._has_trigger_sl_near(new_sl)
        time.sleep(0.4)
        return sl_placed

    def _wait_tp_hung(self, tp_pxs, live_qty=None, retries=5, delay=0.8):
        expected = self._expected_tp_count(tp_pxs)
        matched, pending = 0, []
        for _ in range(retries):
            if live_qty is not None and live_qty > 0:
                audit = self._audit_tp_levels(live_qty)
                matched = audit["matched_full"]
                pending = audit["pending_prices"]
            else:
                matched, pending = self._count_matched_tp_orders(tp_pxs)
            if expected == 0 or matched >= expected:
                return matched, pending
            time.sleep(delay)
        return matched, pending

    def _wait_defense_settled(self, live_qty, dynamic_sl=None, retries=8, delay=0.75):
        sl = dynamic_sl if dynamic_sl is not None else self._radar_sl_to_pass()
        last = self._audit_tp_levels(live_qty)
        for i in range(retries):
            if not self._defense_needs_immediate_fix(last) and self._defenses_fully_ok(live_qty, sl):
                return last
            if i + 1 < retries:
                time.sleep(delay)
                last = self._audit_tp_levels(live_qty)
        return last

    def _has_trigger_sl_near(self, sl_price, tolerance=2.0):
        for t in deepcoin_client.get_trigger_orders_pending(self.symbol):
            for key in ("triggerPx", "slTriggerPrice", "triggerPrice"):
                val = t.get(key)
                if val is not None and str(val).strip() not in ("", "0"):
                    try:
                        if abs(float(val) - sl_price) <= tolerance:
                            return True
                    except (TypeError, ValueError):
                        pass
        return False

    def _wait_verify(self, checks_fn, retries=3, delay=0.6):
        for i in range(retries):
            result = checks_fn()
            if result:
                return result
            time.sleep(delay)
        return checks_fn()

    def _calculate_tp_quantities(self, total_qty: int, ratios: list) -> tuple:
        """深币最小 1 张限制 + 余数吸收：qty1+qty2+qty3 恒等于 total_qty"""
        if total_qty <= 0:
            return 0, 0, 0

        qty1 = max(1, round(total_qty * ratios[0]))
        remaining = total_qty - qty1
        if remaining <= 0:
            return qty1, 0, 0

        ratio_sum_23 = ratios[1] + ratios[2]
        if ratio_sum_23 <= 0:
            return qty1, 0, remaining

        qty2 = max(0, round(remaining * (ratios[1] / ratio_sum_23)))
        qty3 = remaining - qty2
        if qty3 < 0:
            qty3, qty2 = 0, remaining

        if qty2 == 0 and remaining >= 2:
            qty2, qty3 = 1, remaining - 1
        if qty3 == 0 and remaining >= 2 and qty2 > 1:
            qty3, qty2 = 1, remaining - 1

        assert qty1 + qty2 + qty3 == total_qty, f"TP 分档不守恒: {qty1}+{qty2}+{qty3}!={total_qty}"
        return qty1, qty2, qty3

    def _resolve_live_qty(self, fallback_qty: int) -> int:
        """挂 reduceOnly 前重新读取交易所落账张数，避免冻结/部分成交导致数量漂移"""
        pos = self._get_active_position()
        if pos and self._safe_qty(pos.get("size")) > 0:
            live = self._safe_qty(pos["size"])
            if live != fallback_qty:
                logger.info(f"📐 实盘张数校正: 账本 {fallback_qty} → 交易所 {live}")
            return live
        return fallback_qty

    def handle_signal(self, payload):
        """兼容旧调用路径"""
        self.enqueue_signal(payload)

    def _safe_float(self, val, default=0.0):
        try:
            if val is None or val == "":
                return default
            return float(val)
        except (TypeError, ValueError):
            return default

    def _safe_int(self, val, default=3):
        try:
            if val is None or val == "":
                return default
            return int(float(val))
        except (TypeError, ValueError):
            return default

    def _process_signal(self, payload):
        raw_action = str(payload.get("action", "")).strip().upper()
        self.regime = self._safe_int(payload.get("regime"), 3)
        if self.regime not in self.regime_settings:
            self.regime = 3

        self.current_atr = self._safe_float(payload.get("atr"), 30.0)
        self.tv_price = self._safe_float(payload.get("price"), 0.0)
        self.tv_tps = self._sanitize_tp_prices([
            self._safe_float(payload.get("tv_tp1"), 0),
            self._safe_float(payload.get("tv_tp2"), 0),
            self._safe_float(payload.get("tv_tp3"), 0),
        ])
        close_reason = str(payload.get("reason") or "策略指标反转/波动率安全退出").strip()
        close_side = str(payload.get("side") or "").strip().upper()
        pnl_pct = payload.get("pnl_pct")

        if not raw_action:
            logger.warning("TV 信号缺少 action，已忽略")
            return
        if raw_action in ("LONG", "SHORT", "CLOSE", "CLOSE_PROTECT", "CLOSE_TP3") or \
                raw_action.startswith("CLOSE"):
            self._record_tv_signal(payload, raw_action)

        if not self._lock.acquire(timeout=120.0):
            logger.error(f"⏱️ 锁等待 120s 超时，信号 {raw_action} 重新入队")
            self._signal_queue.put(payload)
            return

        try:
            is_close = (
                raw_action in ("CLOSE", "CLOSE_PROTECT", "CLOSE_TP3")
                or raw_action.startswith("CLOSE")
            )
            if is_close:
                self.monitoring = False
            if raw_action == "CLOSE_PROTECT" or raw_action.startswith("CLOSE_PROTECT"):
                extra = ""
                if close_side:
                    extra += f" | TV方向 {close_side}"
                if pnl_pct is not None and pnl_pct != "":
                    extra += f" | 近似盈亏 {pnl_pct}%"
                pos = self._get_active_position()
                if not pos or self._safe_qty(pos.get("size", 0)) <= 0:
                    logger.info(f"🛡️ 保护性全平到达但盘口已空仓 → 撤单复位 | {close_reason}{extra}")
                    self._handle_manual_flat_detected(
                        f"🛡️ TV保护性全平（盘口已空）: {close_reason}{extra}"
                    )
                else:
                    self._close_all(f"🛡️ 保护性全平：{close_reason}{extra}")
            elif raw_action == "CLOSE_TP3":
                self._close_all("🎯 完美胜利：大趋势吃满，TP3 终极收网")
            elif raw_action == "CLOSE":
                self._close_all(f"🧹 换防清场：{close_reason}")
            elif raw_action in ["LONG", "SHORT"]:
                self.last_tv_side = raw_action
                self._save_state()
                self._handle_smart_entry(raw_action)
            else:
                logger.warning(f"未识别的 TV action: {raw_action}")
        finally:
            self._lock.release()

    @staticmethod
    def _pos_side_label(pos):
        return "LONG" if str(pos.get("posSide", "long")).lower() == "long" else "SHORT"

    def _entry_price_diff_pct(self, price_a, price_b, ref_px):
        ref = ref_px or max(abs(price_a), abs(price_b), 1.0)
        return abs(float(price_a) - float(price_b)) / ref * 100.0

    def _is_similar_atr(self, atr_a, atr_b):
        a, b = float(atr_a or 0), float(atr_b or 0)
        if a <= 0 and b <= 0:
            return True
        if a <= 0 or b <= 0:
            return False
        return abs(a - b) / max(a, b) <= ATR_SIMILAR_RATIO

    def _touch_entry_signal_signature(self, action):
        self._last_entry_signal = {
            "action": action,
            "tv_price": self.tv_price,
            "atr": self.current_atr,
            "regime": self.regime,
            "tv_tps": list(self.tv_tps),
            "ts": time.time(),
        }

    def _is_duplicate_flat_entry(self, action, curr_px):
        sig = self._last_entry_signal
        if not sig or sig.get("action") != action:
            return False
        if time.time() - float(sig.get("ts", 0)) > SAME_DIR_DEDUP_SEC:
            return False
        if not self._is_similar_atr(sig.get("atr"), self.current_atr):
            return False
        if int(sig.get("regime", 0)) != int(self.regime):
            return False
        ref_px = curr_px or self.tv_price or sig.get("tv_price") or 1.0
        diff = self._entry_price_diff_pct(sig.get("tv_price", 0), self.tv_price, ref_px)
        return diff < SAME_DIR_MIN_SPREAD_PCT

    def _same_direction_entry_mode(self, action, pos, curr_px):
        """同向智能决策：① ATR → ② 档位 → ③ 理论开仓价差"""
        ref_px = curr_px or self.tv_price or pos["entry_price"]
        live_entry = pos["entry_price"]
        diff_pct = self._entry_price_diff_pct(live_entry, self.tv_price, ref_px)
        open_regime = int(getattr(self, "open_regime", self.regime) or self.regime)
        open_atr = float(getattr(self, "open_atr", self.current_atr) or self.current_atr)
        tv_atr = float(self.current_atr)

        if not self._is_similar_atr(open_atr, tv_atr):
            logger.info(
                f"🔄 同向 [{action}] ATR {open_atr:.2f}→{tv_atr:.2f} 变化 "
                f"(>{ATR_SIMILAR_RATIO:.0%}) → 先平后开重入"
            )
            return "FULL_REENTRY", diff_pct, "atr_changed", open_atr, tv_atr

        if int(self.regime) != open_regime:
            logger.info(
                f"🔄 同向 [{action}] 档位 R{open_regime}→R{self.regime} → 先平后开重入"
            )
            return "FULL_REENTRY", diff_pct, "regime_changed", open_atr, tv_atr

        if diff_pct >= SAME_DIR_MIN_SPREAD_PCT:
            logger.info(
                f"🔄 同向 [{action}] 价差 {diff_pct:.3f}% ≥ {SAME_DIR_MIN_SPREAD_PCT}% → 先平后开"
            )
            return "FULL_REENTRY", diff_pct, "spread_ok", open_atr, tv_atr

        logger.info(
            f"🧠 同向 [{action}] ATR {tv_atr:.2f} 未变 + 价差 {diff_pct:.3f}% "
            f"< {SAME_DIR_MIN_SPREAD_PCT}% → 仅刷新 TP123"
        )
        return "REFRESH_TP", diff_pct, "refresh_tp", open_atr, tv_atr

    def _report_smart_reentry(self, action, pos, diff_pct, reason, open_atr, tv_atr):
        live_entry = pos["entry_price"]
        real_qty = self._safe_qty(pos.get("size"))
        reason_txt = {
            "atr_changed": f"TV ATR `{tv_atr:.2f}` ≠ 持仓 ATR `{open_atr:.2f}` → 刷新仓位",
            "regime_changed": f"档位 R{self.open_regime}→R{self.regime} → 刷新仓位",
            "spread_ok": f"理论价差 {diff_pct:.3f}% ≥ {SAME_DIR_MIN_SPREAD_PCT}% → 刷新仓位",
        }.get(reason, "同向刷新仓位")
        self._call_dingtalk(
            dingtalk.report_smart_same_dir_decision,
            side=action,
            decision=f"reentry_{reason}",
            live_entry=live_entry,
            tv_price=self.tv_price,
            diff_pct=diff_pct,
            threshold_pct=SAME_DIR_MIN_SPREAD_PCT,
            open_regime=self.open_regime,
            tv_regime=self.regime,
            open_atr=open_atr,
            tv_atr=tv_atr,
            qty=real_qty,
            verify_note=(
                f"核实持仓 {real_qty}张 @ {live_entry:.2f} | {reason_txt} | 执行先平后开"
            ),
        )

    def _same_direction_refresh_tp(self, action, pos, curr_px, diff_pct, open_atr, tv_atr):
        live_pos = self._get_active_position()
        if not live_pos or self._safe_qty(live_pos.get("size", 0)) <= 0:
            logger.warning("🧠 同向刷新: 实盘已无持仓，跳过")
            return

        real_qty = self._safe_qty(live_pos["size"])
        entry = live_pos["entry_price"]
        self.current_side = action
        self.watched_qty = real_qty
        self.watched_entry = entry
        self.monitoring = True
        self._save_state()

        sl_to_pass = self._radar_sl_to_pass()
        result = self._smart_realign_defenses(
            real_qty, entry, dynamic_sl=sl_to_pass,
            reason="同向TV智能刷新止盈",
        )
        self._ensure_sentinel_running()

        verify_note = (
            f"核实持仓 {real_qty}张 @ {entry:.2f} | TV理论 {self.tv_price:.2f} | "
            f"持仓ATR {open_atr:.2f} = TV ATR {tv_atr:.2f} | "
            f"价差 {diff_pct:.3f}% (< {SAME_DIR_MIN_SPREAD_PCT}%) | "
            f"止盈 {result['matched']}/{result['expected']} 档 | "
            f"{self._format_audit_summary(result['audit'])}"
        )
        self._call_dingtalk(
            dingtalk.report_smart_same_dir_decision,
            side=action,
            decision="skip_refresh_tp",
            live_entry=entry,
            tv_price=self.tv_price,
            diff_pct=diff_pct,
            threshold_pct=SAME_DIR_MIN_SPREAD_PCT,
            open_regime=self.open_regime,
            tv_regime=self.regime,
            open_atr=open_atr,
            tv_atr=tv_atr,
            qty=real_qty,
            tp_audit=result["audit"],
            verify_note=verify_note,
        )
        logger.info("🧠 同向智能处理完成: ATR未变+价差不足，未再开仓，TP123 已按新 TV 价刷新")

    def _ensure_sentinel_running(self):
        if self.monitoring and not self._sentinel_active:
            threading.Thread(
                target=self._sentinel_loop, daemon=True, name="sentinel",
            ).start()

    def _full_reentry(self, action, close_reason):
        deepcoin_client.cancel_all_open_orders(self.symbol)
        time.sleep(0.5)
        if not self._close_all(close_reason, reset_state=True):
            logger.error("❌ 先平后开中止：平仓未归零，拒绝叠仓开仓")
            dingtalk.report_system_alert(
                "先平后开中止 · 平仓未归零",
                "6 轮强平后盘口仍有持仓，已拒绝新开仓，请人工核查 Deepcoin 盘口",
            )
            return
        if not self._wait_verify(self._verify_flat, retries=8, delay=0.5):
            logger.error("❌ 先平后开中止：空仓核查未通过")
            dingtalk.report_system_alert(
                "先平后开中止 · 空仓核查失败",
                "平仓指令已发但 REST 仍显示持仓，已拒绝叠仓开仓",
            )
            return
        deepcoin_client.cancel_all_open_orders(self.symbol)
        time.sleep(0.5)
        curr_px = deepcoin_client.get_current_price(self.symbol) or self.tv_price
        if curr_px > 0:
            self._open_position(action, curr_px)

    def _handle_manual_flat_detected(self, reason):
        """人工全平 / 止盈吃满：智能复位账本"""
        logger.info(f"📭 感知空仓: {reason}")
        self.monitoring = False
        self.watched_qty = 0
        self.initial_qty = 0
        self.current_side = None
        deepcoin_client.cancel_all_open_orders(self.symbol)
        self._save_state()
        self._report_flat_close(reason or "仓位归零 (人工全平 / 止盈吃满)")

    def _handle_smart_entry(self, action):
        """同向智能筛选；反向一律先平后开"""
        curr_px = deepcoin_client.get_current_price(self.symbol) or self.tv_price
        pos = self._get_active_position()

        if pos and self._safe_qty(pos.get("size", 0)) > 0:
            current_side = self._pos_side_label(pos)
            if current_side != action:
                logger.info(f"⚡ 反方向 [{action}] vs 实盘 [{current_side}] → 先平后开")
                self._full_reentry(action, "反方向指令到达，触发【先平后开】原子对冲换防")
                self._touch_entry_signal_signature(action)
                return

            mode, diff_pct, reason, open_atr, tv_atr = self._same_direction_entry_mode(action, pos, curr_px)
            if mode == "REFRESH_TP":
                self._same_direction_refresh_tp(action, pos, curr_px, diff_pct, open_atr, tv_atr)
                self._touch_entry_signal_signature(action)
                return

            close_msgs = {
                "atr_changed": f"同向 TV ATR 变化 ({open_atr:.2f}→{tv_atr:.2f})，触发【先平后开】刷新仓位",
                "regime_changed": "同向 TV 档位变化，触发【先平后开】重入",
                "spread_ok": f"同向理论价差 {diff_pct:.3f}% 达标，触发【先平后开】重入",
            }
            self._report_smart_reentry(action, pos, diff_pct, reason, open_atr, tv_atr)
            self._full_reentry(action, close_msgs.get(reason, "同方向刷新仓位，触发【先平后开】重入"))
            self._touch_entry_signal_signature(action)
            return

        if self._is_duplicate_flat_entry(action, curr_px):
            ref_px = curr_px or self.tv_price or 1.0
            diff_pct = self._entry_price_diff_pct(
                self._last_entry_signal.get("tv_price", 0), self.tv_price, ref_px,
            )
            logger.info(f"🧠 空仓短时重复同向 TV [{action}] → 忽略开仓")
            self._call_dingtalk(
                dingtalk.report_smart_same_dir_decision,
                side=action,
                decision="skip_duplicate_flat",
                live_entry=0.0,
                tv_price=self.tv_price,
                diff_pct=diff_pct,
                threshold_pct=SAME_DIR_MIN_SPREAD_PCT,
                open_regime=self.regime,
                tv_regime=self.regime,
                open_atr=self._last_entry_signal.get("atr", self.current_atr),
                tv_atr=self.current_atr,
                qty=0.0,
                verify_note=(
                    f"5分钟内重复 {action} | ATR {self.current_atr:.2f} 未变 | "
                    f"TV {self.tv_price:.2f} 价差 {diff_pct:.3f}% | 档位 R{self.regime} | 未重复下单"
                ),
            )
            self._touch_entry_signal_signature(action)
            return

        logger.info(f"⚡ 收到建仓信号 [{action}]，空仓极速开仓")
        if not self._ensure_flat_before_open("空仓开仓"):
            dingtalk.report_system_alert(
                "开仓中止 · 盘口非空",
                f"收到 TV **{action}** 但实盘仍有残留持仓，已拒绝叠仓开仓",
                suggestion="请人工核查盘口，待全平后再等下一 TV 信号",
            )
            return
        deepcoin_client.cancel_all_open_orders(self.symbol)
        time.sleep(0.5)
        curr_px = curr_px or deepcoin_client.get_current_price(self.symbol)
        if curr_px > 0:
            self._open_position(action, curr_px)
        self._touch_entry_signal_signature(action)

    def _open_position(self, action, curr_px):
        if self._open_in_progress:
            logger.error(f"开仓中止：已有开仓流程进行中，拒绝叠仓 [{action}]")
            return
        self._open_in_progress = True
        try:
            self._snapshot_sizing_principal(f"开仓前 R{self.regime}")
            qty, balance, margin_usdt, margin_pct = self._calc_target_open_qty(curr_px)
            if qty <= 0:
                logger.error(f"开仓跳过：目标张数无效 balance={balance:.2f} px={curr_px}")
                return

            deepcoin_client.set_leverage(self.symbol, leverage=self.leverage)
            notional = qty * self.face_value * curr_px
            logger.info(
                f"📐 仓位预算 R{self.regime}: 本金 {balance:.2f}U × {margin_pct:.0%} "
                f"= 保证金 {margin_usdt:.2f}U × {self.leverage}x → 目标 {qty} 张 "
                f"(名义 ~{notional:.0f}U)"
            )

            if not self._wait_verify(self._verify_flat, retries=4, delay=0.35):
                logger.error("开仓中止：市价下单前盘口仍非空")
                dingtalk.report_system_alert(
                    "开仓中止 · 下单前盘口非空",
                    f"TV **{action}** 目标 **{qty}** 张，下单前 REST 仍显示持仓，已拒绝叠仓",
                    suggestion="系统将尝试强制清场，请核查是否有人工挂单或残仓",
                )
                return

            open_side = "buy" if action == "LONG" else "sell"
            pos_side = "long" if action == "LONG" else "short"
            logger.info(f"🚀 [唯一主仓] 极速开仓: {open_side} {qty} 张 | 档位 {self.regime}")
            res = deepcoin_client.place_market_order(self.symbol, open_side, pos_side, qty)
            if not res or not deepcoin_client._is_success(res):
                logger.error("开仓失败：市价单未成交")
                dingtalk.report_system_alert("开仓失败", f"TV {action} {qty} 张 市价单失败")
                return
            time.sleep(2.0)

            pos = self._get_active_position()
            if not pos or self._safe_qty(pos.get("size", 0)) <= 0:
                logger.error("开仓失败：成交后 REST 无持仓")
                return

            real_qty = self._safe_qty(pos["size"])
            if real_qty > qty * OPEN_OVERSIZE_RATIO:
                logger.error(
                    f"🚨 持仓超标: 目标 {qty} 张，实盘 {real_qty} 张 "
                    f"(>{qty * OPEN_OVERSIZE_RATIO:.3f})，启动裁减"
                )
                dingtalk.report_system_alert(
                    "持仓超标 · 自动裁减",
                    f"本金快照 **{balance:.0f}** U · R{self.regime} **{margin_pct:.0%}** → 目标 **{qty}** 张\n"
                    f"实盘 **{real_qty}** 张 @ **{pos['entry_price']:.2f}**，正在 reduceOnly 裁减至档位额度",
                    suggestion="裁减基数=合约本金 cashBal × 档位% × 15x，非可用保证金",
                )
                real_qty = self._trim_position_to_target(qty, action)
                pos = self._get_active_position()
                if pos:
                    pos["size"] = real_qty

            self.current_side = action
            self.open_regime = self.regime
            self.open_atr = self.current_atr
            self.initial_qty = real_qty
            self._protect_and_monitor(
                real_qty, pos['entry_price'],
                budget_note=(
                    f"本金 {balance:.0f}U | R{self.regime} {margin_pct:.0%} "
                    f"→ 保证金 {margin_usdt:.0f}U | 目标 {qty} 张"
                ),
                target_qty=qty,
            )
        finally:
            self._open_in_progress = False

    def _protect_and_monitor(self, qty, entry_price, budget_note="", target_qty=0):
        tp_pxs = self.tv_tps
        self.current_sl = entry_price
        self.best_price = entry_price
        self.shield_active = False
        self.shield_tiers_consumed = []
        self.watched_qty, self.watched_entry, self.monitoring = qty, entry_price, True
        self._save_state()

        self._ensure_price_ws()

        verified = self._wait_verify(lambda: self._verify_position(self.current_side))
        if verified:
            vqty = self._safe_qty(verified["size"])
            if target_qty > 0 and vqty > int(target_qty * OPEN_OVERSIZE_RATIO):
                vqty = self._trim_position_to_target(target_qty, self.current_side)
                self.watched_qty = vqty
                self.initial_qty = vqty
                self._save_state()

            self._scorched_earth_cancel_for_recover()
            self._enforce_defense_alignment(
                vqty, verified["entry_price"],
                dynamic_sl=None, reason="开仓后防线对齐", rounds=4,
                recover_mode=True,
            )
            audit = self._wait_defense_settled(vqty)
            matched, expected = audit["matched_full"], audit["expected"]
            verify_note = (
                f"{budget_note} | " if budget_note else ""
            ) + (
                f"持仓 {vqty}张 @ {verified['entry_price']:.2f} | "
                f"限价止盈 {matched}/{expected} 档 | {self._format_audit_summary(audit)}"
            )
            if target_qty > 0 and vqty > target_qty * OPEN_OVERSIZE_RATIO:
                verify_note += f" | ⚠️ 超标目标 {target_qty} 张"
            self._record_open_log(
                self.current_side, vqty, verified["entry_price"], source="open",
            )
            self._call_dingtalk(
                dingtalk.report_supervisor_open,
                side=self.current_side,
                entry_price=verified['entry_price'],
                tv_price=self.tv_price,
                qty=vqty,
                tp_pxs=tp_pxs,
                atr=self.current_atr,
                regime=self.regime,
                tv_tps=self.tv_tps,
                verify_note=verify_note,
                tp_audit=audit,
                verified=(expected == 0 or matched >= expected),
                principal_balance=self.sizing_principal or deepcoin_client.get_principal_wallet_balance(),
                margin_pct=self.regime_settings.get(self.regime, {}).get("margin"),
                margin_usdt=(self.sizing_principal or 0) * self.regime_settings.get(self.regime, {}).get("margin", 0),
                leverage=self.leverage,
            )
            if expected > 0 and matched < expected:
                self._open_tp_unconfirmed = True
                dupes = [lv for lv in audit.get("levels", []) if lv.get("status") == "duplicate"]
                hint = (
                    "重复 TP 占满可减仓额度 | 雷达将接力纠偏"
                    if dupes else "请查 logs/deepcoin_brain.log"
                )
                dingtalk.report_system_alert(
                    "开仓后限价止盈未全部挂上",
                    f"{self.current_side} {vqty}张 | 仅 {matched}/{expected} 档 | "
                    f"{self._format_audit_summary(audit)} | {hint}",
                )
            curr_px = deepcoin_client.get_current_price(self.symbol) or entry_price
            if self._should_activate_shield(curr_px):
                self._maintain_hard_shield(
                    vqty, curr_px,
                    force=True,
                )
        else:
            logger.warning("开仓钉钉跳过：实盘持仓核查未通过")
        self._ensure_sentinel_running()

    def _ensure_price_ws(self):
        deepcoin_client.start_public_price_ws(self.symbol)

    def _tp1_distance(self):
        if self.tv_tps[0] > 0 and self.watched_entry:
            return abs(self.tv_tps[0] - self.watched_entry)
        return self.current_atr * 1.5

    def _radar_activation_price(self):
        activation_ratio = self.regime_settings[self.regime]["activation"]
        tp1_dist = self._tp1_distance()
        if self.current_side == "LONG":
            return self.watched_entry + tp1_dist * activation_ratio
        return self.watched_entry - tp1_dist * activation_ratio

    def _should_radar_trail(self, curr_px):
        """已激活后不再要求现价回到激活线以上，否则回撤时会停止推止损"""
        if self._is_radar_active():
            return True
        if curr_px <= 0 or not self.watched_entry:
            return False
        if self.current_side == "LONG":
            return curr_px >= self._radar_activation_price()
        return curr_px <= self._radar_activation_price()

    def _compute_radar_sl(self):
        if not self.watched_entry or self.best_price <= 0:
            return None
        trail_offset = self.current_atr * self.regime_settings[self.regime]["trail_offset"]
        fee_buffer = self.watched_entry * 0.0015
        if self.current_side == "LONG":
            return max(round(self.best_price - trail_offset, 2), self.watched_entry + fee_buffer)
        if self.current_side == "SHORT":
            return min(round(self.best_price + trail_offset, 2), self.watched_entry - fee_buffer)
        return None

    def _sync_radar_sl_from_best(self, curr_px):
        """TP 重对齐前刷新内存止损位，避免把旧止损重新挂回交易所"""
        if not self._should_radar_trail(curr_px):
            return self.current_sl
        new_sl = self._compute_radar_sl()
        if new_sl is None:
            return self.current_sl
        if self.current_side == "LONG" and new_sl > self.current_sl:
            logger.info(
                f"📈 雷达止损预算刷新: {self.current_sl:.2f} → {new_sl:.2f} "
                f"(best={self.best_price:.2f})"
            )
            self.current_sl = new_sl
            self._save_state()
        elif self.current_side == "SHORT" and (
                self.current_sl >= self.watched_entry or new_sl < self.current_sl
        ):
            logger.info(
                f"📉 雷达止损预算刷新: {self.current_sl:.2f} → {new_sl:.2f} "
                f"(best={self.best_price:.2f})"
            )
            self.current_sl = new_sl
            self._save_state()
        return self.current_sl

    def _bump_best_on_tp_fill(self, old_qty, new_qty, curr_px):
        """部分止盈后把 best_price 抬到已触及的 TP 价，避免漏记冲高"""
        if new_qty >= old_qty or curr_px <= 0:
            return
        if self.current_side == "LONG":
            candidates = [self.best_price, curr_px]
            for tp in self.tv_tps:
                if tp > 0 and curr_px >= tp - 2.0:
                    candidates.append(tp)
            new_best = max(candidates)
            if new_best > self.best_price + 0.01:
                logger.info(
                    f"📊 止盈吃单刷新 best_price: {self.best_price:.2f} → {new_best:.2f} "
                    f"(qty {old_qty}→{new_qty})"
                )
                self.best_price = new_best
        else:
            candidates = [self.best_price, curr_px]
            for tp in self.tv_tps:
                if tp > 0 and curr_px <= tp + 2.0:
                    candidates.append(tp)
            new_best = min(candidates)
            if new_best < self.best_price - 0.01:
                logger.info(
                    f"📊 止盈吃单刷新 best_price: {self.best_price:.2f} → {new_best:.2f} "
                    f"(qty {old_qty}→{new_qty})"
                )
                self.best_price = new_best

    def _radar_activation_progress(self, curr_px):
        if curr_px <= 0 or not self.watched_entry:
            return 0.0
        tp1_dist = self._tp1_distance()
        activation_ratio = self.regime_settings[self.regime]["activation"]
        if self.current_side == "LONG":
            required = self.watched_entry + tp1_dist * activation_ratio
            span = required - self.watched_entry
            if span <= 0:
                return 0.0
            return max(0.0, min(1.0, (curr_px - self.watched_entry) / span))
        required = self.watched_entry - tp1_dist * activation_ratio
        span = self.watched_entry - required
        if span <= 0:
            return 0.0
        return max(0.0, min(1.0, (self.watched_entry - curr_px) / span))

    def _sentinel_poll_sec(self, curr_px=0.0):
        if self._is_radar_active():
            return SENTINEL_POLL_RADAR
        if curr_px > 0:
            if self._radar_activation_progress(curr_px) >= 0.5:
                return SENTINEL_POLL_ARMING
            if getattr(self, "shield_active", False):
                return SENTINEL_POLL_ARMING
        return SENTINEL_POLL_NORMAL

    def _process_radar_trailing(self, real_amt, curr_px):
        if not self._should_radar_trail(curr_px):
            return False
        new_sl = self._compute_radar_sl()
        if new_sl is None:
            return False

        if not self._is_radar_active():
            fee_buffer = self.watched_entry * 0.0015
            if self.current_side == "LONG":
                boot_sl = max(new_sl, self.watched_entry + fee_buffer)
                if boot_sl > self.current_sl:
                    self.current_sl = boot_sl
            else:
                boot_sl = min(new_sl, self.watched_entry - fee_buffer)
                if boot_sl < self.current_sl or self.current_sl >= self.watched_entry:
                    self.current_sl = boot_sl
            self._save_state()
            if not self._has_trigger_sl_near(self.current_sl):
                self._ensure_radar_sl(real_amt, self.current_sl)
                logger.info(
                    f"📡 雷达首次激活：保本止损 @ {self.current_sl:.2f} | best={self.best_price:.2f}"
                )
                return True

        if self.current_side == "LONG":
            if new_sl > self.current_sl + 1.0:
                self.current_sl = new_sl
                self._save_state()
                sl_placed = self._realign_radar_defenses(real_amt, self.watched_entry, new_sl)
                self._report_radar_intervention(
                    real_amt, new_sl,
                    f"🚀 档位{self.regime} 雷达实时跟踪：保本盾推升至 {new_sl:.2f}",
                    sl_placed=sl_placed,
                )
                return True
        else:
            if self.current_sl >= self.watched_entry or new_sl < self.current_sl - 1.0:
                self.current_sl = new_sl
                self._save_state()
                sl_placed = self._realign_radar_defenses(real_amt, self.watched_entry, new_sl)
                self._report_radar_intervention(
                    real_amt, new_sl,
                    f"🚀 档位{self.regime} 雷达实时跟踪：保本顶线下压至 {new_sl:.2f}",
                    sl_placed=sl_placed,
                )
                return True
        return False

    def _sentinel_loop(self):
        """哨兵：持仓/TP 防线 + 雷达移动保本（自适应轮询 2~6 秒）"""
        self._sentinel_active = True
        last_px = 0.0
        try:
            while self.monitoring:
                try:
                    if not self._lock.acquire(timeout=2.0):
                        continue
                    try:
                        pos = self._get_active_position()
                        real_amt = self._safe_qty(pos.get("size")) if pos else 0
                        actual_side = "LONG" if pos and pos.get('posSide') == "long" else "SHORT"

                        if real_amt == 0:
                            if self.watched_qty > 0:
                                self._handle_manual_flat_detected(
                                    "仓位归零 (止盈吃单 / 人工全平 / TV 强制平仓)"
                                )
                            break

                        if self.watched_qty > 0 and self._should_finalize_tp_victory(real_amt):
                            self._sweep_dust_and_finalize(
                                "仓位归零 (止盈吃单 / 人工全平 / TV 强制平仓)"
                            )
                            break

                        if actual_side != self.last_tv_side:
                            reason = f"致命方向背离：实盘({actual_side}) vs TV({self.last_tv_side})"
                            self._close_all(reason, force_align=(actual_side, self.last_tv_side))
                            break

                        curr_px = deepcoin_client.get_current_price(self.symbol)
                        if curr_px <= 0:
                            curr_px = last_px
                        elif curr_px > 0:
                            last_px = curr_px
                        if curr_px > 0:
                            if self.current_side == "LONG":
                                self.best_price = max(self.best_price, curr_px)
                            else:
                                self.best_price = min(self.best_price, curr_px)

                        qty_changed = False
                        if real_amt != self.watched_qty:
                            if self._is_material_qty_change(self.watched_qty, real_amt):
                                qty_changed = True
                                old_qty = self.watched_qty
                                self.watched_qty = real_amt
                                self.watched_entry = pos['entry_price']
                                change, result = self._handle_smart_qty_change(
                                    old_qty, real_amt, curr_px,
                                )
                                if result:
                                    self._report_qty_change_dingtalk(old_qty, real_amt, result)
                            else:
                                drift = self._qty_change_ratio(self.watched_qty, real_amt)
                                if drift >= QTY_DRIFT_TOLERANCE_PCT:
                                    logger.info(
                                        f"📎 [哨兵] 仓位微漂 {self.watched_qty}→{real_amt} 张 "
                                        f"({drift:.2%}，未达 {QTY_ALIGN_MIN_PCT:.0%} 对齐阈值)，仅同步账本"
                                    )
                                self.watched_qty = real_amt
                                self.watched_entry = pos['entry_price']
                                self._save_state()

                        self._scan_ticks += 1
                        if not qty_changed:
                            self._radar_guardian_audit(real_amt, curr_px)

                        if curr_px <= 0:
                            continue

                        self._process_directional_defenses(real_amt, curr_px)
                        progress = self._radar_activation_progress(curr_px)
                        if (
                            progress >= 0.5
                            and not self._is_radar_active()
                            and self._scan_ticks % 5 == 0
                        ):
                            logger.info(
                                f"📡 雷达预热: 进度 {progress:.0%} | 现价 {curr_px:.2f} | "
                                f"轮询 {SENTINEL_POLL_ARMING}s"
                            )
                    finally:
                        self._lock.release()
                except Exception as e:
                    logger.error(f"哨兵异常: {e}")
                if self.monitoring:
                    time.sleep(self._sentinel_poll_sec(last_px))
        finally:
            self._sentinel_active = False

    def _rebuild_defenses(self, qty, entry, dynamic_sl=None):
        close_side = "sell" if self.current_side == "LONG" else "buy"
        pos_side = "long" if self.current_side == "LONG" else "short"
        ratios = self.regime_settings[self._tp_split_regime()]["ratios"]

        live_qty = self._resolve_live_qty(qty)
        if live_qty <= 0:
            logger.warning(f"重建防线跳过：交易所无可用持仓 (传入 {qty} 张)")
            return 0

        self._cancel_all_tp_limit_orders()
        time.sleep(0.35)

        if live_qty != qty:
            self.watched_qty = live_qty
            self._save_state()

        qty1, qty2, qty3 = self._calculate_tp_quantities(live_qty, ratios)
        tp_pxs = self.tv_tps
        placed = 0

        logger.info(
            f"🕸️ 补挂 TP123: 总 {live_qty}张 → TP1={qty1} TP2={qty2} TP3={qty3} "
            f"(R{self._tp_split_regime()} 比例 {ratios})"
        )

        for q, px in ((qty1, tp_pxs[0]), (qty2, tp_pxs[1]), (qty3, tp_pxs[2])):
            if q > 0 and px > 0:
                res = deepcoin_client.place_limit_order(
                    self.symbol, close_side, pos_side, px, q, reduce_only=True,
                )
                if res and deepcoin_client._is_success(res):
                    placed += 1
                time.sleep(0.35)

        if dynamic_sl:
            sl_qty = self._resolve_live_qty(live_qty)
            deepcoin_client.place_trigger_order(
                self.symbol, close_side, pos_side, sl_qty, dynamic_sl,
                order_type="market", td_mode="cross", mrg_position="merge",
            )
        return placed

    def _close_all(self, reason="", force_align=None, reset_state=True):
        """先撤全部挂单再阶梯强平；返回是否已空仓"""
        deepcoin_client.cancel_all_open_orders(self.symbol)
        time.sleep(0.5)
        self._cancel_all_tp_limit_orders()
        time.sleep(0.3)
        closed_successfully = False

        for round_i in range(6):
            pos = self._get_active_position()
            if not pos or self._safe_qty(pos.get("size")) == 0:
                closed_successfully = True
                break

            close_side = "sell" if pos["posSide"] == "long" else "buy"
            live_sz = self._safe_qty(pos["size"])
            logger.info(f"🔪 强平第 {round_i + 1}/6 轮: {close_side} {live_sz}张 reduceOnly")
            deepcoin_client.place_market_order(
                self.symbol, close_side, pos["posSide"], live_sz, reduce_only=True,
            )
            time.sleep(1.5)

        if not closed_successfully:
            residual = self._get_active_position()
            residual_sz = self._safe_qty(residual["size"]) if residual else 0
            if residual_sz > 0 and self._is_dust_qty(residual_sz):
                close_side = "sell" if residual["posSide"] == "long" else "buy"
                logger.warning(f"🐜 强平后残 {residual_sz}张，触发蚂蚁仓扫尾")
                deepcoin_client.place_market_order(
                    self.symbol, close_side, residual["posSide"], residual_sz, reduce_only=True,
                )
                time.sleep(1.0)
                closed_successfully = self._verify_flat()
            if not closed_successfully:
                residual = self._get_active_position()
                residual_sz = self._safe_qty(residual["size"]) if residual else 0
                logger.error(f"❌ 6 轮强平后仍有残单: {residual_sz}张")
                dingtalk.report_system_alert(
                    "强平未完全归零",
                    f"6 轮市价平仓后仍剩 {residual_sz} 张，请人工核查 Deepcoin 盘口",
                )

        if reset_state:
            if closed_successfully:
                self.monitoring = False
                self.watched_qty = 0
                self.initial_qty = 0
                self.current_side = None
                self.shield_active = False
                self.shield_tiers_consumed = []
                self._snapshot_sizing_principal("全平后本金重置")
            else:
                residual = self._get_active_position()
                if residual:
                    self.watched_qty = self._safe_qty(residual["size"])
                    self.current_side = self._pos_side_label(residual)
                    self.watched_entry = residual["entry_price"]
                    logger.warning(
                        f"强平未归零，账本同步实盘: {self.current_side} {self.watched_qty} 张"
                    )
            self._save_state()

        deepcoin_client.cancel_all_open_orders(self.symbol)

        if reason and closed_successfully:
            if force_align:
                real_side, expected_side = force_align
                flat = self._wait_verify(self._verify_flat, retries=6, delay=0.5)
                verify_note = "盘口无持仓 | 挂单已清空 | 智慧大脑复位待命"
                if not flat:
                    verify_note += " | REST 同步略延迟"
                self._call_dingtalk(
                    dingtalk.report_force_align,
                    real_side=real_side,
                    expected_side=expected_side,
                    verify_note=verify_note,
                )
            else:
                self._report_flat_close(reason)

        return closed_successfully

    def recover_state_on_startup(self):
        """重启闪电接管：对账 TV/开仓日志 → 核实实盘 → 智能补挂 TP123 → 恢复雷达"""
        if not self._try_acquire_recover_singleton():
            return
        try:
            saved_monitoring = False
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r') as f:
                    s = json.load(f)
                    saved_monitoring = bool(s.get("monitoring"))
                    self.last_tv_side = s.get("last_tv_side")
                    self.current_side = s.get("current_side")
                    self.current_sl = s.get("current_sl", 0.0)
                    self.regime = s.get("regime", 3)
                    self.current_atr = s.get("current_atr", 30.0)
                    self.tv_tps = self._sanitize_tp_prices(s.get("tv_tps", [0.0, 0.0, 0.0]))
                    self.tv_price = float(s.get("tv_price", 0.0) or 0.0)
                    self.best_price = s.get("best_price", 0.0)
                    self.watched_qty = s.get("watched_qty", 0)
                    self.watched_entry = s.get("watched_entry", 0.0)
                    self.initial_qty = s.get("initial_qty", 0)
                    self.last_tv_signal = s.get("last_tv_signal")
                    self.open_regime = int(s.get("open_regime", s.get("regime", 3)) or 3)
                    self.open_atr = float(s.get("open_atr", s.get("current_atr", 30.0)) or 30.0)
                    self.shield_active = bool(s.get("shield_active", False))
                    self.shield_tiers_consumed = list(s.get("shield_tiers_consumed", []) or [])
                    self.shield_sized_qty = float(s.get("shield_sized_qty", 0) or 0)
                    if self.shield_sized_qty > 0:
                        self._shield_arm_notified = True
                    self.sizing_principal = float(s.get("sizing_principal", 0) or 0)
                    if self.sizing_principal <= 0:
                        eq = deepcoin_client.get_principal_wallet_balance()
                        if eq > 0:
                            self.sizing_principal = eq

            if self._scan_and_sweep_dust_on_startup(was_monitoring=saved_monitoring):
                return

            if self._recover_missed_flat_on_startup(was_monitoring=saved_monitoring):
                return

            pos = self._get_active_position()
            if pos and self._safe_qty(pos.get("size", 0)) != 0:
                self._recover_in_progress = True
                if not self._lock.acquire(timeout=120.0):
                    logger.error("❌ 重启接管无法获取锁，跳过")
                    self._recover_in_progress = False
                    return
                try:
                    reconcile = self._reconcile_context_on_recover(pos)
                    reconcile_notes = reconcile["notes"]
                    real_amt = self._safe_qty(pos["size"])
                    side = "LONG" if pos.get("posSide") == "long" else "SHORT"
                    self.current_side = side

                    align_notes = self._apply_recover_live_alignment(side, reconcile)
                    reconcile_notes.extend(align_notes)

                    saved_initial = self._safe_qty(self.initial_qty)
                    if saved_initial <= 0:
                        saved_initial = real_amt
                    self.watched_qty = real_amt
                    self.initial_qty = saved_initial
                    self.watched_entry = float(pos["entry_price"])
                    if not getattr(self, "open_regime", None):
                        self.open_regime = self.regime
                    if not getattr(self, "open_atr", None):
                        self.open_atr = self.current_atr
                    qty_change = reconcile.get("qty_manual_change")

                    curr_px = deepcoin_client.get_current_price(self.symbol)
                    self._refresh_radar_state_on_recover(curr_px, self.watched_entry)

                    radar_active = self._is_radar_active()
                    saved_sl = self.current_sl if radar_active else None

                    logger.info(
                        f"🔄 [系统重启点火] 检测到实盘持仓 {self.current_side} {real_amt}张 @ "
                        f"{self.watched_entry:.2f} | 雷达={'已激活' if radar_active else '待命'} | "
                        f"TV对齐 {self.last_tv_side} | 对账 {len(reconcile_notes)} 项"
                    )

                    cap = self._radar_enforce_regime_cap(real_amt, curr_px, force=True)
                    if cap:
                        real_amt = cap["new_qty"]
                        pos = self._get_active_position() or pos
                        if pos:
                            self.watched_qty = real_amt
                            self.initial_qty = real_amt

                    result = self._enforce_defense_alignment(
                        real_amt, self.watched_entry, dynamic_sl=None,
                        reason="重启闪电接管 · 核武撤单重挂",
                        rounds=4, recover_mode=True,
                    )
                    if saved_sl and radar_active:
                        sl_ok = self._ensure_radar_sl(real_amt, saved_sl)
                    else:
                        sl_ok = True
                    _rebuilt = result["rebuilt"]
                    audit = self._wait_defense_settled(
                        real_amt, saved_sl if radar_active else None,
                    )
                    matched = audit["matched_full"]
                    expected = audit["expected"]

                    self.monitoring = True
                    self._save_state()
                    self._ensure_price_ws()
                    self._record_open_log(
                        self.current_side, real_amt, self.watched_entry, source="recover",
                    )

                    verified = self._wait_verify(
                        lambda: self._verify_position_qty(real_amt, self.current_side),
                        retries=8,
                        delay=0.5,
                    )
                    entry_px = float(
                        (verified or pos).get("entry_price", self.watched_entry)
                    )
                    tv_note = ""
                    if self.last_tv_signal:
                        tv_note = (
                            f" | 最新TV: {self.last_tv_signal.get('action')} "
                            f"@{self.last_tv_signal.get('ts', '')}"
                        )
                    reconcile_txt = (" | " + " ; ".join(reconcile_notes)) if reconcile_notes else ""
                    skip_note = " | 盘口已齐全，未重复补挂" if not _rebuilt else ""
                    verify_note = (
                        f"接管 {real_amt}张 @ {entry_px:.2f} | "
                        f"TV方向 {self.last_tv_side} | "
                        f"止盈 {matched}/{expected} 档 | "
                        f"{self._format_audit_summary(audit)}{skip_note}{tv_note}{reconcile_txt}"
                    )
                    if not verified:
                        verify_note += f" | {dingtalk.VERIFY_DELAY_MARK}"
                    if qty_change:
                        old_q, new_q, action_msg = qty_change
                        self._call_dingtalk(
                            dingtalk.report_manual_position_change,
                            action_type=action_msg,
                            old_qty=old_q,
                            new_qty=new_q,
                            new_entry_price=entry_px,
                            verify_note=f"重启接管检测 | {verify_note}",
                            tp_audit=audit,
                            verified=bool(verified),
                        )
                    if expected > 0 and matched < expected:
                        dupes = [lv for lv in audit.get("levels", []) if lv.get("status") == "duplicate"]
                        hint = (
                            "重复 TP 占满可减仓额度→TP3 无法挂 | 非 API 权限问题"
                            if dupes else "请查 logs/deepcoin_brain.log 是否有撤单/限价失败"
                        )
                        self._recover_tp_unconfirmed = True
                        dingtalk.report_system_alert(
                            "重启接管后限价止盈未对齐",
                            f"{self.current_side} {real_amt}张 @ {entry_px:.2f} | "
                            f"仅 {matched}/{expected} 档 | {self._format_audit_summary(audit)} | "
                            f"{hint} | 雷达哨兵将接力纠偏；仍失败请 APP 手动全撤后重启",
                        )
                    else:
                        self._mark_defense_align_ok()

                    health = self._build_recover_health_report(
                        {"side": self.current_side, "size": real_amt, "entry_price": entry_px},
                        curr_px, audit,
                    )
                    policy_actions = self._apply_recover_defense_policy(
                        real_amt, curr_px, health,
                    )
                    health = self._build_recover_health_report(
                        {"side": self.current_side, "size": real_amt, "entry_price": entry_px},
                        curr_px, audit,
                    )
                    radar_active = health["radar_active"] or health["should_radar"]
                    sl_ok = (
                        not radar_active
                        or self._has_trigger_sl_near(self.current_sl)
                    )
                    policy_txt = " | 防线: " + " · ".join(policy_actions) if policy_actions else ""
                    health_txt = (
                        f" | 盈亏态 {health['pnl_label']} | "
                        f"硬止损 {health['shield_status']} | "
                        f"策略 {health['defense_plan']}"
                    )
                    verify_note = verify_note + health_txt + policy_txt

                    self._sentinel_grace_until = time.time() + SENTINEL_GRACE_AFTER_RECOVER_SEC

                    self._call_dingtalk(
                        dingtalk.report_recover_takeover,
                        side=self.current_side,
                        qty=real_amt,
                        entry=entry_px,
                        tv_tps=self.tv_tps,
                        regime=self.regime,
                        radar_active=radar_active,
                        sl_price=self.current_sl,
                        verify_note=verify_note,
                        tp_matched=matched,
                        tp_expected=expected,
                        tp_audit=audit,
                        last_tv_signal=self.last_tv_signal,
                        radar_sl_ok=sl_ok,
                        pnl_label=health["pnl_label"],
                        defense_plan=health["defense_plan"],
                        shield_status=health["shield_status"],
                        radar_progress=health["radar_progress"],
                        tv_aligned=health["tv_match"],
                        qty_aligned=health["qty_match"],
                    )
                    logger.info(
                        f"  -> 🎉 实盘阵地接管完毕 | {health['pnl_label']} | "
                        f"防线 {' · '.join(policy_actions) if policy_actions else '已核实'}"
                    )
                finally:
                    self._recover_in_progress = False
                    self._lock.release()

                if radar_active:
                    logger.info(
                        f"📡 [重启] 雷达哨兵已点火 | SL={self.current_sl:.2f} | "
                        f"止损={'已挂/已确认' if sl_ok else '待哨兵补挂'}"
                    )

                if not self._sentinel_active:
                    threading.Thread(
                        target=self._sentinel_loop, daemon=True, name="sentinel",
                    ).start()
            else:
                deepcoin_client.cancel_all_open_orders(self.symbol)
                logger.info("🔄 [系统重启点火] 盘口干净无持仓，账本复位为空仓待命。")
                self.monitoring = False
                self.watched_qty = 0
                self.initial_qty = 0
                self.current_side = None
                self._save_state()
                flat_ok = self._wait_verify(self._verify_flat, retries=6, delay=0.5)
                standby_note = (
                    f"重启完成 | 盘口无持仓 | 挂单已清空 | "
                    f"{DEEPCOIN_SUPERVISOR_VERSION}"
                )
                if not flat_ok:
                    standby_note += f" | {dingtalk.VERIFY_DELAY_MARK}"
                dingtalk.report_recover_standby(
                    verify_note=standby_note,
                    version=DEEPCOIN_SUPERVISOR_VERSION,
                )
        except Exception as e:
            logger.error(f"❌ 闪电接管异常: {e}")
            dingtalk.report_system_alert("重启接管失败", str(e))


position_supervisor = PositionSupervisor()

# 仅在被 app / gunicorn 导入时执行一次闪电接管（避免 deploy 重复启动双进程）
if __name__ != "__main__":
    position_supervisor.recover_state_on_startup()
