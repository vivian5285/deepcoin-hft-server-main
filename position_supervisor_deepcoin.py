#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# position_supervisor_deepcoin.py — 与币安 VPS 逻辑完全对齐（深币张数/20x 适配）
import logging
import time
import math
import threading
import os
import json
import queue
from datetime import datetime
from logging.handlers import RotatingFileHandler
from deepcoin_client import deepcoin_client, CLIENT_VERSION
from pipeline_bridge import PipelineBridgeMixin
from pipeline_ledger import Role
from radar_reentry_mixin import RadarReentryMixin
import dingtalk
import telegram_notify
from tv_seq import (
    reorder_batch_close_then_open,
    extract_seq_meta,
    is_close_action,
    is_open_action,
    SAME_BAR_SETTLE_SEC,
)
from breath_stop import (
    calculate_breath_stop,
    order_stop_price,
    initial_stop_price,
    get_breathing_coefficient,
    STOP_EXEC_BUFFER_USD,
)
from atr_scenario import (
    hard_stop_price,
    compute_hard_stop_distance,
    TEMP_STOP_BUFFER_MULT,
)
from webhook_parser import (
    enrich_signal_fields,
    enrich_entry_tp_prices,
    format_tv_field_sources,
    classify_tv_close,
    compute_vps_open_qty,
    compute_vps_add_qty,
    format_vps_sizing_note,
    VPS_RISK_PCT,
    get_regime_max_add_times,
    resolve_tv_add_qty_ratio,
    LEG_TP_RATIOS,
    format_regime_tp_ratios_label,
    get_regime_tp_ratios,
    EXCHANGE_LEVERAGE,
    validate_tp_prices_for_side,
    normalize_entry_type,
    ENTRY_TYPE_OPEN,
    ENTRY_TYPE_PYRAMID,
    ENTRY_TYPE_PROFIT_ADD,
    CLOSE_TYPE_TP3,
    CLOSE_TYPE_BREAKEVEN,
    CLOSE_TYPE_VPS_SHIELD,
    check_total_notional_cap,
    MAX_TOTAL_NOTIONAL_MULT,
)
from order_idempotency import (
    blank_ownership_state,
    make_defense_client_order_id,
    MAX_OPEN_ORDERS_HARD_CAP,
)

if not os.path.exists('logs'):
    os.makedirs('logs')
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_LOG_DIR = os.path.join(_BASE_DIR, 'logs')
os.makedirs(_LOG_DIR, exist_ok=True)
_BRAIN_LOG = os.path.join(_LOG_DIR, 'deepcoin_brain.log')
handler = RotatingFileHandler(_BRAIN_LOG, maxBytes=5 * 1024 * 1024, backupCount=3)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] Brain: %(message)s',
    handlers=[handler, logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

DEEPCOIN_SUPERVISOR_VERSION = "v16.16a-hedge-check"

# 开仓成交后：迟到 CLOSE 忽略窗口（覆盖 1–2s 网络差）
LATE_CLOSE_SUPPRESS_SEC = 5.0
# v16.10+：哨兵轮询加快（配合 API 限流放宽，不再需要慢速规避）
SENTINEL_POLL_NORMAL = 8.0
SENTINEL_POLL_ARMING = 6.0
SENTINEL_POLL_RADAR = 6.0
IDLE_PATROL_INTERVAL_SEC = 120
IDLE_PATROL_BACKOFF_SEC = 900
IDLE_TAKEOVER_COOLDOWN_SEC = 30
DUST_ORPHAN_CONTRACTS = 1
TP_COMPLETE_RESIDUAL_RATIO = 0.12
OPEN_OVERSIZE_RATIO = 1.10
SIGNAL_DEDUP_SEC = 45
# v16.10+：防线对齐冷却缩短（配合限流放宽）
DEFENSE_ALIGN_COOLDOWN_SEC = 20
SENTINEL_GRACE_AFTER_RECOVER_SEC = 30
FLAT_CONFIRM_RETRIES = 6
FLAT_CONFIRM_DELAY_SEC = 0.85
STARTUP_FLAT_CONFIRM_RETRIES = 10
STARTUP_FLAT_CONFIRM_DELAY_SEC = 1.0
RECOVER_LOCK_FILE = "logs/.recover_singleton.lock"
RECOVER_LOCK_TTL_SEC = 180
REGIME_CAP_COOLDOWN_SEC = 90
REGIME_CAP_TOLERANCE_CONTRACTS = 0
CAP_MIN_RETAIN_RATIO = 0.25
CAP_TRIM_MAX_ROUNDS = 4
QTY_DRIFT_TOLERANCE_PCT = 0.015  # 微漂 ≤1.5%：仅同步账本
QTY_ALIGN_MIN_PCT = 0.10         # 偏离 ≥10% 才离谱，触发对齐/档位裁减
SHIELD_HARD_STOP_PCT = 0.10  # 历史常量（仅哨兵成交分类标签）；止损价 exclusively TV tv_sl
SHIELD_TIER_PCTS = (SHIELD_HARD_STOP_PCT,)
SHIELD_TIER_RATIOS = (1.0,)
SHIELD_STOP_TOLERANCE = 2.0
SHIELD_MAINTAIN_COOLDOWN_SEC = 60
SHIELD_FAIL_BACKOFF_BASE_SEC = 45
SHIELD_FAIL_BACKOFF_MAX_SEC = 300
SHIELD_QTY_TOLERANCE_PCT = 0.04
SHIELD_MAX_TIER_ORDERS = 1
RADAR_DINGTALK_COOLDOWN_SEC = 120
# TV v6.9.86 雷达呼吸空间（对齐 trailTight / TP2·TP3 trailing，避免 TP1 前被震荡扫出）
TV_TRAIL_TIGHT = 0.62
TV_TRAIL_TP2_ATR = TV_TRAIL_TIGHT * 0.32   # ≈0.20 ATR — TP1 成交后
TV_TRAIL_TP3_ATR = TV_TRAIL_TIGHT * 0.48   # ≈0.30 ATR — TP2 成交后
TV_BOOT_SL_ATR = 0.40                      # strongBull 保本底线 entry ± 0.4 ATR
RADAR_FEE_BUFFER_PCT = 0.0015
RADAR_STOP_MIN_GAP_USD = 2.5
RADAR_STOP_MIN_GAP_PCT = 0.0012
MIN_TP_LEG_QTY = 1
# 同向 TV 智能筛选：① ATR 变化 → 先平后开；② 价差低于该百分比 → 不重复开仓，仅刷新 TP123
SAME_DIR_MIN_SPREAD_PCT = 0.15
SAME_DIR_DEDUP_SEC = 300
OPEN_SAME_DIR_COOLDOWN_SEC = 180  # 同向重复 OPEN：开仓后冷却期内禁止先平后开
ATR_SIMILAR_RATIO = 0.03  # 持仓 ATR 与 TV ATR 偏差 ≤3% 视为未变
TV_JOURNAL = "logs/deepcoin_tv_journal.jsonl"
OPEN_JOURNAL = "logs/deepcoin_open_journal.jsonl"


class PositionSupervisor(PipelineBridgeMixin, RadarReentryMixin):
    def __init__(self, symbol="ETH-USDT-SWAP"):
        from symbol_config import resolve_deepcoin_symbol
        from breath_profiles import get_breath_profile
        meta = resolve_deepcoin_symbol(symbol)
        self.symbol = meta["symbol"]
        self.unit_label = meta.get("unit") or "张"
        self.tag = meta.get("tag") or ("ETH" if "ETH" in self.symbol else "XAU")
        self.breath_profile = meta.get("breath_profile") or get_breath_profile(self.symbol, "deepcoin")
        self.face_value = float(meta.get("face_value") or 0.1)
        try:
            prec = int(meta.get("price_precision") or 2)
            if self.breath_profile:
                self.breath_profile = dict(self.breath_profile)
                self.breath_profile["tick_size"] = 10 ** (-prec)
        except Exception:
            pass
        try:
            info = deepcoin_client.get_instrument_info(self.symbol)
            ct = float(info.get("ctVal") or info.get("contractVal") or 0)
            if ct > 0:
                self.face_value = ct
                logger.info(f"📐 {self.symbol} face_value={ct} (instruments)")
        except Exception as e:
            logger.warning(f"face_value instruments 失败，用 meta {self.face_value}: {e}")

        # v16.16 注：Deepcoin 没有 set-position-mode API，但 posSide(long/short)
        # 参数已在所有订单中正确使用，账户默认行为由 Deepcoin 控制。
        # 杠杆在每次开仓前通过 set_leverage(5x) 设置。
        self.binance_mark = meta.get("binance_mark") or "ETHUSDT"
        self.monitoring = False
        self._lock = threading.Lock()

        # 钉钉展示用档位保证金%（双品种文档）
        self.regime_settings = {
            1: {"margin": 0.05, "ratios": get_regime_tp_ratios(1), "activation": 0.92, "trail_offset": TV_TRAIL_TP2_ATR},
            2: {"margin": 0.10, "ratios": get_regime_tp_ratios(2), "activation": 0.92, "trail_offset": TV_TRAIL_TP2_ATR},
            3: {"margin": 0.15, "ratios": get_regime_tp_ratios(3), "activation": 0.95, "trail_offset": TV_TRAIL_TP3_ATR},
            4: {"margin": 0.18, "ratios": get_regime_tp_ratios(4), "activation": 0.95, "trail_offset": TV_TRAIL_TP3_ATR},
        }
        self.leverage = EXCHANGE_LEVERAGE
        self.tv_sizing_leverage = EXCHANGE_LEVERAGE
        # face_value 已按品种 meta 设定，禁止再写死 0.1

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

        self.initial_stop = 0.0
        self.breathing_coefficient = 1.0
        self._breath_ratio_history = []
        self.breakeven_phase = False
        self._last_open_exec_ts = 0.0
        self._close_open_chain_active = False
        self._last_close_bar_index = None
        self._last_entry_signal = None
        self._recover_in_progress = False
        self._recover_tp_unconfirmed = False
        self._post_recover_radar_pulse = False
        self._open_in_progress = False
        self._open_tp_unconfirmed = False
        self._last_signal_fp = None
        self._last_signal_fp_ts = 0.0
        self._defense_align_in_progress = False
        self._last_defense_align_ok_ts = 0.0
        self._last_rebuild_attempt_ts = 0.0  # 【修复】防止_rebuild_defenses死循环
        self._last_nuclear_realign_ts = 0.0  # 【修复】防止_nuclear_realign_tp死循环
        self._guardian_bad_streak = 0
        self._sentinel_grace_until = 0.0
        self._last_regime_cap_ts = 0.0
        self.shield_active = False
        self.shield_tiers_consumed = []
        self.tp_levels_consumed = []
        # v16.15（根因四修复）：记录 set-position-sltp 最近设置的止盈止损单
        # 用于 audit 检测 + 幂等撤销，防止重复挂/撤导致死循环
        self._shield_sltp_ord_id = ""
        self._shield_sltp_set_at = 0.0
        self._shield_cancelled_ids = set()
        # 规格 v1.0 §5.0：提前保本检查点
        self._early_be_checkpoint_done = False
        # 规格 v1.0 §3：永久硬止损冻结价
        self.frozen_hard_sl_px = 0.0
        # 规格 v1.0 §8-9：防御单幂等标签 + 退出所有权
        _own = blank_ownership_state()
        self.exit_ownership = str(_own["exit_ownership"])
        self.ownership_locked_at = float(_own["ownership_locked_at"] or 0)
        self._pending_order_tags = dict(_own["pending_order_tags"] or {})
        self._mutex_leg = ""
        self._last_shield_maintain_ts = 0.0
        self._shield_fail_streak = 0
        self._last_shield_fail_ts = 0.0
        self._shield_arm_notified = False
        self.shield_sized_qty = 0.0
        self._last_radar_report_ts = 0.0
        self._last_radar_report_sl = 0.0
        self._radar_activation_notified = False
        self._radar_armed_after_tp1 = False
        self._ws_tp1_fill_hint = False
        self._open_settled_qty = 0
        self.sizing_principal = 0.0
        self.tv_sl = 0.0
        self.tv_sl_ref = 0.0
        self._last_applied_tv_sl = 0.0
        self.tv_risk_pct = 0.0
        self.tv_qty_ratio = 1.0
        self.tv_entry_type = ENTRY_TYPE_OPEN
        self.base_qty = 0
        self.add_count = 0
        self._last_idle_takeover_ts = 0.0
        self.early_be_done = False
        self.breathing_coefficient = 1.0
        self._breath_ratio_history = []
        self._last_tier_label = ""  # P0：tier_label 从 webhook 透传

        # 自动重入相关字段（v1.0 规格对齐）
        self.adx_tier = 1
        self.radar_tier = 1
        self.radar_activation_frac = 0.78
        self.radar_pending_arm = True
        self.reentry_active = False
        self.reentry_attempt = 0
        self.reentry_limit_order_id = None
        self.reentry_limit_px = 0.0
        self.reentry_limit_deadline_ts = 0.0
        self.reentry_window_deadline_ts = 0.0
        self.reentry_order_tag = None
        self.tp_levels_consumed = []  # 确保已初始化

        self.state_file = os.path.join(
            _BASE_DIR, f'deepcoin_vps_state_{self.symbol.replace("-", "_")}.json'
        )
        legacy = os.path.join(_BASE_DIR, 'deepcoin_vps_state.json')
        if (
            self.symbol == "ETH-USDT-SWAP"
            and not os.path.exists(self.state_file)
            and os.path.exists(legacy)
        ):
            try:
                import shutil
                shutil.copy2(legacy, self.state_file)
                logger.info(f"📦 已迁移旧状态 → {self.state_file}")
            except Exception as e:
                logger.warning(f"旧状态迁移失败: {e}")
        try:
            self._pipeline_boot(exchange="deepcoin")
        except Exception as e:
            logger.warning(f"[{self.symbol}] pipeline boot: {e}")
        logger.info(
            f"🧠 深币 VPS [{DEEPCOIN_SUPERVISOR_VERSION}/{CLIENT_VERSION}] "
            f"{self.symbol} 军师已加载：双品种 · {self.leverage}x · pipeline"
        )
        self._start_signal_worker()
        self._start_idle_flat_patrol()

    def _init_reentry_runtime(self):
        RadarReentryMixin._init_reentry_runtime(self)

    def _start_idle_flat_patrol(self):
        """空仓待命时激进实盘巡检：反向强平 / 同向接管 / 人工异动 / 漏报全平 / 蚂蚁扫尾"""
        def loop():
            while True:
                time.sleep(IDLE_PATROL_INTERVAL_SEC)
                if self.monitoring:
                    continue
                if not self._lock.acquire(timeout=2.0):
                    continue
                try:
                    if self.monitoring:
                        continue
                    self._run_idle_live_reconcile()
                except Exception as e:
                    logger.error(f"空闲巡检异常: {e}")
                finally:
                    self._lock.release()

        threading.Thread(target=loop, daemon=True, name="idle-live-watch").start()

    def _book_thinks_active(self):
        return (
            self._safe_qty(self.watched_qty) > 0
            or self.current_side in ("LONG", "SHORT")
        )

    def _live_position_qty(self):
        pos = self._get_active_position()
        if not pos:
            return 0
        return self._safe_qty(pos.get("size"))

    def _confirm_position_flat(self, retries=None, delay=None):
        """REST 延迟/重启抖动时多次复核，避免误报空仓触发常规清场"""
        retries = retries if retries is not None else FLAT_CONFIRM_RETRIES
        delay = delay if delay is not None else FLAT_CONFIRM_DELAY_SEC
        for i in range(max(1, int(retries))):
            qty = self._live_position_qty()
            if qty > DUST_ORPHAN_CONTRACTS:
                return False
            if i + 1 < retries:
                time.sleep(delay)
        return self._live_position_qty() <= DUST_ORPHAN_CONTRACTS

    def _reconcile_stale_tp_consumed(self, initial_qty, live_qty, curr_px=0.0):
        initial_qty = self._safe_qty(initial_qty)
        live_qty = self._safe_qty(live_qty)
        consumed = list(getattr(self, "tp_levels_consumed", []) or [])
        if not consumed:
            return False
        inferred = self._infer_tp_consumed_sequential(initial_qty, live_qty, curr_px)
        if initial_qty <= live_qty and not inferred:
            logger.warning(
                f"⚠️ 清除陈旧 tp_levels_consumed={consumed} "
                f"(开单 {initial_qty}≈现仓 {live_qty}张，无减仓证据)"
            )
            self.tp_levels_consumed = []
            self._save_state()
            return True
        if 1 in consumed and self.tv_tps and self.tv_tps[0] > 0:
            if 1 not in inferred and not self._has_tp_limit_at_price(self.tv_tps[0]):
                logger.warning(
                    f"⚠️ TP1 已标记成交但无减仓/无 TP1 挂单 → 重置 {consumed}"
                )
                self.tp_levels_consumed = []
                self._save_state()
                return True
        return False

    def _live_defenses_need_repair(self, live_qty):
        audit = self._audit_tp_levels(live_qty)
        expected = audit.get("expected", 0)
        matched = audit.get("matched_full", 0)
        if expected > 0 and matched < expected:
            return True, audit
        sl = self._radar_sl_to_pass() or float(getattr(self, "tv_sl", 0) or 0)
        if sl > 0 and not self._has_trigger_sl_near(sl):
            return True, audit
        return False, audit

    def _resume_live_monitoring(self, pos, source="空闲巡检"):
        """账本与实盘一致但 monitoring=False → 恢复哨兵与雷达跟踪"""
        curr_px = deepcoin_client.get_current_price(self.symbol) or 0
        entry = float(pos.get("entry_price", 0) or self.watched_entry or 0)
        self._refresh_radar_state_on_recover(curr_px, entry)
        self.monitoring = True
        self._save_state()
        self._ensure_price_ws()
        self._ensure_sentinel_running()
        self._sentinel_grace_until = time.time() + SENTINEL_GRACE_AFTER_RECOVER_SEC
        side = "LONG" if pos.get("posSide") == "long" else "SHORT"
        qty = self._safe_qty(pos.get("size"))
        logger.info(
            f"📡 [{source}] 恢复实盘监督 {side} {qty}张 "
            f"| 雷达={'已激活' if self._is_radar_active() else '待命'}"
        )

    def _perform_live_takeover(self, pos, source="巡检", manual_open=False, qty_change=None):
        """
        实盘有仓但 VPS 未监控 / 防线缺失 → 补挂 TP123+硬止损，启动雷达哨兵。
        """
        real_amt = self._safe_qty(pos.get("size"))
        side = "LONG" if pos.get("posSide") == "long" else "SHORT"
        tv_side = self._resolve_tv_authoritative_side()
        if tv_side and side != tv_side:
            return False

        self.current_side = side
        if not self.last_tv_side:
            self.last_tv_side = tv_side or side

        if manual_open or self._safe_qty(getattr(self, "watched_qty", 0)) <= 0:
            self._reset_fresh_takeover_state()

        pos_ctx = {"side": side, "size": real_amt, "entry_price": float(pos.get("entry_price", 0))}
        self.watched_entry = float(pos.get("entry_price", 0))
        if manual_open:
            self.initial_qty = real_amt
            self.base_qty = int(real_amt)
            self.tp_levels_consumed = []
            saved_initial = real_amt
        else:
            saved_initial = self._resolve_open_initial_qty(real_amt, self.watched_entry)
            if saved_initial <= 0:
                saved_initial = real_amt
            if self.base_qty <= 0:
                self.base_qty = int(saved_initial or real_amt)
            self.initial_qty = saved_initial
        self.watched_qty = real_amt
        if not getattr(self, "open_regime", None):
            self.open_regime = self.regime
        if not getattr(self, "open_atr", None):
            self.open_atr = self.current_atr

        reconcile_notes = self._hydrate_tv_defense_context(pos_ctx)
        curr_px = deepcoin_client.get_current_price(self.symbol)
        stack = self._ensure_full_defense_stack(
            real_amt, self.watched_entry, curr_px,
            source=source, manual_fresh=manual_open,
        )
        audit = stack.get("audit") or {}
        health = stack.get("health") or {}
        sl_ok = stack.get("shield_ok", False)
        matched = audit.get("matched_full", 0)
        expected = audit.get("expected", 0)
        radar_active = self._is_radar_active()
        reconcile_notes.extend(stack.get("notes") or [])

        self.monitoring = True
        self._save_state()
        self._ensure_price_ws()
        log_source = source.split("·")[0].replace(" ", "")
        self._record_open_log(side, real_amt, self.watched_entry, source=log_source)
        self._ensure_sentinel_running()
        self._sentinel_grace_until = time.time() + SENTINEL_GRACE_AFTER_RECOVER_SEC
        self._last_idle_takeover_ts = time.time()

        verified = self._wait_verify(
            lambda: self._verify_position_qty(real_amt, side),
            retries=6,
            delay=0.5,
        )
        entry_px = float((verified or pos_ctx)["entry_price"])

        reconcile_txt = (" | " + " ; ".join(reconcile_notes)) if reconcile_notes else ""
        extra_notes = stack.get("notes") or []
        extra_txt = (" | " + " · ".join(extra_notes)) if extra_notes else ""
        verify_note = (
            f"[{source}] 接管 {real_amt}张 @ {entry_px:.2f} | "
            f"开单 {saved_initial}张 | TV {self.last_tv_side} | "
            f"止盈 {matched}/{expected} 档 | "
            f"tv_sl={float(getattr(self, 'tv_sl', 0) or 0):.2f} | "
            f"雷达={'已激活' if radar_active else '待命(TP1后)'} | "
            f"{self._format_audit_summary(audit)}{extra_txt}{reconcile_txt}"
        )
        if not verified:
            verify_note += " | REST 同步略延迟"

        if manual_open:
            self._call_dingtalk(
                dingtalk.report_manual_position_change,
                action_type=f"人工开仓 · {source}",
                old_qty=0,
                new_qty=real_amt,
                new_entry_price=entry_px,
                verify_note=verify_note,
                tp_audit=audit,
                verified=bool(verified),
            )
        elif qty_change:
            old_q, new_q, action_msg = qty_change
            self._call_dingtalk(
                dingtalk.report_manual_position_change,
                action_type=action_msg,
                old_qty=old_q,
                new_qty=new_q,
                new_entry_price=entry_px,
                verify_note=f"{source} | {verify_note}",
                tp_audit=audit,
                verified=bool(verified),
            )
        else:
            self._call_dingtalk(
                dingtalk.report_recover_takeover,
                side=side,
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
                pnl_label=health.get("pnl_label", ""),
                defense_plan=health.get("defense_plan", ""),
                shield_status=health.get("shield_status", ""),
                initial_qty=saved_initial,
                tp_consumed_levels=getattr(self, "tp_levels_consumed", []) or [],
            )

        if expected > 0 and matched < expected:
            dingtalk.report_system_alert(
                f"{source} · 止盈未完全对齐",
                f"{side} {real_amt}张 @ {entry_px:.2f} | "
                f"仅 {matched}/{expected} 档 | 哨兵将接力纠偏",
            )
        else:
            self._mark_defense_align_ok()

        logger.info(f"✅ [{source}] 实盘接管完成 {side} {real_amt}张 @ {entry_px:.2f}")
        return True

    def _run_idle_live_reconcile(self):
        """VPS 空仓/待命时周期性对账实盘：全场景生产级应对"""
        if self.monitoring or getattr(self, "_recover_in_progress", False):
            return
        if getattr(self, "_open_in_progress", False):
            return

        pos = self._get_active_position()
        live_qty = self._safe_qty(pos.get("size")) if pos else 0

        if live_qty <= 0:
            if self._book_thinks_active():
                if not self._confirm_position_flat():
                    logger.warning(
                        "📭 [空闲巡检] 首次无仓但复核仍有持仓 → 跳过误清场"
                    )
                    return
                curr_px = deepcoin_client.get_current_price(self.symbol)
                logger.warning("📭 [空闲巡检] 账本有仓且复核空仓 → 补发收网钉钉")
                self._handle_manual_flat_detected(
                    "仓位归零 (人工强平 / 止盈吃单 / 止损触发)",
                    curr_px=curr_px,
                )
            return

        if self._enforce_tv_direction_or_flat(pos, source="空闲巡检"):
            return

        if self._is_dust_qty(live_qty) or self._should_finalize_tp_victory(live_qty):
            if not self.current_side:
                self.current_side = "LONG" if pos.get("posSide") == "long" else "SHORT"
            logger.warning(
                f"🐜 [空闲巡检] 发现残量 {self.current_side} {live_qty}张 → 扫尾"
            )
            self._sweep_dust_and_finalize("空闲巡检：盘口蚂蚁仓自动扫平")
            return

        live_side = "LONG" if pos.get("posSide") == "long" else "SHORT"
        tv_side = self._resolve_tv_authoritative_side()
        if not tv_side or live_side != tv_side:
            return

        now = time.time()
        watched = self._safe_qty(self.watched_qty)

        if watched <= 0:
            if now - getattr(self, "_last_idle_takeover_ts", 0) < IDLE_TAKEOVER_COOLDOWN_SEC:
                return
            logger.warning(
                f"🔍 [空闲巡检] VPS空仓但实盘同向持仓 {live_side} {live_qty}张 "
                f"(TV={tv_side}) → 闪电接管+挂TP123"
            )
            self._perform_live_takeover(pos, source="空闲巡检", manual_open=True)
            return

        if self._is_material_qty_change(watched, live_qty):
            logger.warning(
                f"🔍 [空闲巡检] 人工异动 {watched} → {live_qty}张 → 重算TP123+止损"
            )
            curr_px = deepcoin_client.get_current_price(self.symbol)
            old_qty = watched
            self.watched_qty = live_qty
            self.watched_entry = float(pos.get("entry_price", 0))
            self.current_side = live_side
            change, result = self._handle_smart_qty_change(old_qty, live_qty, curr_px)
            if result:
                self._report_qty_change_dingtalk(old_qty, live_qty, result, change=change)
            self.monitoring = True
            self._save_state()
            self._ensure_sentinel_running()
            self._ensure_price_ws()
            self._last_idle_takeover_ts = now
            return

        need_repair, audit = self._live_defenses_need_repair(live_qty)
        if need_repair:
            if now - getattr(self, "_last_idle_takeover_ts", 0) < IDLE_TAKEOVER_COOLDOWN_SEC:
                return
            logger.warning(
                f"🔍 [空闲巡检] 防线不齐 ({audit.get('matched_full', 0)}/"
                f"{audit.get('expected', 0)} 档) → 续挂TP123+止损"
            )
            self._perform_live_takeover(pos, source="空闲巡检·防线续挂")
            return

        if not self.monitoring:
            self._resume_live_monitoring(pos, source="空闲巡检")

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
            fn(**legacy)
        except Exception as e:
            logger.debug(f"钉钉发送异常（静默）: {e}")

    @staticmethod
    def _call_telegram(fn, **kwargs):
        """TG 通知（静默失败，不阻断主流程）。"""
        try:
            fn(**kwargs)
        except Exception as e:
            logger.debug(f"TG 发送异常（静默）: {e}")

    def _start_signal_worker(self):
        if self._signal_worker_started:
            return
        self._signal_worker_started = True
        threading.Thread(target=self._signal_worker_loop, daemon=True, name="tv-signal-worker").start()

    def _signal_worker_loop(self):
        """同秒开+平：短停聚合后强制先平后开（终态必须有仓）。"""
        while True:
            first = self._signal_queue.get()
            batch = [first]
            settle = max(0.3, float(SAME_BAR_SETTLE_SEC))
            deadline = time.time() + settle
            while True:
                remain = deadline - time.time()
                if remain <= 0:
                    break
                try:
                    batch.append(self._signal_queue.get(timeout=remain))
                except queue.Empty:
                    break
            while True:
                try:
                    batch.append(self._signal_queue.get_nowait())
                except queue.Empty:
                    break
            batch = reorder_batch_close_then_open(batch)
            self._annotate_close_open_chain(batch)
            for payload in batch:
                try:
                    bi, sq = extract_seq_meta(payload or {})
                    act = str((payload or {}).get("action", "")).upper()
                    if bi is not None:
                        logger.info(
                            f"⚙️ [{self.symbol}] 时序消费 bar={bi} seq={sq} action={act}"
                        )
                    self._process_signal(payload)
                except Exception as e:
                    logger.error(f"❌ 信号处理异常: {e}", exc_info=True)
                finally:
                    try:
                        self._signal_queue.task_done()
                    except ValueError:
                        pass

    def _annotate_close_open_chain(self, batch):
        """同K线同时开+平 → 标记先平后开链（late close 抑制豁免）。"""
        by_bar = {}
        for p in batch or []:
            bi, sq = extract_seq_meta(p or {})
            act = str((p or {}).get("action", "")).strip().upper()
            key = bi if bi is not None else "_legacy"
            by_bar.setdefault(key, []).append((sq, act, p))
        for bi, items in by_bar.items():
            acts = [a for _, a, _ in items]
            has_close = any(is_close_action(a) for a in acts)
            has_open = any(is_open_action(a) for a in acts)
            if not (has_close and has_open):
                continue
            exec_chain = " → ".join(
                f"seq{sq if sq is not None else '?'}:{a}" for sq, a, _ in items
            )
            tv_by_seq = " → ".join(
                f"seq{sq if sq is not None else '?'}:{a}"
                for sq, a, _ in sorted(items, key=lambda x: (x[0] is None, x[0] or 0))
            )
            logger.info(
                f"📬 [{self.symbol}] 同秒强制先平后开 | 执行序 {exec_chain} | TV {tv_by_seq}"
            )
            if bi is not None:
                self._close_open_chain_active = True
                self._last_close_bar_index = int(bi)
            try:
                dingtalk.report_system_alert(
                    title=f"先平后开链·同秒开平 [{self.symbol}]",
                    detail=(
                        f"执行 {exec_chain} | TV按seq {tv_by_seq} | "
                        f"终态必须开仓（动作优先于seq）"
                    ),
                    level="信息",
                    suggestion="同秒开+平已强制先平后开；核对实盘终态有仓",
                )
            except Exception as e:
                logger.warning(f"先平后开链钉钉失败: {e}")

    def _signal_fingerprint(self, payload):
        action = str(payload.get("action", "")).strip().upper()
        if action.startswith("CLOSE"):
            return (
                action,
                str(payload.get("reason", ""))[:48],
                round(self._safe_float(payload.get("price"), 0), 2),
                round(self._safe_float(payload.get("pnl_pct"), 0), 2),
            )
        if action == "UPDATE_SL":
            return (
                action,
                str(payload.get("side", "")).upper(),
                round(self._safe_float(payload.get("tv_sl"), 0), 2),
            )
        if action == "UPDATE_TP":
            return (
                action,
                str(payload.get("side", "")).upper(),
                round(self._safe_float(payload.get("tv_tp1"), 0), 2),
                round(self._safe_float(payload.get("tv_tp2"), 0), 2),
                round(self._safe_float(payload.get("tv_tp3"), 0), 2),
            )
        if action in ("LONG", "SHORT"):
            return (
                action,
                normalize_entry_type(payload.get("entry_type")),
                round(self._safe_float(payload.get("tv_sl"), 0), 2),
                round(self._safe_float(payload.get("risk_pct"), 0), 3),
                round(self._safe_float(payload.get("qty_ratio"), 1.0), 3),
                round(self._safe_float(payload.get("price"), 0), 2),
            )
        return (
            action,
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
            "side": payload.get("side", ""),
            "pnl_pct": payload.get("pnl_pct"),
            "tv_sl": payload.get("tv_sl"),
            "entry_type": payload.get("entry_type"),
            "risk_pct": payload.get("risk_pct"),
            "leverage": payload.get("leverage"),
            "qty_ratio": payload.get("qty_ratio"),
            "ts": time.time(),
        }
        self.last_tv_signal = entry
        self._append_journal(TV_JOURNAL, entry)
        sizing_note = ""
        et = normalize_entry_type(payload.get("entry_type"))
        open_sizing_meta = None
        if et == ENTRY_TYPE_OPEN and self.tv_price > 0:
            _, open_sizing_meta = self._calc_vps_open_qty(self.tv_price)
            sizing_note = " | " + format_vps_sizing_note(open_sizing_meta, entry_type=ENTRY_TYPE_OPEN)
        elif et in (ENTRY_TYPE_PYRAMID, ENTRY_TYPE_PROFIT_ADD):
            _, sm = self._calc_vps_add_qty()
            sizing_note = " | " + format_vps_sizing_note(sm, entry_type=et)
        logger.info(
            f"📡 TV日志: {raw_action} R{self.regime} @ {self.tv_price:.2f} "
            f"TP={self.tv_tps}"
            + sizing_note
            + (f" | pnl={payload.get('pnl_pct')}%" if payload.get("pnl_pct") is not None else "")
        )
        self._call_dingtalk(
            dingtalk.report_tv_signal_received,
            action=raw_action,
            entry_type=payload.get("entry_type"),
            price=self.tv_price,
            regime=self.regime,
            atr=self.current_atr,
            tv_sl=payload.get("tv_sl"),
            risk_pct=payload.get("risk_pct"),
            leverage=EXCHANGE_LEVERAGE,
            qty_ratio=payload.get("qty_ratio"),
            reason=payload.get("reason", ""),
            vps_sizing_meta=open_sizing_meta,
        )
        # P1 修复：并行 TG 通知（静默失败）
        if raw_action in ("LONG", "SHORT") and self.tv_tps and self.tv_tps[0] > 0:
            self._call_telegram(
                telegram_notify.report_position_opened,
                side=raw_action,
                qty=int(self.watched_qty or self.base_qty or 0),
                entry=self.tv_price or 0,
                regime=self.regime,
                atr=self.current_atr,
                tps=self.tv_tps,
                tv_sl=payload.get("tv_sl") or 0,
                leverage=EXCHANGE_LEVERAGE,
                tier_label=getattr(self, "_last_tier_label", ""),
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

    def _load_active_tv_direction_from_journal(self):
        """从 TV 日志末尾向前：跳过尾部 CLOSE，取当前活跃周期的 LONG/SHORT"""
        if not os.path.exists(TV_JOURNAL):
            return None
        entries = []
        with open(TV_JOURNAL, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        for entry in reversed(entries):
            action = (entry.get("action") or "").upper()
            if action.startswith("CLOSE"):
                continue
            if action in ("LONG", "SHORT"):
                return action
            side = (entry.get("side") or "").upper()
            if side in ("LONG", "SHORT"):
                return side
        return None

    def _collect_credible_tv_directions(self):
        """可信 TV 方向集合：state 最新信号 > 日志末条 > 活跃周期"""
        sides = []
        seen = set()

        def add(raw):
            s = (raw or "").upper()
            if s in ("LONG", "SHORT") and s not in seen:
                seen.add(s)
                sides.append(s)

        if self.last_tv_signal:
            add(self.last_tv_signal.get("action"))
            add(self.last_tv_signal.get("side"))
        last_tv = self._load_last_journal_entry(TV_JOURNAL)
        if last_tv:
            add(last_tv.get("action"))
            add(last_tv.get("side"))
        add(self._load_active_tv_direction_from_journal())
        add(getattr(self, "last_tv_side", None))
        return sides

    def _live_aligns_with_credible_tv(self, live_side):
        """人工同向开仓：任一可信 TV 信源与实盘一致 → 应接管，禁止误杀"""
        return live_side in self._collect_credible_tv_directions()

    def _strict_tv_opposite_side(self, live_side):
        """仅当「最新 TV 指令」与实盘明确反向时才强平（不用陈旧全量扫描）"""
        for src in (self.last_tv_signal, self._load_last_journal_entry(TV_JOURNAL)):
            if not src:
                continue
            action = (src.get("action") or "").upper()
            if action in ("LONG", "SHORT") and action != live_side:
                return action
            side = (src.get("side") or "").upper()
            if side in ("LONG", "SHORT") and side != live_side:
                return side
        return None

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

    def _resolve_tv_authoritative_side(self):
        """TV 战略方向：优先最新信源，避免陈旧全量扫描误杀同向人工单"""
        if self.last_tv_signal:
            action = (self.last_tv_signal.get("action") or "").upper()
            if action in ("LONG", "SHORT"):
                return action
            side = (self.last_tv_signal.get("side") or "").upper()
            if side in ("LONG", "SHORT"):
                return side
        last_tv = self._load_last_journal_entry(TV_JOURNAL)
        if last_tv:
            tv_action = (last_tv.get("action") or "").upper()
            if tv_action in ("LONG", "SHORT"):
                return tv_action
            side = (last_tv.get("side") or "").upper()
            if side in ("LONG", "SHORT"):
                return side
            if tv_action.startswith("CLOSE"):
                active = self._load_active_tv_direction_from_journal()
                if active:
                    return active
        active = self._load_active_tv_direction_from_journal()
        if active:
            return active
        side = getattr(self, "last_tv_side", None)
        if side in ("LONG", "SHORT"):
            return side
        last_open_tv = self._load_last_tv_open_signal()
        if last_open_tv:
            tv_open = (last_open_tv.get("action") or "").upper()
            if tv_open in ("LONG", "SHORT"):
                return tv_open
        return None

    def _live_position_side(self, pos):
        if not pos:
            return None
        if pos.get("side") in ("LONG", "SHORT"):
            return pos["side"]
        pos_side = (pos.get("posSide") or "").lower()
        if pos_side == "long":
            return "LONG"
        if pos_side == "short":
            return "SHORT"
        return None

    def _enforce_tv_direction_or_flat(self, pos, source="sentinel"):
        """实盘与 TV 明确反向 → 核武全平；同向或信源不明 → 交给接管"""
        if not pos or self._safe_qty(pos.get("size")) <= 0:
            return False
        live_side = self._live_position_side(pos)
        if self._live_aligns_with_credible_tv(live_side):
            logger.info(
                f"✅ [{source}] 实盘 {live_side} 与可信 TV 信源同向 → 跳过强平，进入接管"
            )
            return False
        tv_opposite = self._strict_tv_opposite_side(live_side)
        if not tv_opposite or not live_side:
            return False
        reason = (
            f"人工反向手单 vs TV：实盘({live_side}) ≠ 最新TV({tv_opposite}) [{source}]"
        )
        logger.error(f"🚨 {reason} → 核武全平强制对齐 TV")
        verify_note = (
            f"触发源: {source} | 最新TV {tv_opposite} | 实盘反向 {live_side} | "
            "已核武全平，账本归零待命"
        )
        self._close_all(
            reason,
            force_align=(live_side, tv_opposite),
            force_verify_note=verify_note,
        )
        return True

    def _journal_tp_prices(self, entry):
        """从日志条目解析 TP123（支持 tv_tps 列表或 tv_tp1/2/3 字段）"""
        if not entry:
            return [0.0, 0.0, 0.0]
        if entry.get("tv_tps"):
            return self._sanitize_tp_prices(entry.get("tv_tps", []))
        return self._sanitize_tp_prices([
            entry.get("tv_tp1"), entry.get("tv_tp2"), entry.get("tv_tp3"),
        ])

    def _hydrate_tv_defense_context(self, pos):
        """
        人工开仓 / 重启接管：从 TV 日志补全 tp/sl/regime/atr，避免字段缺失导致接管异常。
        """
        notes = []
        side = self.current_side
        if not side and pos:
            side = "LONG" if pos.get("posSide") == "long" else "SHORT"
        entry = float(pos.get("entry_price", 0) or self.watched_entry or 0)
        if not side:
            return notes

        self.current_side = side
        if not self.last_tv_side:
            self.last_tv_side = side

        sources = [
            self.last_tv_signal,
            self._load_last_journal_entry(TV_JOURNAL),
            self._load_last_tv_open_signal(),
            self._load_last_journal_entry(OPEN_JOURNAL),
        ]

        for src in sources:
            if not src:
                continue
            if src.get("regime"):
                self.regime = int(src["regime"])
            if src.get("atr"):
                self.current_atr = float(src["atr"])
            if float(self.tv_price or 0) <= 0 and float(src.get("price", 0) or 0) > 0:
                self.tv_price = float(src["price"])

        tp_ok = sum(1 for t in (self.tv_tps or []) if t > 0)
        if not self._tp_prices_valid_for_side(side, entry):
            if tp_ok >= 3:
                logger.warning(
                    f"⚠️ 接管: 账本 TP{self.tv_tps} 与 {side}@{entry:.2f} 方向不符 → 重载"
                )
            self.tv_tps = [0.0, 0.0, 0.0]
            tp_ok = 0
        if tp_ok < 3:
            for src in sources:
                src_side = (src.get("action") or src.get("side") or "").upper()
                if src_side in ("LONG", "SHORT") and side and src_side != side:
                    continue
                tps = self._journal_tp_prices(src)
                if (
                    sum(1 for t in tps if t > 0) >= 3
                    and self._tp_prices_valid_for_side(side, entry, tps)
                ):
                    self.tv_tps = tps
                    notes.append(f"补全TP123 {tps}")
                    break

        if sum(1 for t in (self.tv_tps or []) if t > 0) < 3 and entry > 0 and self.current_atr > 0:
            payload = enrich_entry_tp_prices(
                side, entry, self.current_atr, self.regime, {},
            )
            tps = self._sanitize_tp_prices([
                payload.get("tv_tp1"), payload.get("tv_tp2"), payload.get("tv_tp3"),
            ])
            if self._tp_prices_valid_for_side(side, entry, tps):
                self.tv_tps = tps
                notes.append(f"ATR本地补全TP {tps}")

        if float(getattr(self, "tv_sl", 0) or 0) <= 0:
            for src in sources:
                sl = float(src.get("tv_sl", 0) or 0)
                if sl > 0:
                    self.tv_sl = sl
                    notes.append(f"补全tv_sl={sl:.2f}")
                    break

        if float(getattr(self, "tv_sl", 0) or 0) <= 0 and entry > 0 and self.current_atr > 0:
            sl_m = {1: 0.9, 2: 1.05, 3: 1.10, 4: 1.25}.get(int(self.regime or 3), 1.10)
            if side == "LONG":
                self.tv_sl = round(entry - self.current_atr * sl_m, 2)
            else:
                self.tv_sl = round(entry + self.current_atr * sl_m, 2)
            notes.append(f"ATR估算tv_sl={self.tv_sl:.2f}")

        self.monitoring = True
        self._save_state()
        for n in notes:
            logger.info(f"💧 接管上下文补全: {n}")
        return notes

    def _reset_fresh_takeover_state(self):
        """人工/孤儿接管：清空陈旧 TP/雷达状态，避免误判已成交导致只挂 TP12"""
        self.tp_levels_consumed = []
        self.shield_tiers_consumed = []
        self._radar_activation_notified = False

        # v13.91.0: freeze ADX activation ratio/price on open/dormant begin
        try:
            from reentry_profiles import (
                radar_activation_ratio_from_adx,
                radar_activation_price_adx,
            )
            _adx = float(getattr(self, "last_adx", 0) or getattr(self, "radar_activation_adx", 0) or 25.0)
            _ratio = radar_activation_ratio_from_adx(_adx)
            self.radar_activation_frac = float(_ratio)
            self.radar_activation_adx = float(_adx)
            _entry = float(getattr(self, "watched_entry", 0) or getattr(self, "cycle_entry", 0) or 0)
            _atr = float(getattr(self, "open_atr", 0) or getattr(self, "cycle_open_atr", 0) or 0)
            if _atr <= 0:
                try:
                    _atr = float(self._get_locked_initial_atr() or 0)
                except Exception:
                    _atr = 0.0
            if _entry > 0 and _atr > 0:
                _px = radar_activation_price_adx(
                    getattr(self, "current_side", None), _entry, _atr, adx=_adx, ratio=_ratio,
                )
                if _px > 0:
                    self.radar_activation_price = float(_px)
        except Exception as _e:
            logger.debug(f"radar ADX freeze skip: {_e}")

        self._shield_handoff_notified = False
        self.shield_active = False
        self.shield_sized_qty = 0.0
        self._shield_sltp_ord_id = ""
        self._shield_sltp_set_at = 0.0
        self._shield_cancelled_ids = set()
        self.tv_tps = [0.0, 0.0, 0.0]
        self.tv_sl = 0.0
        if not getattr(self, "open_regime", None):
            self.open_regime = self.regime
        if not getattr(self, "open_atr", None):
            self.open_atr = self.current_atr

    def _tp_prices_valid_for_side(self, side=None, entry=None, tp_list=None):
        side = side or self.current_side
        entry = float(entry or self.watched_entry or 0)
        tp_list = tp_list if tp_list is not None else (self.tv_tps or [])
        return validate_tp_prices_for_side(side, entry, tp_list)

    def _reload_tv_tp_prices_from_sources(self, side, entry):
        entry = float(entry or 0)
        side = str(side or "").strip().upper()
        sources = [
            self.last_tv_signal,
            self._load_last_journal_entry(TV_JOURNAL),
            self._load_last_tv_open_signal(),
            self._load_last_journal_entry(OPEN_JOURNAL),
        ]
        for src in sources:
            if not src:
                continue
            src_side = (src.get("action") or src.get("side") or "").upper()
            if src_side in ("LONG", "SHORT") and side and src_side != side:
                continue
            tps = self._journal_tp_prices(src)
            if sum(1 for t in tps if t > 0) >= 3 and self._tp_prices_valid_for_side(side, entry, tps):
                return tps, f"TV日志TP {tps}"
        return None, ""

    def _ensure_tp123_prices_from_tv(self, entry):
        """以实盘 entry + open_atr/regime 确保 TP123 三价齐全（人工开仓必跑）"""
        side = self.current_side
        entry = float(entry or self.watched_entry or 0)
        if self._tp_prices_valid_for_side(side, entry):
            return True

        reloaded, note = self._reload_tv_tp_prices_from_sources(side, entry)
        if reloaded:
            self.tv_tps = reloaded
            logger.info(f"📐 接管重载 TP123 @ entry={entry:.2f} → {self.tv_tps} ({note})")
            self._save_state()
            return True

        if sum(1 for t in (self.tv_tps or []) if t > 0) >= 3:
            logger.warning(
                f"⚠️ 陈旧 TP 价位与 {side} @ {entry:.2f} 方向不符 → 丢弃重算"
            )
            self.tv_tps = [0.0, 0.0, 0.0]

        atr = float(getattr(self, "open_atr", None) or self.current_atr or 30)
        regime = int(getattr(self, "open_regime", None) or self.regime or 3)
        if not side or entry <= 0:
            return False
        payload = enrich_entry_tp_prices(side, entry, atr, regime, {})
        self.tv_tps = self._sanitize_tp_prices([
            payload.get("tv_tp1"), payload.get("tv_tp2"), payload.get("tv_tp3"),
        ])
        ok = self._tp_prices_valid_for_side(side, entry)
        if ok:
            logger.info(f"📐 人工接管 ATR 补全 TP123 @ entry={entry:.2f} → {self.tv_tps}")
        return ok

    def _resolve_defense_stop_for_audit(self, radar_sl=None):
        """审计用止损价：TP1 前仅 tv_sl；TP1 后雷达+tv_sl 合并"""
        if radar_sl and float(radar_sl) > 0:
            return float(radar_sl)
        tracked = self._radar_sl_to_pass()
        if tracked and self._tp1_filled_verified():
            return tracked
        return self._shield_stop_price()

    def _normalize_tp_qty_map(self, qty_map, live_qty):
        """
        规格 v1.0 §6.2 蚂蚁单处理 + §9.4 零头并入：
        不足最小张数(MIN_TP_LEG_QTY)的非TP3档零头，全部并入TP3（雷达管理的70%仓位）。
        不满足最小下单量的非TP3档直接市价平仓，不挂单。
        """
        if not qty_map:
            return qty_map
        live_qty = int(live_qty or 0)
        levels = sorted(qty_map.keys())
        if len(levels) <= 1:
            return qty_map

        # 最后一档默认是 TP3（key=3），但健壮处理任何情况
        last_level = levels[-1]

        carry = 0
        out = dict(qty_map)
        for lvl in levels[:-1]:  # TP1, TP2（不碰 TP3）
            q = int(out.get(lvl, 0) or 0)
            if 0 < q < MIN_TP_LEG_QTY:
                carry += q
                out[lvl] = 0  # 不挂单，留给市价平仓

        # 零头并入 TP3（雷达管理的 70%）
        if carry > 0:
            out[last_level] = int(out.get(last_level, 0) or 0) + carry

        # 总数不能超过实际持仓
        total = sum(int(out.get(l, 0) or 0) for l in levels)
        if total > live_qty:
            out[last_level] = max(int(out.get(last_level, 0) or 0) - (total - live_qty), 0)

        return out

    def _ensure_full_defense_stack(self, live_qty, entry, curr_px, source="接管", manual_fresh=False):
        """
        全链防线：TP123 比例限价 + TV tv_sl 硬止损；TP1 成交前雷达待命（呼吸空间）。
        """
        notes = []
        live_qty = int(self._resolve_live_qty(live_qty) or live_qty)
        entry = float(entry or self.watched_entry or 0)
        curr_px = float(curr_px or deepcoin_client.get_current_price(self.symbol) or 0)

        if manual_fresh:
            self._reset_fresh_takeover_state()

        self._disarm_premature_radar(live_qty, curr_px, source=source)
        self._reconcile_stale_tp_consumed(
            self._trusted_initial_qty(live_qty, entry), live_qty, curr_px,
        )
        trusted_initial = self._trusted_initial_qty(live_qty, entry)
        if self._safe_qty(self.initial_qty) != trusted_initial:
            self.initial_qty = trusted_initial
        self._sanitize_tp_consumed(trusted_initial, live_qty, curr_px)
        if not self._ensure_tp123_prices_from_tv(entry):
            notes.append("TP123补全失败")
        if float(getattr(self, "tv_sl", 0) or 0) <= 0:
            pos_ctx = {"side": self.current_side, "size": live_qty, "entry_price": entry}
            self._hydrate_tv_defense_context(pos_ctx)
        if float(getattr(self, "tv_sl", 0) or 0) <= 0 and entry > 0:
            atr = float(getattr(self, "open_atr", None) or self.current_atr or 30)
            regime = int(getattr(self, "open_regime", None) or self.regime or 3)
            sl_m = {1: 0.9, 2: 1.05, 3: 1.10, 4: 1.25}.get(regime, 1.10)
            if self.current_side == "LONG":
                self.tv_sl = round(entry - atr * sl_m, 2)
            elif self.current_side == "SHORT":
                self.tv_sl = round(entry + atr * sl_m, 2)
            if float(getattr(self, "tv_sl", 0) or 0) > 0:
                notes.append(f"boot tv_sl={self.tv_sl:.2f}")
                self._save_state()

        self._enforce_pre_tp1_radar_standby(live_qty, curr_px, source=source)

        try:
            cap = self._radar_enforce_regime_cap(live_qty, curr_px, force=True)
            if cap:
                live_qty = int(cap["new_qty"])
                self.watched_qty = live_qty
                if int(self.initial_qty or 0) <= live_qty:
                    self.initial_qty = live_qty
        except Exception as e:
            logger.warning(f"接管档位限额跳过: {e}")

        tp_repair = {"repaired": False}
        try:
            tp_repair = self._repair_partial_tp_on_recover(
                live_qty, entry, trusted_initial, curr_px,
            )
            if tp_repair.get("repaired"):
                notes.extend(tp_repair.get("actions") or [])
        except Exception as e:
            logger.error(f"接管TP修复跳过: {e}")
            notes.append(f"TP修复跳过:{e}")

        self._refresh_radar_state_on_recover(curr_px, entry)
        radar_sl = self._radar_sl_to_pass() if self._tp1_filled_verified() else None

        if tp_repair.get("repaired") and tp_repair.get("result"):
            result = tp_repair["result"]
        else:
            result = self._enforce_defense_alignment(
                live_qty, entry, dynamic_sl=radar_sl,
                reason=f"{source} TP123+tv_sl", rounds=3, recover_mode=True,
            )

        stop_check = self._resolve_defense_stop_for_audit(radar_sl)
        if not self._tp1_filled_verified(live_qty, curr_px):
            radar_sl = None
            self._enforce_pre_tp1_radar_standby(live_qty, curr_px, source=source)
            stop_check = self._shield_stop_price()
        shield_ok = self._maintain_hard_shield(live_qty, curr_px, force=True)
        if radar_sl and not self._has_trigger_sl_near(radar_sl):
            shield_ok = self._ensure_radar_sl(live_qty, radar_sl) or shield_ok
        audit = self._wait_defense_settled(live_qty, stop_check)

        if not self._tp_audit_ok(audit) or (
            stop_check and not self._has_trigger_sl_near(stop_check)
        ):
            logger.warning(
                f"⚠️ [{source}] TP/止损未齐 ({audit.get('matched_full', 0)}/"
                f"{audit.get('expected', 0)}) → 核武重挂 TP123+tv_sl"
            )
            audit = self._nuclear_realign_tp(live_qty, entry, dynamic_sl=radar_sl, rounds=3)
            shield_ok = self._maintain_hard_shield(live_qty, curr_px, force=True)
            if radar_sl and not self._has_trigger_sl_near(radar_sl):
                shield_ok = self._ensure_radar_sl(live_qty, radar_sl) or shield_ok
            stop_check = self._resolve_defense_stop_for_audit(radar_sl)
            audit = self._wait_defense_settled(live_qty, stop_check)

        health = self._build_recover_health_report(
            {"side": self.current_side, "size": live_qty, "entry_price": entry},
            curr_px, audit,
        )

        if self._tp1_filled_verified(live_qty, curr_px) and (
            health.get("should_radar") or health.get("radar_active")
        ):
            self._process_radar_trailing(live_qty, curr_px)
            sl = self._radar_sl_to_pass()
            if sl and not self._has_trigger_sl_near(sl):
                self._ensure_radar_sl(live_qty, sl)
        else:
            progress = self._radar_activation_progress(curr_px) if curr_px > 0 else 0.0
            gate = float(self._radar_activation_price() or 0)
            logger.info(
                f"📡 [{source}] 雷达待命(等待价格到达阈值) 进度{progress:.0%} | "
                f"阈值={gate:.2f} | tv_sl={float(getattr(self, 'tv_sl', 0) or 0):.2f} | "
                f"TP {audit.get('matched_full', 0)}/{audit.get('expected', 0)}"
            )

        if self._tp_audit_ok(audit):
            self._mark_defense_align_ok()
        else:
            exp = audit.get("expected", 0)
            if exp and audit.get("matched_full", 0) < exp:
                dingtalk.report_system_alert(
                    f"{source} · 止盈未完全对齐",
                    f"{self.current_side} {live_qty}张 @ {entry:.2f} | "
                    f"仅 {audit.get('matched_full', 0)}/{exp} 档 | "
                    f"tv_sl={float(getattr(self, 'tv_sl', 0) or 0):.2f} | 哨兵接力",
                )

        self._post_recover_radar_pulse = True
        return {
            "audit": audit,
            "result": result,
            "health": health,
            "shield_ok": shield_ok,
            "notes": notes,
        }

    def _smart_recover_defenses(self, real_amt, entry, dynamic_sl=None):
        """重启智能补挂：审计齐全则跳过，缺档增量补，避免重复挂单"""
        matched, pending, expected, rebuilt = self._ensure_defenses_on_recover(
            real_amt, entry, dynamic_sl=dynamic_sl,
        )
        audit = self._audit_tp_levels(real_amt)
        return {
            "matched": matched,
            "expected": expected,
            "pending_prices": pending,
            "rebuilt": rebuilt,
            "audit": audit,
        }

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

        if saved_watched <= 0 and real_amt > 0:
            reconcile["manual_open"] = True
            self.initial_qty = real_amt
            self.tp_levels_consumed = []
            if int(getattr(self, "base_qty", 0) or 0) <= 0:
                self.base_qty = int(real_amt)
            notes.append(
                f"人工开仓(重启): 账本空仓 → 实盘 {real_amt}张 {side}，已接管为基准仓"
            )
        elif saved_watched > 0 and real_amt > 0:
            entry_px = float(pos.get("entry_price", 0) or 0)
            je = float(last_open.get("entry", 0) or 0) if last_open else 0.0
            entry_tol = max(3.0, entry_px * 0.003) if entry_px > 0 else 3.0
            if last_open and je > 0 and entry_px > 0 and abs(entry_px - je) > entry_tol:
                reconcile["manual_open"] = True
                self.initial_qty = real_amt
                self.tp_levels_consumed = []
                self.base_qty = int(real_amt)
                notes.append(
                    f"人工新开(入场偏差): 日志 {je:.2f} vs 实盘 {entry_px:.2f} → 重置 TP123"
                )
            elif saved_initial > real_amt:
                trusted = self._trusted_initial_qty(real_amt, entry_px)
                if trusted <= real_amt:
                    reconcile["manual_open"] = True
                    self.initial_qty = real_amt
                    self.tp_levels_consumed = []
                    notes.append(
                        f"人工/重置(重启): 陈旧 initial={saved_initial} > 现仓 {real_amt}张 "
                        f"但无日志锚定 → 全链 TP123"
                    )

        if saved_watched > 0 and self._is_material_qty_change(saved_watched, real_amt):
            action_msg = (
                "手动加仓" if real_amt > saved_watched
                else "部分止盈吃单 / 手动减仓"
            )
            reconcile["qty_manual_change"] = (saved_watched, real_amt, action_msg)
            notes.append(f"人工异动(重启): {saved_watched}张 → {real_amt}张 ({action_msg})")

        if not self.last_tv_side:
            if not reconcile["direction_mismatch"]:
                self.last_tv_side = side
        elif side != self.last_tv_side and not reconcile["tv_close"]:
            if self._live_aligns_with_credible_tv(side):
                notes.append(
                    f"陈旧TV方向{self.last_tv_side}与实盘{side}不一致，"
                    f"但最新TV信源同向 → 以接管为准"
                )
                self.last_tv_side = side
            else:
                reconcile["direction_mismatch"] = True
                if not any("方向背离" in n for n in notes):
                    notes.append(f"方向背离: 实盘{side} vs TV指令{self.last_tv_side}")

        for n in notes:
            logger.warning(f"🔎 重启对账: {n}")
        return reconcile

    def _trusted_initial_qty(self, live_qty, entry=None):
        live_qty = self._safe_qty(live_qty)
        entry = float(entry or self.watched_entry or 0)
        last_open = self._load_last_journal_entry(OPEN_JOURNAL)
        if last_open:
            jq = self._safe_qty(last_open.get("qty", 0))
            je = float(last_open.get("entry", 0) or 0)
            entry_tol = max(3.0, entry * 0.003) if entry > 0 else 3.0
            if jq > 0 and (entry <= 0 or je <= 0 or abs(entry - je) <= entry_tol):
                qty_tol = max(1, int(live_qty * 0.02)) if live_qty > 0 else 1
                if live_qty > 0 and jq > live_qty + qty_tol:
                    logger.warning(
                        f"📖 OPEN日志 {jq}张 @ {je:.2f} > 现仓 {live_qty}张 "
                        f"→ 视作全新人工首仓，不用日志锚定"
                    )
                    return live_qty
                return jq
        saved = self._safe_qty(self.initial_qty)
        if 0 < saved <= live_qty:
            return max(saved, live_qty)
        return live_qty if live_qty > 0 else saved

    def _resolve_open_initial_qty(self, live_qty, entry=None):
        live_qty = self._safe_qty(live_qty)
        trusted = self._trusted_initial_qty(live_qty, entry)
        saved = self._safe_qty(self.initial_qty)
        if saved > live_qty and trusted <= live_qty:
            logger.warning(
                f"📖 丢弃陈旧 initial_qty={saved}张 → 锚定 {trusted}张 "
                f"(现仓 {live_qty}张，无日志锚定减仓证据)"
            )
            self.initial_qty = trusted
            self.tp_levels_consumed = []
            self._save_state()
        elif trusted > live_qty:
            logger.warning(
                f"📖 开单量 {trusted}张 > 现仓 {live_qty}张 → 锚定现仓，清除伪 TP 标记"
            )
            self.initial_qty = live_qty
            self.tp_levels_consumed = []
            self._save_state()
            return live_qty
        return trusted if trusted > 0 else live_qty

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
                    "initial_stop": float(getattr(self, "initial_stop", 0) or 0),
                    "breathing_coefficient": float(
                        getattr(self, "breathing_coefficient", 1.0) or 1.0
                    ),
                    "breath_ratio_history": list(
                        getattr(self, "_breath_ratio_history", []) or []
                    ),
                    "breakeven_phase": bool(getattr(self, "breakeven_phase", False)),
                    "last_open_exec_ts": float(
                        getattr(self, "_last_open_exec_ts", 0) or 0
                    ),
                    "shield_active": getattr(self, "shield_active", False),
                    "shield_tiers_consumed": list(getattr(self, "shield_tiers_consumed", []) or []),
                    "tp_levels_consumed": list(getattr(self, "tp_levels_consumed", []) or []),
                    "shield_sized_qty": float(getattr(self, "shield_sized_qty", 0) or 0),
                    "sizing_principal": float(getattr(self, "sizing_principal", 0) or 0),
                    "tv_sl": float(getattr(self, "tv_sl", 0) or 0),
                    "tv_sl_ref": float(getattr(self, "tv_sl_ref", 0) or 0),
                    "last_applied_tv_sl": float(
                        getattr(self, "_last_applied_tv_sl", 0) or 0
                    ),
                    "tv_risk_pct": float(getattr(self, "tv_risk_pct", 0) or 0),
                    "tv_qty_ratio": float(getattr(self, "tv_qty_ratio", 1.0) or 1.0),
                    "tv_entry_type": getattr(self, "tv_entry_type", ENTRY_TYPE_OPEN),
                    "leverage": EXCHANGE_LEVERAGE,
                    "tv_sizing_leverage": float(
                        getattr(self, "tv_sizing_leverage", EXCHANGE_LEVERAGE) or EXCHANGE_LEVERAGE
                    ),
                    "base_qty": int(getattr(self, "base_qty", 0) or 0),
                    "add_count": int(getattr(self, "add_count", 0) or 0),
                    "radar_armed_after_tp1": bool(
                        getattr(self, "_radar_armed_after_tp1", False)
                    ),
                    "open_settled_qty": int(
                        getattr(self, "_open_settled_qty", 0) or 0
                    ),
                    # 规格 v1.0 §3：永久硬止损冻结价
                    "frozen_hard_sl_px": float(getattr(self, "frozen_hard_sl_px", 0) or 0),
                    # 规格 v1.0 §5.0：提前保本检查点
                    "_early_be_checkpoint_done": bool(getattr(self, "_early_be_checkpoint_done", False)),
                    # 规格 v1.0 §8-9：防御单幂等标签 + 退出所有权
                    "exit_ownership": str(getattr(self, "exit_ownership", "NONE") or "NONE"),
                    "ownership_locked_at": float(getattr(self, "ownership_locked_at", 0) or 0),
                    "pending_order_tags": dict(getattr(self, "_pending_order_tags", {}) or {}),
                    "mutex_leg": str(getattr(self, "_mutex_leg", "") or ""),
                    "pipeline": self._pipeline_state_blob(),
                    # 自动重入字段（v1.0 规格对齐）
                    "reentry_state": self._reentry_state_dict(),
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

    def _get_active_position(self, prefer_ws=False):
        # 限流重试：关键持仓查询增加重试，避免静默期返回None导致开仓失败
        # prefer_ws参数保留兼容性但目前不实际使用WebSocket
        for attempt in range(5):
            try:
                res = deepcoin_client.get_position_info(self.symbol)
                if res and isinstance(res.get('data'), list):
                    for p in res['data']:
                        if self._safe_qty(p.get("pos")) > 0:
                            return {
                                "size": self._safe_qty(p.get("pos")),
                                "entry_price": round(float(p.get("avgPx", p.get("price", 0)) or 0), 2),
                                "posSide": p.get("posSide", "long").lower(),
                            }
                    return None
                # API返回空数据也返回None
                if res is not None:
                    return None
            except Exception as e:
                logger.warning(f"[{self.symbol}] 获取持仓异常: {e}")
            if attempt < 4:
                time.sleep(0.5 + attempt * 0.3)
        return None

    def _get_all_positions(self):
        """获取所有方向的持仓（对冲模式下可能同时有多空持仓）"""
        # 限流重试 + 静默期等待（防止限流导致查不到仓位拒绝开仓）
        for attempt in range(5):
            positions = []
            try:
                res = deepcoin_client.get_position_info(self.symbol)
                if res and isinstance(res.get('data'), list):
                    for p in res['data']:
                        qty = self._safe_qty(p.get("pos"))
                        if qty > 0:
                            positions.append({
                                "size": qty,
                                "entry_price": round(float(p.get("avgPx", p.get("price", 0)) or 0), 2),
                                "posSide": p.get("posSide", "long").lower(),
                            })
                    return positions
                # API返回空数据也返回空列表，不重试
                if res is not None:
                    return []
            except Exception as e:
                logger.warning(f"[{self.symbol}] 获取持仓异常: {e}")
            if attempt < 4:
                time.sleep(0.5 + attempt * 0.3)
        return []

    def _verify_flat(self):
        """验证所有方向的持仓都已清空（对冲模式兼容）- 带重试防止限流误判"""
        # 静默期/限流时重试等待，避免误判导致光平仓不开仓
        for attempt in range(3):
            positions = self._get_all_positions()
            if len(positions) == 0:
                return True
            if attempt < 2:
                time.sleep(0.3 + attempt * 0.2)
        return len(positions) == 0

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
                eff_risk = None
                if "开仓前" in reason and self.tv_price > 0:
                    t, meta = self._calc_vps_open_qty(self.tv_price)
                    target_qty = t
                    eff_risk = float(meta.get("effective_risk_pct", VPS_RISK_PCT) or VPS_RISK_PCT) / 100.0
                    vps_meta = meta
                else:
                    vps_meta = None
                try:
                    dingtalk.report_principal_snapshot(
                        reason=reason,
                        principal=principal,
                        regime=self.regime if "开仓前" in reason else None,
                        margin_pct=eff_risk,
                        target_qty=target_qty,
                        leverage=EXCHANGE_LEVERAGE,
                        vps_sizing_meta=vps_meta,
                    )
                except Exception as e:
                    logger.warning(f"本金快照钉钉跳过: {e}")
        return principal

    def _resolve_cap_sizing_base(self, wallet_balance=None):
        """
        档位额度唯一基数：sizing_principal 快照；下单按 VPS 风险系数公式。
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
        """VPS OPEN 公式 → 仓位上限（已废弃 margin% 口径）"""
        regime = int(regime if regime is not None else self.regime)
        qty, meta = self._calc_vps_open_qty(curr_px, regime=regime)
        balance = float(meta.get("principal", 0) or self._resolve_cap_sizing_base())
        order_amount = float(meta.get("order_amount", 0) or 0)
        eff = float(meta.get("effective_risk_pct", VPS_RISK_PCT) or VPS_RISK_PCT) / 100.0
        return int(qty or 0), balance, order_amount, eff, regime

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

    def _max_add_times_for_regime(self, regime=None):
        """TV v6.9.93：加仓次数上限跟当前信号档位"""
        return get_regime_max_add_times(int(regime if regime is not None else self.regime or 3))

    def _apply_tv_sizing_params(self, payload):
        """解析 entry_type：OPEN 由 VPS 自主 sizing；加仓用 TV qty_ratio × 首仓 base_qty"""
        self.tv_entry_type = normalize_entry_type(payload.get("entry_type"))
        if self.tv_entry_type in (ENTRY_TYPE_PYRAMID, ENTRY_TYPE_PROFIT_ADD):
            self.tv_qty_ratio = resolve_tv_add_qty_ratio(
                self.regime,
                self._safe_float(payload.get("qty_ratio"), None),
            )
        else:
            self.tv_qty_ratio = 1.0
        self.leverage = EXCHANGE_LEVERAGE
        self._save_state()
        max_add = self._max_add_times_for_regime()
        logger.info(
            f"📐 TV参数: type={self.tv_entry_type} "
            f"| VPS风险={VPS_RISK_PCT}% R{self.regime} "
            f"| 加仓=base×{self.tv_qty_ratio:.2f}(TV) 最多{max_add}次 "
            f"| 交易所={EXCHANGE_LEVERAGE}x"
        )

    def _calc_vps_open_qty(self, curr_px, regime=None):
        principal = self._resolve_cap_sizing_base()
        px = float(curr_px or self.tv_price or 0)
        sl = float(getattr(self, "tv_sl", 0) or 0)
        qty, meta = compute_vps_open_qty(
            principal, px, sl, int(regime if regime is not None else self.regime),
            leverage=EXCHANGE_LEVERAGE,
            face_value=self.face_value,
            min_qty=1,
        )
        meta["principal"] = principal
        meta["symbol"] = self.symbol
        return int(qty or 0), meta

    def _other_symbols_notional(self, exclude_symbol=None):
        """账户其它品种名义敞口合计（不含本品种）。"""
        exclude = str(exclude_symbol or self.symbol)
        by_sym, total = deepcoin_client.get_all_swap_position_notionals()
        other = 0.0
        for sym, notion in (by_sym or {}).items():
            if str(sym) == exclude:
                continue
            other += float(notion or 0)
        return round(other, 2), by_sym, total

    def _assert_notional_cap_or_reject(self, qty, price, sizing_meta=None):
        """双品种硬顶：其它品种名义 + 本笔名义 ≤ equity×9。"""
        equity = float(
            (sizing_meta or {}).get("principal")
            or self._resolve_cap_sizing_base()
            or 0
        )
        new_notional = float(qty or 0) * float(self.face_value or 0.1) * float(price or 0)
        other, by_sym, all_total = self._other_symbols_notional(self.symbol)
        existing = other
        ok, meta = check_total_notional_cap(
            equity, existing, new_notional, mult=MAX_TOTAL_NOTIONAL_MULT,
        )
        meta["by_symbol"] = by_sym
        meta["symbol"] = self.symbol
        if ok:
            logger.info(
                f"📐 敞口校验通过 {self.symbol}: 其它 {existing:.0f}U + 本笔 {new_notional:.0f}U "
                f"= {meta['total_notional']:.0f}U ≤ 本金 {equity:.0f}U×{MAX_TOTAL_NOTIONAL_MULT:.0f}"
            )
            return True, meta
        logger.error(
            f"🚫 敞口硬顶拦截 {self.symbol}: 其它 {existing:.0f}U + 本笔 {new_notional:.0f}U "
            f"= {meta['total_notional']:.0f}U > 上限 {meta['cap']:.0f}U "
            f"(本金 {equity:.0f}U×{MAX_TOTAL_NOTIONAL_MULT:.0f}) | 盘口 {by_sym}"
        )
        dingtalk.report_system_alert(
            f"开仓拦截·名义敞口超限 [{self.symbol}]",
            f"本金 {equity:.0f}U · 上限 {meta['cap']:.0f}U ({MAX_TOTAL_NOTIONAL_MULT:.0f}x)\n"
            f"其它品种名义 {existing:.0f}U + 本笔 {new_notional:.0f}U "
            f"= {meta['total_notional']:.0f}U\n"
            f"盘口名义 {by_sym}",
        )
        return False, meta

    def _calc_vps_add_qty(self, qty_ratio=None):
        base = float(getattr(self, "base_qty", 0) or 0)
        if base <= 0:
            base = float(
                getattr(self, "initial_qty", 0) or getattr(self, "watched_qty", 0) or 0
            )
        ratio = resolve_tv_add_qty_ratio(
            self.regime,
            qty_ratio if qty_ratio is not None else getattr(self, "tv_qty_ratio", None),
        )
        qty, meta = compute_vps_add_qty(
            base, ratio, regime=self.regime,
            face_value=self.face_value, min_qty=1,
        )
        meta["principal"] = self._resolve_cap_sizing_base()
        meta["add_count"] = int(getattr(self, "add_count", 0) or 0)
        meta["max_add_times"] = self._max_add_times_for_regime()
        return int(qty or 0), meta

    def _tv_sizing_note(self, qty, meta=None, entry_type="OPEN"):
        return format_vps_sizing_note(meta or {}, qty=qty, entry_type=entry_type)

    def _calc_target_open_qty(self, curr_px, payload=None):
        qty, meta = self._calc_vps_open_qty(curr_px)
        principal = float(meta.get("principal", 0) or 0)
        margin_usdt = float(meta.get("order_amount", 0) or 0)
        margin_pct = float(meta.get("effective_risk_pct", VPS_RISK_PCT) or VPS_RISK_PCT) / 100.0
        return qty, principal, margin_usdt, margin_pct, meta

    def _calc_regime_margin_qty(self, curr_px):
        qty, meta = self._calc_vps_open_qty(curr_px)
        principal = float(meta.get("principal", 0) or 0)
        return qty, principal, float(meta.get("order_amount", 0) or 0), float(
            meta.get("effective_risk_pct", VPS_RISK_PCT) or VPS_RISK_PCT
        ) / 100.0

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
                f"(R{regime} VPS风险{margin_pct:.1%})"
            )
            return None

        _, balance, margin_usdt, margin_pct, regime = self._regime_cap_target_qty(curr_px, regime)
        old_qty = live_qty
        logger.warning(
            f"📡 [雷达档位限额] R{regime} VPS上限 {target} 张 "
            f"(本金 {balance:.0f}U×VPS风险{margin_pct:.1%}×{self.leverage}x) | "
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
            f"VPS {balance:.2f}U × R{regime} 风险{margin_pct:.1%} × {self.leverage}x "
            f"= 下单额 {margin_usdt:.0f}U → 上限 {target} 张 | "
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
        if self._expected_tp_count() > 0 and not self._tp1_filled_verified(real_amt):
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

    def _report_flat_close(self, reason, swept_dust=False, close_meta=None, curr_px=0.0):
        """平仓/止盈收网钉钉：REST 核查重试，与 Pine 四标签对齐"""
        meta = self._enrich_close_meta_live(close_meta, curr_px)
        flat = self._wait_verify(self._verify_flat, retries=6, delay=0.5)
        base_note = "盘口无持仓 | 挂单已清空 | 智慧大脑复位待命"
        if swept_dust:
            base_note = f"蚂蚁仓已市价扫尾 | {base_note}"
        if meta.get("pnl_pct") is not None:
            base_note += f" | 盈亏 {self._safe_float(meta.get('pnl_pct')):+.2f}%"
        if meta.get("side"):
            base_note += f" | 方向 {meta.get('side')}"
        if meta.get("entry_px") and float(meta.get("entry_px") or 0) > 0:
            base_note += f" | 开仓 {float(meta['entry_px']):.2f}"
        if meta.get("closed_qty") and float(meta.get("closed_qty") or 0) > 0:
            base_note += f" | 平仓 {float(meta['closed_qty']):.0f}张"
        if meta.get("live_exit_px") and float(meta.get("live_exit_px") or 0) > 0:
            base_note += f" | 现价 {float(meta['live_exit_px']):.2f}"
        if meta.get("regime"):
            base_note += f" | TV档位 R{int(meta.get('regime'))}"
        if meta.get("atr") and float(meta.get("atr") or 0) > 0:
            base_note += f" | TV ATR {float(meta['atr']):.2f}"
        src_note = format_tv_field_sources(meta.get("field_sources") or {})
        if src_note and "TV透传" not in src_note:
            base_note += f" | {src_note}"
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
        display_reason = meta.get("tv_reason") or reason or "仓位归零"
        self._call_dingtalk(
            dingtalk.report_supervisor_close,
            reason=display_reason,
            verify_note=verify_note,
            verified=flat,
            swept_dust=swept_dust,
            tv_pnl_pct=meta.get("pnl_pct"),
            tv_side=meta.get("side"),
            tv_price=meta.get("tv_price"),
            close_action=meta.get("action"),
            tv_regime=meta.get("regime"),
            tv_atr=meta.get("atr"),
            tv_field_sources=meta.get("field_sources"),
            close_type=meta.get("close_type"),
            tv_reason=meta.get("tv_reason") or display_reason,
            entry_px=meta.get("entry_px"),
            closed_qty=meta.get("closed_qty"),
            live_exit_px=meta.get("live_exit_px"),
        )
        # P1 修复：并行 TG 平仓通知
        from webhook_parser import close_type_display_label as _ctdl
        close_px = meta.get("live_exit_px") or meta.get("tv_price") or self.tv_price or 0
        self._call_telegram(
            telegram_notify.report_position_closed,
            side=meta.get("side") or self.current_side or "LONG",
            qty=int(meta.get("closed_qty", 0) or self.watched_qty or 0),
            entry=meta.get("entry_px") or self.watched_entry or 0,
            close_px=close_px,
            close_type=_ctdl(meta.get("close_type") or ""),
            regime=meta.get("regime") or self.regime or 3,
            pnl=meta.get("pnl_pct"),
            tier_label=getattr(self, "_last_tier_label", ""),
        )
        # 自动重入评估（v1.0 规格对齐）
        self._maybe_evaluate_smart_reentry(reason=reason, close_meta=meta, curr_px=curr_px)

    def _maybe_evaluate_smart_reentry(self, reason="", close_meta=None, curr_px=0.0):
        """
        平仓后评估是否启动智能重入（v1.0 规格）。

        触发条件：
        1. 非硬止损出局（hard_sl / vps_hard_sl）
        2. 未超过最大重入次数（max=1）
        3. TP1 未成交（tp1_ever_filled=False）
        4. 当前趋势档位为强（adx_tier=2）

        重入方式：限价挂单（雷达保本）
        """
        try:
            from smart_reentry_engine import evaluate_flat_for_reentry, open_reentry_window
            from reentry_profiles import reentry_enabled, tier_label
        except ImportError:
            return

        if not reentry_enabled(self.symbol):
            return

        # 硬止损出局 → 禁止重入
        hard_sources = ("vps_hard_sl", "hard_sl", "VPS_HARD_SL")
        if reason and any(s in str(reason) for s in hard_sources):
            logger.info(f"🚫 [{self.symbol}] 硬止损出局，禁止重入")
            self._clear_reentry_state()
            return

        meta = close_meta or {}
        side = str(meta.get("side") or getattr(self, "current_side", "") or "").upper()
        entry_px = float(meta.get("entry_px") or 0)
        exit_px = float(meta.get("live_exit_px") or curr_px or 0)
        atr = float(meta.get("atr") or getattr(self, "open_atr", 0) or 0)
        attempt = int(getattr(self, "reentry_attempt", 0) or 0)

        # 检查 TP1 是否已成交
        tp1_filled = bool(getattr(self, "_tp1_filled_hint", False) or
                         getattr(self, "_ws_tp1_fill_hint", False) or
                         (1 in (getattr(self, "tp_levels_consumed", []) or [])))

        # 检查 ADX 档位
        adx_tier = int(getattr(self, "adx_tier", 1) or 1)

        window_ts = open_reentry_window(self.symbol)
        self.reentry_window_deadline_ts = window_ts

        ok, why = evaluate_flat_for_reentry(
            exit_source=str(reason or ""),
            side=side,
            entry=entry_px,
            exit_px=exit_px,
            atr=atr,
            reentry_attempt=attempt,
            symbol=self.symbol,
            window_deadline_ts=window_ts,
            tp1_ever_filled=tp1_filled,
            adx_tier=adx_tier,
        )

        if not ok:
            logger.info(
                f"🚫 [{self.symbol}] 不启动重入: {why} | "
                f"src={reason} exit={exit_px:.2f} attempt={attempt} "
                f"tp1_filled={tp1_filled} tier={adx_tier}"
            )
            self._clear_reentry_state()
            return

        # 无菌确认后启动重入
        if self._ensure_sterile_for_reentry(reason="智能重入"):
            self._start_smart_reentry_limit(
                side=side, entry=entry_px, exit_px=exit_px,
                atr=atr, attempt=attempt, tp1_filled=tp1_filled,
                adx_tier=adx_tier, reason=reason,
            )

    def _clear_reentry_state(self):
        """清空重入状态"""
        self.reentry_active = False
        self.reentry_limit_order_id = None
        self.reentry_limit_px = 0.0
        self.reentry_limit_deadline_ts = 0.0
        self.reentry_order_tag = None

    def _ensure_sterile_for_reentry(self, reason="") -> bool:
        """确保仓位归零且无挂单"""
        try:
            deepcoin_client.cancel_all_open_orders(self.symbol)
        except Exception:
            pass
        time.sleep(0.5)
        pos = self._get_active_position()
        if not pos:
            return True
        qty = self._safe_qty(pos.get("size"))
        return qty <= 0

    def _start_smart_reentry_limit(self, side, entry, exit_px, atr, attempt, tp1_filled, adx_tier, reason=""):
        """挂限价单启动重入"""
        try:
            from smart_reentry_engine import plan_reentry_limit
        except ImportError:
            return False

        side_str = "LONG" if side.upper() in ("LONG", "BUY") else "SHORT"
        open_side = "buy" if side_str == "LONG" else "sell"
        pos_side = "long" if side_str == "LONG" else "short"
        tv_price = float(getattr(self, "cycle_tv_price", 0) or 0)

        # 拉 K 线计算限价
        k5 = deepcoin_client.fetch_klines(self.symbol, interval="5m", limit=3) if hasattr(deepcoin_client, 'fetch_klines') else None
        k3 = deepcoin_client.fetch_klines(self.symbol, interval="3m", limit=3) if hasattr(deepcoin_client, 'fetch_klines') else None

        plan, why = plan_reentry_limit(
            side=side_str, tv_price=tv_price, symbol=self.symbol,
            klines_5m=k5, klines_3m=k3,
        )

        if not plan:
            logger.warning(f"🚫 [{self.symbol}] 重入限价中止: {why}")
            return False

        lim = float(plan["limit_px"])
        qty = int(getattr(self, "base_qty", 0) or 1)
        if qty <= 0:
            qty = 1

        tag = f"reentry_{self.symbol}_{int(time.time())}"
        self.reentry_order_tag = tag

        order = deepcoin_client.place_limit_order(
            self.symbol, open_side, pos_side, lim, qty, cl_ord_id=tag,
        )

        if not order or not deepcoin_client._is_success(order):
            logger.warning(f"⚠️ [{self.symbol}] 重入限价挂单失败")
            self.reentry_order_tag = None
            return False

        oid = (order.get("data") or {}).get("ordId", "") or order.get("order_id", "")
        self.reentry_active = True
        self.reentry_limit_order_id = oid
        self.reentry_limit_px = lim
        self.reentry_limit_deadline_ts = float(plan["deadline_ts"])

        logger.info(
            f"📥 [{self.symbol}] 智能重入限价已挂 {side_str} {qty}@{lim:.2f} "
            f"attempt={attempt} tag={tag}"
        )

        # 钉钉通知
        try:
            self._call_dingtalk(
                dingtalk.report_system_alert,
                title=f"智能重入限价已挂 [{self.symbol}]",
                detail=(
                    f"{side_str} attempt={attempt} limit@{lim:.2f} "
                    f"TV@{tv_price:.2f} exit={reason}@{exit_px:.2f} "
                    f"tp1_filled={tp1_filled} tier={adx_tier}"
                ),
                level="提示",
            )
        except Exception:
            pass

        self._save_state()
        return True

    def _reentry_state_dict(self) -> dict:
        """§8：从 RadarReentryMixin 继承完整的再入状态字典（含新字段）。"""
        return RadarReentryMixin._reentry_state_dict(self)

    def _reentry_tick(self):
        """重入订单 Tick：检查成交/TTL刷新"""
        if not getattr(self, "reentry_active", False):
            return

        pos = self._get_active_position()
        if not pos:
            return

        qty = self._safe_qty(pos.get("size"))
        if qty > 0:
            # 成交！启动新开仓流程
            self._on_reentry_filled(pos)
            return

        # 检查 TTL
        now = time.time()
        deadline = float(getattr(self, "reentry_limit_deadline_ts", 0) or 0)
        if deadline > 0 and now >= deadline:
            logger.info(f"⏰ [{self.symbol}] 重入限价 TTL 到期，重新挂单")
            self._cancel_reentry_limit(reason="TTL刷新")
            side = str(getattr(self, "cycle_tv_side", "") or "").upper()
            self._start_smart_reentry_limit(
                side=side,
                entry=float(getattr(self, "cycle_entry", 0) or 0),
                exit_px=float(getattr(self, "last_exit_px", 0) or 0),
                atr=float(getattr(self, "cycle_open_atr", 0) or 0),
                attempt=int(getattr(self, "reentry_attempt", 0) or 0),
                tp1_filled=False,
                adx_tier=int(getattr(self, "adx_tier", 1) or 1),
                reason="TTL刷新",
            )

    def _cancel_reentry_limit(self, reason=""):
        """撤销重入限价单"""
        oid = getattr(self, "reentry_limit_order_id", None)
        if oid:
            try:
                deepcoin_client.cancel_order(self.symbol, order_id=oid)
                logger.info(f"🗑️ [{self.symbol}] 撤重入限价 id={oid} | {reason}")
            except Exception as e:
                logger.debug(f"撤单失败: {e}")
        self._clear_reentry_state()

    def _on_reentry_filled(self, pos):
        """重入订单成交处理"""
        side_str = str(pos.get("posSide", "") or "").lower()
        side = "LONG" if side_str == "long" else "SHORT"
        entry = float(pos.get("avgPx") or pos.get("entry_price", 0) or 0)
        qty = self._safe_qty(pos.get("size"))

        if entry <= 0 or qty <= 0:
            return

        logger.info(f"✅ [{self.symbol}] 重入成交 {side} {qty}@{entry:.2f}")

        # 更新 attempt
        prev_attempt = int(getattr(self, "reentry_attempt", 0) or 0)
        self.reentry_attempt = prev_attempt + 1
        self._clear_reentry_state()

        # 恢复开仓状态
        self.current_side = side
        self.watched_entry = entry
        self.watched_qty = qty
        self.initial_qty = qty
        self.monitoring = True
        self.tp_levels_consumed = []

        # 挂硬止损 + TP12
        try:
            self._arm_temp_stop_and_tp12_for_reentry(qty, entry, side)
        except Exception as e:
            logger.error(f"[{self.symbol}] 重入后防线挂单失败: {e}")

        self._save_state()

    def _arm_temp_stop_and_tp12(self, live_qty, entry, side, source=""):
        """Adapter: 统一雷达 mixin 的签名 → 内部已有 _arm_temp_stop_and_tp12_for_reentry。"""
        return self._arm_temp_stop_and_tp12_for_reentry(live_qty, entry, side)

    def _resolve_atr_scenario_after_open(self, entry, side, live_qty):
        """Mixin 兼容：空操作（Deepcoin ATR 已在开仓时锁定）。"""
        pass

    def _arm_temp_stop_and_tp12_for_reentry(self, qty, entry, side):
        """重入成交后挂硬止损 + TP12"""
        from breath_stop import initial_stop_price

        atr = float(getattr(self, "open_atr", 0) or 0)
        profile = getattr(self, "breath_profile", None) or {}

        init = initial_stop_price(side, entry, atr, profile=profile)
        if init <= 0:
            init = entry * 0.98 if side == "LONG" else entry * 1.02

        self.initial_stop = init
        self.current_sl = init
        self.tv_sl = init

        # 挂硬止损
        pos_side = "long" if side == "LONG" else "short"
        deepcoin_client.place_trigger_order(
            self.symbol, "sell" if side == "LONG" else "buy",
            pos_side, qty, init, order_type="market",
        )

        # 挂 TP1/TP2 限价（规格 §3.5：TP1=10%，TP2=20%，剩余70%归雷达）
        LEG_TP_RATIOS_REENTRY = [0.10, 0.20]
        tps = list(getattr(self, "tv_tps", [0, 0, 0]) or [])
        if len(tps) >= 1 and tps[0] > 0:
            tp1 = tps[0]
            tp1_side = "sell" if side == "LONG" else "buy"
            tp1_qty = max(1, int(qty * LEG_TP_RATIOS_REENTRY[0]))
            deepcoin_client.place_limit_order(
                self.symbol, tp1_side, pos_side, tp1, tp1_qty,
            )
        if len(tps) >= 2 and tps[1] > 0:
            tp2 = tps[1]
            tp2_side = "sell" if side == "LONG" else "buy"
            tp2_qty = max(1, int(qty * LEG_TP_RATIOS_REENTRY[1]))
            deepcoin_client.place_limit_order(
                self.symbol, tp2_side, pos_side, tp2, tp2_qty,
            )

        logger.info(f"📌 [{self.symbol}] 重入防线已挂: 硬止损@{init:.2f} TP1/TP2")

    def _sweep_dust_and_finalize(self, reason):
        """哨兵检测：止盈后蚂蚁仓/无 TP 残张 → 撤单 + reduceOnly 扫尾 + 收网钉钉（对冲模式兼容）"""
        logger.warning(f"🐜 止盈扫尾：检测到残量，启动蚂蚁仓强平 → {reason}")
        self.monitoring = False
        deepcoin_client.cancel_all_open_orders(self.symbol)
        time.sleep(0.4)
        for round_i in range(4):
            # 获取所有方向的持仓
            all_positions = self._get_all_positions()
            if not all_positions:
                break
            for pos in all_positions:
                close_side = "sell" if pos["posSide"] == "long" else "buy"
                live_sz = self._safe_qty(pos["size"])
                logger.info(f"🐜 扫尾{round_i + 1}/4: {close_side} {live_sz}张 {pos['posSide']} reduceOnly")
                deepcoin_client.place_market_order(
                    self.symbol, close_side, pos["posSide"], live_sz, reduce_only=True,
                )
                time.sleep(0.5)
            time.sleep(1.0)
        self.watched_qty = 0
        self.initial_qty = 0
        self.base_qty = 0
        self.add_count = 0
        self.tp_levels_consumed = []
        self.shield_active = False
        self._shield_sltp_ord_id = ""
        self._shield_sltp_set_at = 0.0
        self._shield_cancelled_ids = set()
        self.current_side = None
        # 规格 v1.0 §8-9：平仓时清空幂等标签 + 退出所有权
        self.exit_ownership = "NONE"
        self.ownership_locked_at = 0.0
        self._pending_order_tags = {}
        self._mutex_leg = ""
        self._save_state()
        deepcoin_client.cancel_all_open_orders(self.symbol)
        self._report_flat_close(reason, swept_dust=True)

    def _apply_recover_live_alignment(self, side, reconcile):
        """重启对账备注：TV 平仓日志不回放；方向背离由 _enforce_tv_direction_or_flat 核武处理"""
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
        elif reconcile.get("direction_mismatch"):
            tv_side = self._resolve_tv_authoritative_side()
            extra_notes.append(
                f"方向背离: 实盘{side} vs TV{tv_side} → 已由核武全平强制对齐 TV"
            )
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

        if not self._confirm_position_flat(
            retries=STARTUP_FLAT_CONFIRM_RETRIES,
            delay=STARTUP_FLAT_CONFIRM_DELAY_SEC,
        ):
            logger.info(
                "📭 [重启对账] 首次无仓但多次复核仍有持仓 → 跳过误补发收网"
            )
            return False

        logger.warning(
            f"📭 [重启对账] 账本/日志曾有仓 (watched={prev_watched}, side={prev_side}, "
            f"monitoring={was_monitoring}) 但盘口已全平 → 补发收网播报"
        )
        deepcoin_client.cancel_all_open_orders(self.symbol)
        self.monitoring = False
        self.watched_qty = 0
        self.initial_qty = 0
        self.base_qty = 0
        self.add_count = 0
        self.current_side = None
        self._save_state()

        verify_note = (
            f"重启对账补发 | 原账本 {prev_watched}张 {prev_side or ''} | "
            f"盘口无持仓 | 挂单已清空 | 智慧大脑复位待命"
        )
        recover_meta = self._infer_flat_close_meta(hint_reason="重启对账补发收网")
        self._call_dingtalk(
            dingtalk.report_supervisor_close,
            reason=recover_meta.get("tv_reason", "仓位归零 (重启对账补发)"),
            verify_note=verify_note,
            verified=True,
            swept_dust=False,
            tv_pnl_pct=recover_meta.get("pnl_pct"),
            tv_side=recover_meta.get("side") or prev_side,
            close_action=recover_meta.get("action"),
            tv_regime=recover_meta.get("regime"),
            tv_atr=recover_meta.get("atr"),
            close_type=recover_meta.get("close_type"),
            tv_reason=recover_meta.get("tv_reason"),
            entry_px=recover_meta.get("entry_px"),
            closed_qty=prev_watched,
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
        consumed = set(getattr(self, "tp_levels_consumed", []) or [])
        return sum(
            1 for i, t in enumerate((tp_pxs or [])[:2])
            if t > 0 and (i + 1) not in consumed
        )

    def _tp_split_regime(self):
        if self.watched_qty and self._safe_qty(self.watched_qty) > 0:
            return int(getattr(self, "open_regime", self.regime) or self.regime)
        return int(self.regime)

    def _tp_slices_for_initial(self, initial_qty):
        initial_qty = self._safe_qty(initial_qty)
        ratios = self.regime_settings[self._tp_split_regime()]["ratios"]
        o1, o2, o3 = self._calculate_tp_quantities(initial_qty, ratios)
        # v13.81：只返回 TP1+TP2 限价切片；TP3 量留给雷达
        return [
            {"level": 1, "price": self.tv_tps[0], "qty": o1},
            {"level": 2, "price": self.tv_tps[1], "qty": o2},
        ]

    @staticmethod
    def _sequential_tp_prefix(levels):
        out = []
        for lv in (1, 2, 3):
            if lv in levels:
                out.append(lv)
            else:
                break
        return out

    def _infer_tp_consumed_sequential(self, initial_qty, live_qty, curr_px=0.0):
        """
        顺序推断已成交 TP。硬约束防 R4 TP1=5% 误判：
        限价仍在盘口 → 未成交；减仓 <2% 噪声忽略；TP1 须价到区域。
        """
        initial_qty = self._safe_qty(initial_qty)
        live_qty = self._safe_qty(live_qty)
        if initial_qty <= live_qty:
            return []

        reduced = initial_qty - live_qty
        noise = max(1, int(round(initial_qty * 0.02)))
        if reduced < noise:
            return []

        consumed = []
        cum = 0

        for sl in self._tp_slices_for_initial(initial_qty):
            if sl["qty"] <= 0 or sl["price"] <= 0:
                continue
            if self._has_tp_limit_at_price(sl["price"]):
                break
            if int(sl["level"]) == 1 and not (
                self._price_reached_tp1_zone(curr_px, sl["price"])
                or getattr(self, "_radar_armed_after_tp1", False)
                or getattr(self, "_ws_tp1_fill_hint", False)
            ):
                break
            cum += int(sl["qty"])
            tol = max(1, int(round(sl["qty"] * 0.05)))
            if len(consumed) == 0 and reduced < int(sl["qty"]) - tol:
                break
            if reduced >= cum - tol:
                consumed.append(sl["level"])
            else:
                break

        return self._sequential_tp_prefix(consumed)

    def _sanitize_tp_consumed(self, initial_qty, live_qty, curr_px=0.0):
        live_qty = self._safe_qty(live_qty)
        initial_qty = self._safe_qty(initial_qty)
        if live_qty <= DUST_ORPHAN_CONTRACTS:
            self.tp_levels_consumed = []
            self._save_state()
            return []

        saved = self._sequential_tp_prefix(getattr(self, "tp_levels_consumed", []) or [])
        inferred = self._infer_tp_consumed_sequential(initial_qty, live_qty, curr_px)

        if initial_qty <= live_qty and saved and not inferred:
            logger.warning(
                f"⚠️ 无减仓但 tp_levels_consumed={saved} → 清空（避免漏挂 TP1）"
            )
            saved = []
        elif initial_qty <= live_qty and saved and inferred and saved != inferred:
            logger.info(
                f"🎯 无减仓以推断为准: TP{saved} → TP{inferred or '无'}"
            )
            saved = inferred

        if len(saved) >= 3 and live_qty > DUST_ORPHAN_CONTRACTS:
            logger.warning(
                f"⚠️ tp_levels_consumed={saved} 但仍有 {live_qty} 张 → "
                f"按开单 {initial_qty} 张重算为 TP{inferred or '无'}"
            )
            saved = inferred
        elif inferred and (not saved or len(inferred) < len(saved)):
            if saved != inferred:
                logger.info(
                    f"🎯 已成交档修正: TP{saved or '无'} → TP{inferred} "
                    f"(开单 {initial_qty} → 现仓 {live_qty}张)"
                )
            saved = inferred
        elif saved and inferred and saved != inferred:
            logger.info(f"🎯 已成交档以减仓为准: TP{saved} → TP{inferred}")
            saved = inferred

        if saved != list(getattr(self, "tp_levels_consumed", []) or []):
            self.tp_levels_consumed = saved
            self._save_state()
        return saved

    def _mark_tp_levels_consumed(self, levels):
        consumed = set(getattr(self, "tp_levels_consumed", []) or [])
        for lv in levels:
            consumed.add(int(lv))
        self.tp_levels_consumed = self._sequential_tp_prefix(sorted(consumed))
        self._save_state()

    def _split_remaining_tp_quantities(self, live_qty, ratios=None):
        """
        TP1+TP2 各保证至少1张（剩余数量足够时），剩余按比例分配。
        TP3 永不挂限价（由雷达管理），接收所有剩余。
        """
        live_qty = self._safe_qty(live_qty)
        ratios = ratios or self.regime_settings[self._tp_split_regime()]["ratios"]
        consumed = set(getattr(self, "tp_levels_consumed", []) or [])
        remaining = [i for i in range(3) if (i + 1) not in consumed]
        if not remaining or live_qty <= 0:
            return {}

        # 仅剩一档（通常是TP3），全给该档
        if len(remaining) == 1:
            return {remaining[0] + 1: live_qty}

        # TP1+TP2 各保底1张，剩余给TP3
        # 如果live_qty不足2张，则全给TP1，TP2=0
        if live_qty < 2:
            out = {1: live_qty}
            if 2 in remaining:
                out[2] = 0
            if 3 in remaining:
                out[3] = 0
            return out

        tp1_qty = max(1, int(math.ceil(live_qty * ratios[0])))
        tp2_qty = max(1, int(math.ceil(live_qty * ratios[1])))
        # 剩余给 TP3（雷达管理，不挂限价）
        tp3_qty = max(0, live_qty - tp1_qty - tp2_qty)

        out = {}
        for idx in remaining:
            level = idx + 1
            if level == 1:
                out[1] = tp1_qty
            elif level == 2:
                out[2] = tp2_qty
            elif level == 3:
                out[3] = tp3_qty
        return out

    def _expected_tp_levels(self, live_qty):
        consumed = set(getattr(self, "tp_levels_consumed", []) or [])
        qty_map = self._split_remaining_tp_quantities(live_qty)
        qty_map = self._normalize_tp_qty_map(qty_map, live_qty)
        levels = []
        for level in (1, 2):  # v13.81: TP3 永不挂限价
            if level in consumed:
                continue
            price = self.tv_tps[level - 1]
            qty = qty_map.get(level, 0)
            levels.append({"level": level, "qty": qty, "price": price})
        return levels

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

        # 规格 v1.0：tv_tps 全零时说明 TP 价格未初始化（TV 信号缺字段 / 重启恢复失败）
        if not parts:
            tv_tps = getattr(self, "tv_tps", None) or []
            if all((float(t or 0) <= 0) for t in tv_tps):
                parts.append("⚠️ TP价格未初始化（tv_tps=全零）")
            else:
                # 规格 §6.2：TP1/TP2 零头并入 TP3（雷达管理），这是正常状态不是错误
                parts.append("ℹ️ TP1/TP2零头已并入TP3（雷达管理）")

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
        placed = 0

        for lv in self._expected_tp_levels(live_qty):
            q, px = lv["qty"], lv["price"]
            if q <= 0 or px <= 0:
                continue
            orders = self._collect_tp_limit_orders()
            at_px = [o for o in orders if abs(o["price"] - px) <= tolerance]
            if len(at_px) == 1 and at_px[0]["qty"] == q:
                logger.info(f"  ✓ TP{lv['level']} @ {px:.2f} 已存在 {at_px[0]['qty']}张，跳过")
                continue
            # 【紧急修复】撤销前确认交易所侧有该订单
            for o in at_px:
                if o.get("orderId"):
                    deepcoin_client.cancel_order(self.symbol, ord_id=o["orderId"])
                    time.sleep(0.25)
            # 撤销后再次确认交易所侧没有该价格的订单，防止重复挂单
            time.sleep(0.3)
            orders_after = self._collect_tp_limit_orders()
            # 修复：使用正确的字段名 px 而不是 price
            at_px_after = [o for o in orders_after if abs(o.get("px", o.get("price", 0)) - px) <= tolerance]
            if at_px_after:
                logger.warning(f"  ⚠️ 撤销后仍有 TP@{px:.2f}，跳过本次挂单")
                continue
            logger.info(f"  + 补挂 TP{lv['level']} @ {px:.2f} qty={q}张")
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
        # v16.15（根因四修复）：撤销前初始化本轮已撤销列表，防止重复 REST
        self._shield_cancelled_ids = set()
        for t in deepcoin_client.get_trigger_orders_pending(self.symbol):
            if scope == "radar" and not self._is_radar_trigger_order(t):
                continue
            if scope == "shield" and not self._is_shield_trigger_order(t):
                continue
            oid = str(t.get("ordId") or "").strip()
            if not oid:
                continue
            # v16.15（根因四修复）：幂等撤销——本轮已撤销的直接跳过
            if oid in self._shield_cancelled_ids:
                continue
            deepcoin_client.cancel_trigger_order(self.symbol, oid)
            self._shield_cancelled_ids.add(oid)
            # 同时检查是否撤销了本地记录的 sltp 订单
            _local = str(getattr(self, "_shield_sltp_ord_id", "") or "").strip()
            if _local == oid:
                self._shield_sltp_ord_id = ""
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

    def _legacy_shield_stop_price(self, entry=None):
        """已废弃：止损价 exclusively 来自 TV tv_sl"""
        return None

    def _shield_stop_price(self, entry=None):
        """VPS 自主硬止损价（tv_sl 账本字段存 VPS 计算结果）"""
        tv = round(float(getattr(self, "tv_sl", 0) or 0), 2)
        return tv if tv > 0 else None

    def _refresh_vps_hard_sl(self, entry=None, side=None, regime=None, atr=None,
                             tv_sl_ref=None, source=""):
        """
        规格 v1.0 §3：硬止损 = |TV.price − TV.stop_loss| × 1.15。
        不再使用开仓价×档位% 旧路径。
        """
        entry = float(entry or self.watched_entry or self.tv_price or 0)
        side = (side or self.current_side or "").strip().upper()
        regime = int(regime if regime is not None else self.regime or 3)

        if tv_sl_ref is not None:
            ref = round(self._safe_float(tv_sl_ref, 0), 2)
            if ref > 0:
                self.tv_sl_ref = ref

        if entry <= 0 or side not in ("LONG", "SHORT"):
            return False

        hard = float(self._lock_frozen_hard_sl_from_tv(entry=entry, side=side, source=source))
        if hard <= 0:
            return False

        self._save_state()
        logger.info(
            f"[{self.symbol}] VPS硬止损刷新@{hard:.2f} (source={source})"
        )
        return True

    def _defense_buffer_mult(self):
        """硬止损呼吸垫：统一 1.15（规格 3.4），与 adx_tier 无关。"""
        return float(TEMP_STOP_BUFFER_MULT)

    def _temp_hard_stop_from_tv(self, entry=None, side=None, tv_sl=None):
        """
        永久硬止损价（规格 v1.0 §3）：
          dist = |TV价 − TV.SL| × 1.15（统一呼吸垫，不分档）
          挂在成交价外侧。禁止 1.5×ATR / 滑点×2 旧路径。
        """
        fill = float(entry if entry is not None else (self.watched_entry or self.tv_price or 0))
        side = str(side or self.current_side or "").strip().upper()
        tv_sl = float(tv_sl if tv_sl is not None else (getattr(self, "tv_sl_ref", 0) or 0))
        tv_entry = float(getattr(self, "tv_price", 0) or 0)
        if tv_entry <= 0:
            tv_entry = fill
        buf = float(self._defense_buffer_mult())
        return hard_stop_price(
            side,
            fill,
            tv_sl,
            buffer_mult=buf,
            tv_entry=tv_entry,
            fill_entry=fill,
        )

    def _lock_frozen_hard_sl_from_tv(self, entry=None, side=None, source=""):
        """
        规格 v1.0 §3.3：用 |TV.price−TV.stop_loss|×1.15 锁定 frozen_hard_sl_px。
        已锁定则不覆盖。禁止用 0.5×ATR 雷达激活臂冒充永久硬止损。

        v16.11（根因一保险）：最小 ATR 距离兜底。
        若 TV 传来的 stop_loss 导致距离 < 0.5×ATR，视为异常数据，自动扩大并告警。
        """
        cur = float(self._frozen_hard_px() or 0)
        if cur > 0:
            return cur
        hard = float(self._temp_hard_stop_from_tv(entry=entry, side=side) or 0)
        if hard <= 0:
            logger.error(
                f"[{self.symbol}] 无法锁定永久硬止损（缺 TV.stop_loss）| {source}"
            )
            return 0.0

        # v16.11（根因一保险）：最小 ATR 距离兜底检查
        fill_px = float(entry if entry is not None else (self.watched_entry or 0))
        atr_val = float(getattr(self, "open_atr", 0) or 0)
        if atr_val > 0 and fill_px > 0:
            side_check = str(side or self.current_side or "").strip().upper()
            if side_check in ("LONG", "SHORT"):
                dist = abs(fill_px - hard)
                min_dist = atr_val * 0.5
                if dist < min_dist:
                    logger.warning(
                        f"[{self.symbol}] ⚠️ 硬止损距离异常：TV距离 {dist:.4f} < 0.5×ATR({atr_val:.4f}) "
                        f"| entry={fill_px:.2f} hard={hard:.2f} → 自动扩大至 {min_dist:.4f}"
                    )
                    dingtalk.report_system_alert(
                        "硬止损距离异常·ATR兜底",
                        f"TV.stop_loss 距离过小 [{self.symbol}] | "
                        f"entry={fill_px:.2f} hard={hard:.2f} | "
                        f"dist={dist:.4f} < 0.5×ATR({atr_val:.4f}) | "
                        f"已自动扩大至 {min_dist:.4f}（方向 {side_check}）",
                        suggestion="请核查 TV 策略的 stop_loss 设置是否合理",
                    )
                    # 用 ATR 兜底距离重新计算硬止损价
                    if side_check == "LONG":
                        hard = round(fill_px - min_dist, 2)
                    else:
                        hard = round(fill_px + min_dist, 2)

        self.frozen_hard_sl_px = float(hard)
        try:
            self._save_state()
        except Exception:
            pass
        logger.info(
            f"[{self.symbol}] 锁定永久硬止损@{hard:.2f} "
            f"(TV.sl_ref={float(getattr(self, 'tv_sl_ref', 0) or 0):.2f} "
            f"buffer={float(self._defense_buffer_mult()):.2f}) | {source}"
        )
        return float(hard)

    def _frozen_hard_px(self):
        return round(float(getattr(self, "frozen_hard_sl_px", 0) or 0), 2)

    def _hard_stop_distance_meta(self, fill=None, tv_sl=None, tv_entry=None, atr=None):
        """调试/钉钉：硬止损距离拆解。"""
        fill = float(fill if fill is not None else (self.watched_entry or 0))
        tv_entry = float(tv_entry if tv_entry is not None else (getattr(self, "tv_price", 0) or fill))
        tv_sl = float(tv_sl if tv_sl is not None else (getattr(self, "tv_sl_ref", 0) or 0))
        buf = float(self._defense_buffer_mult())
        return compute_hard_stop_distance(tv_entry, tv_sl, fill, 0.0, tv_mult=buf)

    def _apply_tv_sl_from_payload(self, payload, source=""):
        """
        规格 v1.0 §3：TV.stop_loss 决定永久硬止损。
        距离 = |TV.price − TV.stop_loss| × 1.15。
        禁止再用开仓价×档位%的旧 VPS 自主路径。
        """
        tv_ref = payload.get("tv_sl")
        if tv_ref is None or tv_ref == "":
            return self._lock_frozen_hard_sl_from_tv(source=source or "信号")
        ref_px = round(self._safe_float(tv_ref, 0), 2)
        if ref_px <= 0:
            return self._lock_frozen_hard_sl_from_tv(source=source or "TV空值")
        entry = float(self.tv_price or self.watched_entry or 0)
        side = str(payload.get("action") or payload.get("side") or self.current_side or "").upper()
        if side not in ("LONG", "SHORT"):
            side = self.current_side
        self.tv_sl_ref = ref_px
        return self._lock_frozen_hard_sl_from_tv(entry=entry, side=side, source=source or "TV参考")

    def _locked_initial_atr(self):
        """开仓 ATR 锁定：优先 webhook open_atr，全程固定。"""
        atr = float(getattr(self, "open_atr", 0) or 0)
        if atr > 0:
            return atr
        atr = float(getattr(self, "current_atr", 0) or 0)
        return atr if atr > 0 else 0.0

    def _atr_1h_engine(self):
        """已废除：ATR 只信 TV webhook。"""
        return None

    def _refresh_breathing_coefficient(self, force=False):
        """呼吸系数固定 1.0（ATR 只用 TV 锁值）。"""
        init = float(getattr(self, "open_atr", 0) or 0)
        self.breathing_coefficient = 1.0
        self._breath_coeff_meta = {
            "atr_1h": 0.0,
            "initial_atr": init,
            "ratio": 1.0,
            "smoothed": 1.0,
            "source": "tv_fixed",
            "ratio_history": list(getattr(self, "_breath_ratio_history", None) or []),
        }
        if init > 0:
            self.current_atr = float(init)
        return self.breathing_coefficient

    def _should_ignore_late_close(self, payload=None):
        """
        开仓成交后 LATE_CLOSE_SUPPRESS_SEC 内的迟到 CLOSE → 忽略。
        同窗先平后开链（_close_open_chain_active）不忽略。
        """
        if getattr(self, "_close_open_chain_active", False):
            return False
        last_ts = float(getattr(self, "_last_open_exec_ts", 0) or 0)
        if last_ts <= 0:
            return False
        age = time.time() - last_ts
        if age < 0 or age > float(LATE_CLOSE_SUPPRESS_SEC):
            return False
        pos = self._get_active_position()
        if not pos or float(pos.get("size") or 0) <= 0:
            return False
        return True

    def _apply_breath_stop_tick(self, curr_px=0.0):
        """雷达/止损 tick：呼吸系数追踪（非 ADX）。"""
        entry = float(self.watched_entry or 0)
        side = str(self.current_side or "").strip().upper()
        if entry <= 0 or side not in ("LONG", "SHORT"):
            return None
        atr = self._locked_initial_atr()
        profile = getattr(self, "breath_profile", None)
        # §5.2/§5.3：档位分级呼吸参数叠加
        try:
            self._apply_tier_breath_overlay()
        except Exception:
            pass
        init = float(getattr(self, "initial_stop", 0) or 0)
        if init <= 0 and atr > 0:
            init = initial_stop_price(side, entry, atr, profile=profile)
            self.initial_stop = init
        cur = float(getattr(self, "current_sl", 0) or 0) or init
        best = float(getattr(self, "best_price", 0) or 0) or entry
        px = float(curr_px or 0) or best
        phase = bool(getattr(self, "breakeven_phase", False))
        early = bool(getattr(self, "early_be_done", False))
        coeff = float(self._refresh_breathing_coefficient(force=False) or 1.0)
        if coeff <= 0:
            coeff = 1.0

        out = calculate_breath_stop(
            side,
            px,
            entry,
            atr,
            init,
            cur,
            best,
            phase,
            breathing_coefficient=coeff,
            profile=profile,
            early_be_done=early,
        )
        new_stop = float(out["stop"] or 0)
        new_best = float(out["best"] or best)
        new_phase = bool(out["breakeven_phase"])
        self.early_be_done = bool(out.get("early_be_done") or early)
        meta = out.get("meta") or {}
        meta["breathing_coefficient"] = coeff

        if new_best > 0:
            self.best_price = new_best
        if new_stop > 0:
            if side == "LONG":
                self.current_sl = max(cur, new_stop) if cur > 0 else new_stop
            else:
                self.current_sl = min(cur, new_stop) if cur > 0 else new_stop
        was_phase = phase
        self.breakeven_phase = new_phase
        return {
            "stop": float(self.current_sl or 0),
            "best": float(self.best_price or 0),
            "breakeven_phase": new_phase,
            "early_be_done": bool(self.early_be_done),
            "meta": meta,
            "phase_entered": bool(new_phase and not was_phase),
        }

    def _clamp_radar_to_tv_floor(self, radar_sl):
        """雷达保本线不得低于 TV 硬止损底线"""
        if not radar_sl:
            return radar_sl
        floor = self._shield_stop_price()
        if not floor:
            return radar_sl
        radar = round(float(radar_sl), 2)
        if self.current_side == "LONG":
            return max(radar, floor)
        if self.current_side == "SHORT":
            return min(radar, floor)
        return radar

    def _sync_tv_sl_stop(self, live_qty, reason="", force=False):
        """挂/更新 TV 硬止损触发单（幂等）；雷达止损独立运行"""
        live_qty = self._resolve_live_qty(live_qty)
        if live_qty <= 0 or not self.current_side or not self.watched_entry:
            return {"ok": False, "skipped": True, "reason": "no_position"}

        target = self._shield_stop_price()
        if not target or target <= 0:
            return {"ok": False, "skipped": True, "reason": "no_stop_price"}
        target = round(float(target), 2)

        last = round(float(getattr(self, "_last_applied_tv_sl", 0) or 0), 2)
        if (
            not force
            and last > 0
            and abs(target - last) <= SHIELD_STOP_TOLERANCE
            and self._has_shield_stop_at_price(target)
        ):
            return {"ok": True, "skipped": True, "target": target, "reason": "idempotent"}

        ok = self._place_shield_stops(
            live_qty,
            reason=reason or f"TV硬止损 @ {target:.2f}",
            force=True,
        )
        if ok:
            self._last_applied_tv_sl = target
            self._save_state()
            tv_floor = round(float(getattr(self, "tv_sl", 0) or 0), 2)
            logger.warning(
                f"🛡️ [TV硬止损] {reason or '同步止损'} | {live_qty} 张 @ {target:.2f} "
                f"| tv_sl={tv_floor or 'fallback'}"
            )
        return {"ok": ok, "skipped": False, "target": target}

    def _handle_tv_sl_update(self, payload):
        """UPDATE_SL：撤旧挂新 tv_sl（幂等），雷达线独立继续运行"""
        side = str(payload.get("side") or "").strip().upper()
        if not self._apply_tv_sl_from_payload(payload, source="UPDATE_SL"):
            logger.warning("UPDATE_SL 无效或未携带 tv_sl")
            return

        pos = self._get_active_position()
        if not pos or self._safe_qty(pos.get("size", 0)) <= 0:
            logger.info("UPDATE_SL 到达但盘口已空仓 → 仅更新账本 tv_sl")
            return
        pos_side = "LONG" if pos.get("posSide") == "long" else "SHORT"
        if side and side != pos_side:
            logger.warning(f"UPDATE_SL side={side} 与实盘 {pos_side} 不符，已忽略")
            return

        result = self._sync_tv_sl_stop(
            pos["size"],
            reason=f"TV UPDATE_SL @ {self.tv_sl:.2f}",
            force=True,
        )
        if result.get("skipped") and result.get("reason") == "idempotent":
            logger.info(f"UPDATE_SL 幂等跳过 tv_sl={self.tv_sl:.2f} 已在盘口")
        elif result.get("ok"):
            exchange_stop = float(result.get("target") or self.tv_sl)
            radar_sl = None
            if self._is_radar_active():
                radar_sl = self._clamp_radar_to_tv_floor(self.current_sl)
            verified = self._wait_verify(
                lambda: self._has_shield_stop_at_price(self.tv_sl),
                retries=8,
                delay=0.4,
            )
            live_qty = self._resolve_live_qty(pos["size"])
            verify_note = (
                f"UPDATE_SL tv_sl={self.tv_sl:.2f} @ {exchange_stop:.2f}"
                + (f" | 雷达 {radar_sl:.2f}" if radar_sl else "")
                + f" | 持仓 {live_qty} 张 @ {self.watched_entry:.2f}"
            )
            if not verified:
                verify_note += f" | {dingtalk.VERIFY_DELAY_MARK}"
            self._call_dingtalk(
                dingtalk.report_tv_sl_updated,
                side=self.current_side or pos_side,
                live_qty=live_qty,
                entry=self.watched_entry,
                tv_sl=self.tv_sl,
                exchange_stop=exchange_stop,
                radar_active=self._is_radar_active(),
                radar_sl=radar_sl,
                regime=self.regime,
                verify_note=verify_note,
                verified=verified,
            )
        else:
            dingtalk.report_system_alert(
                "TV硬止损更新失败",
                f"UPDATE_SL tv_sl={self.tv_sl:.2f} | 核实未通过，哨兵将继续重试",
            )

    def _place_tp_levels_only(self, live_qty, retries=2):
        """
        规格 v1.0 §8-9：防御 TP 限价必须带 newClientOrderId（幂等标签）。
        本地已有同档未完成标签 → 拒挂（宁可错过）。
        仅挂 TP1+TP2；TP3 永不挂限价。
        """
        close_side = "sell" if self.current_side == "LONG" else "buy"
        pos_side = "long" if self.current_side == "LONG" else "short"
        live_qty = self._resolve_live_qty(live_qty)
        if live_qty <= 0:
            return 0

        # 【关键修复】先撤销所有旧TP订单，等待确认完成后再挂新单
        # 防止旧单未撤干净导致重复挂单
        self._cancel_all_tp_limit_orders(max_rounds=4)
        time.sleep(1.0)  # 等待撤销确认
        # 验证旧单已全部撤销
        existing_before = self._collect_tp_limit_orders()
        if existing_before:
            logger.warning(
                f"[{self.symbol}] 挂新TP前检测到{len(existing_before)}张旧单未撤干净，等待..."
            )
            time.sleep(2.0)
            existing_before = self._collect_tp_limit_orders()
            if existing_before:
                prices = [f"@{o['price']:.2f}" for o in existing_before]
                logger.error(
                    f"[{self.symbol}] 旧TP仍未撤净，放弃本次挂单: {prices}"
                )
                return 0

        placed = 0
        for lv in self._expected_tp_levels(live_qty):
            level = int(lv.get("level") or 0)
            if level >= 3:
                continue  # TP3 永不挂限价
            kind = f"TP{level}"
            q, px = float(lv["qty"] or 0), float(lv["price"] or 0)
            if q <= 0 or px <= 0:
                continue

            # 【关键修复】挂单前必须再次确认交易所侧没有该档的TP订单
            existing_orders = self._collect_tp_limit_orders()
            at_px_existing = [o for o in existing_orders if abs(o.get("price", 0) - px) <= 1.0]
            if at_px_existing:
                logger.warning(
                    f"[{self.symbol}] 交易所侧已有 TP@{px:.2f} ({len(at_px_existing)}张)，复用现有订单"
                )
                # 复用交易所侧已有订单，不挂新单
                existing = at_px_existing[0]
                existing_oid = str(existing.get("ordId") or existing.get("orderId") or "")
                if existing_oid:
                    tag = make_defense_client_order_id(self.symbol, kind, px)
                    self._register_pending_defense_tag(tag, kind, price=px, order_id=existing_oid)
                    self._save_state()
                continue

            # 幂等标签拦截（规格 v1.0 §8-9）
            # 【关键修复】有未完成标签时，必须确认交易所侧没有对应订单才能清理
            blocked, tag0, meta0 = self._has_open_pending_defense_tag(kind)
            if blocked:
                logger.warning(
                    f"[{self.symbol}] 本地未完成标签 tag={tag0} kind={kind} → "
                    f"开始核实原订单是否已失败 | px={px:.2f}"
                )
                # 有 order_id → 查交易所侧
                if meta0.get("order_id"):
                    if not self._confirm_stale_before_clear(tag0, meta0):
                        logger.warning(
                            f"[{self.symbol}] 标签 {tag0} 对应订单仍在处理中 → 拒绝清理，本次跳过"
                        )
                        continue
                    self._complete_pending_defense_tag(tag=tag0)
                    self._save_state()
                    time.sleep(0.15)
                else:
                    # 无 order_id（仅有本地标签）
                    age = time.time() - float(meta0.get("ts", 0) or 0)
                    if age >= 45.0:
                        logger.warning(
                            f"[{self.symbol}] 标签 {tag0} 无 order_id 且陈旧 {age:.0f}s ≥ 45s → 直接清理"
                        )
                        self._complete_pending_defense_tag(tag=tag0)
                        self._save_state()
                        time.sleep(0.15)
                    else:
                        logger.warning(
                            f"[{self.symbol}] 标签 {tag0} 无 order_id 且仅 {age:.0f}s < 45s → "
                            f"拒绝清理（可能还在处理中），本次跳过"
                        )
                        continue

            tag = make_defense_client_order_id(self.symbol, kind, px)
            self._register_pending_defense_tag(tag, kind, price=px)
            try:
                self._save_state()
            except Exception:
                pass
            last = None
            # v16.11（根因二修复）：TP 挂单失败时强制重试 3 次
            max_retries = 3
            for attempt in range(max_retries):
                res = deepcoin_client.place_limit_order(
                    self.symbol, close_side, pos_side, px, q,
                    reduce_only=True, cl_ord_id=tag,
                )
                if res and deepcoin_client._is_success(res):
                    last = res
                    break
                if attempt < max_retries - 1:
                    time.sleep(0.3)
            if not last:
                self._complete_pending_defense_tag(tag=tag)
                try:
                    self._save_state()
                except Exception:
                    pass
                logger.error(
                    f"[{self.symbol}] UPDATE_TP 挂 TP{level} @ {px:.2f} "
                    f"失败（已释放标签，max_retries={max_retries}）"
                )
                continue
            oid = str(last.get("orderId") or last.get("algoId") or "")
            self._register_pending_defense_tag(tag, kind, price=px, order_id=oid)
            placed += 1
            logger.info(f"📈 UPDATE_TP 挂 TP{level} {q} @ {px:.2f} tag={tag}")
            time.sleep(0.25)
        return placed

    def _handle_tv_tp_update(self, payload):
        """
        UPDATE_TP（v6.9.108 动能止盈升级）：
        只撤换限价 TP123，绝不触碰硬止损 / 雷达。
        """
        side = str(payload.get("side") or "").strip().upper()
        new_tps = self._sanitize_tp_prices([
            self._safe_float(payload.get("tv_tp1"), 0),
            self._safe_float(payload.get("tv_tp2"), 0),
            self._safe_float(payload.get("tv_tp3"), 0),
        ])
        if sum(1 for t in new_tps if t > 0) < 3:
            logger.warning(f"UPDATE_TP 无效：TP 不全 {new_tps}")
            return

        pos = self._get_active_position()
        if not pos or self._safe_qty(pos.get("size", 0)) <= 0:
            logger.info("UPDATE_TP 到达但盘口已空仓 → 忽略")
            return

        pos_side = self._pos_side_label(pos)
        if side and side != pos_side:
            logger.warning(f"UPDATE_TP side={side} 与实盘 {pos_side} 不符，已忽略")
            return

        live_qty = self._resolve_live_qty(pos["size"])
        entry = float(pos.get("entry_price") or self.watched_entry or 0)
        curr_px = float(
            deepcoin_client.get_current_price(self.symbol)
            or self.tv_price
            or 0
        )
        self.current_side = pos_side
        if not self.monitoring:
            self.monitoring = True
            self.watched_qty = live_qty
            self.watched_entry = entry

        if not validate_tp_prices_for_side(pos_side, entry, new_tps):
            logger.warning(
                f"UPDATE_TP 方向校验失败 {pos_side} entry={entry:.2f} tps={new_tps} → 忽略"
            )
            dingtalk.report_system_alert(
                "UPDATE_TP 已拒绝",
                f"{pos_side} entry `{entry:.2f}` | 新TP {new_tps} 与持仓方向不符",
            )
            return

        tp1 = float(new_tps[0])
        if curr_px > 0:
            if pos_side == "LONG" and tp1 <= curr_px:
                logger.warning(
                    f"UPDATE_TP 新TP1={tp1:.2f} ≤ 市价 {curr_px:.2f} → 忽略（防即时成交）"
                )
                return
            if pos_side == "SHORT" and tp1 >= curr_px:
                logger.warning(
                    f"UPDATE_TP 新TP1={tp1:.2f} ≥ 市价 {curr_px:.2f} → 忽略（防即时成交）"
                )
                return

        old_tps = list(getattr(self, "_prev_tv_tps_before_update", None) or self.tv_tps or [])
        self.tv_tps = new_tps
        self._save_state()

        same_ledger = (
            len(old_tps) >= 3
            and all(
                abs(float(old_tps[i] or 0) - float(new_tps[i] or 0)) <= 0.51
                for i in range(3)
            )
        )
        audit_before = self._audit_tp_levels(live_qty)
        if same_ledger and self._tp_audit_ok(audit_before):
            logger.info(f"UPDATE_TP 幂等跳过：TP 已是 {new_tps}")
            return

        cancelled = self._cancel_all_tp_limit_orders(max_rounds=4)
        time.sleep(1.5)  # 【关键修复】增加等待时间确保撤销完成
        leftover = self._collect_tp_limit_orders()
        if leftover:
            logger.warning(
                f"UPDATE_TP 撤旧 TP 未净（剩 {len(leftover)}）→ 再次尝试撤销"
            )
            # 【关键修复】旧单未撤净，追加一轮撤销
            self._cancel_all_tp_limit_orders(max_rounds=3)
            time.sleep(1.5)
            leftover = self._collect_tp_limit_orders()
            if leftover:
                logger.error(
                    f"UPDATE_TP 撤旧 TP 仍未净（剩 {len(leftover)}）→ 放弃挂新单，等待下次"
                )
                dingtalk.report_system_alert(
                    "UPDATE_TP 撤单失败",
                    f"旧限价 TP 未清净 {len(leftover)} 张，未挂新价，硬止损/雷达未动",
                )
                if old_tps and sum(1 for t in old_tps if float(t or 0) > 0) >= 2:
                    self.tv_tps = self._sanitize_tp_prices(old_tps)
                    self._save_state()
                return

        placed = self._place_tp_levels_only(live_qty, retries=2)
        time.sleep(0.5)
        audit = self._audit_tp_levels(live_qty)
        verified = self._tp_audit_ok(audit)
        if not verified and placed > 0:
            time.sleep(0.35)
            placed += self._place_tp_levels_only(live_qty, retries=1)
            time.sleep(0.4)
            audit = self._audit_tp_levels(live_qty)
            verified = self._tp_audit_ok(audit)

        verify_note = (
            f"动能 UPDATE_TP | {old_tps} → {new_tps} | "
            f"撤旧 {cancelled} | 新挂 {placed} | "
            f"止盈 {audit.get('matched_full', 0)}/{audit.get('expected', 0)} | "
            f"持仓 {live_qty} 张 @ {entry:.2f} | "
            f"市价 {curr_px:.2f} | 硬止损/雷达未触碰"
        )
        logger.info(f"🚀 UPDATE_TP 完成 verified={verified} | {verify_note}")
        self._call_dingtalk(
            dingtalk.report_tv_tp_updated,
            side=pos_side,
            live_qty=live_qty,
            entry=entry,
            old_tps=old_tps,
            new_tps=new_tps,
            placed=placed,
            regime=self.regime,
            verify_note=verify_note,
            verified=verified,
            curr_px=curr_px,
        )
        if not verified:
            dingtalk.report_system_alert(
                "UPDATE_TP 对齐未完成",
                f"{self._format_audit_summary(audit)} | 哨兵将继续核对",
            )

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
        """FAVORABLE=雷达已/应激活 | SHIELD=维护TV硬止损"""
        if curr_px <= 0 or not self.watched_entry:
            return "SHIELD"
        if self._is_radar_active() or self._should_radar_trail(curr_px):
            return "FAVORABLE"
        return "SHIELD"

    def _shield_present_on_exchange(self):
        stop_px = self._shield_stop_price()
        if stop_px and self._has_shield_stop_at_price(stop_px):
            return True
        audit = self._audit_shield_orders(self._resolve_live_qty(self.watched_qty or 0))
        return audit.get("status") in ("ok", "duplicate", "qty_mismatch")

    def _wait_shield_cleared(self, entry=None, retries=8, delay=0.4):
        def _probe():
            if self._shield_present_on_exchange():
                return None
            return True

        return bool(self._wait_verify(_probe, retries=retries, delay=delay))

    def _radar_min_stop_gap(self, curr_px=0.0):
        px = float(curr_px or 0)
        if px <= 0:
            try:
                px = float(deepcoin_client.get_current_price(self.symbol) or 0)
            except Exception:
                px = 0.0
        if px <= 0:
            return RADAR_STOP_MIN_GAP_USD
        return max(RADAR_STOP_MIN_GAP_USD, px * RADAR_STOP_MIN_GAP_PCT)

    def _clamp_radar_sl_for_market(self, curr_px, sl):
        if not sl or curr_px <= 0:
            return sl
        gap = self._radar_min_stop_gap(curr_px)
        sl = round(float(sl), 2)
        if self.current_side == "LONG":
            safe_cap = round(curr_px - gap, 2)
            if sl >= safe_cap:
                sl = safe_cap
            merged = self._clamp_radar_to_tv_floor(sl)
            sl = min(merged, safe_cap) if merged and merged > safe_cap else (merged or sl)
        elif self.current_side == "SHORT":
            safe_cap = round(curr_px + gap, 2)
            if sl <= safe_cap:
                sl = safe_cap
            merged = self._clamp_radar_to_tv_floor(sl)
            sl = max(merged, safe_cap) if merged and merged < safe_cap else (merged or sl)
        return sl

    def _can_safely_place_radar_sl(self, curr_px, sl):
        if curr_px <= 0 or not sl:
            return False
        gap = self._radar_min_stop_gap(curr_px)
        sl = float(sl)
        if self.current_side == "LONG":
            return sl <= curr_px - gap
        if self.current_side == "SHORT":
            return sl >= curr_px + gap
        return False

    def _notify_shield_handoff_to_radar(self, real_amt, curr_px, new_sl, reason="",
                                        sl_verified=False, cancelled_hint=0):
        if getattr(self, "_shield_handoff_notified", False):
            return
        real_amt = float(self._resolve_live_qty(real_amt) or 0)
        if real_amt <= 0:
            return
        stop_px = self._shield_stop_price()
        progress = self._radar_activation_progress(curr_px) if curr_px > 0 else 1.0
        verify_note = (
            f"先挂雷达保本 @ {new_sl:.2f} 已核实"
            + (f" | 替换 TV硬止损 @ {stop_px:.2f}" if stop_px else "")
            + f" | 持仓 {real_amt} 张"
        )
        if not sl_verified:
            verify_note += f" | {dingtalk.VERIFY_DELAY_MARK}"
        self._call_dingtalk(
            dingtalk.report_shield_disarmed,
            side=self.current_side,
            live_qty=real_amt,
            entry=self.watched_entry,
            cancelled_count=max(cancelled_hint, 1),
            reason=reason or "雷达交棒 · 先挂保本再撤 tv_sl",
            radar_progress=progress,
            verify_note=verify_note,
        )
        self._shield_handoff_notified = True
        self._save_state()

    def _perform_radar_handoff(self, real_amt, curr_px, reason=""):
        """
        【规格 v1.0 · §5.1 雷达延迟激活】
        价格到达TP1-TP2中点（首次开仓）或TP2（重入开仓）时触发雷达交棒。
        不再要求 TP1 先成交。
        """
        real_amt = float(self._resolve_live_qty(real_amt) or 0)
        if real_amt <= 0:
            return False
        if getattr(self, "_open_in_progress", False) or getattr(
            self, "_defense_align_in_progress", False
        ):
            logger.info(f"📡 雷达交棒拒绝：开仓/防线重建中 | {reason or ''}")
            return False
        # 规格 §5.1：价格必须到达TP1-TP2中点（首次）或TP2（重入）
        if not self._should_radar_trail(curr_px):
            gate = float(self._radar_activation_price() or 0)
            logger.info(
                f"📡 雷达交棒拒绝：价格未达激活阈值 | 现价={float(curr_px or 0):.2f} "
                f"| 阈值={gate:.2f} | {reason or ''}"
            )
            return False
        if not self._should_radar_trail(curr_px):
            return False

        new_sl = self._compute_radar_sl(curr_px)
        if new_sl is None:
            return False

        boot_sl = self._radar_breakeven_floor()
        if self.current_side == "LONG":
            boot_sl = self._clamp_radar_to_tv_floor(max(new_sl or 0, boot_sl))
            if boot_sl > float(self.current_sl or 0):
                self.current_sl = boot_sl
        else:
            boot_sl = self._clamp_radar_to_tv_floor(min(new_sl or boot_sl, boot_sl))
            if boot_sl < float(self.current_sl or 999999) or float(self.current_sl or 0) >= self.watched_entry:
                self.current_sl = boot_sl

        safe_sl = self._clamp_radar_sl_for_market(curr_px, self.current_sl)
        if not self._can_safely_place_radar_sl(curr_px, safe_sl):
            gap = self._radar_min_stop_gap(curr_px)
            logger.info(
                f"📡 雷达交棒延迟：保本 {safe_sl:.2f} 距现价 {curr_px:.2f} "
                f"不足 {gap:.2f} USDT，保留 tv_sl 呼吸空间"
            )
            return False

        had_tv_shield = (
            getattr(self, "shield_active", False)
            or self._shield_present_on_exchange()
        )
        old_tv = self._shield_stop_price()
        self.current_sl = safe_sl
        self._radar_armed_after_tp1 = True
        self._save_state()

        sl_placed = self._ensure_radar_sl(real_amt, safe_sl)
        sl_verified = sl_placed and self._wait_verify(
            lambda: self._has_trigger_sl_near(safe_sl),
            retries=10,
            delay=0.45,
        )
        if not sl_verified:
            logger.warning(
                f"📡 雷达交棒中止：保本 @ {safe_sl:.2f} 未核实，不撤 tv_sl 不交棒"
            )
            if had_tv_shield and old_tv:
                self._maintain_hard_shield(real_amt, curr_px, force=True)
            return False

        if had_tv_shield:
            # TODO(v13.81+): 币安单系统已禁止雷达激活时撤 tv_sl/硬止损（永久硬止损并行）。
            # Deepcoin 仍 _disarm_shield — 勿在未测双 STOP 槽位前改，避免裸仓窗口。
            self._disarm_shield("雷达交棒 · 保本已挂", notify=False)

        logger.info(
            f"📡 雷达交棒成功：保本 @ {safe_sl:.2f} | best={self.best_price:.2f} | "
            f"现价 {curr_px:.2f}"
        )
        if had_tv_shield and not getattr(self, "_shield_handoff_notified", False):
            self._notify_shield_handoff_to_radar(
                real_amt, curr_px, safe_sl,
                reason=reason or "雷达交棒 · 先挂保本再撤 tv_sl",
                sl_verified=True,
                cancelled_hint=1 if old_tv else 0,
            )
        if not getattr(self, "_radar_activation_notified", False):
            self._report_radar_first_activation(
                real_amt, curr_px, safe_sl, sl_placed,
            )
        return True

    def _force_disarm_shield_before_radar(self, curr_px, reason="", notify=True):
        real_amt = self._resolve_live_qty(self.watched_qty or 0)
        if real_amt <= 0:
            return 0
        ok = self._perform_radar_handoff(
            real_amt, curr_px, reason=reason or "雷达接管",
        )
        return 1 if ok else 0

    def _should_disarm_shield_for_favorable(self, curr_px):
        """TP1 成交且雷达已激活 → 才撤 tv_sl 交棒移动保本（TP1 前保留宽硬止损）
        TODO(v13.81+): 币安 _should_disarm_shield_for_favorable 已恒 False（不撤硬止损）；
        本函数仍 True 时会走 _perform_radar_handoff → _disarm_shield，待对齐规格再改。"""
        if not self._tp1_filled_verified():
            return False
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
        双层风控：雷达移动保本（VPS）+ TV tv_sl 硬止损底线（双轨独立挂单）。
        雷达线不得低于 tv_sl；UPDATE_SL 只更新底线，雷达逻辑独立运行。
        """
        self._disarm_premature_radar(real_amt, curr_px, source="哨兵防线")
        if self._resolve_defense_regime(curr_px) == "FAVORABLE":
            if self._should_radar_trail(curr_px) or self._is_radar_active():
                self._process_radar_trailing(real_amt, curr_px)
        self._maintain_hard_shield(real_amt, curr_px)

    def _should_activate_shield(self, curr_px):
        """始终维护 TV 硬止损底线（可与雷达并行）"""
        if not self.watched_entry or not self.current_side:
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
        # v16.15（根因四修复）：本地有 set-position-sltp 记录时，直接返回 True
        # 避免因交易所查询不到而误判，导致反复挂单
        _local_ord = str(getattr(self, "_shield_sltp_ord_id", "") or "").strip()
        _cancelled = getattr(self, "_shield_cancelled_ids", set()) or set()
        _set_at = float(getattr(self, "_shield_sltp_set_at", 0) or 0)
        if _local_ord and _local_ord not in _cancelled and (time.time() - _set_at) < 300:
            return True
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
            oid = str(t.get("ordId") or "").strip()
            if not oid:
                continue
            # v16.15（根因四修复）：幂等撤销——本轮已撤销的直接跳过
            if oid in self._shield_cancelled_ids:
                continue
            deepcoin_client.cancel_trigger_order(self.symbol, oid)
            self._shield_cancelled_ids.add(oid)
            _local = str(getattr(self, "_shield_sltp_ord_id", "") or "").strip()
            if _local == oid:
                self._shield_sltp_ord_id = ""
            cancelled += 1
            time.sleep(0.15)
        return cancelled

    def _split_shield_quantities(self, total_qty):
        return self._calculate_tp_quantities(self._safe_qty(total_qty), list(SHIELD_TIER_RATIOS))

    def _can_maintain_shield_now(self, force=False, audit=None):
        if force:
            return True
        now = time.time()
        audit = audit or {}
        missing_shield = audit.get("status") == "missing"
        if now < getattr(self, "_sentinel_grace_until", 0):
            if missing_shield:
                if now - getattr(self, "_last_shield_maintain_ts", 0) < 12:
                    return False
                return True
            return False
        if now - getattr(self, "_last_shield_maintain_ts", 0) < SHIELD_MAINTAIN_COOLDOWN_SEC:
            if missing_shield and now - getattr(self, "_last_shield_maintain_ts", 0) >= 12:
                return True
            return False
        streak = getattr(self, "_shield_fail_streak", 0)
        if streak > 0:
            backoff = min(
                SHIELD_FAIL_BACKOFF_BASE_SEC * (2 ** (streak - 1)),
                SHIELD_FAIL_BACKOFF_MAX_SEC,
            )
            if now - getattr(self, "_last_shield_fail_ts", 0) < backoff:
                if missing_shield and now - getattr(self, "_last_shield_fail_ts", 0) >= 12:
                    return True
                return False
        return True

    def _wait_shield_audit_ok(self, live_qty, entry=None, retries=10, delay=0.45):
        entry = float(entry or self.watched_entry or 0)
        live_qty = self._safe_qty(self._resolve_live_qty(live_qty))

        def _probe():
            audit = self._audit_shield_orders(live_qty, entry)
            return audit if self._shield_orders_adequate(audit) else None

        verified = self._wait_verify(_probe, retries=retries, delay=delay)
        return verified or self._audit_shield_orders(live_qty, entry)

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

        # v16.15（根因四修复）：本地已记录 set-position-sltp 的 order_id，
        # 且不在本轮已撤销列表中 → 视为已设置（防止 Deepcoin 内部订单查询不到导致的死循环）
        # 条件：order_id 非空 + 未被标记撤销 + 最近5分钟内设置（防重启后残留）
        _local_ord_id = str(getattr(self, "_shield_sltp_ord_id", "") or "").strip()
        _cancelled_ids = getattr(self, "_shield_cancelled_ids", set()) or set()
        _set_at = float(getattr(self, "_shield_sltp_set_at", 0) or 0)
        _local_valid = (
            _local_ord_id
            and _local_ord_id not in _cancelled_ids
            and (time.time() - _set_at) < 300
        )

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
                # v16.15（根因四修复）：本地已有 set-position-sltp 记录时，跳过条件单缺失判断
                if not _local_valid:
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
                # v16.15：本地已有 set-position-sltp 记录时，不把交易所订单视为孤儿
                if not _local_valid:
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

    @staticmethod
    def _recover_lock_pid_alive(info):
        if not info:
            return False
        for part in info.replace("\n", " ").split():
            if part.startswith("pid="):
                try:
                    pid = int(part.split("=", 1)[1])
                except (TypeError, ValueError):
                    return False
                if pid <= 0:
                    return False
                try:
                    os.kill(pid, 0)
                    return True
                except OSError:
                    return False
                except Exception:
                    return False
        return False

    def _try_acquire_recover_singleton(self):
        """多 worker 导入时仅允许一个进程执行重启接管，避免双钉钉/双撤挂"""
        try:
            os.makedirs("logs", exist_ok=True)
            if os.path.exists(RECOVER_LOCK_FILE):
                age = time.time() - os.path.getmtime(RECOVER_LOCK_FILE)
                try:
                    with open(RECOVER_LOCK_FILE, encoding="utf-8") as f:
                        info = f.read().strip()
                except Exception:
                    info = "?"
                holder_alive = self._recover_lock_pid_alive(info)
                if age < RECOVER_LOCK_TTL_SEC and holder_alive:
                    logger.info(
                        f"🔄 跳过重复重启接管 (进程 {info} 仍存活, {age:.0f}s 前)"
                    )
                    return False
                if age < RECOVER_LOCK_TTL_SEC and not holder_alive:
                    logger.info(
                        f"🔄 旧接管锁已失效 (原 {info})，重新执行闪电接管"
                    )
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
            defense_plan = "雷达移动保本 + TV硬止损底线 (双轨)"
        elif adverse > 0.001:
            pnl_label = f"浮亏 {adverse:.1%}"
            defense_plan = "持有 TP123 + TV硬止损全平"
        elif favorable > 0.001:
            pnl_label = f"微盈 {favorable:.1%}·未达雷达激活"
            defense_plan = "持有 TP123 + TV硬止损 (朝TP1迈进中)"
        else:
            pnl_label = "保本附近"
            defense_plan = "持有 TP123 + TV硬止损"

        stop_px = self._shield_stop_price(entry)
        if should_radar or radar_active:
            radar_sl = (
                self._clamp_radar_to_tv_floor(self.current_sl)
                if self._is_radar_active() else None
            )
            shield_status = (
                f"TV底线 @ {stop_px:.2f}" if stop_px else "TV底线待核实"
            )
            if radar_sl:
                shield_status += f" | 雷达 @ {radar_sl:.2f}"
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
        """重启一次性防线：TV tv_sl 硬止损 + 雷达（若应激活）双轨维护"""
        actions = []
        if health.get("should_radar") or health.get("radar_active"):
            if not self._is_radar_active():
                self._refresh_radar_state_on_recover(curr_px, self.watched_entry)
            sl = self._clamp_radar_to_tv_floor(self.current_sl) if self._is_radar_active() else None
            if sl and not self._has_trigger_sl_near(sl):
                if self._ensure_radar_sl(real_amt, sl):
                    actions.append(f"雷达止损@{sl:.2f}")
                else:
                    actions.append(f"雷达止损待补@{sl:.2f}")
            elif sl:
                actions.append(f"雷达止损已齐@{sl:.2f}")

        ok = self._maintain_hard_shield(real_amt, curr_px, force=True)
        stop_px = self._shield_stop_price()
        tv_note = (
            "TV硬止损"
            if getattr(self, "tv_sl", 0) > 0
            else "TV tv_sl 缺失"
        )
        tag = f"{tv_note}@{stop_px:.2f}" if stop_px else tv_note
        actions.append(f"{tag}已齐" if ok else f"{tag}待补")
        return actions

    def _bootstrap_live_defenses_after_recover(self, real_amt, curr_px, audit=None):
        """
        重启/关机后全域自适应：核查 TP123+止损 → 缺则补挂不重复 → 雷达立即干活锁利。
        """
        if real_amt <= 0 or not self.current_side:
            return {"actions": [], "audit": audit or {}}

        curr_px = float(curr_px or deepcoin_client.get_current_price(self.symbol) or 0)
        actions = []
        try:
            audit = audit or self._audit_tp_levels(real_amt)

            if not self._tp_audit_ok(audit):
                repaired, n_actions = self._surgical_repair_tp_defenses(
                    real_amt, self.watched_entry,
                )
                if n_actions > 0:
                    actions.append(f"智能补挂TP({n_actions}步)")
                    audit = repaired

            self._refresh_radar_state_on_recover(curr_px, self.watched_entry)
            health = self._build_recover_health_report(
                {"side": self.current_side, "size": real_amt, "entry_price": self.watched_entry},
                curr_px, audit,
            )
            actions.extend(self._apply_recover_defense_policy(real_amt, curr_px, health))

            if curr_px > 0 and (health.get("should_radar") or health.get("radar_active")):
                self._process_radar_trailing(real_amt, curr_px)
                sl = self._radar_sl_to_pass()
                if sl and not self._has_trigger_sl_near(sl):
                    if self._ensure_radar_sl(real_amt, sl):
                        actions.append(f"雷达SL@{sl:.2f}")
                if self._is_radar_active() and not getattr(self, "_radar_activation_notified", False):
                    self._report_radar_first_activation(
                        real_amt, curr_px, self._clamp_radar_to_tv_floor(self.current_sl),
                        self._has_trigger_sl_near(self.current_sl),
                    )
                actions.append(f"雷达激活·进度{health.get('radar_progress', 0):.0%}")

            self._radar_guardian_audit(real_amt, curr_px)
        except Exception as e:
            logger.error(f"重启全域核查部分失败(继续哨兵): {e}")
            actions.append(f"核查异常:{e}")
            audit = audit or self._audit_tp_levels(real_amt)
            health = {}

        self._post_recover_radar_pulse = True
        self._save_state()
        logger.info(
            f"📡 [重启全域核查] {' · '.join(actions) if actions else '盘口已齐，雷达待命'} | "
            f"TP {audit.get('matched_full', 0)}/{audit.get('expected', 0)}"
        )
        return {"actions": actions, "audit": audit, "health": health}

    def _reconcile_shield_on_recover(self, live_qty, curr_px):
        if live_qty <= 0 or not self.watched_entry:
            return
        if self._is_radar_active() or (curr_px > 0 and self._should_radar_trail(curr_px)):
            return

        audit = self._audit_shield_orders(live_qty)
        if self._shield_orders_adequate(audit):
            self.shield_active = True
            self._shield_fail_streak = 0
            self.shield_sized_qty = live_qty
            self._shield_arm_notified = True
            stop_px = self._shield_stop_price()
            logger.info(
                f"🛡️ 重启：盘口 TV硬止损已齐"
                + (f" @ {stop_px:.2f}" if stop_px else "")
                + "，跳过重挂"
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
                "🛡️ 重启：TV硬止损待补挂（宽限期后哨兵按冷却处理）"
            )
            self._save_state()

    def _disarm_shield(self, reason="", notify=False):
        # v16.15（根因四修复）：优化 REST 消耗——只查一次 pending 订单列表
        # 用本地跟踪的已撤销 ID + 查询结果缓存，避免重复 REST 查询
        all_pending = deepcoin_client.get_trigger_orders_pending(self.symbol)
        cancelled = 0

        # 第一次：撤销所有 shield 订单
        for t in all_pending:
            if not self._is_shield_trigger_order(t):
                continue
            oid = str(t.get("ordId") or "").strip()
            if not oid or oid in self._shield_cancelled_ids:
                continue
            deepcoin_client.cancel_trigger_order(self.symbol, oid)
            self._shield_cancelled_ids.add(oid)
            _local = str(getattr(self, "_shield_sltp_ord_id", "") or "").strip()
            if _local == oid:
                self._shield_sltp_ord_id = ""
            cancelled += 1
            time.sleep(0.2)

        # 用本地已撤销 ID 判断是否还有剩余 shield（在 exchange 未查到但本地记录有）
        _local_ord = str(getattr(self, "_shield_sltp_ord_id", "") or "").strip()
        _local_set = float(getattr(self, "_shield_sltp_set_at", 0) or 0)
        still_have_local = (
            _local_ord
            and _local_ord not in self._shield_cancelled_ids
            and (time.time() - _local_set) < 300
        )

        # 检查是否还有未撤销的 shield 订单
        still_on_exchange = False
        for t in all_pending:
            if not self._is_shield_trigger_order(t):
                continue
            oid = str(t.get("ordId") or "").strip()
            if oid and oid not in self._shield_cancelled_ids:
                still_on_exchange = True
                break

        if still_on_exchange or still_have_local:
            # 第二次查询（仅在第一次撤销后仍有订单时）
            all_pending2 = deepcoin_client.get_trigger_orders_pending(self.symbol)
            for t in all_pending2:
                px = self._trigger_order_price(t)
                if px is None:
                    continue
                stop_px = self._shield_stop_price()
                if not stop_px:
                    continue
                if abs(px - stop_px) <= SHIELD_STOP_TOLERANCE:
                    oid = str(t.get("ordId") or "").strip()
                    if oid and oid not in self._shield_cancelled_ids:
                        deepcoin_client.cancel_trigger_order(self.symbol, oid)
                        self._shield_cancelled_ids.add(oid)
                        if _local_ord == oid:
                            self._shield_sltp_ord_id = ""
                        cancelled += 1
                        time.sleep(0.15)
            time.sleep(0.4)

        had = getattr(self, "shield_active", False) or bool(
            getattr(self, "shield_tiers_consumed", [])
        ) or (still_on_exchange or still_have_local)
        live_qty = self._resolve_live_qty(self.watched_qty or 0)
        entry = self.watched_entry
        self.shield_active = False
        self.shield_tiers_consumed = []
        self.shield_sized_qty = 0.0
        self._shield_arm_notified = False
        self._shield_sltp_ord_id = ""
        self._shield_sltp_set_at = 0.0
        self._shield_cancelled_ids = set()
        self._save_state()
        if reason and (had or cancelled):
            logger.info(f"🛡️ [硬止损解除] {reason} | 撤销 {cancelled} 笔 TV硬止损")
        if notify and cancelled > 0 and live_qty > 0:
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
                cancelled_count=cancelled,
                reason=reason,
                radar_progress=progress,
                verify_note=(
                    f"撤 {cancelled} 笔 TV硬止损 | "
                    + (
                        "雷达已激活，专注移动保本"
                        if self._is_radar_active()
                        else f"雷达进度 {progress:.0%}，TP1成交后推升止损"
                    )
                ),
            )

    def _place_shield_stops(self, live_qty, entry=None, reason="", force=False,
                            recover_mode=False, suppress_alert=False):
        """
        使用 set-position-sltp 接口设置硬止损（市价触发），替代条件单。
        根据 Deepcoin API 文档，set-position-sltp 是为止有持仓设置止盈止损的正确接口，
        支持市价委托（slOrdPx=-1），避免价格跳空时限价单无法成交的问题。
        """
        entry = float(entry or self.watched_entry or 0)
        live_qty = self._resolve_live_qty(live_qty)
        if live_qty <= 0 or entry <= 0 or not self.current_side:
            return False

        # 【修复】价格已穿过硬止损线时，跳过无效操作（防死循环）
        curr_px = deepcoin_client.get_current_price(self.symbol) or 0
        if curr_px > 0:
            stop_px = self._shield_stop_price()
            if stop_px > 0:
                crossed = False
                if self.current_side == "LONG" and curr_px <= stop_px:
                    crossed = True
                elif self.current_side == "SHORT" and curr_px >= stop_px:
                    crossed = True
                if crossed:
                    logger.warning(
                        f"🛡️ 硬止损@{stop_px:.2f} 已被价格@{curr_px:.2f}穿过，"
                        f"标记已触发（防死循环）"
                    )
                    self.shield_active = True
                    self.shield_sized_qty = live_qty
                    self._save_state()
                    return True

        pos_side = "long" if self.current_side == "LONG" else "short"
        stop_px = self._shield_stop_price()
        if not stop_px or stop_px <= 0:
            logger.warning(f"🛡️ 硬止损无效：无有效止损价格")
            return False

        # 计算交易所止损价（含滑点保护）
        exchange_sl = float(order_stop_price(
            self.current_side, stop_px,
            profile=getattr(self, "breath_profile", None)
        ) or stop_px)

        # v16.15（根因四修复）：幂等挂单——价格相同且最近15s内设置过则跳过
        _last = float(getattr(self, "_last_applied_tv_sl", 0) or 0)
        _last_set = float(getattr(self, "_shield_sltp_set_at", 0) or 0)
        _local_ord = str(getattr(self, "_shield_sltp_ord_id", "") or "").strip()
        if (
            abs(exchange_sl - _last) <= SHIELD_STOP_TOLERANCE
            and _local_ord
            and (time.time() - _last_set) < 15.0
        ):
            self.shield_active = True
            self.shield_sized_qty = live_qty
            self._save_state()
            logger.info(
                f"🛡️ [TV硬止损] 幂等跳过 | {live_qty} 张 @ {exchange_sl:.2f} | "
                f"ordId={_local_ord} | {_last_set > 0 and (time.time() - _last_set):.1f}s前已设置"
            )
            return True

        # v16.15（根因四修复）：设置前先撤销旧订单（防止 Deepcoin 拒绝重复设置）
        if _local_ord and _local_ord not in getattr(self, "_shield_cancelled_ids", set()):
            deepcoin_client.cancel_trigger_order(self.symbol, _local_ord)
            self._shield_cancelled_ids = self._shield_cancelled_ids or set()
            self._shield_cancelled_ids.add(_local_ord)
            logger.info(f"🛡️ [TV硬止损] 撤销旧订单 {_local_ord} 后重新设置")
            time.sleep(0.3)

        # 使用 set-position-sltp 接口，设置市价止损（slOrdPx=-1）
        res = deepcoin_client.set_position_sltp(
            symbol=self.symbol,
            pos_side=pos_side,
            sl_trigger_px=exchange_sl,
            tp_trigger_px=None,  # 止盈由 TP123 分批处理
            td_mode="cross",
            mrg_position="merge",
            trigger_px_type="last",
            sl_ord_px="-1",  # 市价止损
            tp_ord_px="-1",
        )

        # 验证接口调用结果
        if res and str(res.get("code", "0")) in ("0", "00000", ""):
            self.shield_active = True
            self.shield_sized_qty = live_qty
            self._shield_fail_streak = 0
            # v16.15（根因四修复）：从响应中提取 order_id 并存储
            _new_ord_id = ""
            _data = res.get("data") if res else None
            if isinstance(_data, dict):
                _new_ord_id = str(_data.get("ordId") or _data.get("slOrdId") or _data.get("orderId") or "").strip()
            elif isinstance(_data, list) and _data:
                _first = _data[0]
                _new_ord_id = str(_first.get("ordId") or _first.get("slOrdId") or _first.get("orderId") or "").strip()
            if not _new_ord_id:
                _new_ord_id = f"_sltp_{exchange_sl}_{int(time.time())}"
            self._shield_sltp_ord_id = _new_ord_id
            self._shield_sltp_set_at = time.time()
            self._save_state()
            logger.warning(
                f"🛡️ [TV硬止损] 已设置 | {live_qty} 张 @ {exchange_sl:.2f} | "
                f"触发后市价平仓 | 雷达激活后自动覆盖"
            )
            if not getattr(self, "_shield_arm_notified", False):
                self._shield_arm_notified = True
                self._call_dingtalk(
                    dingtalk.report_adverse_shield_armed,
                    side=self.current_side,
                    entry=entry,
                    live_qty=live_qty,
                    adverse_pct=0,
                    tier_prices=[exchange_sl],
                    tier_pcts=SHIELD_TIER_PCTS,
                    verify_note=(
                        (reason or f"TV硬止损 @ {exchange_sl:.2f}")
                        + f" | 实盘 {live_qty} 张 | 市价止损 | 仅播报一次"
                    ),
                )
            return True
        else:
            # 接口调用失败，记录错误
            err_msg = str(res.get("msg", "") if res else "未知错误")
            self._shield_fail_streak = getattr(self, "_shield_fail_streak", 0) + 1
            logger.error(f"🛡️ 硬止损设置失败: {err_msg} | 重试次数: {self._shield_fail_streak}")

            if not suppress_alert and self._shield_fail_streak >= 3:
                dingtalk.report_system_alert(
                    "TV硬止损设置失败",
                    f"连续失败 {self._shield_fail_streak} 次 | 错误: {err_msg}",
                    suggestion="请检查持仓状态和 API 权限",
                )
            return False

    def _maintain_hard_shield(self, real_amt, curr_px=None, force=False):
        """维护 TV tv_sl 硬止损底线；雷达止损独立运行"""
        if real_amt <= 0 or not self.watched_entry:
            return False
        if getattr(self, "tv_sl", 0) > 0:
            if not force and not self._can_maintain_shield_now(force=force):
                return getattr(self, "shield_active", False)
            return self._sync_tv_sl_stop(
                real_amt,
                reason="维护TV硬止损",
                force=force,
            ).get("ok", False)

        if real_amt > 0 and not getattr(self, "_tv_sl_missing_alerted", False):
            logger.error("维护TV硬止损失败：缺少 tv_sl，拒绝 fallback 旧逻辑")
            dingtalk.report_system_alert(
                "TV硬止损缺失",
                f"持仓 {real_amt} 张 但未收到 tv_sl，无法挂止损",
                suggestion="请确认 TV 策略已透传 tv_sl，或发送 UPDATE_SL",
            )
            self._tv_sl_missing_alerted = True
        return False

    def _process_adverse_shield(self, real_amt, curr_px):
        """兼容旧调用 → 维护硬止损"""
        return self._maintain_hard_shield(real_amt, curr_px)

    def _is_radar_active(self):
        """
        【规格 v1.0 · §5.1】
        雷达激活判断：价格已到达TP1-TP2中点（首次开仓）或TP2（重入开仓），
        且止损已移动到保本位（current_sl 已高于/低于 entry）。
        不再要求 TP1 成交才激活雷达。
        """
        if not self.watched_entry or not self.current_sl:
            return False
        # 雷达激活后不撤销，保持激活状态直到仓位平仓
        if getattr(self, "radar_activated", False):
            return True
        # 价格必须到达雷达激活阈值
        gate = float(self._radar_activation_price() or 0)
        if gate <= 0:
            return False
        curr_px = deepcoin_client.get_current_price(self.symbol) or 0
        if curr_px <= 0:
            return False
        side = str(self.current_side or "").upper()
        if side == "LONG":
            price_reached = curr_px >= gate
        elif side == "SHORT":
            price_reached = curr_px <= gate
        else:
            price_reached = False
        # 止损必须已移动到保本位
        if not price_reached:
            return False
        if side == "LONG":
            return self.current_sl > self.watched_entry
        if side == "SHORT":
            return self.current_sl < self.watched_entry
        return False

    def _radar_is_dormant(self):
        """雷达休眠：未激活且未武装。"""
        return not self._is_radar_active() and not getattr(self, "radar_activated", False)

    def _check_early_be_checkpoint(self, curr_px):
        """
        【规格 v1.0 · §5.0 提前保本检查点】
        当价格到达 entry + tp1_distance × 0.5 时，将止损从当前值移动到保本位。
        仅触发一次，触发后标记_done=True，不再重复。
        不启动雷达，不影响雷达的激活状态判断。
        """
        if bool(getattr(self, "_early_be_checkpoint_done", False)):
            return None
        if bool(getattr(self, "radar_activated", False)):
            return None
        entry = float(self.watched_entry or 0)
        side = str(self.current_side or "").strip().upper()
        curr_px_f = float(curr_px or 0)
        if entry <= 0 or side not in ("LONG", "SHORT") or curr_px_f <= 0:
            return None
        tps = list(getattr(self, "tv_tps", None) or [])
        ref = float(getattr(self, "tv_price", 0) or entry)
        tp1_px = float(tps[0] or 0) if len(tps) > 0 else 0.0
        if tp1_px <= 0:
            return None
        tp1_dist = abs(tp1_px - ref)
        if tp1_dist <= 0:
            return None
        trigger_px = entry + tp1_dist * 0.5
        if side == "LONG" and curr_px_f < trigger_px:
            return None
        if side == "SHORT" and curr_px_f > trigger_px:
            return None
        current_sl = float(self.current_sl or 0)
        tick = 0.01
        fee_pct = 0.0008
        fee = entry * fee_pct
        if side == "LONG":
            new_sl = max(entry + tick + fee, current_sl)
        else:
            new_sl = min(entry - tick - fee, current_sl)
        if new_sl == current_sl:
            self._early_be_checkpoint_done = True
            return None
        old_sl = current_sl
        self.current_sl = new_sl
        self._early_be_checkpoint_done = True
        self._save_state()
        logger.info(
            f"[{self.symbol}] 规格 v1.0 §5.0 提前保本检查点触发 "
            f"({side}) | entry={entry:.4f} trigger={trigger_px:.4f} "
            f"old_sl={old_sl:.4f} → new_sl={new_sl:.4f}"
        )

    # ── 规格 v1.0 §8-9：防御单幂等标签 ────────────────────────────────────────

    def _clear_pending_tags_for_kind(self, kind_prefix, save=False):
        """释放某类防御单本地标签（TP1/TP2/HARD/RADAR）。"""
        pref = str(kind_prefix or "").upper()
        tags = dict(getattr(self, "_pending_order_tags", {}) or {})
        changed = False
        for tag, meta in list(tags.items()):
            k = str((meta or {}).get("kind") or "").upper()
            if pref and (k == pref or k.startswith(pref)):
                tags.pop(tag, None)
                changed = True
        if changed:
            self._pending_order_tags = tags
            if save:
                self._save_state()

    def _gc_stale_pending_defense_tags_on_startup(self):
        """
        v16.14（根因一修复）：VPS重启时安全清理所有旧标签。
        重启前旧 VPS 进程已终止，其挂的单（不管成交/失败/撤销）都无法再被确认。
        若不清理，_pending_order_tags 会残留旧标签永久阻止挂新单。
        """
        tags = dict(getattr(self, "_pending_order_tags", {}) or {})
        if not tags:
            return
        dropped = []
        for tag, meta in list(tags.items()):
            dropped.append(f"{tag}({meta.get('kind','?')}/{meta.get('status','?')}/{meta.get('order_id','no-oid')})")
        self._pending_order_tags = {}
        logger.warning(
            f"[{self.symbol}] v16.14 重启清理旧标签 {len(dropped)} 个: {dropped[:8]}"
            f"{'…' if len(dropped) > 8 else ''}"
        )
        self._save_state()

    def _gc_stale_pending_defense_tags(self, max_pending_age_sec=45.0, save=True):
        """清理陈旧防御单本地标签（超时/已完成）。"""
        tags = dict(getattr(self, "_pending_order_tags", {}) or {})
        if not tags:
            return 0
        now = time.time()
        dropped = []
        for tag, meta in list(tags.items()):
            meta = meta or {}
            st = str(meta.get("status") or "open").lower()
            oid = str(meta.get("order_id") or "").strip()
            ts = float(meta.get("ts") or 0)
            age = (now - ts) if ts > 0 else 9999.0
            if st in ("done", "filled", "cancelled", "canceled", "acked"):
                tags.pop(tag, None)
                dropped.append(f"{tag}:{st}")
                continue
            # 【BUG修复】有 order_id 不代表订单已完成！
            # 订单已提交（有 order_id）但尚未终态，不能清理标签
            # 必须等收到成交/撤销确认，或订单超时（由 _confirm_stale_before_clear 处理）才能清理
            # if oid:
            #     tags.pop(tag, None)
            #     dropped.append(f"{tag}:acked")
            #     continue
            if st == "pending" and age >= float(max_pending_age_sec or 45.0):
                tags.pop(tag, None)
                dropped.append(f"{tag}:stale:{age:.0f}s")
                continue
        if not dropped:
            return 0
        self._pending_order_tags = tags
        logger.warning(
            f"[{self.symbol}] 清理陈旧防御标签 {len(dropped)} 个: "
            f"{dropped[:6]}{'…' if len(dropped) > 6 else ''}"
        )
        if save:
            try:
                self._save_state()
            except Exception:
                pass
        return len(dropped)

    def _confirm_stale_before_clear(self, tag, meta):
        """
        v16.11（根因二修复）：清残留标签前，必须先确认原订单已失败/超时。
        不允许"看到残留就清"——只查本地状态表是远远不够的，必须核实交易所侧真实状态。

        返回 True = 已确认可安全清理；False = 不能清理（订单可能还在处理中）。
        """
        if not tag or not meta:
            return False
        meta = meta or {}
        oid = str(meta.get("order_id") or "").strip()
        ts = float(meta.get("ts") or 0)
        age = (time.time() - ts) if ts > 0 else 0.0

        # 条件1：有 order_id → 查交易所真实状态
        if oid:
            try:
                order_res = deepcoin_client.get_order(self.symbol, ord_id=oid)
                if order_res:
                    state = str(order_res.get("state", "") or order_res.get("status", "")).lower().strip()
                    # v16.14（根因一修复）：空状态/查不到订单视为已撤销
                    # 订单在交易所侧已不存在（已成交被移除、已撤销、已过期、或从未成功提交）
                    if state in ("filled", "cancelled", "canceled", "done", "完全成交", "已撤", ""):
                        reason = "已终态" if state else "交易所侧不存在"
                        logger.info(
                            f"[{self.symbol}] 标签 {tag} 的订单 {oid} 状态=[{state or 'N/A'}]，{reason}，确认可清理"
                        )
                        return True
                    # 状态未知或非终态，不清理
                    logger.warning(
                        f"[{self.symbol}] 标签 {tag} 的订单 {oid} 仍在 {state}，拒绝清理"
                    )
                    return False
                else:
                    # v16.14（根因一修复）：order_res 为空（订单不存在/网络异常）→ 视为已撤销
                    logger.warning(
                        f"[{self.symbol}] 标签 {tag} 的订单 {oid} 交易所侧无记录，视为已撤销，确认可清理"
                    )
                    return True
            except Exception as e:
                logger.warning(f"[{self.symbol}] 查询订单 {oid} 状态失败: {e}，拒绝清理")
                return False

        # 条件2：无 order_id（仅本地标签）→ 必须满足陈旧阈值
        STALE_THRESHOLD_SEC = 45.0
        if age >= STALE_THRESHOLD_SEC:
            logger.warning(
                f"[{self.symbol}] 标签 {tag} 无 order_id 但已陈旧 {age:.0f}s >= {STALE_THRESHOLD_SEC}s，"
                f"确认可清理"
            )
            return True
        # 不够陈旧，拒绝清理（防止误清正在处理中的订单）
        logger.warning(
            f"[{self.symbol}] 标签 {tag} 无 order_id 且仅 {age:.0f}s < {STALE_THRESHOLD_SEC}s，"
            f"拒绝清理（可能还在处理中）"
        )
        return False

    def _has_open_pending_defense_tag(self, kind=None):
        """本地已有同档未完成标签 → 拒挂（宁可错过）。"""
        self._gc_stale_pending_defense_tags(save=False)
        want = str(kind or "").upper()
        for tag, meta in dict(getattr(self, "_pending_order_tags", {}) or {}).items():
            st = str((meta or {}).get("status") or "open").lower()
            # 【BUG修复】只要状态不是终态，无论有没有 order_id，都算未完成
            if st in ("done", "filled", "cancelled", "canceled", "acked"):
                continue
            k = str((meta or {}).get("kind") or "").upper()
            if want and k != want and not k.startswith(want):
                continue
            # 有未完成的标签（可能只有本地 pending 或已有 order_id 但未终态）
            return True, tag, meta
        return False, "", {}

    def _register_pending_defense_tag(self, tag, kind, price=0.0, order_id=""):
        tags = dict(getattr(self, "_pending_order_tags", {}) or {})
        oid = str(order_id or "")
        tags[str(tag)] = {
            "kind": str(kind or "").upper(),
            "ts": time.time(),
            "price": float(price or 0),
            "order_id": oid,
            "status": "acked" if oid else "pending",
        }
        self._pending_order_tags = tags

    def _complete_pending_defense_tag(self, tag=None, kind=None, order_id=None):
        tags = dict(getattr(self, "_pending_order_tags", {}) or {})
        changed = False
        for t, meta in list(tags.items()):
            if tag and str(t) != str(tag):
                continue
            if kind and str((meta or {}).get("kind") or "").upper() != str(kind).upper():
                if not (tag or order_id):
                    continue
            if order_id and str((meta or {}).get("order_id") or "") != str(order_id):
                if not tag:
                    continue
            tags.pop(t, None)
            changed = True
        if changed:
            self._pending_order_tags = tags

    def _radar_sl_to_pass(self):
        """
        【规格 v1.0 · §5.1】
        雷达激活后返回当前止损价。
        不再要求 TP1 成交，只要雷达已激活（价格到达阈值）即可。
        """
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
            # 【关键修复】增加等待时间确保撤销完成（从0.6秒改为1.5秒）
            time.sleep(1.5)
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

    def _ensure_radar_sl(self, sl_price, live_qty, for_handoff=False):
        if not sl_price:
            return False
        clamped = self._clamp_radar_to_tv_floor(sl_price)
        if self._has_trigger_sl_near(clamped):
            return True
        self._cancel_stop_orders(scope="radar")
        time.sleep(0.35)
        self._place_radar_sl(live_qty, clamped)
        time.sleep(0.35)
        return self._has_trigger_sl_near(clamped)

    def _report_radar_first_activation(self, real_amt, curr_px, new_sl, sl_placed):
        """
        【规格 v1.0 · §5.1 雷达延迟激活】
        价格到达TP1-TP2中点（首次开仓）或TP2（重入开仓）时发送雷达激活通知。
        不再要求 TP1 先成交。
        """
        if getattr(self, "_radar_activation_notified", False):
            return
        # 规格 §5.1：只要价格到达阈值就激活，不要求TP1成交
        # 止损必须高于入场价（多单）或低于入场价（空单）
        if self.current_side == "LONG" and float(new_sl or 0) <= float(self.watched_entry or 0):
            logger.warning(
                f"📡 雷达激活钉钉跳过：LONG 止损 {new_sl:.2f} 未高于 entry"
            )
            return
        if self.current_side == "SHORT" and float(new_sl or 0) >= float(self.watched_entry or 0):
            logger.warning(
                f"📡 雷达激活钉钉跳过：SHORT 止损 {new_sl:.2f} 未低于 entry"
            )
            return
        verified = self._wait_verify(
            lambda: self._has_trigger_sl_near(new_sl),
            retries=10,
            delay=0.45,
        )
        progress = self._radar_activation_progress(curr_px) if curr_px > 0 else 1.0
        tv_floor = round(float(getattr(self, "tv_sl", 0) or 0), 2)
        verify_note = (
            f"雷达进度 {progress:.0%} | 保本止损 @ {new_sl:.2f} | "
            f"TV底线 tv_sl={tv_floor or 'fallback'} | "
            f"持仓 {real_amt} 张 @ {self.watched_entry:.2f}"
        )
        if not verified and not sl_placed:
            logger.warning(f"雷达首次激活钉钉跳过：止损 @ {new_sl:.2f} 未核实")
            return
        if not verified:
            verify_note += f" | {dingtalk.VERIFY_DELAY_MARK}"
        breath_meta = getattr(self, "_breath_coeff_meta", None) or {}
        trail_dist = float(
            (getattr(self, "open_atr", 0) or 0)
            * float(getattr(self, "breathing_coefficient", 1.0) or 1.0)
        )
        # 规格 v1.0 §5.1：钉钉通知标注门类型
        reentry_attempt = int(getattr(self, "reentry_attempt", 0) or 0)
        if reentry_attempt >= 1:
            open_kind = "重入开仓"
            trigger_gate = "重入·TP2绝对价"
        else:
            open_kind = "首次开仓"
            trigger_gate = "首次·TP1-TP2中点"
        self._call_dingtalk(
            dingtalk.report_radar_activated,
            side=self.current_side,
            qty=real_amt,
            entry=self.watched_entry,
            new_sl=new_sl,
            radar_progress=progress,
            regime=self.regime,
            shield_cleared=True,
            verify_note=verify_note,
            verified=verified,
            breathing_coefficient=float(
                getattr(self, "breathing_coefficient", 1.0) or 1.0
            ),
            trail_dist=trail_dist,
            open_kind=open_kind,
            trigger_gate=trigger_gate,
        )
        self._radar_activation_notified = True
        # P1 修复：并行 TG 雷达激活通知
        self._call_telegram(
            telegram_notify.report_radar_activation,
            side=self.current_side,
            qty=real_amt,
            entry=self.watched_entry,
            curr_px=curr_px,
            new_sl=new_sl,
            regime=self.regime,
        )

    def _tp_level_consumed(self, level):
        return level in (getattr(self, "tp_levels_consumed", []) or [])

    def _tp_filled_verified(self, level, live_qty=None, curr_px=0.0):
        level = int(level)
        if not self._tp_level_consumed(level):
            return False
        live_qty = self._safe_qty(live_qty if live_qty is not None else self.watched_qty)
        initial = self._trusted_initial_qty(live_qty)
        inferred = self._infer_tp_consumed_sequential(initial, live_qty, curr_px)
        if level not in inferred:
            return False
        idx = level - 1
        if 0 <= idx < len(self.tv_tps) and self.tv_tps[idx] > 0:
            if self._has_tp_limit_at_price(self.tv_tps[idx]):
                return False
        return True

    def _price_reached_tp1_zone(self, curr_px=0.0, tp1_px=None):
        tp1_px = float(
            tp1_px
            if tp1_px is not None
            else ((self.tv_tps[0] if self.tv_tps else 0) or 0)
        )
        entry = float(self.watched_entry or 0)
        if tp1_px <= 0 or entry <= 0:
            return False
        px_tol = max(3.0, tp1_px * 0.003)
        for px in (float(curr_px or 0), float(self.best_price or 0)):
            if px <= 0:
                continue
            if self.current_side == "LONG" and px >= tp1_px - px_tol:
                return True
            if self.current_side == "SHORT" and px <= tp1_px + px_tol:
                return True
        return False

    def _tp_fill_ok_to_arm_radar(self, tp_fills, curr_px, old_qty, new_qty):
        if getattr(self, "_open_in_progress", False) or getattr(
            self, "_defense_align_in_progress", False
        ):
            return False
        fills = list(tp_fills or [])
        if not fills or not any(int(f.get("level") or 0) == 1 for f in fills):
            return False
        f1 = next(f for f in fills if int(f.get("level") or 0) == 1)
        src = str(f1.get("source") or "")
        tp1_px = float(
            f1.get("price") or ((self.tv_tps[0] if self.tv_tps else 0) or 0)
        )
        if tp1_px > 0 and self._has_tp_limit_at_price(tp1_px):
            logger.warning(
                f"📡 三角对账拒绝：TP1 限价仍在盘口 @{tp1_px:.2f}"
            )
            return False
        if not self._price_reached_tp1_zone(curr_px, tp1_px):
            return False
        baseline = self._safe_qty(
            getattr(self, "_open_settled_qty", 0) or self.initial_qty or old_qty
        )
        live = self._safe_qty(new_qty)
        if baseline <= live:
            return False
        slices = {
            sl["level"]: sl for sl in self._tp_slices_for_initial(baseline)
        }
        tp1 = slices.get(1)
        if not tp1:
            return False
        reduced = baseline - live
        noise = max(1, int(round(baseline * 0.02)))
        if reduced < noise:
            return False
        tol = max(1, int(round(int(tp1["qty"]) * 0.05)))
        if reduced < int(tp1["qty"]) - tol:
            return False
        if src and src not in ("order_gone", "trades"):
            return False
        return True

    def _tp1_filled_verified(self, live_qty=None, curr_px=0.0):
        """TP1 三角对账：减仓+限价消失+价到TP1。"""
        if getattr(self, "_open_in_progress", False) or getattr(
            self, "_defense_align_in_progress", False
        ):
            return False
        if getattr(self, "_radar_armed_after_tp1", False):
            return True
        tp1_px = float(self.tv_tps[0] or 0) if self.tv_tps else 0.0
        if tp1_px > 0 and self._has_tp_limit_at_price(tp1_px):
            return False
        if not self._tp_filled_verified(1, live_qty, curr_px):
            return False
        live_qty = self._safe_qty(live_qty if live_qty is not None else self.watched_qty)
        baseline = self._safe_qty(
            getattr(self, "_open_settled_qty", 0) or self.initial_qty or live_qty
        )
        if baseline <= live_qty:
            return False
        slices = {
            sl["level"]: sl for sl in self._tp_slices_for_initial(baseline)
        }
        tp1 = slices.get(1)
        if not tp1:
            return False
        reduced = baseline - live_qty
        noise = max(1, int(round(baseline * 0.02)))
        if reduced < noise:
            return False
        tol = max(1, int(round(int(tp1["qty"]) * 0.05)))
        if reduced < int(tp1["qty"]) - tol:
            return False
        if not (
            self._price_reached_tp1_zone(curr_px, tp1_px)
            or getattr(self, "_ws_tp1_fill_hint", False)
        ):
            return False
        return True

    def _likely_exchange_stop_exit(self, curr_px=0.0):
        px = float(curr_px or deepcoin_client.get_current_price(self.symbol) or 0)
        sl = float(
            getattr(self, "_last_applied_tv_sl", 0)
            or getattr(self, "tv_sl", 0)
            or 0
        )
        if sl <= 0 or px <= 0:
            return False
        return abs(px - sl) <= max(2.5, px * 0.002)

    def _enforce_pre_tp1_radar_standby(self, live_qty=None, curr_px=0.0, source=""):
        """
        【规格 v1.0 · §5.1】
        雷达延迟激活：价格到达TP1-TP2中点（首次开仓）或TP2（重入开仓）之前，
        止损仅维持 tv_sl 宽线保护，不做移动保本。
        """
        if self._tp1_filled_verified(live_qty, curr_px):
            return False

        tv = float(getattr(self, "tv_sl", 0) or 0)
        entry = float(self.watched_entry or 0)
        changed = False

        consumed = list(getattr(self, "tp_levels_consumed", []) or [])
        if consumed:
            self.tp_levels_consumed = []
            changed = True
            logger.warning(
                f"📡 [{source or '雷达'}] 清除伪 TP{consumed} 标记 "
                f"(TP1 未实盘成交)"
            )

        if entry > 0 and self.current_sl:
            sl = float(self.current_sl)
            if self.current_side == "LONG" and sl > entry + 0.01:
                self.current_sl = tv if tv > 0 else sl
                changed = True
            elif self.current_side == "SHORT" and sl < entry - 0.01:
                self.current_sl = tv if tv > 0 else sl
                changed = True

        if tv > 0 and entry > 0:
            if self.current_side == "LONG" and (
                not self.current_sl or float(self.current_sl) > entry + 0.01
            ):
                self.current_sl = tv
                changed = True
            elif self.current_side == "SHORT" and (
                not self.current_sl or float(self.current_sl) < entry - 0.01
            ):
                self.current_sl = tv
                changed = True

        if getattr(self, "_radar_activation_notified", False):
            self._radar_activation_notified = False
            changed = True
        if getattr(self, "_shield_handoff_notified", False):
            self._shield_handoff_notified = False
            changed = True
        if getattr(self, "_radar_armed_after_tp1", False):
            self._radar_armed_after_tp1 = False
            changed = True
        if getattr(self, "_ws_tp1_fill_hint", False):
            self._ws_tp1_fill_hint = False
            changed = True

        if changed:
            self.best_price = entry if entry > 0 else self.best_price
            self._save_state()
            logger.info(
                f"📡 [{source or '雷达'}] TP1前待命 | tv_sl={tv:.2f} | entry={entry:.2f}"
            )
        return changed

    def _disarm_premature_radar(self, live_qty=None, curr_px=0.0, source=""):
        live_qty = self._safe_qty(live_qty or self.watched_qty)
        if self._tp1_filled_verified(live_qty, curr_px):
            return False
        disarmed = False
        stale = list(getattr(self, "tp_levels_consumed", []) or [])
        tv = float(getattr(self, "tv_sl", 0) or 0)
        entry = float(self.watched_entry or 0)
        if stale:
            self.tp_levels_consumed = []
            disarmed = True
        if entry > 0 and self.current_sl:
            if self.current_side == "LONG" and float(self.current_sl) > entry + 0.01:
                self.current_sl = tv if tv > 0 else float(self.current_sl)
                disarmed = True
            elif self.current_side == "SHORT" and float(self.current_sl) < entry - 0.01:
                self.current_sl = tv if tv > 0 else float(self.current_sl)
                disarmed = True
        if not disarmed:
            return False
        self._radar_activation_notified = False


        self._shield_handoff_notified = False
        self._radar_armed_after_tp1 = False
        self._ws_tp1_fill_hint = False
        self._save_state()
        logger.warning(
            f"📡 [{source or '雷达'}] 解除过早雷达/伪TP{stale or '标记'} "
            f"→ 恢复 tv_sl={tv:.2f}"
        )
        dingtalk.report_system_alert(
            "雷达解除·恢复呼吸空间",
            f"{self.current_side} {live_qty}张 @ {entry:.2f} | "
            f"清除伪TP{stale or '标记'} | tv_sl={tv:.2f} | "
            f"等待价格到达TP1-TP2中点后激活雷达",
        )
        if live_qty > 0 and tv > 0:
            self._maintain_hard_shield(live_qty, curr_px, force=True)
        return True

    def _radar_tv_trail_atr_mult(self):
        if self._tp_filled_verified(2):
            return TV_TRAIL_TP3_ATR
        if self._tp1_filled_verified():
            return TV_TRAIL_TP2_ATR
        return TV_TRAIL_TP2_ATR

    def _radar_breakeven_floor(self):
        entry = float(self.watched_entry or 0)
        if entry <= 0:
            return 0.0
        atr = float(self.current_atr or 30.0)
        cushion = max(atr * TV_BOOT_SL_ATR, entry * RADAR_FEE_BUFFER_PCT)
        if self.current_side == "LONG":
            return round(entry + cushion, 2)
        if self.current_side == "SHORT":
            return round(entry - cushion, 2)
        return entry

    def _radar_trail_offset_price(self):
        return float(self.current_atr or 30.0) * self._radar_tv_trail_atr_mult()

    def _refresh_radar_state_on_recover(self, curr_px, entry):
        """重启：按现价恢复 best_price；仅 TP1 已成交才恢复雷达追踪位"""
        if curr_px <= 0 or not entry:
            return

        if self.best_price == 0.0:
            self.best_price = entry
        if self.current_side == "LONG":
            self.best_price = max(self.best_price, curr_px)
        else:
            self.best_price = min(self.best_price, curr_px)

        if not self._tp1_filled_verified():
            if self.current_sl == 0.0 and float(getattr(self, "tv_sl", 0) or 0) > 0:
                self.current_sl = float(self.tv_sl)
            self._radar_armed_after_tp1 = False
            self._ws_tp1_fill_hint = False
            gate = float(self._radar_activation_price() or 0)
            logger.info(
                f"📡 重启雷达待命: 等待价格到达TP1-TP2中点(阈值{gate:.2f}) "
                f"(进度 {self._radar_activation_progress(curr_px):.0%})"
            )
            return
        self._radar_armed_after_tp1 = True

        progress = self._radar_activation_progress(curr_px)
        trail_offset = self._radar_trail_offset_price()
        floor_px = self._radar_breakeven_floor()
        if progress >= self.regime_settings[self.regime]["activation"]:
            if self.current_side == "LONG":
                trail_sl = max(round(self.best_price - trail_offset, 2), floor_px)
                trail_sl = self._clamp_radar_sl_for_market(curr_px, trail_sl)
                if not self._is_radar_active() or trail_sl > self.current_sl:
                    self.current_sl = max(self.current_sl or entry, trail_sl)
            else:
                trail_sl = min(round(self.best_price + trail_offset, 2), floor_px)
                trail_sl = self._clamp_radar_sl_for_market(curr_px, trail_sl)
                if not self._is_radar_active() or trail_sl < self.current_sl:
                    self.current_sl = min(self.current_sl or entry, trail_sl)
            logger.info(
                f"📡 重启雷达恢复: TP1已成交 进度 {progress:.0%} | best={self.best_price:.2f} | "
                f"SL={self.current_sl:.2f} | 追踪 {self._radar_tv_trail_atr_mult():.2f}ATR"
            )
        elif self.current_sl == 0.0:
            self.current_sl = floor_px

    def _nuclear_realign_tp(self, live_qty, entry, dynamic_sl=None, rounds=3):
        """
        核武级止盈对齐：只撤限价 TP → 重挂 TP123 → 始终续挂 tv_sl/雷达合并止损。
        """
        # 【关键修复】防止核武死循环：短时间重复调用直接返回
        now = time.time()
        if now - getattr(self, "_last_nuclear_realign_ts", 0) < 15.0:
            audit = self._audit_tp_levels(live_qty)
            if self._tp_audit_ok(audit):
                logger.warning(
                    f"☢️ 核武冷却中（{now - getattr(self, '_last_nuclear_realign_ts', 0):.1f}s < 15s），TP已对齐，跳过"
                )
                return audit
            logger.warning(
                f"☢️ 核武冷却中（{now - getattr(self, '_last_nuclear_realign_ts', 0):.1f}s < 15s），仍需对齐但跳过"
            )
            return audit
        self._last_nuclear_realign_ts = now

        last_audit = self._audit_tp_levels(live_qty)
        for r in range(rounds):
            logger.warning(
                f"☢️ 核武级止盈清场重挂 {r + 1}/{rounds} | 持仓 {live_qty}张 | "
                f"当前 {last_audit['matched_full']}/{last_audit['expected']} | "
                f"{self._format_audit_summary(last_audit)}"
            )
            # v16.14（根因一修复）：核武每轮开始前清理所有本地标签
            # 旧 VPS 重启/断线导致的残留标签、或前一轮已挂成功的订单对应的标签，
            # 都不应阻止本轮重建。核武必须"无条件全清"后重挂。
            self._pending_order_tags = {}
            self._save_state()
            self._cancel_all_tp_limit_orders()
            time.sleep(1.0)
            placed = self._rebuild_defenses(live_qty, entry, dynamic_sl=None)
            logger.info(f"☢️ 核武轮 {r + 1} 新挂 {placed} 笔限价止盈")
            curr_px = deepcoin_client.get_current_price(self.symbol)
            self._maintain_hard_shield(live_qty, curr_px, force=True)
            if dynamic_sl and not self._has_trigger_sl_near(dynamic_sl):
                self._ensure_radar_sl(live_qty, dynamic_sl)
            time.sleep(1.0)
            last_audit = self._audit_tp_levels(live_qty)
            stop_px = self._resolve_defense_stop_for_audit(dynamic_sl)
            if self._defenses_fully_ok(live_qty, stop_px):
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
        tp_prices = sum(1 for t in (self.tv_tps or []) if t > 0)
        if (
            tp_prices >= 3
            and not self._tp_level_consumed(1)
            and expected < 3
        ):
            return False
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

    def _ensure_defenses_on_recover(self, live_qty, entry, dynamic_sl=None):
        """
        重启/异动接管：审计 → 齐全跳过 → 增量补挂 → 仍失败才清场重建
        返回 (matched, pending_prices, expected, rebuilt)
        """
        live_qty = self._resolve_live_qty(live_qty)
        audit = self._audit_tp_levels(live_qty)
        expected = audit["expected"]
        matched = audit["matched_full"]
        pending_prices = audit["pending_prices"]
        logger.info(
            f"📊 防线审计: 持仓 {live_qty}张 | TP {matched}/{expected} | "
            f"{self._format_audit_summary(audit)}"
        )

        if self._audit_requires_nuclear(audit) or self._has_duplicate_tp_orders():
            logger.warning(
                f"☢️ 审计触发核武级重挂: {len(self._collect_tp_limit_orders())} 张止盈 | "
                f"{self._format_audit_summary(audit)}"
            )
            audit = self._nuclear_realign_tp(live_qty, entry, dynamic_sl=dynamic_sl, rounds=3)
            return audit["matched_full"], audit["pending_prices"], audit["expected"], True

        if self._defenses_fully_ok(live_qty, dynamic_sl):
            logger.info(
                f"✅ TP123 比例齐全 ({matched}/{expected}) @ {pending_prices}，跳过补挂"
            )
            if dynamic_sl and not self._has_trigger_sl_near(dynamic_sl):
                self._ensure_radar_sl(live_qty, dynamic_sl)
            return matched, pending_prices, expected, False

        self._cancel_orphan_tp_orders(live_qty)
        logger.info(f"📋 止盈未齐 ({matched}/{expected})，增量补挂缺失档（保留已有正确单）")
        self._patch_missing_tp_levels(live_qty)
        time.sleep(0.8)
        matched, pending_prices = self._wait_tp_hung(
            self.tv_tps, live_qty=live_qty, retries=5, delay=1.0,
        )
        audit = self._audit_tp_levels(live_qty)
        matched = audit["matched_full"]

        if self._defenses_fully_ok(live_qty, dynamic_sl):
            logger.info(f"✅ 增量补挂成功 ({matched}/{expected}) @ {audit['pending_prices']}")
            if dynamic_sl and not self._has_trigger_sl_near(dynamic_sl):
                self._ensure_radar_sl(live_qty, dynamic_sl)
            return matched, audit["pending_prices"], expected, True

        logger.warning(
            f"⚠️ 增量补挂仍不足 ({matched}/{expected}) {audit['issues']}，升级核武级重挂"
        )
        audit = self._nuclear_realign_tp(live_qty, entry, dynamic_sl=dynamic_sl, rounds=3)
        return audit["matched_full"], audit["pending_prices"], audit["expected"], True

    def _enforce_defense_alignment(self, live_qty, entry, dynamic_sl=None, reason="", rounds=3,
                                   recover_mode=False):
        live_qty = self._resolve_live_qty(live_qty)
        if live_qty <= 0:
            audit = self._audit_tp_levels(live_qty)
            return {
                "matched": 0, "expected": audit.get("expected", 0),
                "pending_prices": [], "rebuilt": False, "audit": audit, "nuclear": False,
            }

        # 【修复】防抖冷却：TP 已对齐时跳过；TP 未对齐时强制补挂
        now = time.time()
        if not recover_mode and not getattr(self, "_force_defense_realign", False):
            last_align = getattr(self, "_last_defense_align_ok_ts", 0) or 0
            audit = self._audit_tp_levels(live_qty)
            if now - last_align < DEFENSE_ALIGN_COOLDOWN_SEC:
                if self._tp_audit_ok(audit):
                    logger.info(
                        f"🛡️ 防线对齐冷却中（距上次 {now - last_align:.0f}s < {DEFENSE_ALIGN_COOLDOWN_SEC}s）"
                        f"，TP 已齐，跳过撤挂 | {self._format_audit_summary(audit)}"
                    )
                    return {
                        "matched": audit["matched_full"],
                        "expected": audit["expected"],
                        "pending_prices": audit["pending_prices"],
                        "rebuilt": False,
                        "audit": audit,
                        "nuclear": False,
                    }
                # 【关键修复】TP 未对齐时，即使在冷却期也必须补挂
                logger.warning(
                    f"🛡️ 防线对齐冷却中（距上次 {now - last_align:.0f}s < {DEFENSE_ALIGN_COOLDOWN_SEC}s）"
                    f"，但 TP 未对齐 → 强制补挂 | {self._format_audit_summary(audit)}"
                )

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
        exchange_sl = float(order_stop_price(self.current_side, sl_price, profile=getattr(self, "breath_profile", None)) or sl_price)
        deepcoin_client.place_trigger_order(
            self.symbol, close_side, pos_side, sl_qty, exchange_sl,
            order_type="market", td_mode="cross", mrg_position="merge",
        )

    def _has_tp_limit_at_price(self, price, tolerance=1.0):
        if price <= 0:
            return False
        for o in self._collect_tp_limit_orders():
            if abs(o["price"] - price) <= tolerance:
                return True
        return False

    def _detect_tp_fills(self, old_qty, new_qty, curr_px=0.0):
        """仅成交史/盘口消失+价到+量匹配；禁止纯减仓软推断启雷达"""
        if new_qty >= old_qty:
            return []
        initial = self._safe_qty(
            getattr(self, "_open_settled_qty", 0)
            or self._resolve_open_initial_qty(old_qty)
            or old_qty
        )
        if self._safe_qty(old_qty) > initial:
            initial = self._safe_qty(old_qty)
            self._open_settled_qty = initial
            self.initial_qty = initial
            self._save_state()

        # Deepcoin 无成交明细时：盘口 TP 消失 + 价到 + 减仓匹配
        consumed_before = set(getattr(self, "tp_levels_consumed", []) or [])
        reduced = self._safe_qty(old_qty) - self._safe_qty(new_qty)
        noise = max(1, int(round(initial * 0.02)))
        if reduced < noise:
            return []
        fills = []
        for sl in sorted(self._tp_slices_for_initial(initial), key=lambda x: x["level"]):
            if sl["level"] in consumed_before or sl["qty"] <= 0 or sl["price"] <= 0:
                continue
            if self._has_tp_limit_at_price(sl["price"]):
                break
            if int(sl["level"]) == 1 and not self._price_reached_tp1_zone(
                curr_px, sl["price"]
            ):
                break
            tol = max(1, int(round(sl["qty"] * 0.05)))
            if abs(reduced - int(sl["qty"])) <= tol:
                fills.append({
                    "level": sl["level"],
                    "price": sl["price"],
                    "qty": sl["qty"],
                    "source": "order_gone",
                })
                break
        if fills:
            return fills

        soft = self._infer_tp_consumed_sequential(initial, new_qty, curr_px)
        if soft:
            logger.info(
                f"🧮 软推断 TP{soft} 已忽略作成交证据 "
                f"(基线 {initial}→{new_qty} | 需价到TP+限价消失+量匹配)"
            )
        return []

    def _detect_tp_fills_by_reduction(self, old_qty, new_qty, curr_px=0.0, initial=None):
        return []

    def _cancel_tp_orders_at_levels(self, levels):
        cancelled = 0
        for level in levels:
            idx = int(level) - 1
            if idx < 0 or idx >= len(self.tv_tps):
                continue
            px = self.tv_tps[idx]
            if px <= 0:
                continue
            for o in self._collect_tp_limit_orders():
                if abs(o["price"] - px) <= 1.0 and o.get("orderId"):
                    deepcoin_client.cancel_order(self.symbol, ord_id=o["orderId"])
                    cancelled += 1
                    time.sleep(0.2)
        if cancelled:
            logger.info(f"🧹 撤净已成交 TP 残留单 {cancelled} 张")
        return cancelled

    def _cancel_mismatched_remaining_tps(self, live_qty, tolerance=1.0):
        cancelled = 0
        for lv in self._expected_tp_levels(live_qty):
            px, target_q = lv["price"], lv["qty"]
            if px <= 0 or target_q <= 0:
                continue
            at_px = [
                o for o in self._collect_tp_limit_orders()
                if abs(o["price"] - px) <= tolerance
            ]
            for o in at_px:
                if o["qty"] != target_q and o.get("orderId"):
                    deepcoin_client.cancel_order(self.symbol, ord_id=o["orderId"])
                    cancelled += 1
                    time.sleep(0.2)
                    logger.info(
                        f"🔧 撤偏差 TP{lv['level']} @{px:.2f}: "
                        f"盘口 {o['qty']} → 应 {target_q}张"
                    )
        return cancelled

    def _detect_stale_consumed_tp_levels(self, initial_qty, live_qty, curr_px=0.0):
        initial_qty = self._safe_qty(initial_qty)
        live_qty = self._safe_qty(live_qty)
        if initial_qty <= 0 or live_qty <= 0:
            return []
        consumed = self._sanitize_tp_consumed(initial_qty, live_qty, curr_px)
        for lv in consumed:
            idx = int(lv) - 1
            px = self.tv_tps[idx] if 0 <= idx < len(self.tv_tps) else 0
            if px > 0 and self._has_tp_limit_at_price(px):
                logger.warning(
                    f"⚠️ 多余 TP{lv} @{px:.2f} "
                    f"(开单 {initial_qty} → 现仓 {live_qty}张，该档应已成交)"
                )
        return consumed

    def _realign_remaining_tps_after_fill(self, live_qty, dynamic_sl=None, reason=""):
        live_qty = self._resolve_live_qty(live_qty)
        if live_qty <= 0:
            audit = self._audit_tp_levels(live_qty)
            return {
                "matched": 0, "expected": 0, "pending_prices": [],
                "rebuilt": False, "audit": audit, "nuclear": False,
            }
        consumed = getattr(self, "tp_levels_consumed", []) or []
        logger.info(
            f"🎯 TP 成交后静默对齐: 剩余 {live_qty}张 | "
            f"已成交 TP{consumed} | 只补未成交档"
        )
        self._cancel_tp_orders_at_levels(consumed)
        time.sleep(0.35)
        n_fix = self._cancel_mismatched_remaining_tps(live_qty)
        if n_fix:
            logger.info(f"🔧 TP 成交对齐：撤偏差剩余档 {n_fix} 张")
            time.sleep(0.35)
        placed = self._patch_missing_tp_levels(live_qty)
        time.sleep(0.5)
        audit = self._audit_tp_levels(live_qty)
        if dynamic_sl and not self._has_trigger_sl_near(dynamic_sl):
            self._ensure_radar_sl(live_qty, dynamic_sl)
        if placed == 0 and self._tp_audit_ok(audit):
            logger.info(
                f"✅ TP 成交后盘口已齐 ({audit['matched_full']}/{audit['expected']})"
            )
        elif not self._tp_audit_ok(audit):
            repaired, _ = self._surgical_repair_tp_defenses(live_qty, self.watched_entry)
            audit = repaired
        self._mark_defense_align_ok()
        return {
            "matched": audit["matched_full"],
            "expected": audit["expected"],
            "pending_prices": audit["pending_prices"],
            "rebuilt": placed > 0,
            "audit": audit,
            "nuclear": False,
        }

    def _repair_partial_tp_on_recover(self, live_qty, entry, initial_qty, curr_px=0.0):
        live_qty = self._resolve_live_qty(live_qty)
        initial_qty = self._safe_qty(initial_qty or live_qty)
        actions = []

        self._sanitize_tp_consumed(initial_qty, live_qty, curr_px)
        consumed = getattr(self, "tp_levels_consumed", []) or []
        if consumed and initial_qty <= live_qty:
            inferred = self._infer_tp_consumed_sequential(initial_qty, live_qty, curr_px)
            if not inferred:
                logger.warning(
                    f"跳过部分止盈修复：无减仓证据，清除 TP{consumed}"
                )
                self.tp_levels_consumed = []
                self._save_state()
                return {"repaired": False, "actions": actions, "result": None, "consumed": []}

        stale_levels = self._detect_stale_consumed_tp_levels(
            initial_qty, live_qty, curr_px,
        )
        if stale_levels:
            prev = list(getattr(self, "tp_levels_consumed", []) or [])
            if stale_levels != prev:
                self.tp_levels_consumed = stale_levels
                self._save_state()
            actions.append(
                f"已成交档 TP{stale_levels} | 开单 {initial_qty} → 现仓 {live_qty}张"
            )

        consumed = getattr(self, "tp_levels_consumed", []) or []
        if not consumed and initial_qty > live_qty:
            inferred = self._infer_tp_consumed_sequential(
                initial_qty, live_qty, curr_px,
            )
            if inferred:
                self.tp_levels_consumed = inferred
                self._save_state()
                consumed = inferred
                actions.append(f"推断已成交 TP{inferred}")
        if not consumed:
            return {"repaired": False, "actions": actions, "result": None, "consumed": []}

        if live_qty > DUST_ORPHAN_CONTRACTS and self._expected_tp_count() == 0:
            self._sanitize_tp_consumed(initial_qty, live_qty, curr_px)
            if self._expected_tp_count() == 0 and live_qty > DUST_ORPHAN_CONTRACTS:
                logger.warning(
                    f"⚠️ 仍有 {live_qty}张 且无待挂 TP → TP1+TP2 已尽，"
                    f"余仓交雷达（不挂TP3限价）"
                )
                self.tp_levels_consumed = [1, 2]
                try:
                    self._cancel_tp_orders_at_levels([3])
                except Exception:
                    pass
                self._save_state()

        n_stale = self._cancel_tp_orders_at_levels(consumed)
        if n_stale:
            actions.append(f"撤多余已成交档 {n_stale} 张")
        n_mismatch = self._cancel_mismatched_remaining_tps(live_qty)
        if n_mismatch:
            actions.append(f"撤偏差 TP {n_mismatch} 张")
        time.sleep(0.4)

        sl_to_pass = self._radar_sl_to_pass()
        if sl_to_pass is None and curr_px and curr_px > 0:
            top_level = max(consumed)
            px = self.tv_tps[top_level - 1] if top_level <= len(self.tv_tps) else 0
            if px > 0:
                sl_to_pass = self._advance_radar_on_tp_fill(
                    [{"level": top_level, "price": px, "qty": 0}],
                    curr_px, live_qty,
                )

        result = self._realign_remaining_tps_after_fill(
            live_qty, dynamic_sl=sl_to_pass, reason="重启部分止盈修复",
        )
        rem_levels = self._expected_tp_levels(live_qty)
        rem_sum = sum(lv["qty"] for lv in rem_levels)
        audit = result.get("audit") or {}
        actions.append(
            f"剩余 TP 重分 {rem_sum}/{live_qty}张 | "
            f"对齐 {audit.get('matched_full', 0)}/{audit.get('expected', 0)} 档"
        )
        return {
            "repaired": True,
            "actions": actions,
            "result": result,
            "consumed": consumed,
            "initial_qty": initial_qty,
            "rem_sum": rem_sum,
        }

    def _detect_shield_fills(self, old_qty, new_qty, curr_px):
        if not getattr(self, "shield_active", False):
            return []
        if new_qty >= old_qty:
            return []
        if self._detect_tp_fills(old_qty, new_qty, curr_px):
            return []
        stop_px = self._shield_stop_price()
        if not stop_px:
            return []
        if curr_px > 0 and self._should_radar_trail(curr_px):
            return []
        if self._has_shield_stop_at_price(stop_px):
            return []
        if curr_px > 0:
            px_tol = max(3.0, stop_px * 0.002)
            if self.current_side == "LONG" and curr_px > stop_px + px_tol:
                return []
            if self.current_side == "SHORT" and curr_px < stop_px - px_tol:
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
        if getattr(self, "_open_in_progress", False) or getattr(
            self, "_defense_align_in_progress", False
        ):
            return {"kind": "reduce_unknown", "tp_fills": [], "shield_fills": []}
        tp_fills = self._detect_tp_fills(old_qty, new_qty, curr_px)
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
        if not getattr(self, "_radar_armed_after_tp1", False) and not self._price_reached_tp1_zone(curr_px):
            logger.warning(
                "📡 [雷达推进] 跳过：TP1 证据不足，保持阶段0宽硬止损"
            )
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
        self._radar_armed_after_tp1 = True
        new_sl = self._compute_radar_sl(curr_px)
        floor_px = self._radar_breakeven_floor()
        if new_sl is not None:
            if self.current_side == "LONG":
                self.current_sl = max(self.current_sl or floor_px, new_sl, floor_px)
            else:
                self.current_sl = min(self.current_sl or floor_px, new_sl, floor_px)
        elif max_level >= 1:
            self.current_sl = floor_px
        note = f"TP{max_level}成交"
        if max_level >= 2 and tp3 > 0:
            note += f" → 雷达止损向 TP3({tp3:.2f}) 动态收紧"
        elif max_level == 1:
            note += " → 雷达保本启动，静默守 TP2/TP3"
        logger.info(
            f"📈 [雷达推进] {note} | SL={self.current_sl:.2f} | best={self.best_price:.2f}"
        )
        self._save_state()
        return self.current_sl if self.current_sl else None

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
            evidence_ok = self._tp_fill_ok_to_arm_radar(
                change["tp_fills"], curr_px, old_qty, new_qty,
            )
            if not evidence_ok:
                logger.warning(
                    f"🎯 [智慧大脑] {levels} 疑似伪成交"
                    f"（价未达TP1 / 开仓重建竞态）→ 不标记·不启雷达 | "
                    f"{old_qty}→{new_qty}"
                )
                dingtalk.report_system_alert(
                    "雷达拒启·伪TP1拦截",
                    f"{self.current_side} {old_qty}→{new_qty}张 | {levels} | "
                    f"现价 {float(curr_px or 0):.2f} | "
                    f"规则：TP1限价实盘成交前不激活雷达",
                )
                result = self._smart_realign_defenses(
                    new_qty, self.watched_entry, dynamic_sl=None,
                    reason="伪TP拦截·保宽硬止损",
                )
                if self._should_activate_shield(curr_px) or getattr(
                    self, "shield_active", False
                ):
                    self._maintain_hard_shield(new_qty, curr_px, force=True)
                change = {"kind": "reduce_unknown", "tp_fills": [], "shield_fills": []}
                self._save_state()
                return change, result
            logger.info(
                f"🎯 [智慧大脑] {levels} 成交减仓 {old_qty} ➔ {new_qty} → 雷达推进 + 守剩余TP"
            )
            self._mark_tp_levels_consumed([f["level"] for f in change["tp_fills"]])
            self._radar_armed_after_tp1 = True
            curr_px_safe = curr_px or deepcoin_client.get_current_price(self.symbol) or 0
            sl_to_pass = self._clamp_radar_to_tv_floor(
                self._advance_radar_on_tp_fill(
                    change["tp_fills"], curr_px, new_qty,
                )
            )
            result = self._realign_remaining_tps_after_fill(
                new_qty, dynamic_sl=sl_to_pass,
                reason=f"{levels} 成交静默对齐",
            )
            if sl_to_pass and not getattr(self, "_radar_activation_notified", False):
                self._perform_radar_handoff(
                    new_qty, curr_px_safe, reason=f"{levels} 成交雷达接管",
                )
            elif sl_to_pass and not self._has_trigger_sl_near(sl_to_pass):
                clamped = self._clamp_radar_sl_for_market(
                    curr_px_safe, self._clamp_radar_to_tv_floor(sl_to_pass),
                )
                if self._can_safely_place_radar_sl(curr_px_safe, clamped):
                    self._ensure_radar_sl(new_qty, clamped)
        elif kind == "shield_fill":
            f = change["shield_fills"][0]
            logger.warning(
                f"🛡️ [智慧大脑] TV硬止损成交 "
                f"{old_qty} ➔ {new_qty} @ {f['price']:.2f}"
            )
            if new_qty <= 0 or self._is_dust_qty(new_qty):
                flat_meta = self._build_close_meta(
                    "CLOSE_STOPLOSS",
                    self.current_side,
                    self._estimate_pnl_pct(curr_px),
                    "触碰硬止损平仓（TV tv_sl）",
                )
                flat_meta["close_type"] = CLOSE_TYPE_VPS_SHIELD
                self._disarm_shield("TV硬止损全平", notify=False)
                self._handle_manual_flat_detected(
                    flat_meta["tv_reason"],
                    close_meta=flat_meta,
                    curr_px=curr_px,
                )
                self._save_state()
                return change, None
            self._disarm_shield("TV硬止损成交", notify=True)
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
            retry_fills = self._detect_tp_fills(old_qty, new_qty, curr_px)
            if retry_fills and self._tp_fill_ok_to_arm_radar(
                retry_fills, curr_px, old_qty, new_qty,
            ):
                change = {"kind": "tp_fill", "tp_fills": retry_fills, "shield_fills": []}
                return self._handle_smart_qty_change(old_qty, new_qty, curr_px)
            self._bump_best_on_tp_fill(old_qty, new_qty, curr_px)
            self._sync_radar_sl_from_best(curr_px)
            sl_to_pass = self._radar_sl_to_pass()
            result = self._smart_realign_defenses(
                new_qty, self.watched_entry, dynamic_sl=sl_to_pass,
                reason="人工异动重对齐",
            )
            if self._should_disarm_shield_for_favorable(curr_px):
                self._perform_radar_handoff(
                    new_qty, curr_px, reason="TP后切换雷达保本",
                )
            elif self._should_activate_shield(curr_px) or getattr(self, "shield_active", False):
                self._maintain_hard_shield(new_qty, curr_px, force=True)

        self._save_state()
        return change, result

    def _report_qty_change_dingtalk(self, old_qty, new_qty, realign_result, change=None):
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

        fills = []
        if change and change.get("kind") == "tp_fill":
            fills = change.get("tp_fills") or []
        if not fills:
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
        new_sl = self._clamp_radar_to_tv_floor(new_sl)
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
        """给撤单/重挂留 REST 同步窗口，避免接管未完成时误报"""
        sl = dynamic_sl if dynamic_sl is not None else self._resolve_defense_stop_for_audit()
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
        # 规格 §9.6 平仓幂等性：无持仓直接返回0，不使用fallback避免幽灵单
        logger.warning(f"📐 实盘张数归零: 账本 {fallback_qty} → 交易所 0")
        return 0

    def handle_signal(self, payload):
        """兼容旧调用路径"""
        payload = self._enrich_tv_payload(dict(payload or {}))
        self.enqueue_signal(payload)

    def _enrich_tv_payload(self, payload):
        """v6.9.75：TV 全量 regime/atr/tp 优先，仅缺失项本地补全。"""
        action = str(payload.get("action", "")).strip().upper()
        live_px = deepcoin_client.get_current_price(self.symbol) or self.tv_price or 0.0
        return enrich_signal_fields(
            payload,
            action,
            fallback_regime=self.regime or 3,
            fallback_atr=self.current_atr or 30.0,
            fallback_price=live_px,
        )

    def _tv_field_source_note(self, payload):
        return format_tv_field_sources(payload or {})

    def _format_close_extra(self, close_side, pnl_pct, tv_price, regime=None, atr=None):
        parts = []
        if close_side:
            parts.append(f"TV方向 {close_side}")
        if regime:
            parts.append(f"TV档位 R{int(regime)}")
        if atr and float(atr) > 0:
            parts.append(f"TV ATR {float(atr):.2f}")
        if tv_price and float(tv_price) > 0:
            parts.append(f"TV价 {float(tv_price):.2f}")
        if pnl_pct is not None and pnl_pct != "":
            parts.append(f"TV盈亏 {self._safe_float(pnl_pct):+.2f}%")
        return (" | " + " | ".join(parts)) if parts else ""

    def _estimate_pnl_pct(self, curr_px):
        entry = float(self.watched_entry or 0)
        px = float(curr_px or 0)
        if entry <= 0 or px <= 0 or not self.current_side:
            return None
        if self.current_side == "LONG":
            return (px - entry) / entry * 100.0
        return (entry - px) / entry * 100.0

    def _build_close_meta(self, raw_action, close_side, pnl_pct, tv_reason=""):
        reason = str(tv_reason or "").strip()
        close_type = classify_tv_close(raw_action, reason, pnl_pct)
        return {
            "action": raw_action,
            "close_type": close_type,
            "side": close_side or self.current_side,
            "pnl_pct": pnl_pct,
            "tv_reason": reason,
            "tv_price": self.tv_price,
            "regime": self.regime,
            "atr": self.current_atr,
            "field_sources": getattr(self, "_last_tv_field_sources", {}),
            "entry_px": self.watched_entry,
            "closed_qty": self._safe_qty(self.watched_qty or self.initial_qty),
        }

    def _infer_flat_close_meta(self, curr_px=0.0, hint_reason=""):
        if self._likely_exchange_stop_exit(curr_px) and not getattr(
            self, "_radar_activation_notified", False
        ):
            est = self._estimate_pnl_pct(curr_px)
            sl = float(getattr(self, "tv_sl", 0) or 0)
            return self._build_close_meta(
                "CLOSE_STOPLOSS",
                self.current_side,
                est,
                f"交易所止损触发 @ {sl:.2f} (TP1前宽止损/非雷达保本钉钉) | {hint_reason}",
            )

        last = self.last_tv_signal or {}
        if (
            last.get("action") in ("CLOSE_TP3", "CLOSE_PROTECT", "CLOSE_STOPLOSS",
                                    "CLOSE_QUICK_EXIT", "CLOSE_RSI_EXIT")
            and time.time() - float(last.get("ts", 0) or 0) < 180
        ):
            return self._build_close_meta(
                last.get("action"),
                last.get("side") or self.current_side,
                last.get("pnl_pct"),
                last.get("reason") or hint_reason,
            )
        consumed = set(getattr(self, "tp_levels_consumed", []) or [])
        if consumed >= {1, 2, 3}:
            return self._build_close_meta(
                "CLOSE_TP3", self.current_side,
                self._estimate_pnl_pct(curr_px), "TP3完美收网",
            )
        if getattr(self, "_shield_handoff_notified", False) or getattr(
            self, "_radar_activation_notified", False
        ) or self._is_radar_active():
            est = self._estimate_pnl_pct(curr_px)
            sl = float(
                getattr(self, "current_sl", 0)
                or getattr(self, "tv_sl", 0)
                or 0
            )
            return self._build_close_meta(
                "CLOSE_STOPLOSS", self.current_side, est,
                f"雷达保本止损触发 @ {sl:.2f} | {hint_reason}",
            )
        if getattr(self, "shield_active", False):
            return self._build_close_meta(
                "CLOSE_STOPLOSS", self.current_side,
                self._estimate_pnl_pct(curr_px),
                "触碰硬止损平仓（TV tv_sl）",
            )
        return self._build_close_meta("CLOSE", self.current_side, None, hint_reason or "仓位归零")

    def _enrich_close_meta_live(self, meta, curr_px=0.0):
        out = dict(meta or {})
        if not out.get("entry_px"):
            out["entry_px"] = self.watched_entry
        if not out.get("closed_qty"):
            out["closed_qty"] = self._safe_qty(self.watched_qty or self.initial_qty)
        if not out.get("side"):
            out["side"] = self.current_side
        px = float(curr_px or 0) or deepcoin_client.get_current_price(self.symbol) or 0.0
        if px > 0:
            out["live_exit_px"] = px
            if out.get("pnl_pct") is None:
                saved_side = out.get("side") or self.current_side
                entry = float(out.get("entry_px") or 0)
                if entry > 0 and saved_side:
                    if saved_side == "LONG":
                        out["pnl_pct"] = (px - entry) / entry * 100.0
                    else:
                        out["pnl_pct"] = (entry - px) / entry * 100.0
        if not out.get("close_type"):
            out["close_type"] = classify_tv_close(
                out.get("action", ""), out.get("tv_reason", ""), out.get("pnl_pct"),
            )
        return out

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
        is_tp_sl_update = raw_action in ("UPDATE_TP", "UPDATE_SL")
        try:
            if raw_action in ("LONG", "SHORT"):
                self._pipeline_signal_received(payload)
        except Exception:
            pass

        if not is_tp_sl_update or payload.get("regime") is not None:
            self.regime = self._safe_int(payload.get("regime"), self.regime or 3)
            if self.regime not in self.regime_settings:
                self.regime = 3

        # P0 修复：tier / tier_label 从 webhook 读取（TV 优先；缺失时本地推算）
        tier_in = self._safe_int(payload.get("tier"), None)
        if tier_in is not None and tier_in in (0, 1, 2):
            self.adx_tier = tier_in
            self.radar_tier = tier_in
            tier_src = "tv"
        else:
            tier_src = "local"
        # tier_label 透传至钉钉展示（不存状态，仅 payload 携带时透传）
        self._last_tier_label = payload.get("tier_label") or ""

        atr_in = self._safe_float(payload.get("atr"), 0.0)
        if atr_in > 0:
            self.current_atr = atr_in
        elif not is_tp_sl_update:
            self.current_atr = self._safe_float(payload.get("atr"), 30.0)

        px_in = self._safe_float(payload.get("price"), 0.0)
        if px_in > 0:
            self.tv_price = px_in
        elif not is_tp_sl_update:
            self.tv_price = 0.0

        new_tps = self._sanitize_tp_prices([
            self._safe_float(payload.get("tv_tp1"), 0),
            self._safe_float(payload.get("tv_tp2"), 0),
            self._safe_float(payload.get("tv_tp3"), 0),
        ])
        if raw_action == "UPDATE_TP":
            self._prev_tv_tps_before_update = list(self.tv_tps or [0.0, 0.0, 0.0])
            self.tv_tps = new_tps
        elif raw_action in ("LONG", "SHORT"):
            self.tv_tps = new_tps
            if self.tv_price > 0:
                if not validate_tp_prices_for_side(raw_action, self.tv_price, self.tv_tps):
                    enriched = enrich_entry_tp_prices(
                        raw_action, self.tv_price, self.current_atr, self.regime, payload,
                    )
                    self.tv_tps = self._sanitize_tp_prices([
                        self._safe_float(enriched.get("tv_tp1"), 0),
                        self._safe_float(enriched.get("tv_tp2"), 0),
                        self._safe_float(enriched.get("tv_tp3"), 0),
                    ])
        elif sum(1 for t in new_tps if t > 0) >= 2:
            self.tv_tps = new_tps

        self._last_tv_field_sources = {
            "regime": payload.get("_regime_source", "tv"),
            "atr": payload.get("_atr_source", "tv"),
            "tp": payload.get("_tp_source", "tv"),
            "price": payload.get("_price_source", "tv"),
            "tier": tier_src,
        }
        close_reason = str(payload.get("reason") or "策略指标反转/波动率安全退出").strip()
        close_side = str(payload.get("side") or "").strip().upper()
        pnl_pct = payload.get("pnl_pct")
        # 【BUG修复】is_close 在 _process_signal 中使用，但定义在 _process_close_action 中
        # 对于非 close 信号 is_close 永远为 False（因为 side 不是 CLOSE 类）
        is_close = (
            raw_action in ("CLOSE", "CLOSE_PROTECT", "CLOSE_TP3", "CLOSE_STOPLOSS", "CLOSE_QUICK_EXIT", "CLOSE_RSI_EXIT")
            or str(raw_action or "").startswith("CLOSE")
        )

        # P0 修复：CLOSE 方向校验 — 反向信号立即丢弃并告警
        if is_close and close_side in ("LONG", "SHORT"):
            live_pos = self._get_active_position()
            if live_pos and self._safe_qty(live_pos.get("size", 0)) > 0:
                pos_side = "LONG" if str(live_pos.get("posSide") or "").lower() == "long" else "SHORT"
                if close_side != pos_side:
                    logger.warning(
                        f"🚫 CLOSE 方向不匹配 已拒绝 | "
                        f"TV side={close_side} 持仓={pos_side} | "
                        f"忽略反向 CLOSE，防止多空混乱"
                    )
                    try:
                        dingtalk.report_system_alert(
                            f"🚫 CLOSE 方向不匹配 已拒绝 [{self.symbol}]",
                            f"TV 发 {close_side} 但持仓 {pos_side} | "
                            f"忽略反向信号，保留当前持仓 | {close_reason or raw_action}",
                            level="危险",
                            suggestion="检查 TV 策略信号是否错发方向",
                        )
                        self._call_telegram(
                            telegram_notify.report_system_alert,
                            title=f"CLOSE 方向不匹配 [{self.symbol}]",
                            detail=f"TV={close_side} 持仓={pos_side} | 已拒绝 | {close_reason or raw_action}",
                            level="危险",
                            suggestion="检查 TV 策略信号是否错发方向",
                        )
                    except Exception:
                        pass
                    return
        elif is_close and close_side not in ("LONG", "SHORT"):
            # 无 side 字段时用持仓方向补全（向后兼容）
            live_pos = self._get_active_position()
            if live_pos and self._safe_qty(live_pos.get("size", 0)) > 0:
                close_side = "LONG" if str(live_pos.get("posSide") or "").lower() == "long" else "SHORT"

        close_meta = self._build_close_meta(raw_action, close_side, pnl_pct, close_reason)
        close_extra = self._format_close_extra(
            close_side, pnl_pct, self.tv_price, self.regime, self.current_atr,
        )

        if not raw_action:
            logger.warning("TV 信号缺少 action，已忽略")
            return
        if raw_action in (
            "LONG", "SHORT", "CLOSE", "CLOSE_PROTECT", "CLOSE_TP3",
            "CLOSE_STOPLOSS", "UPDATE_SL", "UPDATE_TP",
        ) or raw_action.startswith("CLOSE"):
            self._record_tv_signal(payload, raw_action)

        if not self._lock.acquire(timeout=120.0):
            logger.error(f"⏱️ 锁等待 120s 超时，信号 {raw_action} 重新入队")
            self._signal_queue.put(payload)
            return

        try:
            is_close = (
                raw_action in ("CLOSE", "CLOSE_PROTECT", "CLOSE_TP3", "CLOSE_STOPLOSS")
                or raw_action.startswith("CLOSE")
            )
            if is_close and self._should_ignore_late_close(payload):
                age = time.time() - float(
                    getattr(self, "_last_open_exec_ts", 0) or 0
                )
                logger.warning(
                    f"🛡️ [{self.symbol}] 迟到平仓已忽略 | {raw_action} "
                    f"开仓后 {age:.2f}s < {LATE_CLOSE_SUPPRESS_SEC}s · 保持开仓"
                )
                try:
                    dingtalk.report_system_alert(
                        f"迟到平仓已忽略·保持开仓 [{self.symbol}]",
                        f"{raw_action} 距开仓成交仅 {age:.2f}s "
                        f"(窗={LATE_CLOSE_SUPPRESS_SEC}s) → 不执行平仓，"
                        f"防刚开又平；同窗先平后开链不受影响",
                        level="警告",
                        suggestion="若确需平仓请人工或等待抑制窗结束后再发 CLOSE",
                    )
                except Exception:
                    pass
                return
            if is_close:
                self.monitoring = False
            if raw_action in ("CLOSE_PROTECT", "CLOSE_QUICK_EXIT", "CLOSE_RSI_EXIT") or raw_action.startswith("CLOSE_PROTECT"):
                pos = self._get_active_position()
                tv_reason = close_reason or ("保护性全平" if "PROTECT" in raw_action else f"TV平仓:{raw_action}")
                if not pos or self._safe_qty(pos.get("size", 0)) <= 0:
                    logger.info(f"🛡️ 保护性全平到达但盘口已空仓 → 撤单复位 | {tv_reason}{close_extra}")
                    self._handle_manual_flat_detected(
                        tv_reason,
                        close_meta=close_meta,
                        curr_px=self.tv_price,
                    )
                else:
                    self._close_all(
                        f"🛡️ 风控拦截：{tv_reason}{close_extra}",
                        close_meta=close_meta,
                    )
            elif raw_action == "CLOSE_TP3":
                pos = self._get_active_position()
                tv_reason = close_reason or "TP3完美收网"
                if not pos or self._safe_qty(pos.get("size", 0)) <= 0:
                    self._handle_manual_flat_detected(
                        tv_reason,
                        close_meta=close_meta,
                        curr_px=self.tv_price,
                    )
                else:
                    self._close_all(
                        f"🏆 TP3止盈：{tv_reason}{close_extra}",
                        close_meta=close_meta,
                    )
            elif raw_action == "CLOSE_STOPLOSS":
                pos = self._get_active_position()
                tv_reason = close_reason or "被动止损/保本"
                if not pos or self._safe_qty(pos.get("size", 0)) <= 0:
                    self._handle_manual_flat_detected(
                        tv_reason,
                        close_meta=close_meta,
                        curr_px=self.tv_price,
                    )
                else:
                    tag = (
                        "防回吐保本"
                        if close_meta.get("close_type") == CLOSE_TYPE_BREAKEVEN
                        else "硬止损"
                    )
                    self._close_all(
                        f"🛑 {tag}：{tv_reason}{close_extra}",
                        close_meta=close_meta,
                    )
            elif raw_action == "CLOSE":
                pos = self._get_active_position()
                tv_reason = close_reason or "TV单独平仓清场"
                if not pos or self._safe_qty(pos.get("size", 0)) <= 0:
                    logger.info(
                        f"🧹 TV单独平仓但盘口已空 → 撤净挂单复位等待 | {tv_reason}{close_extra}"
                    )
                    self._handle_manual_flat_detected(
                        tv_reason,
                        close_meta=close_meta,
                        curr_px=self.tv_price,
                    )
                else:
                    self._close_all(
                        f"🧹 TV单独平仓清场：{tv_reason}{close_extra}",
                        close_meta=close_meta,
                    )
            elif raw_action == "UPDATE_SL":
                self._handle_tv_sl_update(payload)
            elif raw_action == "UPDATE_TP":
                self._handle_tv_tp_update(payload)
            elif raw_action in ["LONG", "SHORT"]:
                self._apply_tv_sl_from_payload(payload, source=f"{raw_action}开仓")
                self._apply_tv_sizing_params(payload)
                self.last_tv_side = raw_action
                self._save_state()
                self._handle_smart_entry(raw_action, payload)
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
        # §6.2：启动私有 WS（部分成交动态同步）
        if not getattr(self, "_deepcoin_private_ws_started", False):
            self._start_deepcoin_private_ws()

    def _start_deepcoin_private_ws(self):
        """§6.2：订阅 Deepcoin 私有 WebSocket → 实时成交回报 → 触发数量重同步。"""
        self._deepcoin_private_ws_started = True
        self._ws_hard_sl_fill_hint = None
        self._ws_tp1_fill_hint = False
        self._ws_tp_fill_levels = set()
        try:
            deepcoin_client.start_private_ws(on_message=self._on_deepcoin_ws_message)
        except Exception as e:
            logger.warning(f"[{self.symbol}] 私有WS启动失败（稍后重试）: {e}")
            self._deepcoin_private_ws_started = False

    def _on_deepcoin_ws_message(self, data):
        """§6.2：Deepcoin 私有 WS 事件 → TP 成交提示 + 部分成交数量重同步。"""
        if not isinstance(data, dict):
            return
        table = str(data.get("table") or "").lower()
        if table not in ("order", "position", "trade", "triggerorder"):
            return
        # 脉冲哨兵
        self._ws_defense_pulse = True
        self._ws_fast_poll = True

        if table == "trade":
            self._handle_deepcoin_trade_event(data)
        elif table == "order":
            self._handle_deepcoin_order_event(data)

    def _handle_deepcoin_trade_event(self, data):
        """Trade 成交事件：记录 TP1 成交提示（用于雷达提前武装）。"""
        rows = data.get("data") or []
        if not isinstance(rows, list):
            rows = [rows]
        for row in rows:
            sym = str(row.get("instrument_id") or row.get("symbol") or "").upper()
            if sym and sym != self.symbol.upper():
                continue
            side = str(row.get("side") or "").upper()
            if side not in ("BUY", "SELL"):
                continue
            px_str = str(row.get("price") or "0")
            try:
                px = float(px_str)
            except (ValueError, TypeError):
                px = 0.0
            # 非反向成交 → TP 方向
            if px <= 0:
                continue
            # 尝试匹配 TP 档位
            tps = list(getattr(self, "tv_tps", None) or [])
            for lv in (1, 2):
                tp_px = float(tps[lv - 1] or 0) if lv <= len(tps) else 0.0
                if tp_px <= 0:
                    continue
                tol = max(1.5, tp_px * 0.0012)
                if abs(px - tp_px) <= tol:
                    if lv == 1:
                        self._ws_tp1_fill_hint = True
                    levels = getattr(self, "_ws_tp_fill_levels", None)
                    if not isinstance(levels, set):
                        levels = set()
                    levels.add(lv)
                    self._ws_tp_fill_levels = levels

    def _handle_deepcoin_order_event(self, data):
        """Order 事件：FILLED/PARTIALLY_FILLED → 触发部分成交重同步。"""
        rows = data.get("data") or []
        if not isinstance(rows, list):
            rows = [rows]
        for row in rows:
            sym = str(row.get("instrument_id") or row.get("symbol") or "").upper()
            if sym and sym != self.symbol.upper():
                continue
            status = str(row.get("status") or "").upper()
            if status in ("FILLED", "PARTIALLY_FILLED"):
                self._schedule_partial_fill_resize(source=f"dc_ws_{status.lower()}")
            elif status in ("CANCELED", "CANCELLED"):
                self._handle_unilateral_order_cancel(row)

    def _handle_unilateral_order_cancel(self, order):
        """§12.3：非本地发起的取消 → 异常取消告警 + 强制对账。"""
        try:
            sym = str(order.get("instrument_id") or order.get("symbol") or "").upper()
            if sym and sym != self.symbol.upper():
                return
            oid = order.get("orderId") or order.get("algoId") or ""
            status = str(order.get("status") or "").upper()
            logger.warning(
                f"🚨 [{self.symbol}] 外部撤单 event: id={oid} status={status} | "
                f"触发强制对账"
            )
            self._ws_defense_pulse = True
            self._ws_fast_poll = True
        except Exception as e:
            logger.debug(f"外部撤单解析跳过: {e}")

    def _purge_all_defense_orders_on_flat(self, reason="", max_rounds=6):
        """§6.2：空仓时撤掉所有防御单（TP + 雷达 STOP）。"""
        cancelled = 0
        for rnd in range(max_rounds):
            deepcoin_client.cancel_all_open_orders(self.symbol)
            remaining = self._collect_tp_limit_orders()
            if not remaining:
                break
            for _o in remaining:
                try:
                    oid = _o.get("orderId")
                    if oid:
                        deepcoin_client.cancel_order(self.symbol, order_id=oid)
                        cancelled += 1
                except Exception:
                    pass
                time.sleep(0.2)
            if not self._collect_tp_limit_orders():
                break
            time.sleep(0.3)
        if cancelled:
            logger.info(f"🧹 [{self.symbol}] 空仓撤防：{cancelled} 张 | {reason}")
        return cancelled

    def _schedule_partial_fill_resize(self, source=""):
        """§6.2：TP/减仓成交后按实时头寸同步硬/雷达数量（防超卖变反向）。"""
        if getattr(self, "api_monitor_only", False) or getattr(self, "trading_paused", False):
            return
        if not getattr(self, "monitoring", False):
            return
        now = time.time()
        last = float(getattr(self, "_last_partial_resize_ts", 0) or 0)
        if now - last < 0.75:
            setattr(self, "_partial_resize_pending", True)
            return
        self._last_partial_resize_ts = now
        self._partial_resize_pending = False
        try:
            pos = self._get_active_position()
            if pos == "QUERY_FAILED" or pos is None:
                self._partial_resize_pending = True
                logger.warning(
                    f"⏳ [{self.symbol}] 部分成交核算跳过·持仓不可读 | {source}"
                )
                return
            live = float((pos or {}).get("size") or 0)
            if live <= 0:
                logger.info(
                    f"🧹 [{self.symbol}] WS成交后已空仓 → 清挂单 | {source}"
                )
                self._purge_all_defense_orders_on_flat(f"WS成交空仓|{source}")
                return
            if self._is_dust_qty(live):
                logger.warning(
                    f"🐜 [{self.symbol}] 部分成交后零头={live} → 市价扫尾 | {source}"
                )
                self._sweep_dust_and_finalize(f"partial_fill_dust|{source}")
                return
            self.watched_qty = live
            ep = float((pos or {}).get("entry_price") or 0)
            if ep > 0:
                self.watched_entry = ep
            self._save_state()
        except Exception as e:
            logger.error(f"[{self.symbol}] partial_fill_resize: {e}")

    def _full_reentry(self, action, close_reason):
        """铁律：先平现有仓 → 净挂单 → 再开仓刷新；钉钉核实。"""
        reason = close_reason or "TV开仓·一律先平后开"
        try:
            self._pipeline_pending_clear(note=reason)
        except Exception:
            pass
        deepcoin_client.cancel_all_open_orders(self.symbol)
        time.sleep(0.5)
        if not self._close_all(reason, reset_state=True):
            logger.error("❌ 先平后开中止：平仓未归零，拒绝叠仓开仓")
            try:
                self._pipeline_fail(Role.AUDITOR_POS, "CLEAR_FAIL")
            except Exception:
                pass
            dingtalk.report_system_alert(
                "先平后开中止 · 平仓未归零",
                "强平后盘口仍有持仓，已拒绝新开仓，请人工核查 Deepcoin 盘口",
            )
            self._close_open_chain_active = False
            return
        if not self._wait_verify(self._verify_flat, retries=8, delay=0.5):
            logger.error("❌ 先平后开中止：空仓核查未通过")
            try:
                self._pipeline_fail(Role.AUDITOR_POS, "CLEAR_VERIFY_FAIL")
            except Exception:
                pass
            dingtalk.report_system_alert(
                "先平后开中止 · 空仓核查失败",
                "平仓指令已发但 REST 仍显示持仓，已拒绝叠仓开仓",
            )
            self._close_open_chain_active = False
            return
        try:
            self._pipeline_cleared(note="sterile_ok")
        except Exception:
            pass
        deepcoin_client.cancel_all_open_orders(self.symbol)
        time.sleep(0.5)
        curr_px = deepcoin_client.get_current_price(self.symbol) or self.tv_price
        if curr_px <= 0:
            logger.error("❌ 先平后开中止：无有效市价")
            return
        try:
            dingtalk.report_system_alert(
                title=f"先平后开·执行 [{self.symbol}]",
                detail=(
                    f"{reason} | 无菌通过 @ {float(curr_px):.2f} → 开 {action} "
                    f"| 终态必须有仓"
                ),
                level="信息",
                suggestion="已先平后开；核对实盘仓位与 TP/止损",
            )
        except Exception:
            pass
        self._open_position(action, curr_px)
        self._close_open_chain_active = False

    def _handle_manual_flat_detected(self, reason, close_meta=None, curr_px=0.0):
        """人工全平 / 止盈吃满 / 止损触发：智能复位账本 + 四标签收网钉钉"""
        meta = self._enrich_close_meta_live(
            close_meta or self._infer_flat_close_meta(curr_px, hint_reason=reason),
            curr_px,
        )
        logger.info(f"📭 感知空仓: {meta.get('tv_reason') or reason}")
        try:
            self._pipeline_reset_flat(note=str(meta.get("tv_reason") or reason or "flat"))
        except Exception:
            pass
        self.monitoring = False
        self.watched_qty = 0
        self.initial_qty = 0
        self.base_qty = 0
        self.add_count = 0
        self.tp_levels_consumed = []
        self.shield_active = False
        self._shield_sltp_ord_id = ""
        self._shield_sltp_set_at = 0.0
        self._shield_cancelled_ids = set()
        self.current_side = None
        deepcoin_client.cancel_all_open_orders(self.symbol)
        self._save_state()
        self._report_flat_close(
            meta.get("tv_reason") or reason or "仓位归零",
            close_meta=meta,
            curr_px=curr_px,
        )

    def _realign_after_position_add(self, new_qty, new_entry, curr_px, entry_type):
        """
        加仓成功后：按 TV TP123 价格 + 新总头寸重挂止盈，并同步 tv_sl / 雷达。
        加仓必撤旧 TP（数量已过期），再按 open_regime 比例重算剩余档位。
        """
        self._ensure_tp123_prices_from_tv(new_entry)
        tp_txt = "/".join(
            f"{float(p):.0f}" for p in (self.tv_tps or []) if float(p or 0) > 0
        ) or "—"
        ratios = self.regime_settings[self._tp_split_regime()]["ratios"]
        consumed = list(getattr(self, "tp_levels_consumed", []) or [])

        sl_to_pass = None
        radar_note = "雷达待命(TP1前)"
        if self._tp1_filled_verified(new_qty, curr_px):
            self._refresh_radar_state_on_recover(curr_px, new_entry)
            sl_to_pass = self._radar_sl_to_pass()
            radar_note = (
                f"雷达已激活 SL={sl_to_pass:.2f}"
                if sl_to_pass else "雷达跟进中"
            )

        logger.info(
            f"🕸️ [{entry_type}] 加仓后防线重挂 | 新仓 {new_qty} 张 @ {new_entry:.2f} "
            f"| TV TP={tp_txt} | R{self._tp_split_regime()} 比例 {ratios} "
            f"| 已成交档 {consumed or '无'}"
        )
        self._cancel_all_tp_limit_orders()
        time.sleep(0.45)

        result = self._enforce_defense_alignment(
            new_qty, new_entry,
            dynamic_sl=sl_to_pass,
            reason=f"{entry_type}加仓后TP123重挂",
            rounds=4,
        )
        audit = result.get("audit") or self._audit_tp_levels(new_qty)
        if not self._tp_audit_ok(audit):
            logger.warning(
                f"⚠️ [{entry_type}] 加仓后 TP 未齐 → 核武重挂 | "
                f"{self._format_audit_summary(audit)}"
            )
            audit = self._nuclear_realign_tp(
                new_qty, new_entry, dynamic_sl=sl_to_pass, rounds=3,
            )
            result["audit"] = audit

        shield_ok = self._maintain_hard_shield(
            new_qty, curr_px, force=True, radar_sl=sl_to_pass,
        )
        if self._tp1_filled_verified(new_qty, curr_px):
            self._process_radar_trailing(new_qty, curr_px)
            sl = self._radar_sl_to_pass()
            if sl:
                shield_ok = self._maintain_hard_shield(
                    new_qty, curr_px, force=True, radar_sl=sl,
                ) or shield_ok
                radar_note = f"雷达推升 SL={sl:.2f}"

        self.shield_sized_qty = float(new_qty)
        self._save_state()
        return {
            "shield_ok": shield_ok,
            "audit": audit,
            "radar_note": radar_note,
            "tp_prices": tp_txt,
            "ratios": ratios,
            "result": result,
        }

    def _add_to_position(self, action, payload):
        """PYRAMID / PROFIT_ADD：base_qty × TV qty_ratio 追加，并重挂 TP123 + 同步雷达"""
        entry_type = normalize_entry_type(payload.get("entry_type"))
        max_add = self._max_add_times_for_regime()
        tv_ratio = float(getattr(self, "tv_qty_ratio", 0) or 0)
        pos = self._get_active_position()
        if not pos or self._safe_qty(pos.get("size", 0)) <= 0:
            logger.warning(f"{entry_type} 到达但盘口无持仓，已忽略")
            return
        if self._pos_side_label(pos) != action:
            dingtalk.report_system_alert(
                f"{entry_type} 方向不符",
                f"TV {action} vs 实盘 {self._pos_side_label(pos)}，已拒绝加仓",
            )
            return
        if tv_ratio <= 0:
            logger.warning(
                f"{entry_type} 跳过：R{self.regime} TV加仓比例={tv_ratio:.2f}（档位禁止加仓）"
            )
            dingtalk.report_system_alert(
                f"{entry_type} 加仓跳过",
                f"R{self.regime} TV qty_ratio={tv_ratio:.2f} ≤ 0 | "
                f"base={getattr(self, 'base_qty', 0)} 张",
            )
            return
        if int(getattr(self, "add_count", 0) or 0) >= max_add:
            logger.warning(
                f"{entry_type} 跳过：已达 R{self.regime} 最大加仓次数 {max_add} "
                f"(base={getattr(self, 'base_qty', 0)})"
            )
            dingtalk.report_system_alert(
                f"{entry_type} 加仓跳过",
                f"R{self.regime} 已达最大加仓 {max_add} 次 | base={getattr(self, 'base_qty', 0)} "
                f"| 现仓 {self._safe_qty(pos.get('size', 0))} 张",
            )
            return

        curr_px = deepcoin_client.get_current_price(self.symbol) or self.tv_price
        old_qty = self._safe_qty(pos.get("size", 0))
        old_entry = float(pos.get("entry_price", 0))
        add_qty, meta = self._calc_vps_add_qty(tv_ratio)
        if add_qty <= 0:
            logger.error(f"{entry_type} 跳过：计算加仓量无效 {meta}")
            dingtalk.report_system_alert(
                f"{entry_type} 数量无效",
                f"加仓计算失败: {self._tv_sizing_note(add_qty, meta, entry_type=entry_type)}",
            )
            return

        deepcoin_client.set_position_mode(self.symbol, mode="both")
        deepcoin_client.set_leverage(self.symbol, leverage=EXCHANGE_LEVERAGE)
        logger.info(
            f"➕ [{entry_type}] {action} 追加 {add_qty} 张 | "
            f"{self._tv_sizing_note(add_qty, meta, entry_type=entry_type)}"
        )
        open_side = "buy" if action == "LONG" else "sell"
        pos_side = "long" if action == "LONG" else "short"
        res = deepcoin_client.place_market_order(self.symbol, open_side, pos_side, add_qty)
        if not res or not deepcoin_client._is_success(res):
            dingtalk.report_system_alert(
                f"{entry_type} 下单失败",
                f"{action} 追加 {add_qty} 张 市价单未成交",
            )
            return
        time.sleep(1.5)

        new_pos = self._get_active_position()
        if not new_pos or self._safe_qty(new_pos.get("size", 0)) <= old_qty:
            dingtalk.report_system_alert(
                f"{entry_type} 核实失败",
                f"追加 {add_qty} 张 后实盘未增长",
            )
            return

        new_qty = self._safe_qty(new_pos.get("size", 0))
        new_entry = float(new_pos.get("entry_price", 0))
        self.watched_qty = new_qty
        self.watched_entry = new_entry
        self.current_side = action
        self.monitoring = True
        self._save_state()

        realign = self._realign_after_position_add(
            new_qty, new_entry, curr_px, entry_type,
        )
        sl_ok = realign.get("shield_ok", False)
        audit = realign.get("audit") or {}
        self.add_count = int(getattr(self, "add_count", 0) or 0) + 1
        self._save_state()
        type_label = "浮盈加仓" if entry_type == ENTRY_TYPE_PROFIT_ADD else "金字塔加仓"
        tp_summary = self._format_audit_summary(audit)
        verify_note = (
            f"{type_label} | {self._tv_sizing_note(add_qty, meta, entry_type=entry_type)} "
            f"| base={getattr(self, 'base_qty', 0)} "
            f"| 加仓次数 {self.add_count}/{max_add} "
            f"| 持仓 {old_qty}→{new_qty} 张 @ {new_entry:.2f} "
            f"| TV TP={realign.get('tp_prices', '—')} "
            f"| TP {audit.get('matched_full', 0)}/{audit.get('expected', 0)} "
            f"| {tp_summary} "
            f"| {realign.get('radar_note', '')} "
            f"| tv_sl={getattr(self, 'tv_sl', 0):.2f} "
            f"| {'防线已核实' if sl_ok and self._tp_audit_ok(audit) else '防线待核实'}"
        )
        self._call_dingtalk(
            dingtalk.report_tv_position_add,
            side=action,
            entry_type=entry_type,
            add_qty=add_qty,
            old_qty=old_qty,
            new_qty=new_qty,
            old_entry=old_entry,
            new_entry=new_entry,
            tv_sl=getattr(self, "tv_sl", 0),
            risk_pct=self.tv_risk_pct,
            leverage=self.tv_sizing_leverage,
            qty_ratio=tv_ratio,
            base_qty=getattr(self, "base_qty", 0),
            vps_sizing_meta=meta,
            add_count=self.add_count,
            max_add_times=max_add,
            regime=self.regime,
            tp_audit=tp_summary,
            radar_note=realign.get("radar_note", ""),
            open_regime=self._tp_split_regime(),
            tp_ratio_label=format_regime_tp_ratios_label(self._tp_split_regime()),
            verify_note=verify_note,
            verified=sl_ok and self._tp_audit_ok(audit),
        )
        self._ensure_sentinel_running()

    def _is_fresh_open_cooldown(self, pos=None, cooldown_sec=None):
        """刚开仓同向冷却窗：重复 OPEN 禁止立刻先平后开"""
        cooldown_sec = float(
            cooldown_sec if cooldown_sec is not None else OPEN_SAME_DIR_COOLDOWN_SEC
        )
        if cooldown_sec <= 0:
            return False
        now = time.time()
        sig = getattr(self, "_last_entry_signal", None) or {}
        live_side = self._pos_side_label(pos) if pos else self.current_side
        if (
            self.monitoring
            and self.current_side
            and (not pos or live_side == self.current_side)
            and float(sig.get("ts") or 0) > 0
            and now - float(sig["ts"]) < cooldown_sec
        ):
            return True
        last_open = self._load_last_journal_entry(OPEN_JOURNAL)
        if not last_open:
            return False
        side = str(last_open.get("side") or "").upper()
        if pos and side and side != live_side:
            return False
        ts_raw = last_open.get("ts")
        try:
            if isinstance(ts_raw, (int, float)):
                age = now - float(ts_raw)
            else:
                age = now - datetime.strptime(str(ts_raw), "%Y-%m-%d %H:%M:%S").timestamp()
            return 0 <= age < cooldown_sec
        except Exception:
            return False

    def _handle_smart_entry(self, action, payload=None):
        """
        铁律：带开仓的 TV → 一律先平后开刷新；单独平仓由 CLOSE* 清零等待。
        """
        payload = payload or {}
        entry_type = normalize_entry_type(payload.get("entry_type"))

        if entry_type in (ENTRY_TYPE_PYRAMID, ENTRY_TYPE_PROFIT_ADD):
            self._add_to_position(action, payload)
            self._touch_entry_signal_signature(action)
            return

        curr_px = deepcoin_client.get_current_price(self.symbol) or self.tv_price
        if self._verify_flat() and self._is_duplicate_flat_entry(action, curr_px):
            logger.info(f"🧠 空仓短时重复开仓 TV [{action}] → 忽略，干净等待下次")
            try:
                self._call_dingtalk(
                    dingtalk.report_smart_same_dir_decision,
                    side=action,
                    decision="skip_duplicate_flat",
                    live_entry=0.0,
                    tv_price=self.tv_price,
                    diff_pct=0.0,
                    threshold_pct=SAME_DIR_MIN_SPREAD_PCT,
                    open_regime=self.regime,
                    tv_regime=self.regime,
                    open_atr=self._last_entry_signal.get("atr", self.current_atr),
                    tv_atr=self.current_atr,
                    qty=0.0,
                    verify_note="空仓重复开仓信号已忽略 | 状态干净等待",
                )
            except Exception:
                pass
            self._touch_entry_signal_signature(action)
            return

        pos = self._get_active_position()
        live_sz = self._safe_qty((pos or {}).get("size", 0))
        live_side = self._pos_side_label(pos) if pos else None
        logger.info(
            f"⚡ TV开仓 [{action}] entry={entry_type} → 铁律先平后开刷新 "
            f"| 现仓 {live_side or 'FLAT'} {live_sz}张"
        )
        self._full_reentry(
            action,
            "TV开仓·一律先平后开刷新仓位（有仓先平；无仓净挂单再开）",
        )
        self._touch_entry_signal_signature(action)

    def _open_position(self, action, curr_px, payload=None):
        payload = payload or {}
        if self._open_in_progress:
            logger.error(f"开仓中止：已有开仓流程进行中，拒绝叠仓 [{action}]")
            return
        self._open_in_progress = True
        try:
            self._snapshot_sizing_principal(
                f"开仓前 {normalize_entry_type(payload.get('entry_type'))} R{self.regime}"
            )
            qty, balance, margin_usdt, margin_pct, sizing_meta = self._calc_target_open_qty(
                curr_px, payload=payload,
            )
            if qty <= 0:
                logger.error(f"开仓跳过：目标张数无效 balance={balance:.2f} px={curr_px}")
                return

            deepcoin_client.set_position_mode(self.symbol, mode="both")
            deepcoin_client.set_leverage(self.symbol, leverage=EXCHANGE_LEVERAGE)
            notional = qty * self.face_value * curr_px
            budget_txt = format_vps_sizing_note(sizing_meta, qty=qty, entry_type=ENTRY_TYPE_OPEN)
            logger.info(f"📐 仓位预算 [{self.symbol}]: {budget_txt} (名义 ~{notional:.0f}U)")

            cap_ok, _cap_meta = self._assert_notional_cap_or_reject(
                qty, curr_px, sizing_meta=sizing_meta,
            )
            if not cap_ok:
                return

            if not self._wait_verify(self._verify_flat, retries=4, delay=0.35):
                logger.error("开仓中止：市价下单前盘口仍非空")
                dingtalk.report_system_alert(
                    f"开仓中止 · 下单前盘口非空 [{self.symbol}]",
                    f"TV **{action}** 目标 **{qty}** 张，下单前 REST 仍显示持仓，已拒绝叠仓",
                    suggestion="系统将尝试强制清场，请核查是否有人工挂单或残仓",
                )
                return

            open_side = "buy" if action == "LONG" else "sell"
            pos_side = "long" if action == "LONG" else "short"
            logger.info(f"🚀 [唯一主仓] 极速开仓: {open_side} {qty} 张 | {self.symbol} | 档位 {self.regime}")
            try:
                self._pipeline_entry_submitted(action, qty)
            except Exception:
                pass
            res = deepcoin_client.place_market_order(self.symbol, open_side, pos_side, qty)
            if not res or not deepcoin_client._is_success(res):
                logger.error("开仓失败：市价单未成交")
                try:
                    self._pipeline_fail(Role.EXECUTION, "ENTRY_SUBMIT_FAIL")
                except Exception:
                    pass
                dingtalk.report_system_alert("开仓失败", f"TV {action} {qty} 张 市价单失败")
                return
            time.sleep(2.0)

            pos = self._get_active_position()
            if not pos or self._safe_qty(pos.get("size", 0)) <= 0:
                logger.error("开仓失败：成交后 REST 无持仓")
                try:
                    self._pipeline_fail(Role.EXECUTION, "ENTRY_CONFIRM_FAIL")
                except Exception:
                    pass
                return

            real_qty = self._safe_qty(pos["size"])
            if real_qty > qty * OPEN_OVERSIZE_RATIO:
                logger.error(
                    f"🚨 持仓超标: 目标 {qty} 张，实盘 {real_qty} 张 "
                    f"(>{qty * OPEN_OVERSIZE_RATIO:.3f})，启动裁减"
                )
                dingtalk.report_system_alert(
                    "持仓超标 · 自动裁减",
                    f"目标 {qty} 张 (下单额 {margin_usdt:.0f}U)，"
                    f"实盘 {real_qty} 张 @ {pos['entry_price']:.2f}，正在 reduceOnly 裁减",
                    suggestion="裁减基数=VPS风险公式，非可用保证金",
                )
                real_qty = self._trim_position_to_target(qty, action)
                pos = self._get_active_position()
                if pos:
                    pos["size"] = real_qty

            self.current_side = action
            self.open_regime = self.regime
            payload_atr = self._safe_float((payload or {}).get("atr"), 0.0)
            if payload_atr <= 0:
                try:
                    dingtalk.report_system_alert(
                        f"[{getattr(self, 'tag', '?')}] 开仓拒绝·缺TV atr",
                        f"{self.symbol} webhook 无 atr → 拒绝开仓",
                        level="紧急",
                        suggestion="检查 TV get_entry_json 是否传 atr",
                    )
                except Exception:
                    pass
                logger.error(f"拒绝开仓缺 TV atr [{self.symbol}]")
                return
            self.open_atr = payload_atr
            self.current_atr = payload_atr
            self.early_be_done = False
            self._early_be_checkpoint_done = False
            self.initial_qty = real_qty
            self.base_qty = int(real_qty)
            self.add_count = 0
            self._last_open_exec_ts = time.time()
            try:
                self._pipeline_entry_confirmed(
                    action, float(real_qty), float(pos.get("entry_price") or 0),
                )
            except Exception:
                pass
            self._protect_and_monitor(
                real_qty, pos['entry_price'],
                budget_note=f"[{self.symbol}] {budget_txt} | ",
                target_qty=qty,
                sizing_meta=sizing_meta,
            )
        finally:
            self._open_in_progress = False

    def _protect_and_monitor(self, qty, entry_price, budget_note="", target_qty=0, sizing_meta=None):
        """规格 v1.0 §3：永久硬止损 = |TV.price − TV.stop_loss| × 1.15。"""
        self._lock_frozen_hard_sl_from_tv(
            entry=entry_price, side=self.current_side, source="开仓后",
        )
        hard_sl = float(self._frozen_hard_px() or 0)
        self.current_sl = hard_sl if hard_sl > 0 else 0.0
        self.best_price = entry_price
        self.shield_active = False
        self.shield_tiers_consumed = []
        self.tp_levels_consumed = []
        self._shield_sltp_ord_id = ""
        self._shield_sltp_set_at = 0.0
        self._shield_cancelled_ids = set()
        self.breathing_coefficient = 1.0
        self._breath_ratio_history = []
        self.breakeven_phase = False
        self.radar_activated = False
        self._early_be_checkpoint_done = False
        open_atr = float(getattr(self, "open_atr", None) or self.current_atr or 0)
        if open_atr > 0 and entry_price > 0 and self.current_side:
            self.initial_stop = initial_stop_price(self.current_side, entry_price, open_atr, profile=getattr(self, "breath_profile", None))
        else:
            self.initial_stop = 0.0
        try:
            self._breath_ratio_history = []
        except Exception:
            pass
        self._refresh_breathing_coefficient(force=True)

        # 规格 §3.7：从 tv_stop_distance / atr 推导 adx_tier（弱0/中1/强2）
        # tv_stop_distance = |TV.price − TV.stop_loss|；1.3×ATR = 强趋势档边界
        open_atr = float(getattr(self, "open_atr", None) or self.current_atr or 0)
        tv_sl_val = float(getattr(self, "tv_sl_ref", 0) or 0)
        tv_price_val = float(getattr(self, "tv_price", 0) or entry_price or 0)
        if open_atr > 0 and tv_sl_val > 0 and tv_price_val > 0:
            tv_stop_dist = abs(tv_price_val - tv_sl_val)
            tier_threshold = 1.3 * open_atr
            if tv_stop_dist > tier_threshold:
                derived_tier = 0  # 弱趋势
            else:
                derived_tier = 2  # 强趋势（实际是 tv_stop_dist <= tier_threshold）
            self.adx_tier = derived_tier
            self.radar_tier = derived_tier
            logger.info(
                f"[{self.symbol}] 规格 §3.7 adx_tier={derived_tier} "
                f"(tv_dist={tv_stop_dist:.2f} ATR={open_atr:.2f} "
                f"阈值={tier_threshold:.2f})"
            )

        if hasattr(self, "_init_reentry_runtime"):
            self._init_reentry_runtime()

        if hasattr(self, "_radar_stage_last"):
            self._radar_stage_last = 0
        self._radar_activation_notified = False

        # 规格 v1.0：冻结雷达激活价格（使用 TP1/TP2 绝对锚点）
        try:
            from reentry_profiles import radar_gate_price_from_tps
            tps = list(getattr(self, "tv_tps", None) or [])
            tp1_px = float(tps[0] or 0) if len(tps) > 0 else 0.0
            tp2_px = float(tps[1] or 0) if len(tps) > 1 else 0.0
            if tp1_px > 0 and tp2_px > 0:
                self.radar_activation_price = radar_gate_price_from_tps(tp1_px, tp2_px)
        except Exception:
            pass

        self._radar_armed_after_tp1 = False
        self._ws_tp1_fill_hint = False
        self._open_settled_qty = self._safe_qty(qty)
        self.initial_qty = self._safe_qty(qty)
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
                self._open_settled_qty = vqty
                self._save_state()
            else:
                self.watched_qty = vqty
                self.initial_qty = vqty
                self._open_settled_qty = vqty
                self._save_state()

            self._scorched_earth_cancel_for_recover()
            self._enforce_pre_tp1_radar_standby(
                vqty, verified["entry_price"], source="开仓保护",
            )
            self._enforce_defense_alignment(
                vqty, verified["entry_price"],
                dynamic_sl=None, reason="开仓后防线对齐", rounds=4,
                recover_mode=True,
            )
            audit = self._wait_defense_settled(vqty)
            matched, expected = audit["matched_full"], audit["expected"]
            curr_px = deepcoin_client.get_current_price(self.symbol) or entry_price
            if expected > 0 and matched < expected:
                logger.warning(
                    f"⚠️ 开仓首轮 TP 仅 {matched}/{expected} → 追加核武重挂"
                )
                audit = self._nuclear_realign_tp(
                    vqty, verified["entry_price"], dynamic_sl=None, rounds=3,
                )
                self._maintain_hard_shield(vqty, curr_px, force=True)
                audit = self._wait_defense_settled(vqty)
                matched, expected = audit["matched_full"], audit["expected"]
            verify_note = (
                f"{budget_note} | " if budget_note else ""
            ) + (
                f"持仓 {vqty}张 @ {verified['entry_price']:.2f} | "
                f"限价止盈 {matched}/{expected} 档 | {self._format_audit_summary(audit)} | "
                f"{self._tv_field_source_note(getattr(self, '_last_tv_field_sources', {}))}"
            )
            if target_qty > 0 and vqty > target_qty * OPEN_OVERSIZE_RATIO:
                verify_note += f" | ⚠️ 超标目标 {target_qty} 张"
            self._record_open_log(
                self.current_side, vqty, verified["entry_price"], source="open",
            )
            try:
                hard_px = float(getattr(self, "tv_sl", 0) or getattr(self, "current_sl", 0) or 0)
                ratios = list(
                    (self.regime_settings.get(self._tp_split_regime()) or {}).get("ratios")
                    or [0.10, 0.20, 0.70]
                )
                self._pipeline_orders_placed(
                    hard_sl_px=hard_px,
                    hard_sl_live=hard_px > 0,
                    tp1={"px": float((tp_pxs or [0])[0] or 0), "qty": round(vqty * float(ratios[0]), 4), "filled": False},
                    tp2={"px": float((tp_pxs or [0, 0])[1] or 0) if len(tp_pxs or []) > 1 else 0.0, "qty": round(vqty * float(ratios[1]), 4), "filled": False},
                )
                self._pipeline_run_chief_audit(source="deepcoin_open")
            except Exception as e:
                logger.warning(f"[{self.symbol}] chief audit wire: {e}")
            self._call_dingtalk(
                dingtalk.report_supervisor_open,
                side=self.current_side,
                entry_price=verified['entry_price'],
                tv_price=self.tv_price,
                qty=vqty,
                tp_pxs=tp_pxs,
                atr=self.current_atr,
                regime=self.open_regime,
                tv_tps=self.tv_tps,
                verify_note=verify_note,
                tp_audit=audit,
                verified=(expected == 0 or matched >= expected),
                principal_balance=self.sizing_principal or deepcoin_client.get_principal_wallet_balance(),
                margin_pct=float((sizing_meta or {}).get("effective_risk_pct", VPS_RISK_PCT) or VPS_RISK_PCT) / 100.0,
                margin_usdt=float((sizing_meta or {}).get("order_amount", 0) or 0),
                leverage=EXCHANGE_LEVERAGE,
                vps_sizing_meta=sizing_meta,
                tv_field_sources=getattr(self, "_last_tv_field_sources", {}),
                symbol=self.symbol,
                unit_label=self.unit_label,
            )
            try:
                self._pipeline_reported(note="supervisor_open")
            except Exception:
                pass
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

    def _radar_activation_ratio(self):
        """返回冻结的 ADX 启动比例（0.70~0.90）。"""
        from reentry_profiles import normalize_activation_ratio
        frac = float(getattr(self, "radar_activation_frac", 0) or 0)
        adx = float(
            getattr(self, "radar_activation_adx", 0)
            or getattr(self, "last_adx", 0)
            or 25.0
        )
        ratio = normalize_activation_ratio(frac, adx)
        if abs(ratio - frac) > 1e-9:
            self.radar_activation_frac = ratio
        return float(ratio)

    def _radar_activation_price(self):
        """
        【规格 v1.0 · 绝对价格锚定】
        首次开仓：雷达激活价 = (TP1 + TP2) / 2（TP1/TP2 为 webhook 原始信号价格）
        重入开仓：雷达激活价 = TP2（价格必须真正到达 TP2 才接管）
        不再使用 ADX 比例 × TP1 距离的旧公式。
        """
        from reentry_profiles import radar_gate_price_from_tps

        frozen = float(getattr(self, "radar_activation_price", 0) or 0)
        activated = bool(getattr(self, "radar_activated", False))
        tps = list(getattr(self, "tv_tps", None) or [])
        tp1_px = float(tps[0] or 0) if len(tps) > 0 else 0.0
        tp2_px = float(tps[1] or 0) if len(tps) > 1 else 0.0
        attempt = int(getattr(self, "reentry_attempt", 0) or 0)

        # 已冻结且有效：持仓期不漂移
        if frozen > 0 and tp1_px > 0 and tp2_px > 0:
            if not activated:
                expect = radar_gate_price_from_tps(tp1_px, tp2_px, attempt)
                if expect > 0 and abs(frozen - expect) / max(expect, 1e-9) > 0.002:
                    self.radar_activation_price = expect
                    return expect
            return frozen

        # 首次计算
        if tp1_px > 0 and tp2_px > 0:
            px = radar_gate_price_from_tps(tp1_px, tp2_px, attempt)
            if px > 0:
                self.radar_activation_price = px
                return px
        return 0.0

    def _should_radar_trail(self, curr_px):
        """
        【规格 v1.0 · §5.1 雷达延迟激活】
        价格到达TP1-TP2中点（首次开仓）或TP2（重入开仓）后，雷达开始追踪。
        不再要求 TP1 成交才激活雷达。
        """
        if getattr(self, "_radar_armed_after_tp1", False) and self._is_radar_active():
            return True
        if curr_px <= 0 or not self.watched_entry:
            return False
        gate = float(self._radar_activation_price() or 0)
        if gate <= 0:
            return False
        side = str(self.current_side or "").upper()
        if side == "LONG":
            return curr_px >= gate
        elif side == "SHORT":
            return curr_px <= gate
        return False

    def _compute_radar_sl(self, curr_px=0.0):
        if not self.watched_entry or self.best_price <= 0:
            return None
        if self._is_radar_active():
            tick = self._apply_breath_stop_tick(curr_px)
            if tick and float(tick.get("stop") or 0) > 0:
                sl = float(tick["stop"])
                floor_px = self._radar_breakeven_floor()
                if self.current_side == "LONG":
                    return max(sl, floor_px)
                if self.current_side == "SHORT":
                    return min(sl, floor_px)
                return sl
        trail_offset = self._radar_trail_offset_price()
        floor_px = self._radar_breakeven_floor()
        if self.current_side == "LONG":
            return max(round(self.best_price - trail_offset, 2), floor_px)
        if self.current_side == "SHORT":
            return min(round(self.best_price + trail_offset, 2), floor_px)
        return None

    def _sync_radar_sl_from_best(self, curr_px):
        """TP 重对齐前刷新内存止损位，避免把旧止损重新挂回交易所"""
        if not self._should_radar_trail(curr_px):
            return self.current_sl
        new_sl = self._compute_radar_sl(curr_px)
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
        """0~1：朝 ADX 激活绝对价推进；已激活后走阶段进度（若有）。"""
        try:
            if hasattr(self, "_radar_legitimately_armed") and (
                self._radar_legitimately_armed(self.watched_qty, curr_px)
                or self._is_radar_active()
            ):
                if hasattr(self, "_effective_radar_stage"):
                    return min(1.0, self._effective_radar_stage(curr_px) / 5.0)
                return 1.0
        except Exception:
            if self._is_radar_active():
                return 1.0
        if self._is_radar_active():
            return 1.0
        curr_px = float(curr_px or 0)
        entry = float(self.watched_entry or 0)
        gate = float(self._radar_activation_price() or 0)
        if curr_px <= 0 or entry <= 0 or gate <= 0:
            return 0.0
        side = str(self.current_side or "").upper()
        if side == "LONG":
            span = gate - entry
            if span <= 0:
                return 1.0 if curr_px >= gate else 0.0
            return max(0.0, min(1.0, (curr_px - entry) / span))
        if side == "SHORT":
            span = entry - gate
            if span <= 0:
                return 1.0 if curr_px <= gate else 0.0
            return max(0.0, min(1.0, (entry - curr_px) / span))
        return 0.0

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
        real_amt = float(self._resolve_live_qty(real_amt) or 0)
        if real_amt <= 0:
            return False

        if not self._is_radar_active():
            return self._perform_radar_handoff(
                real_amt, curr_px, reason="雷达激活 · 移动保本",
            )

        tick = self._apply_breath_stop_tick(curr_px)
        new_sl = float((tick or {}).get("stop") or 0)
        if new_sl <= 0:
            trail_offset = self._radar_trail_offset_price()
            floor_px = self._radar_breakeven_floor()
            if self.current_side == "LONG":
                new_sl = max(round(self.best_price - trail_offset, 2), floor_px)
            elif self.current_side == "SHORT":
                new_sl = min(round(self.best_price + trail_offset, 2), floor_px)
            else:
                return False
        new_sl = self._clamp_radar_sl_for_market(curr_px, new_sl)
        if not self._can_safely_place_radar_sl(curr_px, new_sl):
            return False

        if self.current_side == "LONG":
            if new_sl > self.current_sl + 1.0:
                new_sl = self._clamp_radar_to_tv_floor(new_sl)
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
                new_sl = self._clamp_radar_to_tv_floor(new_sl)
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
                            if time.time() < getattr(self, "_sentinel_grace_until", 0):
                                logger.debug(
                                    "哨兵宽限期：跳过空仓判定（防重启误清场）"
                                )
                                continue
                            if self.watched_qty > 0:
                                if not self._confirm_position_flat():
                                    logger.warning(
                                        "⚠️ [哨兵] 首次无仓但复核仍有持仓 → 跳过误清场"
                                    )
                                    continue
                                flat_meta = self._infer_flat_close_meta(
                                    curr_px=last_px,
                                    hint_reason="仓位归零 (止盈吃单 / 人工全平 / 止损触发)",
                                )
                                self._handle_manual_flat_detected(
                                    flat_meta.get("tv_reason", "仓位归零"),
                                    close_meta=flat_meta,
                                    curr_px=last_px,
                                )
                            break

                        if self.watched_qty > 0 and self._should_finalize_tp_victory(real_amt):
                            self._sweep_dust_and_finalize(
                                "仓位归零 (止盈吃单 / 人工全平 / TV 强制平仓)"
                            )
                            break

                        tv_opposite = self._strict_tv_opposite_side(actual_side)
                        if (
                            tv_opposite
                            and actual_side
                            and not self._live_aligns_with_credible_tv(actual_side)
                        ):
                            reason = (
                                f"致命方向背离：实盘({actual_side}) vs "
                                f"最新TV({tv_opposite}) [实盘监督]"
                            )
                            verify_note = (
                                f"触发源: 实盘监督 | 最新TV {tv_opposite} | "
                                f"实盘反向 {actual_side}"
                            )
                            self._close_all(
                                reason,
                                force_align=(actual_side, tv_opposite),
                                force_verify_note=verify_note,
                            )
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
                                    self._report_qty_change_dingtalk(
                                        old_qty, real_amt, result, change=change,
                                    )
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
                        if getattr(self, "_post_recover_radar_pulse", False):
                            self._post_recover_radar_pulse = False
                            if curr_px > 0:
                                self._process_radar_trailing(real_amt, curr_px)
                            self._radar_guardian_audit(real_amt, curr_px)
                            logger.info("📡 [哨兵] 重启后立即雷达脉冲完成")
                        elif not qty_changed:
                            self._radar_guardian_audit(real_amt, curr_px)

                        if curr_px <= 0:
                            continue

                        # 规格 v1.0 §5.0：提前保本检查点（在雷达激活判断之前独立检查）
                        if self._radar_is_dormant() and curr_px > 0:
                            self._check_early_be_checkpoint(curr_px)

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

        live_qty = self._resolve_live_qty(qty)
        if live_qty <= 0:
            logger.warning(f"重建防线跳过：交易所无可用持仓 (传入 {qty} 张)")
            return 0

        # 【关键修复】防止死循环：短时间重复调用_rebuild_defenses直接返回
        now = time.time()
        if now - getattr(self, "_last_rebuild_attempt_ts", 0) < 10.0:
            logger.warning(
                f"[{self.symbol}] _rebuild_defenses 冷却中（{now - getattr(self, '_last_rebuild_attempt_ts', 0):.1f}s < 10s），跳过"
            )
            return 0
        self._last_rebuild_attempt_ts = now

        self._cancel_all_tp_limit_orders()
        # 【关键修复】增加等待时间确保撤销完成（从0.35秒改为2秒）
        time.sleep(2.0)
        # 【关键修复】撤销后必须验证旧单已全部撤销，防止死循环挂单
        leftover = self._collect_tp_limit_orders()
        if leftover:
            prices = [f"@{o['price']:.2f}" for o in leftover]
            logger.warning(
                f"[{self.symbol}] 撤销后仍有{len(leftover)}张旧TP未撤净，等待..."
            )
            time.sleep(2.0)
            leftover = self._collect_tp_limit_orders()
            if leftover:
                prices = [f"@{o['price']:.2f}" for o in leftover]
                logger.error(
                    f"[{self.symbol}] 旧TP仍未撤净，放弃本次补挂: {prices}"
                )
                return 0

        if live_qty != qty:
            self.watched_qty = live_qty
            self._save_state()

        consumed = getattr(self, "tp_levels_consumed", []) or []
        placed = 0

        # 规格 v1.0 §6 保护：tv_tps 全零说明 TP 价格从未被正确初始化（TV 信号缺字段 / 重启恢复失败）。
        # 用 open_atr + open_regime 做紧急 fallback，不让仓位裸奔。
        if self.tv_tps and all(float(t or 0) <= 0 for t in self.tv_tps):
            if self.current_side and entry > 0:
                atr = float(getattr(self, "open_atr", None) or self.current_atr or 0)
                regime = int(getattr(self, "open_regime", None) or self.regime or 3)
                if atr > 0:
                    from webhook_parser import enrich_entry_tp_prices
                    payload = enrich_entry_tp_prices(self.current_side, entry, atr, regime, {})
                    tps = [
                        self._safe_float(payload.get("tv_tp1"), 0),
                        self._safe_float(payload.get("tv_tp2"), 0),
                        self._safe_float(payload.get("tv_tp3"), 0),
                    ]
                    self.tv_tps = self._sanitize_tp_prices(tps)
                    logger.warning(
                        f"⚠️ tv_tps=全零 → ATR紧急Fallback TP123={self.tv_tps} "
                        f"| entry={entry:.2f} ATR={atr:.2f} R{regime}"
                    )
                    dingtalk.report_system_alert(
                        "TP价格缺失·ATR紧急Fallback",
                        f"tv_tps全零 | {self.current_side} {live_qty}张 @ {entry:.2f} | "
                        f"ATR={atr:.2f} R{regime} → 强制Fallback {self.tv_tps}",
                        suggestion="请确认 TV 策略开仓消息包含 tp1/tp2/tp3 字段",
                    )
                    self._save_state()

        logger.info(
            f"🕸️ 补挂 TP: 总 {live_qty}张 | 已成交 TP{consumed or '无'} | "
            f"R{self._tp_split_regime()}"
        )

        for lv in self._expected_tp_levels(live_qty):
            q, px = lv["qty"], lv["price"]
            kind = f"TP{int(lv.get('level', 0))}"
            if q > 0 and px > 0:
                # v16.11（根因二修复）：标签残留时，必须先确认原订单已失败/超时才能清
                blocked, tag0, meta0 = self._has_open_pending_defense_tag(kind)
                if blocked:
                    logger.warning(
                        f"[{self.symbol}] 本地未完成标签 tag={tag0} kind={kind} → "
                        f"开始核实原订单是否已失败 | px={px:.2f}"
                    )
                    if not meta0.get("order_id"):
                        age = time.time() - float(meta0.get("ts", 0) or 0)
                        if age >= 45.0:
                            logger.warning(
                                f"[{self.symbol}] 标签 {tag0} 无 order_id 且陈旧 {age:.0f}s ≥ 45s → 直接清理"
                            )
                            self._complete_pending_defense_tag(tag=tag0)
                            self._save_state()
                            time.sleep(0.15)
                        else:
                            logger.warning(
                                f"[{self.symbol}] 标签 {tag0} 无 order_id 且仅 {age:.0f}s < 45s → "
                                f"拒绝清理（可能还在处理中），本次跳过"
                            )
                            continue
                    else:
                        if not self._confirm_stale_before_clear(tag0, meta0):
                            logger.warning(
                                f"[{self.symbol}] 标签 {tag0} 对应订单仍在处理中 → 拒绝清理，本次跳过"
                            )
                            continue
                        self._complete_pending_defense_tag(tag=tag0)
                        self._save_state()
                        time.sleep(0.15)
                tag = make_defense_client_order_id(self.symbol, kind, px)
                # 【紧急修复】挂单前必须确认交易所侧没有该价格的TP订单
                # 防止因本地标签被清理后，交易所侧订单仍存在导致重复挂单
                existing_orders = self._collect_tp_limit_orders()
                # 修复：使用正确的字段名 px 而不是 price
                at_px_existing = [o for o in existing_orders if abs(o.get("px", o.get("price", 0)) - px) <= 1.0]
                if at_px_existing:
                    logger.warning(
                        f"[{self.symbol}] 交易所侧已有 TP@{px:.2f} ({len(at_px_existing)}张)，跳过挂单"
                    )
                    # 更新标签，不挂新单
                    existing = at_px_existing[0]
                    existing_oid = str(existing.get("orderId") or existing.get("ordId") or "")
                    if existing_oid:
                        self._register_pending_defense_tag(
                            make_defense_client_order_id(self.symbol, kind, px),
                            kind, price=px, order_id=existing_oid
                        )
                        self._save_state()
                    continue
                self._register_pending_defense_tag(tag, kind, price=px)
                try:
                    self._save_state()
                except Exception:
                    pass
                last = None
                # v16.11（根因二修复）：TP 挂单失败时强制重试，不轻易放弃
                max_retries = 3
                for attempt in range(max_retries):
                    res = deepcoin_client.place_limit_order(
                        self.symbol, close_side, pos_side, px, q,
                        reduce_only=True, cl_ord_id=tag,
                    )
                    if res and deepcoin_client._is_success(res):
                        last = res
                        oid = str(last.get("orderId") or last.get("algoId") or "")
                        self._register_pending_defense_tag(tag, kind, price=px, order_id=oid)
                        placed += 1
                        logger.info(f"📈 TP{int(lv.get('level', 0))} {q} @ {px:.2f} tag={tag}")
                        break
                    if attempt < max_retries - 1:
                        logger.warning(
                            f"[{self.symbol}] TP{int(lv.get('level', 0))} @ {px:.2f} "
                            f"挂单失败，重试 {attempt + 1}/{max_retries}"
                        )
                        time.sleep(0.3)
                if not last:
                    self._complete_pending_defense_tag(tag=tag)
                    try:
                        self._save_state()
                    except Exception:
                        pass
                    logger.error(
                        f"[{self.symbol}] TP{int(lv.get('level', 0))} @ {px:.2f} "
                        f"失败（已释放标签，max_retries={max_retries}）"
                    )
                else:
                    time.sleep(0.25)

        curr_px = deepcoin_client.get_current_price(self.symbol)
        self._maintain_hard_shield(live_qty, curr_px, force=True)
        if dynamic_sl and not self._has_trigger_sl_near(dynamic_sl):
            self._ensure_radar_sl(live_qty, dynamic_sl)
        return placed

    def _close_all(self, reason="", force_align=None, reset_state=True, close_meta=None,
                   force_verify_note=""):
        """先撤全部挂单再强平所有方向持仓；对冲模式兼容"""
        deepcoin_client.cancel_all_open_orders(self.symbol)
        time.sleep(0.5)
        self._cancel_all_tp_limit_orders()
        time.sleep(0.3)
        closed_successfully = False

        for round_i in range(6):
            # 获取所有方向的持仓
            all_positions = self._get_all_positions()
            if not all_positions:
                closed_successfully = True
                break

            # 清理所有方向的持仓
            for pos in all_positions:
                close_side = "sell" if pos["posSide"] == "long" else "buy"
                live_sz = self._safe_qty(pos["size"])
                logger.info(f"🔪 强平{round_i + 1}/6: {close_side} {live_sz}张 {pos['posSide']} reduceOnly")
                deepcoin_client.place_market_order(
                    self.symbol, close_side, pos["posSide"], live_sz, reduce_only=True,
                )
                time.sleep(0.5)  # 每个方向之间短暂间隔
            time.sleep(1.5)  # 轮次之间等待成交

        # 清理残余持仓（蚂蚁仓扫尾）
        if not closed_successfully:
            all_residual = self._get_all_positions()
            if all_residual:
                total_residual = sum(self._safe_qty(p["size"]) for p in all_residual)
                # 对每方向分别处理
                for residual in all_residual:
                    residual_sz = self._safe_qty(residual["size"])
                    if residual_sz > 0:
                        if self._is_dust_qty(residual_sz):
                            close_side = "sell" if residual["posSide"] == "long" else "buy"
                            logger.warning(f"🐜 蚂蚁仓扫尾: {close_side} {residual_sz}张 {residual['posSide']}")
                            deepcoin_client.place_market_order(
                                self.symbol, close_side, residual["posSide"], residual_sz, reduce_only=True,
                            )
                            time.sleep(1.0)
                        else:
                            logger.warning(f"⚠️ 残仓: {residual['posSide']} {residual_sz}张 非蚂蚁量级")
                closed_successfully = self._verify_flat()

            if not closed_successfully:
                all_residual = self._get_all_positions()
                residual_info = ", ".join(
                    f"{p['posSide']}:{self._safe_qty(p['size'])}张" for p in all_residual
                ) if all_residual else "无"
                logger.error(f"❌ 6 轮强平后仍有残单: {residual_info}")
                dingtalk.report_system_alert(
                    "强平未完全归零",
                    f"6 轮市价平仓后仍剩: {residual_info}，请人工核查 Deepcoin 盘口",
                )

        if reset_state:
            if closed_successfully:
                self.monitoring = False
                self.watched_qty = 0
                self.initial_qty = 0
                self.base_qty = 0
                self.add_count = 0
                self.current_side = None
                self.shield_active = False
                self.shield_tiers_consumed = []
                self.tp_levels_consumed = []
                self._shield_sltp_ord_id = ""
                self._shield_sltp_set_at = 0.0
                self._shield_cancelled_ids = set()
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
                    verify_note=force_verify_note or verify_note,
                )
            else:
                self._report_flat_close(reason, close_meta=close_meta)

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
                    self.initial_stop = float(s.get("initial_stop", 0) or 0)
                    self.breathing_coefficient = float(
                        s.get("breathing_coefficient", 1.0) or 1.0
                    )
                    self._breath_ratio_history = list(
                        s.get("breath_ratio_history", []) or []
                    )
                    self.breakeven_phase = bool(s.get("breakeven_phase", False))
                    self._last_open_exec_ts = float(
                        s.get("last_open_exec_ts", 0) or 0
                    )
                    self.shield_active = bool(s.get("shield_active", False))
                    self.shield_tiers_consumed = list(s.get("shield_tiers_consumed", []) or [])
                    self.tp_levels_consumed = list(s.get("tp_levels_consumed", []) or [])
                    self.shield_sized_qty = float(s.get("shield_sized_qty", 0) or 0)
                    if self.shield_sized_qty > 0:
                        self._shield_arm_notified = True
                    self.sizing_principal = float(s.get("sizing_principal", 0) or 0)
                    self.tv_sl = float(s.get("tv_sl", 0) or 0)
                    self.tv_sl_ref = float(s.get("tv_sl_ref", 0) or 0)
                    self._last_applied_tv_sl = float(
                        s.get("last_applied_tv_sl", 0) or 0
                    )
                    self.tv_risk_pct = float(s.get("tv_risk_pct", 0) or 0)
                    self.tv_qty_ratio = float(s.get("tv_qty_ratio", 1.0) or 1.0)
                    self.tv_entry_type = s.get("tv_entry_type", ENTRY_TYPE_OPEN)
                    self.tv_sizing_leverage = float(
                        s.get("tv_sizing_leverage", s.get("leverage", EXCHANGE_LEVERAGE))
                        or EXCHANGE_LEVERAGE
                    )
                    self.leverage = EXCHANGE_LEVERAGE
                    self.base_qty = int(s.get("base_qty", 0) or 0)
                    self.add_count = int(s.get("add_count", 0) or 0)
                    self._radar_armed_after_tp1 = bool(
                        s.get("radar_armed_after_tp1", False)
                    )
                    self._open_settled_qty = int(
                        s.get("open_settled_qty", s.get("initial_qty", 0)) or 0
                    )
                    # 规格 v1.0 §3：永久硬止损冻结价
                    self.frozen_hard_sl_px = float(s.get("frozen_hard_sl_px", 0) or 0)
                    # 规格 v1.0 §5.0：提前保本检查点
                    self._early_be_checkpoint_done = bool(s.get("_early_be_checkpoint_done", False))
                    # 规格 v1.0 §8-9：防御单幂等标签 + 退出所有权
                    self.exit_ownership = str(s.get("exit_ownership", "NONE") or "NONE")
                    self.ownership_locked_at = float(s.get("ownership_locked_at", 0) or 0)
                    raw_tags = s.get("pending_order_tags") or {}
                    self._pending_order_tags = dict(raw_tags) if isinstance(raw_tags, dict) else {}
                    # v16.14（根因一修复）：重启时清理陈旧标签，避免旧单残留阻止新单挂载
                    self._gc_stale_pending_defense_tags_on_startup()
                    self._mutex_leg = str(s.get("mutex_leg", "") or "")
                    if self.sizing_principal <= 0:
                        eq = deepcoin_client.get_principal_wallet_balance()
                        if eq > 0:
                            self.sizing_principal = eq

            if self.base_qty <= 0 and os.path.exists(self.state_file):
                last_open = self._load_last_journal_entry(OPEN_JOURNAL)
                if last_open:
                    jq = int(last_open.get("qty", 0) or 0)
                    if jq > 0:
                        self.base_qty = jq
                        logger.info(f"📖 恢复 base_qty 取自开仓日志 {jq} 张")

            if self._scan_and_sweep_dust_on_startup(was_monitoring=saved_monitoring):
                return

            if self._recover_missed_flat_on_startup(was_monitoring=saved_monitoring):
                return

            pos = self._get_active_position()
            if pos and self._safe_qty(pos.get("size", 0)) != 0:
                self._recover_in_progress = True
                recover_ok = False
                recover_err = ""
                radar_active = False
                sl_ok = False
                if not self._lock.acquire(timeout=120.0):
                    logger.error("❌ 重启接管无法获取锁，跳过")
                    self._recover_in_progress = False
                    dingtalk.report_system_alert(
                        "重启接管失败",
                        "无法获取仓位锁（120s超时），请稍后重启或检查是否有僵死进程",
                    )
                    return
                try:
                    reconcile = self._reconcile_context_on_recover(pos)
                    reconcile_notes = reconcile["notes"]
                    side = "LONG" if pos.get("posSide") == "long" else "SHORT"

                    if self._live_aligns_with_credible_tv(side):
                        if reconcile.get("direction_mismatch"):
                            logger.warning(
                                f"🔄 [重启] 陈旧对账报方向背离，但实盘 {side} "
                                f"与最新TV信源同向 → 闪电接管"
                            )
                            self.last_tv_side = side
                            reconcile["direction_mismatch"] = False
                    elif self._enforce_tv_direction_or_flat(pos, source="VPS重启"):
                        self._recover_in_progress = False
                        return

                    if reconcile.get("manual_open") or self._safe_qty(self.watched_qty) <= 0:
                        logger.info(
                            f"🔄 [重启] 人工/孤儿同向仓 {side} "
                            f"{self._safe_qty(pos.get('size'))}张 → 闪电接管 TP123+止损+雷达"
                        )
                        self._perform_live_takeover(
                            pos,
                            source="VPS重启",
                            manual_open=bool(reconcile.get("manual_open")),
                            qty_change=reconcile.get("qty_manual_change"),
                        )
                        recover_ok = True
                        self._recover_in_progress = False
                        return

                    real_amt = self._safe_qty(pos["size"])
                    self.current_side = side

                    hydrate_notes = self._hydrate_tv_defense_context(pos)
                    reconcile_notes.extend(hydrate_notes)

                    align_notes = self._apply_recover_live_alignment(side, reconcile)
                    reconcile_notes.extend(align_notes)

                    saved_initial = self._resolve_open_initial_qty(real_amt, self.watched_entry)
                    if saved_initial <= 0:
                        saved_initial = real_amt
                    if self.base_qty <= 0:
                        self.base_qty = int(saved_initial or real_amt)
                    self.watched_qty = real_amt
                    self.initial_qty = saved_initial
                    self.watched_entry = float(pos["entry_price"])
                    if not getattr(self, "open_regime", None):
                        self.open_regime = self.regime
                    if not getattr(self, "open_atr", None):
                        self.open_atr = self.current_atr
                    qty_change = reconcile.get("qty_manual_change")

                    curr_px = deepcoin_client.get_current_price(self.symbol)
                    stack = self._ensure_full_defense_stack(
                        real_amt, self.watched_entry, curr_px or 0,
                        source="VPS重启", manual_fresh=bool(reconcile.get("manual_open")),
                    )
                    audit = stack.get("audit") or {}
                    result = stack.get("result") or {}
                    health = stack.get("health") or {}
                    sl_ok = stack.get("shield_ok", False)
                    matched = audit.get("matched_full", 0)
                    expected = audit.get("expected", 0)
                    radar_active = (
                        health.get("radar_active")
                        or health.get("should_radar")
                        or self._is_radar_active()
                    )
                    reconcile_notes.extend(stack.get("notes") or [])
                    _rebuilt = result.get("rebuilt", False)

                    logger.info(
                        f"🔄 [系统重启点火] 检测到实盘持仓 {self.current_side} {real_amt}张 @ "
                        f"{self.watched_entry:.2f} | 开单 {saved_initial}张 | "
                        f"已成交 TP{getattr(self, 'tp_levels_consumed', []) or '无'} | "
                        f"雷达={'已激活' if radar_active else '待命(TP1后)'} | "
                        f"TV对齐 {self.last_tv_side} | 对账 {len(reconcile_notes)} 项"
                    )

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

                    if reconcile.get("manual_open"):
                        self._call_dingtalk(
                            dingtalk.report_manual_position_change,
                            action_type="人工开仓 · 重启接管",
                            old_qty=0,
                            new_qty=real_amt,
                            new_entry_price=entry_px,
                            verify_note=(
                                f"TV方向 {self.last_tv_side} | TP123 {self.tv_tps} | "
                                f"tv_sl={getattr(self, 'tv_sl', 0):.2f}"
                            ),
                            tp_audit=audit,
                            verified=bool(verified),
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
                        f"开单 {saved_initial}张 | "
                        f"已成交 TP{getattr(self, 'tp_levels_consumed', []) or '无'} | "
                        f"TV方向 {self.last_tv_side} | "
                        f"tv_sl={float(getattr(self, 'tv_sl', 0) or 0):.2f} | "
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

                    health_txt = (
                        f" | 盈亏态 {health.get('pnl_label', '未知')} | "
                        f"硬止损 {health.get('shield_status', '待核实')} | "
                        f"策略 {health.get('defense_plan', 'TP123+硬止损')}"
                    )
                    verify_note = verify_note + health_txt

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
                        pnl_label=health.get("pnl_label", ""),
                        defense_plan=health.get("defense_plan", ""),
                        shield_status=health.get("shield_status", ""),
                        radar_progress=health.get("radar_progress", 0),
                        tv_aligned=health.get("tv_match", True),
                        qty_aligned=health.get("qty_match", True),
                        initial_qty=saved_initial,
                        tp_consumed_levels=getattr(self, "tp_levels_consumed", []) or [],
                    )
                    policy_actions = stack.get("notes") or []
                    logger.info(
                        f"  -> 🎉 实盘阵地接管完毕 | {health.get('pnl_label', '')} | "
                        f"防线 {' · '.join(policy_actions) if policy_actions else '已核实'}"
                    )
                    recover_ok = True
                except Exception as e:
                    import traceback
                    recover_err = f"{e}\n{traceback.format_exc()[-800:]}"
                    logger.error(f"❌ 重启接管步骤异常: {recover_err}")
                    self.monitoring = True
                    self._save_state()
                    dingtalk.report_system_alert(
                        "重启接管部分失败",
                        f"实盘仍有仓，已尽力启动哨兵接力 | {recover_err}",
                    )
                finally:
                    self._recover_in_progress = False
                    self._lock.release()

                if recover_ok and radar_active:
                    logger.info(
                        f"📡 [重启] 雷达哨兵已点火 | SL={self.current_sl:.2f} | "
                        f"止损={'已挂/已确认' if sl_ok else '待哨兵补挂'}"
                    )

                if not self._sentinel_active:
                    threading.Thread(
                        target=self._sentinel_loop, daemon=True, name="sentinel",
                    ).start()
                elif recover_err:
                    self._post_recover_radar_pulse = True
            else:
                deepcoin_client.cancel_all_open_orders(self.symbol)
                logger.info("🔄 [系统重启点火] 盘口干净无持仓，账本复位为空仓待命。")
                self.monitoring = False
                self.watched_qty = 0
                self.initial_qty = 0
                self.base_qty = 0
                self.add_count = 0
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


position_supervisor = None
SUPERVISORS = {}


def get_supervisor(symbol="ETH-USDT-SWAP"):
    from symbol_config import resolve_deepcoin_symbol
    meta = resolve_deepcoin_symbol(symbol)
    sym = meta["symbol"]
    if sym not in SUPERVISORS:
        SUPERVISORS[sym] = PositionSupervisor(sym)
    return SUPERVISORS[sym]


def get_supervisor_for_payload(data):
    from symbol_config import extract_symbol_from_payload, resolve_deepcoin_symbol, active_deepcoin_symbols
    raw = extract_symbol_from_payload(data) if isinstance(data, dict) else ""
    meta = resolve_deepcoin_symbol(raw or "ETH-USDT-SWAP")
    sym = meta["symbol"]
    allowed = set(active_deepcoin_symbols())
    if sym not in allowed:
        return None, sym
    return get_supervisor(sym), sym


def bootstrap_supervisors():
    from symbol_config import active_deepcoin_symbols
    global position_supervisor
    for sym in active_deepcoin_symbols():
        get_supervisor(sym)
    position_supervisor = SUPERVISORS.get("ETH-USDT-SWAP") or next(iter(SUPERVISORS.values()), None)
    if __name__ != "__main__":
        for sym, sup in SUPERVISORS.items():
            try:
                logger.info(f"🔄 启动恢复 [{sym}] …")
                sup.recover_state_on_startup()
            except Exception as e:
                logger.error(f"启动恢复失败 [{sym}]: {e}")
    return SUPERVISORS


bootstrap_supervisors()
