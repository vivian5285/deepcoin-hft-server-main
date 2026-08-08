#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# position_supervisor_deepcoin.py ? ??? VPS ???????????/20x ???
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

DEEPCOIN_SUPERVISOR_VERSION = "v16.29-binance-parity"

# ???????? CLOSE ??????? 1?2s ????
LATE_CLOSE_SUPPRESS_SEC = 5.0
# v16.17???????? API ?????????
SENTINEL_POLL_NORMAL = 20.0
SENTINEL_POLL_ARMING = 15.0
SENTINEL_POLL_RADAR = 15.0
IDLE_PATROL_INTERVAL_SEC = 180
IDLE_PATROL_BACKOFF_SEC = 900
IDLE_TAKEOVER_COOLDOWN_SEC = 30
DUST_ORPHAN_CONTRACTS = 1
TP_COMPLETE_RESIDUAL_RATIO = 0.12
OPEN_OVERSIZE_RATIO = 1.10
SIGNAL_DEDUP_SEC = 45
# v16.17???????????????? API ??
DEFENSE_ALIGN_COOLDOWN_SEC = 30
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
QTY_DRIFT_TOLERANCE_PCT = 0.015  # ?? ?1.5%??????
QTY_ALIGN_MIN_PCT = 0.10         # ?? ?10% ????????/????
SHIELD_HARD_STOP_PCT = 0.10  # ??????????????????? exclusively TV tv_sl
SHIELD_TIER_PCTS = (SHIELD_HARD_STOP_PCT,)
SHIELD_TIER_RATIOS = (1.0,)
SHIELD_STOP_TOLERANCE = 2.0
SHIELD_MAINTAIN_COOLDOWN_SEC = 60
SHIELD_FAIL_BACKOFF_BASE_SEC = 45
SHIELD_FAIL_BACKOFF_MAX_SEC = 300
SHIELD_QTY_TOLERANCE_PCT = 0.04
SHIELD_MAX_TIER_ORDERS = 1
RADAR_DINGTALK_COOLDOWN_SEC = 120
# TV v6.9.86 ????????? trailTight / TP2?TP3 trailing??? TP1 ???????
TV_TRAIL_TIGHT = 0.62
TV_TRAIL_TP2_ATR = TV_TRAIL_TIGHT * 0.32   # ?0.20 ATR ? TP1 ???
TV_TRAIL_TP3_ATR = TV_TRAIL_TIGHT * 0.48   # ?0.30 ATR ? TP2 ???
TV_BOOT_SL_ATR = 0.40                      # strongBull ???? entry ? 0.4 ATR
RADAR_FEE_BUFFER_PCT = 0.0015
RADAR_STOP_MIN_GAP_USD = 2.5
RADAR_STOP_MIN_GAP_PCT = 0.0012
MIN_TP_LEG_QTY = 1
# ?? TV ?????? ATR ?? ? ?????? ???????? ? ????????? TP123
SAME_DIR_MIN_SPREAD_PCT = 0.15
SAME_DIR_DEDUP_SEC = 300
OPEN_SAME_DIR_COOLDOWN_SEC = 180  # ???? OPEN??????????????
ATR_SIMILAR_RATIO = 0.03  # ?? ATR ? TV ATR ?? ?3% ????
TV_JOURNAL = "logs/deepcoin_tv_journal.jsonl"
OPEN_JOURNAL = "logs/deepcoin_open_journal.jsonl"

# binance parity (52d26bd): symbol-aware ATR fallback. A flat 30 (correct
# order-of-magnitude for XAU) badly overstates BNB (~4) and understates
# nothing meaningfully for ETH (~12), so any code path that falls back to a
# hardcoded ATR when open_atr/current_atr are both unavailable must use this
# instead of a flat constant, or it will reconstruct wildly wrong TP/SL
# distances for whichever symbol isn't XAU.
_SYMBOL_ATR_FALLBACK = {"ETH": 12.0, "XAU": 20.0, "BNB": 4.0}


def symbol_aware_atr_fallback(symbol):
    sym = str(symbol or "").upper()
    for tag, val in _SYMBOL_ATR_FALLBACK.items():
        if tag in sym:
            return val
    return 30.0
# ============================================================
# ???????v16.18 TP???????
# ============================================================
# TP???????????? vs ???????????????
RECOVER_TP_VERIFY_ATTEMPTS = 3       # ??? TP ????????
RECOVER_TP_VERIFY_DELAY_SEC = 1.2    # ??????
RECOVER_TP_PLACE_MAX_ATTEMPTS = 2     # ?? TP ???????????50?????
RECOVER_TP_PLACE_GUARD_MAX = 5        # ?? TP ????????????
RECOVER_TP_FILL_CONFIRM_ROUNDS = 2   # ??????
# ???????live_qty ?????????????
RECOVER_TP_CONSERVATIVE_THRESHOLD = 0.05  # 5% ????


class PositionSupervisor(PipelineBridgeMixin, RadarReentryMixin):
    def __init__(self, symbol="ETH-USDT-SWAP"):
        from symbol_config import resolve_deepcoin_symbol
        from breath_profiles import get_breath_profile
        meta = resolve_deepcoin_symbol(symbol)
        self.symbol = meta["symbol"]
        self.unit_label = meta.get("unit") or "?"
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
                logger.info(f"?? {self.symbol} face_value={ct} (instruments)")
        except Exception as e:
            logger.warning(f"face_value instruments ???? meta {self.face_value}: {e}")

        # v16.16 ??Deepcoin ?? set-position-mode API?? posSide(long/short)
        # ????????????????????? Deepcoin ???
        # ?????????? set_leverage(5x) ???
        self.binance_mark = meta.get("binance_mark") or "ETHUSDT"
        self.monitoring = False
        self._lock = threading.Lock()

        # ??????????%???????
        self.regime_settings = {
            1: {"margin": 0.05, "ratios": get_regime_tp_ratios(1), "activation": 0.92, "trail_offset": TV_TRAIL_TP2_ATR},
            2: {"margin": 0.10, "ratios": get_regime_tp_ratios(2), "activation": 0.92, "trail_offset": TV_TRAIL_TP2_ATR},
            3: {"margin": 0.15, "ratios": get_regime_tp_ratios(3), "activation": 0.95, "trail_offset": TV_TRAIL_TP3_ATR},
            4: {"margin": 0.18, "ratios": get_regime_tp_ratios(4), "activation": 0.95, "trail_offset": TV_TRAIL_TP3_ATR},
        }
        self.leverage = EXCHANGE_LEVERAGE
        self.tv_sizing_leverage = EXCHANGE_LEVERAGE
        # face_value ???? meta ???????? 0.1

        self.regime = 3
        self.current_atr = symbol_aware_atr_fallback(self.symbol)
        self.best_price = 0.0
        self._adverse_worst_px = 0.0
        self._adverse_worst_px_ts = 0.0
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
        self._recover_confirmed_levels = {}   # v16.21??????? TP ?? {level: timestamp}
        self._post_recover_radar_pulse = False
        self._open_in_progress = False
        self._open_tp_unconfirmed = False
        self._last_signal_fp = None
        self._last_signal_fp_ts = 0.0
        self._defense_align_in_progress = False
        self._last_defense_align_ok_ts = 0.0
        self._last_rebuild_attempt_ts = 0.0  # ??????_rebuild_defenses???
        self._last_nuclear_realign_ts = 0.0  # ??????_nuclear_realign_tp???
        self._guardian_bad_streak = 0
        self._sentinel_grace_until = 0.0
        self._last_regime_cap_ts = 0.0
        self.shield_active = False
        self.shield_tiers_consumed = []
        self.tp_levels_consumed = []
        # v16.15?????????? set-position-sltp ??????????
        # ?? audit ?? + ??????????/??????
        self._shield_sltp_ord_id = ""
        self._shield_sltp_set_at = 0.0
        self._shield_cancelled_ids = set()
        # ?? v1.0 ?5.0????????
        self._early_be_checkpoint_done = False
        # ?? v1.0 ?3?????????
        self.frozen_hard_sl_px = 0.0
        # ?? v1.0 ?8-9???????? + ?????
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
        self._last_tier_label = ""  # P0?tier_label ? webhook ??

        # ?????????v1.0 ?????
        self.adx_tier = 1
        self.radar_tier = 1
        self.radar_activation_frac = 0.78
        self.radar_pending_arm = True
        self._last_flat_qty_zero_ts = 0.0  # ???????????????????
        self.reentry_active = False
        self.reentry_attempt = 0
        self.reentry_limit_order_id = None
        self.reentry_limit_px = 0.0
        self.reentry_limit_deadline_ts = 0.0
        self.reentry_window_deadline_ts = 0.0
        self.reentry_order_tag = None
        self.tp_levels_consumed = []  # ??????

        # v16.18?TP??????
        self._tp_place_guard_count = 0           # ??? TP ???????????50???
        self._tp_place_guard_session_ts = 0.0  # ??????????
        self._tp_recover_verified_levels = {}   # ????????? TP ?? {level: timestamp}

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
                logger.info(f"?? ?????? ? {self.state_file}")
            except Exception as e:
                logger.warning(f"???????: {e}")
        try:
            self._pipeline_boot(exchange="deepcoin")
        except Exception as e:
            logger.warning(f"[{self.symbol}] pipeline boot: {e}")
        logger.info(
            f"?? ?? VPS [{DEEPCOIN_SUPERVISOR_VERSION}/{CLIENT_VERSION}] "
            f"{self.symbol} ????????? ? {self.leverage}x ? pipeline"
        )
        self._start_signal_worker()
        self._start_idle_flat_patrol()

    def _init_reentry_runtime(self):
        RadarReentryMixin._init_reentry_runtime(self)

    def _start_idle_flat_patrol(self):
        """???????????????? / ???? / ???? / ???? / ????"""
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
                    logger.error(f"idle patrol failed: {e}", exc_info=True)
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
        if pos == "QUERY_FAILED":
            logger.error("?? _live_position_qty: ???????? ? ??????(???0)")
            return float("inf")
        if not pos:
            return 0
        return self._safe_qty(pos.get("size"))

    def _confirm_position_flat(self, retries=None, delay=None):
        """REST ??/??????????????????????"""
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
                f"?? ???? tp_levels_consumed={consumed} "
                f"(?? {initial_qty}??? {live_qty}???????)"
            )
            self.tp_levels_consumed = []
            self._save_state()
            return True
        if 1 in consumed and self.tv_tps and self.tv_tps[0] > 0:
            if 1 not in inferred and not self._has_tp_limit_at_price(self.tv_tps[0]):
                logger.warning(
                    f"?? TP1 ?????????/? TP1 ?? ? ?? {consumed}"
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

    def _resume_live_monitoring(self, pos, source="????"):
        """???????? monitoring=False ? ?????????"""
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
            f"?? [{source}] ?????? {side} {qty}? "
            f"| ??={'???' if self._is_radar_active() else '??'}"
        )

    def _perform_live_takeover(self, pos, source="??", manual_open=False, qty_change=None):
        """
        ????? VPS ??? / ???? ? ?? TP123+???????????
        """
        # v16.22 ????? pos=None ?????_recover_missed_flat_on_startup ????????????
        # v16.23 ????? None ?????????? pos=None ????
        if pos is None:
            logger.warning(f"?? _perform_live_takeover ???pos=None | source={source}")
            return False
        if not pos or self._safe_qty(pos.get("size", 0)) <= 0:
            logger.warning(f"?? _perform_live_takeover ???????? | source={source}")
            return False
        real_amt = self._safe_qty(pos.get("size"))
        side = "LONG" if pos.get("posSide") == "long" else "SHORT"
        tv_side = self._resolve_tv_authoritative_side()
        if tv_side and side != tv_side:
            return False

        self.current_side = side
        if not self.last_tv_side:
            self.last_tv_side = tv_side or side

        # v16.21????????????????
        self._clear_recover_confirmed_levels()

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
        log_source = source.split("?")[0].replace(" ", "")
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
        extra_txt = (" | " + " ? ".join(extra_notes)) if extra_notes else ""
        verify_note = (
            f"[{source}] ?? {real_amt}? @ {entry_px:.2f} | "
            f"?? {saved_initial}? | TV {self.last_tv_side} | "
            f"?? {matched}/{expected} ? | "
            f"tv_sl={float(getattr(self, 'tv_sl', 0) or 0):.2f} | "
            f"??={'???' if radar_active else '??(TP1?)'} | "
            f"{self._format_audit_summary(audit)}{extra_txt}{reconcile_txt}"
        )
        if not verified:
            verify_note += " | REST ?????"

        if manual_open:
            self._call_telegram_notify(
                telegram_notify.report_manual_position_change,
                action_type=f"???? ? {source}",
                old_qty=0,
                new_qty=real_amt,
                new_entry_price=entry_px,
                verify_note=verify_note,
                tp_audit=audit,
                verified=bool(verified),
            )
        elif qty_change:
            old_q, new_q, action_msg = qty_change
            self._call_telegram_notify(
                telegram_notify.report_manual_position_change,
                action_type=action_msg,
                old_qty=old_q,
                new_qty=new_q,
                new_entry_price=entry_px,
                verify_note=f"{source} | {verify_note}",
                tp_audit=audit,
                verified=bool(verified),
            )
        else:
            self._call_telegram_notify(
                telegram_notify.report_recover_takeover,
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
            logger.warning(
                f"[?????] {source} ? ??????? | "
                f"{side} {real_amt}? @ {entry_px:.2f} | ? {matched}/{expected} ?"
            )
        else:
            self._mark_defense_align_ok()

        logger.info(f"? [{source}] ?????? {side} {real_amt}? @ {entry_px:.2f}")
        return True

    def _run_idle_live_reconcile(self):
        """VPS ??/???????????????????"""
        if self.monitoring or getattr(self, "_recover_in_progress", False):
            return
        if getattr(self, "_open_in_progress", False):
            return

        pos = self._get_active_position()
        if pos == "QUERY_FAILED":
            logger.warning("?? [????] ??????????????? ? ????")
            return
        live_qty = self._safe_qty(pos.get("size")) if pos else 0

        if live_qty <= 0:
            if self._book_thinks_active():
                if not self._confirm_position_flat():
                    logger.warning(
                        "?? [????] ??????????? ? ?????"
                    )
                    return
                curr_px = deepcoin_client.get_current_price(self.symbol)
                logger.warning("?? [????] ????????? ? ??????")
                self._handle_manual_flat_detected(
                    "???? (???? / ???? / ????)",
                    curr_px=curr_px,
                )
            return

        if self._enforce_tv_direction_or_flat(pos, source="????"):
            return

        # v16.23 ???????? pos ??????? pos.get() ??
        if pos is None:
            logger.warning("?? [????] pos=None?????")
            return

        if self._is_dust_qty(live_qty) or self._should_finalize_tp_victory(live_qty):
            if not self.current_side:
                self.current_side = "LONG" if pos.get("posSide") == "long" else "SHORT"
            logger.warning(
                f"?? [????] ???? {self.current_side} {live_qty}? ? ??"
            )
            self._sweep_dust_and_finalize("??????????????")
            return

        # v16.24 ???sweep ? pos ???? None?????
        if pos is None:
            logger.warning("?? [????] sweep? pos=None???????")
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
                f"?? [????] VPS????????? {live_side} {live_qty}? "
                f"(TV={tv_side}) ? ????+?TP123"
            )
            self._perform_live_takeover(pos, source="????", manual_open=True)
            # v16.25 ???takeover ? pos ?????
            pos_after = self._get_active_position()
            if pos_after == "QUERY_FAILED":
                pos_after = None
            if pos_after is None or self._safe_qty(pos_after.get("size", 0)) <= 0:
                logger.warning("?? [????] takeover???????????")
                return
            return

        if self._is_material_qty_change(watched, live_qty):
            logger.warning(
                f"?? [????] ???? {watched} ? {live_qty}? ? ??TP123+??"
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
                f"?? [????] ???? ({audit.get('matched_full', 0)}/"
                f"{audit.get('expected', 0)} ?) ? ??TP123+??"
            )
            self._perform_live_takeover(pos, source="?????????")
            return

        if not self.monitoring:
            self._resume_live_monitoring(pos, source="????")

    @staticmethod
    def _call_telegram_notify(fn, **kwargs):
        """?? TG Bot ?????"""
        try:
            fn(**kwargs)
        except Exception as e:
            logger.warning(f"[TG????] {getattr(fn, '__name__', fn)} | {e}")

    @staticmethod
    def _call_telegram(fn, **kwargs):
        """?? TG Bot ?????"""
        try:
            fn(**kwargs)
        except Exception as e:
            logger.warning(f"[TG????] {getattr(fn, '__name__', fn)} | {e}")

    def _start_signal_worker(self):
        if self._signal_worker_started:
            return
        self._signal_worker_started = True
        threading.Thread(target=self._signal_worker_loop, daemon=True, name="tv-signal-worker").start()

    def _signal_worker_loop(self):
        """???+??????????????????????"""
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
                            f"?? [{self.symbol}] ???? bar={bi} seq={sq} action={act}"
                        )
                    self._process_signal(payload)
                except Exception as e:
                    logger.error(f"? ??????: {e}", exc_info=True)
                finally:
                    try:
                        self._signal_queue.task_done()
                    except ValueError:
                        pass

    def _annotate_close_open_chain(self, batch):
        """?K????+? ? ????????late close ??????"""
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
            exec_chain = " ? ".join(
                f"seq{sq if sq is not None else '?'}:{a}" for sq, a, _ in items
            )
            tv_by_seq = " ? ".join(
                f"seq{sq if sq is not None else '?'}:{a}"
                for sq, a, _ in sorted(items, key=lambda x: (x[0] is None, x[0] or 0))
            )
            logger.info(
                f"?? [{self.symbol}] ???????? | ??? {exec_chain} | TV {tv_by_seq}"
            )
            if bi is not None:
                self._close_open_chain_active = True
                self._last_close_bar_index = int(bi)
            try:
                logger.info(
                    f"[?????] ?????????? [{self.symbol}] | "
                    f"?? {exec_chain} | TV?seq {tv_by_seq} | "
                    f"????????????seq?"
                )
            except Exception as e:
                logger.debug(f"?????????: {e}")

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
                f"?? TV??????: {action} | {SIGNAL_DEDUP_SEC}s ?????"
            )
            return
        if self._open_in_progress and action in ("LONG", "SHORT"):
            logger.warning(f"?? ?????????????? {action}")
            return
        self._last_signal_fp = fp
        self._last_signal_fp_ts = now
        depth = self._signal_queue.qsize()
        self._signal_queue.put(payload)
        logger.info(f"?? TV????: {action} | ???? {depth + 1}")

    def signal_queue_depth(self):
        return self._signal_queue.qsize()

    def _append_journal(self, path, record):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        record = dict(record)
        record["ts"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        record["symbol"] = self.symbol  # v16.26: journal ? symbol ??
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    def _load_last_journal_entry(self, path, symbol=None):
        if not os.path.exists(path):
            return None
        last = None
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if symbol and entry.get("symbol") != symbol:
                    continue
                last = entry
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
            f"?? TV??: {raw_action} R{self.regime} @ {self.tv_price:.2f} "
            f"TP={self.tv_tps}"
            + sizing_note
            + (f" | pnl={payload.get('pnl_pct')}%" if payload.get("pnl_pct") is not None else "")
        )
        self._call_telegram_notify(
            telegram_notify.report_tv_signal_received,
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
        # P1 ????? TG ????????
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
            "atr": getattr(self, "current_atr", 0) or 0,
            "tv_tps": self.tv_tps,
            "tv_sl": float(getattr(self, "tv_sl", 0) or 0),
            "tv_price": self.tv_price,
            "last_tv_side": self.last_tv_side,
        })

    def _load_active_tv_direction_from_journal(self):
        """? TV ??????????? CLOSE????????? LONG/SHORT?? symbol ???"""
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
            if entry.get("symbol") != self.symbol:  # v16.26: ? symbol ??
                continue
            action = str(entry.get("action") or "").upper()
            if action.startswith("CLOSE"):
                continue
            if action in ("LONG", "SHORT"):
                return action
            side = (entry.get("side") or "").upper()
            if side in ("LONG", "SHORT"):
                return side
        return None

    def _collect_credible_tv_directions(self):
        """?? TV ?????state ???? > ???? > ????"""
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
        last_tv = self._load_last_journal_entry(TV_JOURNAL, self.symbol)
        if last_tv:
            add(last_tv.get("action"))
            add(last_tv.get("side"))
        add(self._load_active_tv_direction_from_journal())
        add(getattr(self, "last_tv_side", None))
        return sides

    def _live_aligns_with_credible_tv(self, live_side):
        """??????????? TV ??????? ? ????????"""
        return live_side in self._collect_credible_tv_directions()

    def _strict_tv_opposite_side(self, live_side):
        """????? TV ????????????????????????"""
        for src in (self.last_tv_signal, self._load_last_journal_entry(TV_JOURNAL, self.symbol)):
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
        """TV ??????? LONG/SHORT?CLOSE ???????????? symbol ???"""
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
                if entry.get("symbol") != self.symbol:  # v16.26: ? symbol ??
                    continue
                action = (entry.get("action") or "").upper()
                if action in ("LONG", "SHORT"):
                    last_open = entry
        return last_open

    def _resolve_tv_authoritative_side(self):
        """TV ???????????????????????????"""
        if self.last_tv_signal:
            action = (self.last_tv_signal.get("action") or "").upper()
            if action in ("LONG", "SHORT"):
                return action
            side = (self.last_tv_signal.get("side") or "").upper()
            if side in ("LONG", "SHORT"):
                return side
        last_tv = self._load_last_journal_entry(TV_JOURNAL, self.symbol)
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
        """??? TV ???? ? ???????????? ? ????"""
        # v16.23 ???pos=None ???????????????
        if pos is None:
            return False
        if not pos or self._safe_qty(pos.get("size")) <= 0:
            return False
        live_side = self._live_position_side(pos)
        if self._live_aligns_with_credible_tv(live_side):
            logger.info(
                f"? [{source}] ?? {live_side} ??? TV ???? ? ?????????"
            )
            return False
        tv_opposite = self._strict_tv_opposite_side(live_side)
        if not tv_opposite or not live_side:
            return False
        reason = (
            f"?????? vs TV???({live_side}) ? ??TV({tv_opposite}) [{source}]"
        )
        logger.error(f"?? {reason} ? ???????? TV")
        verify_note = (
            f"???: {source} | ??TV {tv_opposite} | ???? {live_side} | "
            "????????????"
        )
        self._close_all(
            reason,
            force_align=(live_side, tv_opposite),
            force_verify_note=verify_note,
        )
        return True

    def _journal_tp_prices(self, entry):
        """??????? TP123??? tv_tps ??? tv_tp1/2/3 ???"""
        if not entry:
            return [0.0, 0.0, 0.0]
        if entry.get("tv_tps"):
            return self._sanitize_tp_prices(entry.get("tv_tps", []))
        return self._sanitize_tp_prices([
            entry.get("tv_tp1"), entry.get("tv_tp2"), entry.get("tv_tp3"),
        ])

    def _hydrate_tv_defense_context(self, pos):
        """
        ???? / ?????? TV ???? tp/sl/regime/atr??????????????
        v16.27: ?? pos=None ? current_side/watched_entry ?????????
        """
        notes = []
        side = self.current_side
        if not side and pos:
            side = "LONG" if pos.get("posSide") == "long" else "SHORT"
        entry = float((pos.get("entry_price") if pos else None) or self.watched_entry or 0)
        if not side:
            return notes

        self.current_side = side
        if not self.last_tv_side:
            self.last_tv_side = side

        sources = [
            self.last_tv_signal,
            self._load_last_journal_entry(TV_JOURNAL, self.symbol),
            self._load_last_tv_open_signal(),
            self._load_last_journal_entry(OPEN_JOURNAL, self.symbol),
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
                    f"?? ??: ?? TP{self.tv_tps} ? {side}@{entry:.2f} ???? ? ??"
                )
            self.tv_tps = [0.0, 0.0, 0.0]
            tp_ok = 0
        if tp_ok < 3:
            for src in sources:
                if not src:
                    continue
                src_side = (src.get("action") or src.get("side") or "").upper()
                if src_side in ("LONG", "SHORT") and side and src_side != side:
                    continue
                tps = self._journal_tp_prices(src)
                if (
                    sum(1 for t in tps if t > 0) >= 3
                    and self._tp_prices_valid_for_side(side, entry, tps)
                ):
                    self.tv_tps = tps
                    notes.append(f"??TP123 {tps}")
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
                notes.append(f"ATR????TP {tps}")

        if float(getattr(self, "tv_sl", 0) or 0) <= 0:
            for src in sources:
                if not src:
                    continue
                sl = float(src.get("tv_sl", 0) or 0)
                if sl > 0:
                    self.tv_sl = sl
                    notes.append(f"??tv_sl={sl:.2f}")
                    break

        if float(getattr(self, "tv_sl", 0) or 0) <= 0 and entry > 0 and self.current_atr > 0:
            sl_m = {1: 0.9, 2: 1.05, 3: 1.10, 4: 1.25}.get(int(self.regime or 3), 1.10)
            if side == "LONG":
                self.tv_sl = round(entry - self.current_atr * sl_m, 2)
            else:
                self.tv_sl = round(entry + self.current_atr * sl_m, 2)
            notes.append(f"ATR??tv_sl={self.tv_sl:.2f}")

        self.monitoring = True
        self._save_state()
        for n in notes:
            logger.info(f"?? ???????: {n}")
        return notes

    def _reset_fresh_takeover_state(self):
        # Bug fix (2026-08-02): DO NOT clear tv_tps here.
        # _hydrate_tv_defense_context needs the original values
        # to decide whether re-hydration is needed.
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
            self._load_last_journal_entry(TV_JOURNAL, self.symbol),
            self._load_last_tv_open_signal(),
            self._load_last_journal_entry(OPEN_JOURNAL, self.symbol),
        ]
        for src in sources:
            if not src:
                continue
            src_side = (src.get("action") or src.get("side") or "").upper()
            if src_side in ("LONG", "SHORT") and side and src_side != side:
                continue
            tps = self._journal_tp_prices(src)
            if sum(1 for t in tps if t > 0) >= 3 and self._tp_prices_valid_for_side(side, entry, tps):
                return tps, f"TV??TP {tps}"
        return None, ""

    def _ensure_tp123_prices_from_tv(self, entry):
        """??? entry + open_atr/regime ?? TP123 ????????????"""
        side = self.current_side
        entry = float(entry or self.watched_entry or 0)
        if self._tp_prices_valid_for_side(side, entry):
            return True

        reloaded, note = self._reload_tv_tp_prices_from_sources(side, entry)
        if reloaded:
            self.tv_tps = reloaded
            logger.info(f"?? ???? TP123 @ entry={entry:.2f} ? {self.tv_tps} ({note})")
            self._save_state()
            return True

        if sum(1 for t in (self.tv_tps or []) if t > 0) >= 3:
            logger.warning(
                f"?? ?? TP ??? {side} @ {entry:.2f} ???? ? ????"
            )
            self.tv_tps = [0.0, 0.0, 0.0]

        atr = float(getattr(self, "open_atr", None) or self.current_atr or symbol_aware_atr_fallback(self.symbol))
        regime = int(getattr(self, "open_regime", None) or self.regime or 3)
        if not side or entry <= 0:
            return False
        payload = enrich_entry_tp_prices(side, entry, atr, regime, {})
        self.tv_tps = self._sanitize_tp_prices([
            payload.get("tv_tp1"), payload.get("tv_tp2"), payload.get("tv_tp3"),
        ])
        ok = self._tp_prices_valid_for_side(side, entry)
        if ok:
            logger.info(f"?? ???? ATR ?? TP123 @ entry={entry:.2f} ? {self.tv_tps}")
        return ok

    def _resolve_defense_stop_for_audit(self, radar_sl=None):
        """???????TP1 ?? tv_sl?TP1 ???+tv_sl ??"""
        if radar_sl and float(radar_sl) > 0:
            return float(radar_sl)
        tracked = self._radar_sl_to_pass()
        if tracked and self._tp1_filled_verified():
            return tracked
        return self._shield_stop_price()

    def _normalize_tp_qty_map(self, qty_map, live_qty):
        """
        ?? v1.0 ?6.2 ????? + ?9.4 ?????
        ??????(MIN_TP_LEG_QTY)??TP3????????TP3??????70%????
        ??????????TP3????????????
        """
        if not qty_map:
            return qty_map
        live_qty = int(live_qty or 0)
        levels = sorted(qty_map.keys())
        if len(levels) <= 1:
            return qty_map

        # ??????? TP3?key=3???????????
        last_level = levels[-1]

        carry = 0
        out = dict(qty_map)
        for lvl in levels[:-1]:  # TP1, TP2??? TP3?
            q = int(out.get(lvl, 0) or 0)
            if 0 < q < MIN_TP_LEG_QTY:
                carry += q
                out[lvl] = 0  # ??????????

        # ???? TP3?????? 70%?
        if carry > 0:
            out[last_level] = int(out.get(last_level, 0) or 0) + carry

        # ??????????
        total = sum(int(out.get(l, 0) or 0) for l in levels)
        if total > live_qty:
            out[last_level] = max(int(out.get(last_level, 0) or 0) - (total - live_qty), 0)

        return out

    def _ensure_full_defense_stack(self, live_qty, entry, curr_px, source="??", manual_fresh=False,
                                   recover_mode=False):
        """
        ?????TP123 ???? + TV tv_sl ????TP1 ??????????????
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
            notes.append("TP123????")
        if float(getattr(self, "tv_sl", 0) or 0) <= 0:
            pos_ctx = {"side": self.current_side, "size": live_qty, "entry_price": entry}
            self._hydrate_tv_defense_context(pos_ctx)
        if float(getattr(self, "tv_sl", 0) or 0) <= 0 and entry > 0:
            atr = float(getattr(self, "open_atr", None) or self.current_atr or symbol_aware_atr_fallback(self.symbol))
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
            logger.warning(f"????????: {e}")

        tp_repair = {"repaired": False}
        try:
            tp_repair = self._repair_partial_tp_on_recover(
                live_qty, entry, trusted_initial, curr_px,
            )
            if tp_repair.get("repaired"):
                notes.extend(tp_repair.get("actions") or [])
        except Exception as e:
            logger.error(f"??TP????: {e}")
            notes.append(f"TP????:{e}")

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
                f"?? [{source}] TP/???? ({audit.get('matched_full', 0)}/"
                f"{audit.get('expected', 0)}) ? ???? TP123+tv_sl"
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
                f"?? [{source}] ????(????????) ??{progress:.0%} | "
                f"??={gate:.2f} | tv_sl={float(getattr(self, 'tv_sl', 0) or 0):.2f} | "
                f"TP {audit.get('matched_full', 0)}/{audit.get('expected', 0)}"
            )

        if self._tp_audit_ok(audit):
            self._mark_defense_align_ok()
        else:
            exp = audit.get("expected", 0)
            if exp and audit.get("matched_full", 0) < exp:
                logger.warning(
                    f"[?????] {source} ? ??????? | "
                    f"{self.current_side} {live_qty}? @ {entry:.2f} | "
                    f"? {audit.get('matched_full', 0)}/{exp} ? | "
                    f"tv_sl={float(getattr(self, 'tv_sl', 0) or 0):.2f} | ????"
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
        """???????????????????????????"""
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
        """????????? vs ?? vs ?? TV / ????"""
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

        last_tv = self._load_last_journal_entry(TV_JOURNAL, self.symbol)
        last_open = self._load_last_journal_entry(OPEN_JOURNAL, self.symbol)
        last_open_tv = self._load_last_tv_open_signal()

        if last_tv:
            self.last_tv_signal = last_tv
            tv_action = (last_tv.get("action") or "").upper()
            tv_tps_saved = self._sanitize_tp_prices(last_tv.get("tv_tps", []))
            tv_tp_count = sum(1 for t in tv_tps_saved if t > 0)

            tv_price_j = float(last_tv.get("price", 0) or 0)
            # v16.27: ??? journal ???? symbol ???????? ATR/regime?
            # ?? journal price ????????????????20%??
            price_contaminated = False
            if tv_price_j > 0 and entry > 0:
                if side == "SHORT" and tv_price_j < entry * 0.8:
                    price_contaminated = True
                    notes.append(f"?? journal??{tv_price_j:.2f}<??80%={entry*0.8:.2f}???ATR/regime??")
                elif side == "LONG" and tv_price_j > entry * 1.2:
                    price_contaminated = True
                    notes.append(f"?? journal??{tv_price_j:.2f}>??120%={entry*1.2:.2f}???ATR/regime??")
            # ??????BNB SHORT ? journal ? ETH LONG????? ATR/regime ??
            journal_side = (last_tv.get("action") or last_tv.get("side") or "").upper()
            if journal_side in ("LONG", "SHORT") and journal_side != side:
                price_contaminated = True
                notes.append(f"?? journal??{journal_side}???{side}???ATR/regime??")

            if not price_contaminated:
                if last_tv.get("regime"):
                    self.regime = int(last_tv["regime"])
                if last_tv.get("atr"):
                    self.current_atr = float(last_tv["atr"])
                if self.tv_price <= 0 and tv_price_j > 0:
                    self.tv_price = tv_price_j

            if tv_action in ("LONG", "SHORT"):
                self.last_tv_side = tv_action
                if tv_tp_count > 0 and not price_contaminated:
                    self.tv_tps = tv_tps_saved
                    notes.append(f"TV??????? {self.tv_tps}")
                if side != tv_action:
                    reconcile["direction_mismatch"] = True
                    notes.append(
                        f"????: ??{side} vs TV??{tv_action} ({last_tv.get('ts', '')})"
                    )
            elif tv_action.startswith("CLOSE"):
                reconcile["tv_close"] = True
                notes.append(
                    f"TV???{tv_action} ({last_tv.get('ts', '')})?????? ? ???"
                )
                if last_open_tv:
                    self.last_tv_side = (last_open_tv.get("action") or "").upper()
                    open_tps = self._sanitize_tp_prices(last_open_tv.get("tv_tps", []))
                    if sum(1 for t in open_tps if t > 0) > 0:
                        self.tv_tps = open_tps

        # v16.27: ? journal ?????? ATR?????????????
        # ??? last_open_tv??????? TV ????? ATR/regime
        if float(self.current_atr or 0) <= 0 and last_open_tv:
            open_atr = float(last_open_tv.get("atr", 0) or 0)
            if open_atr > 0:
                self.current_atr = open_atr
                notes.append(f"?? ???????ATR={open_atr:.2f}")
            if last_open_tv.get("regime") and not self.regime:
                self.regime = int(last_open_tv["regime"])

        # v16.22???????? ATR fallback ?????? TV TPS ? tv_sl
        # ???TV ? SHORT?? state ???? LONG ? TP ?? ? TP ???? ? ??? ? TP=0
        if side != self.last_tv_side and not reconcile.get("tv_close"):
            notes.append(
                f"????: ??{side} vs TV??{self.last_tv_side} "
                f"? ATR?? TP123 + tv_sl"
            )
            atr = float(getattr(self, "open_atr", None) or self.current_atr or symbol_aware_atr_fallback(self.symbol))
            regime = int(getattr(self, "open_regime", None) or self.regime or 3)
            entry_px = float(pos.get("entry_price", 0) or 0)
            if atr > 0 and entry_px > 0:
                from webhook_parser import enrich_entry_tp_prices
                payload = enrich_entry_tp_prices(side, entry_px, atr, regime, {})
                new_tps = [
                    self._safe_float(payload.get("tv_tp1"), 0),
                    self._safe_float(payload.get("tv_tp2"), 0),
                    self._safe_float(payload.get("tv_tp3"), 0),
                ]
                self.tv_tps = self._sanitize_tp_prices(new_tps)
                self.last_tv_side = side
                # ???? tv_sl ??
                if side == "LONG":
                    self.tv_sl = round(entry_px - atr * 1.10, 2)
                else:
                    self.tv_sl = round(entry_px + atr * 1.10, 2)
                notes.append(f"ATR?? TP123={self.tv_tps} tv_sl={self.tv_sl:.2f}")
                logger.warning(
                    f"??? ???? ATR ??: {side} TP={self.tv_tps} SL={self.tv_sl:.2f} "
                    f"| ATR={atr:.2f} R{regime} @ {entry_px:.2f}"
                )
                self._save_state()

        if not self.last_tv_side and last_open_tv:
            self.last_tv_side = (last_open_tv.get("action") or "").upper()

        if last_open:
            open_side = last_open.get("side")
            if open_side and side != open_side:
                notes.append(f"?????? {open_side} ? ?? {side}")
            open_entry = float(last_open.get("entry", 0) or 0)
            entry = float(pos.get("entry_price", 0) or 0)
            if open_entry > 0 and abs(entry - open_entry) > 3.0:
                notes.append(f"????: ???? {open_entry:.2f} vs ?? {entry:.2f}")

        if saved_watched <= 0 and real_amt > 0:
            reconcile["manual_open"] = True
            self.initial_qty = real_amt
            self.tp_levels_consumed = []
            if int(getattr(self, "base_qty", 0) or 0) <= 0:
                self.base_qty = int(real_amt)
            notes.append(
                f"????(??): ???? ? ?? {real_amt}? {side}????????"
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
                    f"????(????): ?? {je:.2f} vs ?? {entry_px:.2f} ? ?? TP123"
                )
            elif saved_initial > real_amt:
                trusted = self._trusted_initial_qty(real_amt, entry_px)
                if trusted <= real_amt:
                    reconcile["manual_open"] = True
                    self.initial_qty = real_amt
                    self.tp_levels_consumed = []
                    notes.append(
                        f"??/??(??): ?? initial={saved_initial} > ?? {real_amt}? "
                        f"?????? ? ?? TP123"
                    )

        if saved_watched > 0 and self._is_material_qty_change(saved_watched, real_amt):
            action_msg = (
                "????" if real_amt > saved_watched
                else "?????? / ????"
            )
            reconcile["qty_manual_change"] = (saved_watched, real_amt, action_msg)
            notes.append(f"????(??): {saved_watched}? ? {real_amt}? ({action_msg})")

        if not self.last_tv_side:
            if not reconcile["direction_mismatch"]:
                self.last_tv_side = side
        elif side != self.last_tv_side and not reconcile["tv_close"]:
            if self._live_aligns_with_credible_tv(side):
                notes.append(
                    f"??TV??{self.last_tv_side}???{side}????"
                    f"???TV???? ? ?????"
                )
                self.last_tv_side = side
            else:
                reconcile["direction_mismatch"] = True
                if not any("????" in n for n in notes):
                    notes.append(f"????: ??{side} vs TV??{self.last_tv_side}")

        for n in notes:
            logger.warning(f"?? ????: {n}")
        return reconcile

    def _trusted_initial_qty(self, live_qty, entry=None):
        live_qty = self._safe_qty(live_qty)
        entry = float(entry or self.watched_entry or 0)
        last_open = self._load_last_journal_entry(OPEN_JOURNAL, self.symbol)
        if last_open:
            jq = self._safe_qty(last_open.get("qty", 0))
            je = float(last_open.get("entry", 0) or 0)
            entry_tol = max(3.0, entry * 0.003) if entry > 0 else 3.0
            if jq > 0 and (entry <= 0 or je <= 0 or abs(entry - je) <= entry_tol):
                qty_tol = max(1, int(live_qty * 0.02)) if live_qty > 0 else 1
                if live_qty > 0 and jq > live_qty + qty_tol:
                    logger.warning(
                        f"?? OPEN?? {jq}? @ {je:.2f} > ?? {live_qty}? "
                        f"? ???????????????"
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
                f"?? ???? initial_qty={saved}? ? ?? {trusted}? "
                f"(?? {live_qty}???????????)"
            )
            self.initial_qty = trusted
            self.tp_levels_consumed = []
            self._save_state()
        elif trusted > live_qty:
            logger.warning(
                f"?? ??? {trusted}? > ?? {live_qty}? ? ???????? TP ??"
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
        """???????? ?10% ?????????????"""
        old = self._safe_qty(old_qty)
        new = self._safe_qty(new_qty)
        delta = abs(new - old)
        if delta <= REGIME_CAP_TOLERANCE_CONTRACTS:
            return False
        ratio = self._qty_change_ratio(old, new)
        return ratio >= QTY_ALIGN_MIN_PCT

    @staticmethod
    def _sanitize_tp_prices(tp_list):
        """TV/?????????????? 2 ?????? 1517.4 ?? PriceNotOnTick"""
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
                    "tv_sl": float(getattr(self, "tv_sl", 0) or 0),
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
                    # ?? v1.0 ?3?????????
                    "frozen_hard_sl_px": float(getattr(self, "frozen_hard_sl_px", 0) or 0),
                    # ?? v1.0 ?5.0????????
                    "_early_be_checkpoint_done": bool(getattr(self, "_early_be_checkpoint_done", False)),
                    # ?? v1.0 ?8-9???????? + ?????
                    "exit_ownership": str(getattr(self, "exit_ownership", "NONE") or "NONE"),
                    "ownership_locked_at": float(getattr(self, "ownership_locked_at", 0) or 0),
                    "pending_order_tags": dict(getattr(self, "_pending_order_tags", {}) or {}),
                    "mutex_leg": str(getattr(self, "_mutex_leg", "") or ""),
                    "pipeline": self._pipeline_state_blob(),
                    # ???????v1.0 ?????
                    "reentry_state": self._reentry_state_dict(),
                }, f)
        except Exception as e:
            logger.error(f"save_state failed: {e}", exc_info=True)

    @staticmethod
    def _safe_qty(val, default=0):
        """Deepcoin API ??? '1.000000' ?????? float ? int"""
        if val is None or val == "":
            return default
        try:
            return int(float(val))
        except (TypeError, ValueError):
            return default

    def _get_active_position(self, prefer_ws=False):
        # binance parity (ff01342): distinguish "confirmed flat" (return None,
        # only when the exchange actually returned a well-formed empty
        # position list) from "query failed after retries" (return the
        # sentinel string "QUERY_FAILED"). Collapsing both into None violates
        # spec 9/12 (a failed/timed-out query must never be read as "no
        # position" -- it must be retried / block new actions instead).
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
                # ?????? data ??? list ??????????????????
            except Exception as e:
                logger.warning(f"[{self.symbol}] ??????: {e}")
            if attempt < 4:
                time.sleep(0.5 + attempt * 0.3)
        logger.error(f"[{self.symbol}] ??????5??????? QUERY_FAILED??????")
        return "QUERY_FAILED"

    def _get_all_positions(self):
        """?????????????????????????"""
        # ???? + ??????????????????????
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
                # API???????????????
                if res is not None:
                    return []
            except Exception as e:
                logger.warning(f"[{self.symbol}] ??????: {e}")
            if attempt < 4:
                time.sleep(0.5 + attempt * 0.3)
        return []

    def _verify_flat(self):
        """??????????????????????
        v16.19???????????WS ???????????????????
        """
        for attempt in range(3):
            positions = self._get_all_positions()
            if len(positions) == 0:
                return True
            if attempt < 2:
                # v16.19: 0.3s?0.15s / 0.5s?0.25s?WS ???????
                time.sleep(0.15 if attempt == 0 else 0.25)
        return len(positions) == 0

    def _ensure_flat_before_open(self, reason_tag="???"):
        if self._wait_verify(self._verify_flat, retries=4, delay=0.4):
            return True
        logger.warning(f"?? {reason_tag}???????????????")
        if self._close_all(f"{reason_tag} ? ????", reset_state=True):
            return self._wait_verify(self._verify_flat, retries=6, delay=0.5)
        return False

    def _snapshot_sizing_principal(self, reason=""):
        """??/?????? USDT ????????????????????"""
        principal = deepcoin_client.get_principal_wallet_balance()
        if principal > 0:
            self.sizing_principal = principal
            self._save_state()
            logger.info(f"?? ???? {principal:.2f} USDT ({reason})")
            if reason and ("??" in reason or "???" in reason):
                target_qty = None
                eff_risk = None
                if "???" in reason and self.tv_price > 0:
                    t, meta = self._calc_vps_open_qty(self.tv_price)
                    target_qty = t
                    eff_risk = float(meta.get("effective_risk_pct", VPS_RISK_PCT) or VPS_RISK_PCT) / 100.0
                    vps_meta = meta
                else:
                    vps_meta = None
                try:
                    telegram_notify.report_principal_snapshot(
                        reason=reason,
                        principal=principal,
                        regime=self.regime if "???" in reason else None,
                        margin_pct=eff_risk,
                        target_qty=target_qty,
                        leverage=EXCHANGE_LEVERAGE,
                        vps_sizing_meta=vps_meta,
                    )
                except Exception as e:
                    logger.warning(f"????????: {e}")
        return principal

    def _resolve_cap_sizing_base(self, wallet_balance=None):
        """
        ?????????sizing_principal ?????? VPS ???????
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
        """VPS OPEN ?? ? ???????? margin% ???"""
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
            return "?????????"
        if trim <= 0:
            return None
        retain = target / live if live > 0 else 0
        if retain < CAP_MIN_RETAIN_RATIO and live > target * 2:
            return (
                f"????????? {retain:.1%}????????????????????"
                f"??? {target} ? vs ?? {live} ??"
            )
        if trim > live * 0.85 and target < live * 0.15:
            return (
                f"?????????? {trim} ????? {target} ??????????"
            )
        expected = live - target
        if abs(trim - expected) > max(int(live * 0.05), 1):
            return f"???????? {trim} ???? {expected} ?"
        return None

    def _max_add_times_for_regime(self, regime=None):
        """TV v6.9.93??????????????"""
        return get_regime_max_add_times(int(regime if regime is not None else self.regime or 3))

    def _apply_tv_sizing_params(self, payload):
        """?? entry_type?OPEN ? VPS ?? sizing???? TV qty_ratio ? ?? base_qty"""
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
            f"?? TV??: type={self.tv_entry_type} "
            f"| VPS??={VPS_RISK_PCT}% R{self.regime} "
            f"| ??=base?{self.tv_qty_ratio:.2f}(TV) ??{max_add}? "
            f"| ???={EXCHANGE_LEVERAGE}x"
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
        """????????????????????"""
        exclude = str(exclude_symbol or self.symbol)
        by_sym, total = deepcoin_client.get_all_swap_position_notionals()
        other = 0.0
        for sym, notion in (by_sym or {}).items():
            if str(sym) == exclude:
                continue
            other += float(notion or 0)
        return round(other, 2), by_sym, total

    def _assert_notional_cap_or_reject(self, qty, price, sizing_meta=None):
        """???????????? + ???? ? equity?9?"""
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
                f"?? ?????? {self.symbol}: ?? {existing:.0f}U + ?? {new_notional:.0f}U "
                f"= {meta['total_notional']:.0f}U ? ?? {equity:.0f}U?{MAX_TOTAL_NOTIONAL_MULT:.0f}"
            )
            return True, meta
        logger.error(
            f"?? ?????? {self.symbol}: ?? {existing:.0f}U + ?? {new_notional:.0f}U "
            f"= {meta['total_notional']:.0f}U > ?? {meta['cap']:.0f}U "
            f"(?? {equity:.0f}U?{MAX_TOTAL_NOTIONAL_MULT:.0f}) | ?? {by_sym}"
        )
        logger.warning(
            f"[?????] ??????????? [{self.symbol}] | "
            f"?? {equity:.0f}U ? ?? {meta['cap']:.0f}U ({MAX_TOTAL_NOTIONAL_MULT:.0f}x) | "
            f"?????? {existing:.0f}U + ?? {new_notional:.0f}U = {meta['total_notional']:.0f}U | "
            f"???? {by_sym}"
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
        """??????????? ? ?? ?10% ??"""
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
                f"?? [????] ?? {live_qty} > {target} ? "
                f"(+{excess}, {excess / max(target, 1):.2%} ? {QTY_ALIGN_MIN_PCT:.0%} ??)?????"
            )
        return live_qty > int(target) + tol, target, margin_pct, reg

    def _trim_position_to_target(self, target_qty, action, reason_tag="??Remediation"):
        """??Remediation???? excess=??-????????"""
        target_qty = int(target_qty or 0)
        pos = self._get_active_position()
        if pos == "QUERY_FAILED":
            logger.error(f"?? {reason_tag} ??????? ? ?????? watched_qty={self.watched_qty}")
            return self._safe_qty(self.watched_qty)
        real = self._safe_qty(pos.get("size")) if pos else 0
        if not pos or target_qty <= 0:
            return real
        cap_tol = self._regime_cap_tolerance(target_qty)
        if real <= target_qty + cap_tol:
            return real
        trim_qty = real - target_qty
        plan_err = self._validate_cap_trim_plan(real, target_qty, trim_qty)
        if plan_err:
            logger.error(f"?? {reason_tag} ??: {plan_err} | live={real} target={target_qty}")
            logger.warning(
                f"[?????] ?????????????| "
                f"???{reason_tag} | ?? {real}? ? ?? {target_qty}? | ???{plan_err}"
            )
            return real
        close_side = "sell" if action == "LONG" else "buy"
        pos_side = "long" if action == "LONG" else "short"
        logger.warning(
            f"?? {reason_tag}: ?? {trim_qty} ? "
            f"(?? {real} ? ?? {target_qty})"
        )
        deepcoin_client.cancel_all_open_orders(self.symbol)
        time.sleep(0.5)
        self._cancel_all_tp_limit_orders(max_rounds=3)
        time.sleep(0.3)
        new_sz = real
        for _ in range(CAP_TRIM_MAX_ROUNDS):
            pos = self._get_active_position()
            if pos == "QUERY_FAILED":
                logger.error(f"?? {reason_tag} ??????????trim ??")
                break
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
            res = deepcoin_client.place_market_order(
                self.symbol, close_side, pos_side, slice_trim, reduce_only=True,
            )
            # ?????reduceOnly ??????
            if deepcoin_client.is_reduce_only_rejected(res):
                logger.warning(
                    f"?? ?? reduceOnly ?? {close_side} {slice_trim}? "
                    f"? force_rest ??????????"
                )
                time.sleep(0.5)
                fresh = deepcoin_client.force_rest_get_all_positions(self.symbol)
                if fresh:
                    for fp in fresh:
                        fpsz = self._safe_qty(fp["size"])
                        if fpsz <= 0:
                            continue
                        fp_side = "sell" if fp["posSide"] == "long" else "buy"
                        retry_res = deepcoin_client.place_market_order(
                            self.symbol, fp_side, fp["posSide"], fpsz, reduce_only=True,
                        )
                        if deepcoin_client.is_reduce_only_rejected(retry_res):
                            logger.error(
                                f"? ????? reduceOnly ??? {fp_side} {fpsz}? ? ??"
                            )
                        time.sleep(0.5)
                break
            time.sleep(1.0)
            verified = self._wait_verify(
                lambda: self._get_active_position(),
                retries=6,
                delay=0.5,
            )
            if verified == "QUERY_FAILED":
                logger.error(f"?? {reason_tag} trim ??????????? ??")
                break
            new_sz = self._safe_qty(verified.get("size")) if verified else cur
            if new_sz <= target_qty + cap_tol:
                break
        if new_sz < target_qty * 0.5 and real > target_qty * 1.5:
            logger.warning(
                f"[?????] ?????? | "
                f"?? {target_qty}?????? {new_sz}??? {real}??"
            )
        elif new_sz > target_qty * OPEN_OVERSIZE_RATIO:
            logger.warning(
                f"[?????] ??????? | "
                f"?? {target_qty}?????? {new_sz}?"
            )
        return new_sz

    def _radar_enforce_regime_cap(self, live_qty, curr_px, force=False):
        """
        ??????????? TV ??????? ? reduceOnly ?? ? ?? TP123?
        ??????????????? STOP?
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
                f"?? [??????] ?? {live_qty}>{target} ? ???? "
                f"(R{regime} VPS??{margin_pct:.1%})"
            )
            return None

        _, balance, margin_usdt, margin_pct, regime = self._regime_cap_target_qty(curr_px, regime)
        old_qty = live_qty
        logger.warning(
            f"?? [??????] R{regime} VPS?? {target} ? "
            f"(?? {balance:.0f}U?VPS??{margin_pct:.1%}?{self.leverage}x) | "
            f"?? {live_qty} ? ?? ? ????"
        )

        new_qty = self._trim_position_to_target(
            target, self.current_side, reason_tag=f"??R{regime}????",
        )
        pos = self._get_active_position()
        if pos == "QUERY_FAILED":
            logger.warning(f"?? R{regime} trim ????????pos=QUERY_FAILED?????? watched_entry")
            pos = None
        entry = float(pos["entry_price"]) if pos else self.watched_entry
        self.watched_qty = new_qty
        self.initial_qty = new_qty
        if pos:
            self.watched_entry = entry
        self._save_state()

        sl = self._radar_sl_to_pass()
        result = self._enforce_defense_alignment(
            new_qty, entry, dynamic_sl=sl,
            reason=f"?????? R{regime} ??? TP ??", rounds=3,
        )
        if sl and not self._has_trigger_sl_near(sl):
            self._ensure_radar_sl(new_qty, sl)

        self._last_regime_cap_ts = now
        verify_note = (
            f"VPS {balance:.2f}U ? R{regime} ??{margin_pct:.1%} ? {self.leverage}x "
            f"= ??? {margin_usdt:.0f}U ? ?? {target} ? | "
            f"?? {old_qty} ? {new_qty} ? | "
            f"TP {result['matched']}/{result['expected']} | "
            f"{self._format_audit_summary(result['audit'])} | "
            f"??SL={'???/??' if sl else '??'}"
        )
        self._call_telegram_notify(
            telegram_notify.report_radar_regime_cap_trim,
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
        """???? 1 ??????????? 1 ??????"""
        q = self._safe_qty(qty)
        if q <= 0:
            return False
        ref = self._safe_qty(self.initial_qty) + self._safe_qty(self.watched_qty)
        return q == DUST_ORPHAN_CONTRACTS and ref == 0

    def _should_finalize_tp_victory(self, real_amt):
        """??????????? TP ??????????? ? ????"""
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
        """??/???????REST ?????? Pine ?????"""
        meta = self._enrich_close_meta_live(close_meta, curr_px)
        flat = self._wait_verify(self._verify_flat, retries=6, delay=0.5)
        base_note = "????? | ????? | ????????"
        if swept_dust:
            base_note = f"???????? | {base_note}"
        if meta.get("pnl_pct") is not None:
            base_note += f" | ?? {self._safe_float(meta.get('pnl_pct')):+.2f}%"
        if meta.get("side"):
            base_note += f" | ?? {meta.get('side')}"
        if meta.get("entry_px") and float(meta.get("entry_px") or 0) > 0:
            base_note += f" | ?? {float(meta['entry_px']):.2f}"
        if meta.get("closed_qty") and float(meta.get("closed_qty") or 0) > 0:
            base_note += f" | ?? {float(meta['closed_qty']):.0f}?"
        if meta.get("live_exit_px") and float(meta.get("live_exit_px") or 0) > 0:
            base_note += f" | ?? {float(meta['live_exit_px']):.2f}"
        if meta.get("regime"):
            base_note += f" | TV?? R{int(meta.get('regime'))}"
        if meta.get("atr") and float(meta.get("atr") or 0) > 0:
            base_note += f" | TV ATR {float(meta['atr']):.2f}"
        src_note = format_tv_field_sources(meta.get("field_sources") or {})
        if src_note and "TV??" not in src_note:
            base_note += f" | {src_note}"
        if flat:
            verify_note = base_note
        else:
            pos = self._get_active_position()
            if pos == "QUERY_FAILED":
                logger.warning(f"?????????????? | ??????? | reason={reason}")
                return
            residual = self._safe_qty(pos["size"]) if pos else 0
            if residual > 0 and not self._is_dust_qty(residual):
                logger.warning(
                    f"?????????????? | ?? {residual}? | reason={reason}"
                )
                return
            verify_note = f"{base_note} | REST ?????"
            logger.info(f"?????REST ?????????? | reason={reason}")
        display_reason = meta.get("tv_reason") or reason or "????"
        self._call_telegram_notify(
            telegram_notify.report_supervisor_close,
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
        # P1 ????? TG ????
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
        # ???????v1.0 ?????
        self._maybe_evaluate_smart_reentry(reason=reason, close_meta=meta, curr_px=curr_px)

    def _maybe_evaluate_smart_reentry(self, reason="", close_meta=None, curr_px=0.0):
        """
        ??????????????v1.0 ????

        ?????
        1. ???????hard_sl / vps_hard_sl?
        2. ??????????max=1?
        3. TP1 ????tp1_ever_filled=False?
        4. ?????????adx_tier=2?

        ???????????????
        """
        try:
            from smart_reentry_engine import evaluate_flat_for_reentry, open_reentry_window
            from reentry_profiles import reentry_enabled, tier_label
        except ImportError:
            return

        if not reentry_enabled(self.symbol):
            return

        # ????? ? ????
        hard_sources = ("vps_hard_sl", "hard_sl", "VPS_HARD_SL")
        if reason and any(s in str(reason) for s in hard_sources):
            logger.info(f"?? [{self.symbol}] ??????????")
            self._clear_reentry_state()
            return

        meta = close_meta or {}
        side = str(meta.get("side") or getattr(self, "current_side", "") or "").upper()
        entry_px = float(meta.get("entry_px") or 0)
        exit_px = float(meta.get("live_exit_px") or curr_px or 0)
        atr = float(meta.get("atr") or getattr(self, "open_atr", 0) or 0)
        attempt = int(getattr(self, "reentry_attempt", 0) or 0)

        # ?? TP1 ?????
        tp1_filled = bool(getattr(self, "_tp1_filled_hint", False) or
                         getattr(self, "_ws_tp1_fill_hint", False) or
                         (1 in (getattr(self, "tp_levels_consumed", []) or [])))

        # ?? ADX ??
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
                f"?? [{self.symbol}] ?????: {why} | "
                f"src={reason} exit={exit_px:.2f} attempt={attempt} "
                f"tp1_filled={tp1_filled} tier={adx_tier}"
            )
            self._clear_reentry_state()
            return

        # ?????????
        if self._ensure_sterile_for_reentry(reason="????"):
            self._start_smart_reentry_limit(
                side=side, entry=entry_px, exit_px=exit_px,
                atr=atr, attempt=attempt, tp1_filled=tp1_filled,
                adx_tier=adx_tier, reason=reason,
            )

    def _clear_reentry_state(self):
        """??????"""
        self.reentry_active = False
        self.reentry_limit_order_id = None
        self.reentry_limit_px = 0.0
        self.reentry_limit_deadline_ts = 0.0
        self.reentry_order_tag = None

    def _ensure_sterile_for_reentry(self, reason="") -> bool:
        """??????????"""
        try:
            deepcoin_client.cancel_all_open_orders(self.symbol)
        except Exception:
            pass
        time.sleep(0.5)
        pos = self._get_active_position()
        if pos == "QUERY_FAILED":
            logger.error(f"?? [{self.symbol}] ??????????????????")
            return False
        if not pos:
            return True
        qty = self._safe_qty(pos.get("size"))
        return qty <= 0

    def _start_smart_reentry_limit(self, side, entry, exit_px, atr, attempt, tp1_filled, adx_tier, reason=""):
        """????????"""
        try:
            from smart_reentry_engine import plan_reentry_limit
        except ImportError:
            return False

        side_str = "LONG" if side.upper() in ("LONG", "BUY") else "SHORT"
        open_side = "buy" if side_str == "LONG" else "sell"
        pos_side = "long" if side_str == "LONG" else "short"
        tv_price = float(getattr(self, "cycle_tv_price", 0) or 0)

        # ? K ?????
        k5 = deepcoin_client.fetch_klines(self.symbol, interval="5m", limit=3) if hasattr(deepcoin_client, 'fetch_klines') else None
        k3 = deepcoin_client.fetch_klines(self.symbol, interval="3m", limit=3) if hasattr(deepcoin_client, 'fetch_klines') else None

        plan, why = plan_reentry_limit(
            side=side_str, tv_price=tv_price, symbol=self.symbol,
            klines_5m=k5, klines_3m=k3,
        )

        if not plan:
            logger.warning(f"?? [{self.symbol}] ??????: {why}")
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
            logger.warning(f"?? [{self.symbol}] ????????")
            self.reentry_order_tag = None
            return False

        oid = (order.get("data") or {}).get("ordId", "") or order.get("order_id", "")
        self.reentry_active = True
        self.reentry_limit_order_id = oid
        self.reentry_limit_px = lim
        self.reentry_limit_deadline_ts = float(plan["deadline_ts"])

        logger.info(
            f"?? [{self.symbol}] ???????? {side_str} {qty}@{lim:.2f} "
            f"attempt={attempt} tag={tag}"
        )

        try:
            self._call_telegram_notify(
                telegram_notify.report_system_alert,
                title=f"???????? [{self.symbol}]",
                detail=(
                    f"{side_str} attempt={attempt} limit@{lim:.2f} "
                    f"TV@{tv_price:.2f} exit={reason}@{exit_px:.2f} "
                    f"tp1_filled={tp1_filled} tier={adx_tier}"
                ),
                level="??",
            )
        except Exception:
            pass

        self._save_state()
        return True

    def _reentry_state_dict(self) -> dict:
        """?8?? RadarReentryMixin ??????????????????"""
        return RadarReentryMixin._reentry_state_dict(self)

    def _reentry_tick(self):
        """???? Tick?????/TTL??"""
        if not getattr(self, "reentry_active", False):
            return

        pos = self._get_active_position()
        if not pos or pos == "QUERY_FAILED":
            return

        qty = self._safe_qty(pos.get("size"))
        if qty > 0:
            # ??????????
            self._on_reentry_filled(pos)
            return

        # ?? TTL
        now = time.time()
        deadline = float(getattr(self, "reentry_limit_deadline_ts", 0) or 0)
        if deadline > 0 and now >= deadline:
            logger.info(f"? [{self.symbol}] ???? TTL ???????")
            self._cancel_reentry_limit(reason="TTL??")
            side = str(getattr(self, "cycle_tv_side", "") or "").upper()
            self._start_smart_reentry_limit(
                side=side,
                entry=float(getattr(self, "cycle_entry", 0) or 0),
                exit_px=float(getattr(self, "last_exit_px", 0) or 0),
                atr=float(getattr(self, "cycle_open_atr", 0) or 0),
                attempt=int(getattr(self, "reentry_attempt", 0) or 0),
                tp1_filled=False,
                adx_tier=int(getattr(self, "adx_tier", 1) or 1),
                reason="TTL??",
            )

    def _cancel_reentry_limit(self, reason=""):
        """???????"""
        oid = getattr(self, "reentry_limit_order_id", None)
        if oid:
            try:
                deepcoin_client.cancel_order(self.symbol, order_id=oid)
                logger.info(f"??? [{self.symbol}] ????? id={oid} | {reason}")
            except Exception as e:
                logger.debug(f"????: {e}")
        self._clear_reentry_state()

    def _on_reentry_filled(self, pos):
        """????????"""
        side_str = str(pos.get("posSide", "") or "").lower()
        side = "LONG" if side_str == "long" else "SHORT"
        entry = float(pos.get("avgPx") or pos.get("entry_price", 0) or 0)
        qty = self._safe_qty(pos.get("size"))

        if entry <= 0 or qty <= 0:
            return

        logger.info(f"? [{self.symbol}] ???? {side} {qty}@{entry:.2f}")

        # ?? attempt
        prev_attempt = int(getattr(self, "reentry_attempt", 0) or 0)
        self.reentry_attempt = prev_attempt + 1
        self._clear_reentry_state()

        # ??????
        self.current_side = side
        self.watched_entry = entry
        self.watched_qty = qty
        self.initial_qty = qty
        self.monitoring = True
        self.tp_levels_consumed = []

        # ???? + TP12
        try:
            self._arm_temp_stop_and_tp12_for_reentry(qty, entry, side)
        except Exception as e:
            logger.error(f"[{self.symbol}] ?????????: {e}")

        self._save_state()

    def _arm_temp_stop_and_tp12(self, live_qty, entry, side, source=""):
        """Adapter: ???? mixin ??? ? ???? _arm_temp_stop_and_tp12_for_reentry?"""
        return self._arm_temp_stop_and_tp12_for_reentry(live_qty, entry, side)

    def _resolve_atr_scenario_after_open(self, entry, side, live_qty):
        """Mixin ???????Deepcoin ATR ?????????"""
        pass

    def _arm_temp_stop_and_tp12_for_reentry(self, qty, entry, side):
        """????????? + TP12"""
        from breath_stop import initial_stop_price

        atr = float(getattr(self, "open_atr", 0) or 0)
        profile = getattr(self, "breath_profile", None) or {}

        init = initial_stop_price(side, entry, atr, profile=profile)
        if init <= 0:
            init = entry * 0.98 if side == "LONG" else entry * 1.02

        self.initial_stop = init
        self.current_sl = init
        self.tv_sl = init

        # ????
        pos_side = "long" if side == "LONG" else "short"
        deepcoin_client.place_trigger_order(
            self.symbol, "sell" if side == "LONG" else "buy",
            pos_side, qty, init, order_type="market",
        )

        # ? TP1/TP2 ????? ?3.5?TP1=10%?TP2=20%???70%????
        LEG_TP_RATIOS_REENTRY = [0.10, 0.20]
        tps = list(getattr(self, "tv_tps", [0, 0, 0]) or [])
        if len(tps) >= 1 and tps[0] > 0:
            tp1 = tps[0]
            tp1_side = "sell" if side == "LONG" else "buy"
            # ?? floor ????? qty?1 ?? tp1=1, tp2=0
            tp1_qty = max(1, int(qty * LEG_TP_RATIOS_REENTRY[0]))
            tp2_qty = max(0, int(qty * LEG_TP_RATIOS_REENTRY[1]))
            if qty == 1:
                tp1_qty = 1
                tp2_qty = 0
            deepcoin_client.place_limit_order(
                self.symbol, tp1_side, pos_side, tp1, tp1_qty,
            )
        if len(tps) >= 2 and tps[1] > 0 and tp2_qty > 0:
            tp2 = tps[1]
            tp2_side = "sell" if side == "LONG" else "buy"
            deepcoin_client.place_limit_order(
                self.symbol, tp2_side, pos_side, tp2, tp2_qty,
            )

        logger.info(f"?? [{self.symbol}] ??????: ???@{init:.2f} TP1/TP2")

    def _sweep_dust_and_finalize(self, reason):
        """???????????/? TP ?? ? ?? + reduceOnly ?? + ????????????
        ?? ?????reduceOnly ?????? ? ?? force_rest ????????
        """
        logger.warning(f"?? ?????????????????? ? {reason}")
        self.monitoring = False
        deepcoin_client.cancel_all_open_orders(self.symbol)
        time.sleep(0.4)
        for round_i in range(4):
            # ?????????
            all_positions = self._get_all_positions()
            if not all_positions:
                break
            for pos in all_positions:
                close_side = "sell" if pos["posSide"] == "long" else "buy"
                live_sz = self._safe_qty(pos["size"])
                logger.info(f"?? ??{round_i + 1}/4: {close_side} {live_sz}? {pos['posSide']} reduceOnly")
                res = deepcoin_client.place_market_order(
                    self.symbol, close_side, pos["posSide"], live_sz, reduce_only=True,
                )
                # ?????reduceOnly ??????
                if deepcoin_client.is_reduce_only_rejected(res):
                    logger.warning(
                        f"?? ??? reduceOnly ?? {close_side} {live_sz}? "
                        f"? force_rest ???????"
                    )
                    time.sleep(0.5)
                    fresh = deepcoin_client.force_rest_get_all_positions(self.symbol)
                    if fresh:
                        for fp in fresh:
                            fpsz = self._safe_qty(fp["size"])
                            if fpsz <= 0:
                                continue
                            fp_side = "sell" if fp["posSide"] == "long" else "buy"
                            retry_res = deepcoin_client.place_market_order(
                                self.symbol, fp_side, fp["posSide"], fpsz, reduce_only=True,
                            )
                            if deepcoin_client.is_reduce_only_rejected(retry_res):
                                logger.error(
                                    f"? ?????? reduceOnly ??? {fp_side} {fpsz}? ? ?????"
                                )
                            time.sleep(0.5)
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
        # ?? v1.0 ?8-9?????????? + ?????
        self.exit_ownership = "NONE"
        self.ownership_locked_at = 0.0
        self._pending_order_tags = {}
        self._mutex_leg = ""
        self._save_state()
        deepcoin_client.cancel_all_open_orders(self.symbol)
        self._report_flat_close(reason, swept_dust=True)

    def _apply_recover_live_alignment(self, side, reconcile):
        """???????TV ????????????? _enforce_tv_direction_or_flat ????"""
        extra_notes = []
        if reconcile.get("tv_close"):
            action = (self.last_tv_signal or {}).get("action", "CLOSE")
            msg = (
                f"TV????? {action}???????? ? ??? {side} ??????"
            )
            logger.warning(f"?? [??] {msg}")
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
                f"????: ??{side} vs TV{tv_side} ? ?????????? TV"
            )
        elif not self.last_tv_side:
            self.last_tv_side = side
        return extra_notes

    def _scan_and_sweep_dust_on_startup(self, was_monitoring=False):
        """??????????/???? ? ???????????????"""
        pos = self._get_active_position()
        if pos == "QUERY_FAILED":
            logger.error("?? [????] ??????????? ? ???????")
            return False
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
                    f"?? [????] ???? {real_amt}? (ref={ref})???????"
                )
                return False
        if not self._is_dust_qty(real_amt) and not self._should_finalize_tp_victory(real_amt):
            return False
        if self._safe_qty(self.initial_qty) > 0 or self._safe_qty(self.watched_qty) > 0:
            reason = "???? (???? / ???? / TV ????)"
        else:
            reason = "??????????????"
        logger.warning(
            f"?? [????] {self.current_side} ?? {real_amt}? "
            f"(initial={self.initial_qty}, watched={self.watched_qty}) ? ????"
        )
        self._sweep_dust_and_finalize(reason)
        # ?? ?9.4????????????????????
        flat = self._wait_verify(self._verify_flat, retries=4, delay=0.5)
        if not flat:
            logger.warning(
                f"?? [????] ?????????????? {self.symbol} ??"
            )
        return True

    def _recover_missed_flat_on_startup(self, was_monitoring=False):
        """????????????????????? ? ??????"""
        pos = self._get_active_position()
        if pos == "QUERY_FAILED":
            logger.warning("?? [????] ??????????????????")
            return False
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
            last_open = self._load_last_journal_entry(OPEN_JOURNAL, self.symbol)
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
                "?? [????] ????????????? ? ???????"
            )
            return False

        logger.warning(
            f"?? [????] ??/????? (watched={prev_watched}, side={prev_side}, "
            f"monitoring={was_monitoring}) ?????? ? ??????"
        )
        deepcoin_client.cancel_all_open_orders(self.symbol)
        self.monitoring = False
        self.watched_qty = 0
        self.initial_qty = 0
        self.base_qty = 0
        self.add_count = 0
        self.current_side = None
        # v16.27: ?????????????????tv_sl/tv_tps/journal?
        # ??????????????? ATR/regime/tp/sl?
        self.last_tv_signal = None
        self.last_tv_side = None
        self.tv_sl = 0.0
        self.tv_tps = [0.0, 0.0, 0.0]
        self.tv_price = 0.0
        self.watched_entry = 0.0
        self.current_atr = 30.0
        self.regime = 3
        self.open_atr = 30.0
        self.open_regime = 3
        self.initial_stop = 0.0
        self.tp_levels_consumed = []
        self.shield_tiers_consumed = []
        self.shield_active = False
        self._save_state()

        verify_note = (
            f"?????? | ??? {prev_watched}? {prev_side or ''} | "
            f"????? | ????? | ????????"
        )
        recover_meta = self._infer_flat_close_meta(hint_reason="????????")
        self._call_telegram_notify(
            telegram_notify.report_supervisor_close,
            reason=recover_meta.get("tv_reason", "???? (??????)"),
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
        if pos == "QUERY_FAILED":
            logger.warning("?? [????] _verify_position ??????? ? ???????")
            return None
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
        # v13.81???? TP1+TP2 ?????TP3 ?????
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
        ??????? TP????? R4 TP1=5% ???
        ?????? ? ?????? <2% ?????TP1 ??????
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
                f"?? ???? tp_levels_consumed={saved} ? ??????? TP1?"
            )
            saved = []
        elif initial_qty <= live_qty and saved and inferred and saved != inferred:
            logger.info(
                f"?? ????????: TP{saved} ? TP{inferred or '?'}"
            )
            saved = inferred

        if len(saved) >= 3 and live_qty > DUST_ORPHAN_CONTRACTS:
            logger.warning(
                f"?? tp_levels_consumed={saved} ??? {live_qty} ? ? "
                f"??? {initial_qty} ???? TP{inferred or '?'}"
            )
            saved = inferred
        elif inferred and (not saved or len(inferred) < len(saved)):
            if saved != inferred:
                logger.info(
                    f"?? ??????: TP{saved or '?'} ? TP{inferred} "
                    f"(?? {initial_qty} ? ?? {live_qty}?)"
                )
            saved = inferred
        elif saved and inferred and saved != inferred:
            logger.info(f"?? ?????????: TP{saved} ? TP{inferred}")
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
        TP1+TP2 ?????1???????????????????
        TP3 ????????????????????
        """
        live_qty = self._safe_qty(live_qty)
        ratios = ratios or self.regime_settings[self._tp_split_regime()]["ratios"]
        consumed = set(getattr(self, "tp_levels_consumed", []) or [])
        remaining = [i for i in range(3) if (i + 1) not in consumed]
        if not remaining or live_qty <= 0:
            return {}

        # ????????TP3??????
        if len(remaining) == 1:
            return {remaining[0] + 1: live_qty}

        # TP1+TP2 ???1?????TP3
        # ??live_qty??2?????TP1?TP2=0
        if live_qty < 2:
            out = {1: live_qty}
            if 2 in remaining:
                out[2] = 0
            if 3 in remaining:
                out[3] = 0
            return out

        # ?? floor() ??????? live_qty
        tp1_qty = max(1, int(math.floor(live_qty * ratios[0])))
        tp2_qty = max(1, int(math.ceil(live_qty * ratios[1])))
        # ?? tp1 + tp2 ??? live_qty??????
        if tp1_qty + tp2_qty > live_qty:
            # ???? TP1?TP2 ?????
            tp1_qty = max(1, live_qty - 1)
            tp2_qty = max(0, live_qty - tp1_qty)
        # ??? TP3???????????
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
        for level in (1, 2):  # v13.81: TP3 ?????
            if level in consumed:
                continue
            price = self.tv_tps[level - 1]
            qty = qty_map.get(level, 0)
            levels.append({"level": level, "qty": qty, "price": price})
        return levels

    def _audit_tp_levels(self, live_qty, tolerance=0.1):
        """??????????? + ???? regime ?? + ????"""
        live_qty = self._resolve_live_qty(live_qty)
        orders = self._collect_tp_limit_orders()
        # binance parity (eb0dc4c): a transient pending-orders query failure
        # must not be read as "0 orders on the book" -- that manufactures a
        # false "all levels missing / severe" audit result, which under
        # binance's history triggered a cancel-place loop roughly every 70s
        # during API propagation delay / IP rate-limit windows. Callers that
        # gate on audit severity must check this flag before escalating.
        query_failed = not getattr(deepcoin_client, "_last_pending_orders_query_ok", True)
        # binance parity (26d5c65): a *successful* query that comes back
        # empty can still be stale (propagation lag) rather than genuinely
        # confirming every TP order vanished. If we aligned successfully
        # recently and the position/expected-TP-count says orders should
        # exist, treat an empty result the same as a failed query instead of
        # "all missing" -- prevents cancel/re-place cycling off a lagged read.
        expected_now = self._expected_tp_count()
        if (
            not orders and not query_failed
            and self._safe_qty(live_qty) > 0 and expected_now > 0
            and (time.time() - float(getattr(self, "_last_defense_align_ok_ts", 0) or 0)) < 1800.0
        ):
            logger.warning(
                f"[{self.symbol}] TP query empty but recently aligned -> "
                f"treat as unreadable, skip cancel/replace this cycle"
            )
            return {
                "matched_full": 0,
                "expected": expected_now,
                "levels": [],
                "issues": ["orders_unreadable"],
                "orphans": [],
                "pending_prices": [],
                "live_qty": live_qty,
                "query_failed": True,
                "orders_unreadable": True,
            }
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
                issues.append(f"TP{lv['level']} @{lv['price']:.2f} ??")
            elif len(at_px) > 1:
                status = "duplicate"
                actual_qty = sum(o["qty"] for o in at_px)
                issues.append(f"TP{lv['level']} @{lv['price']:.2f} ?? {len(at_px)} ?")
            elif at_px[0]["qty"] != lv["qty"]:
                status = "qty_mismatch"
                actual_qty = at_px[0]["qty"]
                issues.append(
                    f"TP{lv['level']} {actual_qty}? ? ?? {lv['qty']}? "
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
            issues.append(f"???? @{o['price']:.2f} {o['qty']}?")

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
            "query_failed": query_failed,
        }

    def _format_audit_summary(self, audit):
        parts = []
        for lv in audit.get("levels", []):
            if lv["price"] <= 0:
                continue
            icon = "?" if lv["status"] == "ok" else "?"
            line = f"{icon}TP{lv['level']} {lv['qty']}?@{lv['price']:.2f}"
            if lv["status"] != "ok":
                line += f"({lv['status']})"
            parts.append(line)

        # ?? v1.0?tv_tps ????? TP ???????TV ????? / ???????
        if not parts:
            tv_tps = getattr(self, "tv_tps", None) or []
            if all((float(t or 0) <= 0) for t in tv_tps):
                parts.append("?? TP???????tv_tps=???")
            else:
                # ?? ?6.2?TP1/TP2 ???? TP3?????????????????
                parts.append("?? TP1/TP2?????TP3??????")

        if audit.get("issues"):
            parts.append("??:" + "; ".join(audit["issues"][:3]))
        return " | ".join(parts) if parts else "??? TP"

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
            logger.warning("??????????/??/?????????")
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
                logger.info(f"  ? TP{lv['level']} @ {px:.2f} ??? {at_px[0]['qty']}????")
                continue
            # ???????????????????
            for o in at_px:
                if o.get("orderId"):
                    deepcoin_client.cancel_order(self.symbol, ord_id=o["orderId"])
                    time.sleep(0.25)
            # ??????????????????????????
            time.sleep(0.3)
            orders_after = self._collect_tp_limit_orders()
            at_px_after = [o for o in orders_after if abs(o.get("px", o.get("price", 0)) - px) <= tolerance]
            if at_px_after:
                logger.warning(f"  ?? ????? TP@{px:.2f}???????")
                continue
            # v16.18???????????????50???
            allowed, guard_reason = self._check_tp_place_guard(lv["level"])
            if not allowed:
                logger.warning(
                    f"  ? ?????TP{lv['level']}@{px:.2f} | {guard_reason} | "
                    f"??{getattr(self, '_tp_place_guard_count', 0)}/{RECOVER_TP_PLACE_GUARD_MAX}"
                )
                continue
            logger.info(f"  + ?? TP{lv['level']} @ {px:.2f} qty={q}?")
            res = deepcoin_client.place_limit_order(
                self.symbol, close_side, pos_side, px, q, reduce_only=True,
            )
            # ?????reduceOnly ?? ? ????????
            if deepcoin_client.is_reduce_only_rejected(res):
                logger.warning(
                    f"?? ?? TP{lv['level']} reduceOnly ?? ? force_rest ????"
                )
                time.sleep(0.3)
                fresh = deepcoin_client.force_rest_get_all_positions(self.symbol)
                live_q = 0
                if fresh:
                    for fp in fresh:
                        if fp.get("posSide", "").lower() == pos_side:
                            live_q = self._safe_qty(fp.get("size", 0))
                            break
                if live_q > 0:
                    res = deepcoin_client.place_limit_order(
                        self.symbol, close_side, pos_side, px,
                        min(q, live_q), reduce_only=True,
                    )
            if res and deepcoin_client._is_success(res):
                placed += 1
                self._increment_tp_place_guard()
            time.sleep(0.4)
        return placed

    def _cancel_orphan_tp_orders(self, live_qty, tolerance=1.0):
        """
        v16.22??????????????? current_side ????? TP?
        ?????TV ?????????????????? TP?
        ????????????????? TP ????
        """
        audit = self._audit_tp_levels(live_qty, tolerance)
        cancelled = 0
        for o in audit["orphans"]:
            # v16.21???????????????????????
            # ???tv_tps ???????"??"????????????
            if getattr(self, "_recover_confirmed_levels", None):
                confirmed_levels = list((self._recover_confirmed_levels or {}).keys())
                if confirmed_levels:
                    logger.info(
                        f"??? Recovery???????????? {confirmed_levels}?"
                        f"?? @{o['price']:.2f} {o['qty']}?"
                    )
                    continue
            # v16.22????????????? current_side ????? TP
            # ???TV ?? SHORT ????????? LONG ? TP ? LONG ??? ? ???
            if not self._is_tp_order_directionally_valid(o):
                logger.info(
                    f"??? ??????????????@{o['price']:.2f} {o['qty']}? "
                    f"????? current_side={self.current_side} ????????"
                )
                continue
            if o.get("orderId"):
                deepcoin_client.cancel_order(self.symbol, ord_id=o["orderId"])
                cancelled += 1
                self._decrement_tp_place_guard("????")
                time.sleep(0.2)
        if cancelled:
            logger.info(f"?? ?? {cancelled} ??????")
        return cancelled

    def _is_tp_order_directionally_valid(self, order):
        """
        v16.22??? TP ??????? current_side ???
        - LONG ???????? sell???????
        - SHORT ???????? buy???????
        - side ?????????? True?
        """
        if not self.current_side or self.current_side not in ("LONG", "SHORT"):
            return True
        if not order:
            return True
        side = str(order.get("side", "") or "").strip().lower()
        if self.current_side == "LONG":
            return side == "sell"
        elif self.current_side == "SHORT":
            return side == "buy"
        return True

    def _pick_best_tp_order(self, orders, target_qty):
        if not orders:
            return None
        return min(orders, key=lambda o: abs(o["qty"] - target_qty))

    def _surgical_repair_tp_defenses(self, live_qty, entry, tolerance=1.0):
        """
        ?????????? ? ?? ? ??/??????????????
        v16.22 ???
        - ?????????? TP ??????? current_side ???????????
        - ????????????????????
        """
        live_qty = self._resolve_live_qty(live_qty)
        if live_qty <= 0:
            return self._audit_tp_levels(live_qty), 0

        close_side = "sell" if self.current_side == "LONG" else "buy"
        pos_side = "long" if self.current_side == "LONG" else "short"
        actions = 0
        audit = self._audit_tp_levels(live_qty, tolerance)

        # v16.22 ????????????? TP ??????? current_side ??
        for o in audit.get("levels", []):
            if o.get("status") in ("ok", "qty_mismatch") and o.get("price", 0) > 0:
                existing_orders = self._collect_tp_limit_orders()
                at_px = [ord for ord in existing_orders if abs(ord.get("price", 0) - o["price"]) <= tolerance]
                if at_px and not self._is_tp_order_directionally_valid(at_px[0]):
                    logger.warning(
                        f"??? [_surgical] ????????TP{o['level']}@{o['price']:.2f} "
                        f"side={at_px[0].get('side')} ? current_side={self.current_side} ????????"
                    )
                    continue
                if at_px:
                    self._mark_tp_level_confirmed(o["level"])

        # ????????????????????????????
        # ???_surgical_repair ?????????? cancel ?????????
        # ????? _scorched_earth ?????? TP ??
        # _rebuild_defenses ?????????? ? TP ?? ? ?????
        # ??????? "acked" ???? order_id ????????
        # ????????????????
        self._gc_stale_pending_defense_tags(save=True)
        time.sleep(0.2)

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
                    # v16.21 guard ???????????????
                    self._decrement_tp_place_guard("????")
                    time.sleep(0.2)
                logger.info(
                    f"?? ???? TP{lv['level']} @{price:.2f}?"
                    f"? {len(at_px) - 1} ? {keep['qty']} ?"
                )
                time.sleep(0.35)
                at_px = [keep]

            if len(at_px) == 1:
                if at_px[0]["qty"] != target_q:
                    deepcoin_client.cancel_order(self.symbol, ord_id=at_px[0]["orderId"])
                    actions += 1
                    # v16.21 guard ???????????????
                    self._decrement_tp_place_guard("????")
                    time.sleep(0.3)
                    # v16.18 ?????????
                    allowed, guard_reason = self._check_tp_place_guard(lv["level"])
                    if not allowed:
                        logger.warning(
                            f"  ? ?????TP{lv['level']}@{price:.2f} | {guard_reason}"
                        )
                    else:
                        res = deepcoin_client.place_limit_order(
                            self.symbol, close_side, pos_side, price, target_q,
                            reduce_only=True,
                        )
                        # ?????reduceOnly ?? ? ????????
                        if deepcoin_client.is_reduce_only_rejected(res):
                            logger.warning(
                                f"?? ?? TP{lv['level']} reduceOnly ?? ? force_rest ??"
                            )
                            time.sleep(0.3)
                            fresh = deepcoin_client.force_rest_get_all_positions(self.symbol)
                            live_q = 0
                            if fresh:
                                for fp in fresh:
                                    if fp.get("posSide", "").lower() == pos_side:
                                        live_q = self._safe_qty(fp.get("size", 0))
                                        break
                            if live_q > 0:
                                res = deepcoin_client.place_limit_order(
                                    self.symbol, close_side, pos_side, price,
                                    min(target_q, live_q), reduce_only=True,
                                )
                        if res and deepcoin_client._is_success(res):
                            actions += 1
                            self._increment_tp_place_guard()
                            logger.info(
                                f"?? ???? TP{lv['level']} @{price:.2f} ? {target_q} ?"
                            )
                    time.sleep(0.35)
                continue

            # v16.18 ?????????
            allowed, guard_reason = self._check_tp_place_guard(lv["level"])
            if not allowed:
                logger.warning(
                    f"  ? ?????TP{lv['level']}@{price:.2f} | {guard_reason}"
                )
                continue
            res = deepcoin_client.place_limit_order(
                self.symbol, close_side, pos_side, price, target_q, reduce_only=True,
            )
            # ?????reduceOnly ?? ? ????????
            if deepcoin_client.is_reduce_only_rejected(res):
                logger.warning(
                    f"?? ?? TP{lv['level']} reduceOnly ?? ? force_rest ??"
                )
                time.sleep(0.3)
                fresh = deepcoin_client.force_rest_get_all_positions(self.symbol)
                live_q = 0
                if fresh:
                    for fp in fresh:
                        if fp.get("posSide", "").lower() == pos_side:
                            live_q = self._safe_qty(fp.get("size", 0))
                            break
                if live_q > 0:
                    res = deepcoin_client.place_limit_order(
                        self.symbol, close_side, pos_side, price,
                        min(target_q, live_q), reduce_only=True,
                    )
            if res and deepcoin_client._is_success(res):
                actions += 1
                self._increment_tp_place_guard()
                # v16.21???????????????????????
                self._mark_tp_level_confirmed(lv["level"])
                logger.info(f"?? ???? TP{lv['level']} @{price:.2f} qty={target_q} ?")
            time.sleep(0.35)

        # v16.21?????????????????????????
        for lv in self._expected_tp_levels(live_qty):
            price = lv["price"]
            at_px = [
                o for o in self._collect_tp_limit_orders()
                if abs(o["price"] - price) <= tolerance
            ]
            if len(at_px) == 1 and at_px[0]["qty"] == lv["qty"]:
                self._mark_tp_level_confirmed(lv["level"])

        final = self._audit_tp_levels(live_qty, tolerance)
        if actions:
            logger.info(
                f"?? ???????? {actions} ? | "
                f"{final['matched_full']}/{final['expected']} | "
                f"{self._format_audit_summary(final)}"
            )
        return final, actions

    # ============================================================
    # v16.18?TP ??????
    # ============================================================

    def _reset_tp_place_guard(self):
        """?? TP ???????????/??????"""
        self._tp_place_guard_count = 0
        self._tp_place_guard_session_ts = time.time()

    # ?? v16.21?Recovery ??????????? ?????????????????????
    def _mark_tp_level_confirmed(self, level):
        """????? TP ?????????????"""
        self._recover_confirmed_levels = getattr(self, "_recover_confirmed_levels", {}) or {}
        self._recover_confirmed_levels[int(level)] = time.time()
        logger.info(f"??? Recovery???TP{int(level)} ????????????")

    def _is_tp_level_confirmed(self, level):
        """????? TP ????????????"""
        confirmed = getattr(self, "_recover_confirmed_levels", None) or {}
        return int(level) in confirmed

    def _clear_recover_confirmed_levels(self):
        """???????????????"""
        if getattr(self, "_recover_confirmed_levels", None):
            self._recover_confirmed_levels = {}
            logger.info("??? Recovery????????????")

    def _check_tp_place_guard(self, level=0):
        """
        ?? TP ???????????
        ?? (allowed, reason)?allowed=True = ??????False = ????
        v16.21?guard ?????????????????? TP ?????
        """
        now = time.time()
        # ?5??????????????
        if now - self._tp_place_guard_session_ts > 300:
            self._reset_tp_place_guard()
            logger.info("??? TP?? Guard ??????5?????")

        count = getattr(self, "_tp_place_guard_count", 0) or 0
        max_allowed = RECOVER_TP_PLACE_GUARD_MAX
        if count >= max_allowed:
            logger.warning(
                f"??? TP????????? {count} ? >= {max_allowed} ???"
                f"???????level={level}?| ?????"
            )
            return False, f"guard_limit:{count}>={max_allowed}"
        return True, ""

    def _increment_tp_place_guard(self):
        """???? TP ????"""
        self._tp_place_guard_count = (getattr(self, "_tp_place_guard_count", 0) or 0) + 1
        logger.info(
            f"??? TP???? +1 ? {self._tp_place_guard_count}/{RECOVER_TP_PLACE_GUARD_MAX}"
        )

    def _decrement_tp_place_guard(self, reason=""):
        """
        v16.21???/?? TP ???????????????????????
        guard ?? 0 ??_check_tp_place_guard ????????????
        ??? guard ??????????????
        """
        current = getattr(self, "_tp_place_guard_count", 0) or 0
        if current <= 0:
            return
        self._tp_place_guard_count = current - 1
        logger.info(
            f"??? TP???? -1 ? {self._tp_place_guard_count}/{RECOVER_TP_PLACE_GUARD_MAX}"
            + (f" ({reason})" if reason else "")
        )

    def _verify_tp_order_on_exchange(self, level, price, tolerance=1.0):
        """
        ????????????? TP ?????
        ?? (exists, actual_orders)?
        """
        price = round(float(price), 2)
        all_orders = list(deepcoin_client.get_pending_orders(self.symbol))
        tp_orders = []
        for o in all_orders:
            if not self._is_tp_limit_order(o):
                continue
            o_px = round(float(o.get("px", 0) or 0), 2)
            if abs(o_px - price) <= tolerance:
                tp_orders.append({
                    "orderId": o.get("ordId"),
                    "price": o_px,
                    "qty": self._safe_qty(o.get("sz")),
                })
        exists = len(tp_orders) >= 1
        return exists, tp_orders

    def _verify_live_tp_completeness(self, live_qty, initial_qty=None):
        """
        v16.18 ??????? live_qty ? expected_qty????? TP ????

        ???
        1. ????? TP ?? + live_qty ?? expected_initial
        2. ? saved_initial???????????
        3. ????? TP ????

        ?? {
            "live_qty": float,
            "inferred_initial": float or None,
            "inferred_consumed": list[int],
            "confidence": "high" | "medium" | "low",
            "discrepancy_pct": float,
            "needs_conservative_mode": bool,
        }
        """
        live_qty = self._safe_qty(live_qty)
        regime = self._tp_split_regime()
        ratios = self.regime_settings[regime]["ratios"]

        # ????????
        initial_guess = initial_qty if initial_qty and initial_qty > 0 else live_qty
        tp1_qty_theory = self._calculate_tp_quantities(initial_guess, ratios)[0]
        tp2_qty_theory = self._calculate_tp_quantities(initial_guess, ratios)[1]

        # ??????? TP2 ???TP1 ??/?????expected_live_qty = initial - tp2
        # ?? TP1 ???expected_live_qty = initial - tp1
        # ???????
        scenarios = {}
        for consumed_pattern, label in [
            ([], "none"),
            ([1], "tp1_only"),
            ([2], "tp2_only"),
            ([1, 2], "both"),
        ]:
            rem = live_qty
            if 1 in consumed_pattern:
                rem += tp1_qty_theory
            if 2 in consumed_pattern:
                rem += tp2_qty_theory
            scenarios[label] = {
                "consumed": consumed_pattern,
                "inferred_initial": rem,
                "label": label,
            }

        # ???? initial_qty ???
        saved_initial = self._safe_qty(
            initial_qty if initial_qty and initial_qty > 0 else getattr(self, "initial_qty", 0)
        )

        best = None
        best_diff = 99999
        for s in scenarios.values():
            diff = abs(s["inferred_initial"] - saved_initial) if saved_initial > 0 else 0
            if diff < best_diff:
                best_diff = diff
                best = s

        inferred_consumed = (best or scenarios["none"])["consumed"]
        inferred_initial = (best or scenarios["none"])["inferred_initial"]

        # ???????
        if saved_initial > 0:
            discrepancy_pct = abs(inferred_initial - saved_initial) / saved_initial
        else:
            discrepancy_pct = abs(inferred_initial - live_qty) / max(live_qty, 1)

        # ?????
        if discrepancy_pct <= 0.03:
            confidence = "high"
        elif discrepancy_pct <= RECOVER_TP_CONSERVATIVE_THRESHOLD:
            confidence = "medium"
        else:
            confidence = "low"

        needs_conservative = (
            confidence == "low"
            or discrepancy_pct > RECOVER_TP_CONSERVATIVE_THRESHOLD
            or saved_initial <= 0
        )

        logger.info(
            f"?? [TP??] live={live_qty} | saved_init={saved_initial} | "
            f"inferred_init={inferred_initial:.1f} | ??={discrepancy_pct:.1%} | "
            f"?????={inferred_consumed} | ??={confidence} | "
            f"????={'?' if needs_conservative else '?'}"
        )

        return {
            "live_qty": live_qty,
            "inferred_initial": inferred_initial,
            "inferred_consumed": list(inferred_consumed),
            "confidence": confidence,
            "discrepancy_pct": discrepancy_pct,
            "needs_conservative_mode": needs_conservative,
            "tp1_theory": tp1_qty_theory,
            "tp2_theory": tp2_qty_theory,
        }

    def _conservative_tp_recover(self, live_qty, entry, dynamic_sl=None, rounds=1):
        """
        v16.18 ?? TP ?????
        - ????? tp_levels_consumed??????
        - ??????? TP ?????????????
        - ?????????????
        - ??????????
        """
        live_qty = self._resolve_live_qty(live_qty)
        if live_qty <= 0:
            return {"placed": 0, "skipped": 0, "guarded": 0, "levels": []}

        close_side = "sell" if self.current_side == "LONG" else "buy"
        pos_side = "long" if self.current_side == "LONG" else "short"

        # ?????????????
        if not getattr(self, "_tp_place_guard_session_ts", 0) or \
           time.time() - self._tp_place_guard_session_ts > 300:
            self._reset_tp_place_guard()

        results = {"placed": 0, "skipped": 0, "guarded": 0, "levels": []}
        expected_levels = self._expected_tp_levels(live_qty)

        for lv in expected_levels:
            level = int(lv.get("level", 0))
            price = round(float(lv["price"]), 2)
            target_q = self._safe_qty(lv["qty"])
            if price <= 0 or target_q <= 0:
                continue

            # Step 1: ??????????????
            exists, exchange_orders = self._verify_tp_order_on_exchange(level, price)
            if exists:
                logger.info(
                    f"  ? [??] TP{level} @{price:.2f} ??????? "
                    f"({len(exchange_orders)} ?)???"
                )
                results["skipped"] += 1
                results["levels"].append({"level": level, "price": price, "status": "exists_exchange"})
                continue

            # Step 2: ????????
            allowed, guard_reason = self._check_tp_place_guard(level)
            if not allowed:
                logger.warning(
                    f"  ? [??] TP{level} @{price:.2f} ????????????? | {guard_reason}"
                )
                results["guarded"] += 1
                results["levels"].append({"level": level, "price": price, "status": "guarded"})
                continue

            # Step 3: ????????????????
            time.sleep(RECOVER_TP_VERIFY_DELAY_SEC)
            exists2, _ = self._verify_tp_order_on_exchange(level, price)
            if exists2:
                logger.info(
                    f"  ? [??] TP{level} @{price:.2f} ???????????"
                )
                results["skipped"] += 1
                results["levels"].append({"level": level, "price": price, "status": "exists_double_check"})
                continue

            # Step 4: ??
            logger.info(
                f"  + [??] TP{level} @{price:.2f} qty={target_q}? "
                f"??????????????"
            )
            res = deepcoin_client.place_limit_order(
                self.symbol, close_side, pos_side, price, target_q, reduce_only=True,
            )
            # ?????reduceOnly ?? ? ????????
            if deepcoin_client.is_reduce_only_rejected(res):
                logger.warning(
                    f"?? [??] TP{level} reduceOnly ?? ? force_rest ????"
                )
                time.sleep(0.3)
                fresh = deepcoin_client.force_rest_get_all_positions(self.symbol)
                live_q = 0
                if fresh:
                    for fp in fresh:
                        if fp.get("posSide", "").lower() == pos_side:
                            live_q = self._safe_qty(fp.get("size", 0))
                            break
                if live_q > 0:
                    res = deepcoin_client.place_limit_order(
                        self.symbol, close_side, pos_side, price,
                        min(target_q, live_q), reduce_only=True,
                    )
            if res and deepcoin_client._is_success(res):
                self._increment_tp_place_guard()
                results["placed"] += 1
                results["levels"].append({"level": level, "price": price, "status": "placed"})
                logger.info(f"  ? [??] TP{level} ????")
            else:
                results["levels"].append({"level": level, "price": price, "status": "failed"})
                logger.warning(f"  ? [??] TP{level} ????: {res}")
            time.sleep(0.5)

        logger.info(
            f"?? [??TP??] live={live_qty}? | "
            f"??={results['placed']} | ??={results['skipped']} | "
            f"??={results['guarded']} | "
            f"????={getattr(self, '_tp_place_guard_count', 0)}/{RECOVER_TP_PLACE_GUARD_MAX}"
        )
        return results

    def _cancel_stop_orders(self, scope="all"):
        cancelled = 0
        # v16.15?????????????????????????? REST
        self._shield_cancelled_ids = set()
        for t in deepcoin_client.get_trigger_orders_pending(self.symbol):
            if scope == "radar" and not self._is_radar_trigger_order(t):
                continue
            if scope == "shield" and not self._is_shield_trigger_order(t):
                continue
            oid = str(t.get("ordId") or "").strip()
            if not oid:
                continue
            # v16.15????????????????????????
            if oid in self._shield_cancelled_ids:
                continue
            deepcoin_client.cancel_trigger_order(self.symbol, oid)
            self._shield_cancelled_ids.add(oid)
            # ?????????????? sltp ??
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
        """??????? exclusively ?? TV tv_sl"""
        return None

    def _shield_stop_price(self, entry=None):
        """VPS ???????tv_sl ????? VPS ?????"""
        tv = round(float(getattr(self, "tv_sl", 0) or 0), 2)
        return tv if tv > 0 else None

    def _refresh_vps_hard_sl(self, entry=None, side=None, regime=None, atr=None,
                             tv_sl_ref=None, source=""):
        """
        ?? v1.0 ?3???? = |TV.price ? TV.stop_loss| ? 1.15?
        ??????????% ????
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
            f"[{self.symbol}] VPS?????@{hard:.2f} (source={source})"
        )
        return True

    def _defense_buffer_mult(self):
        """????????? 1.15??? 3.4??? adx_tier ???"""
        return float(TEMP_STOP_BUFFER_MULT)

    def _temp_hard_stop_from_tv(self, entry=None, side=None, tv_sl=None):
        """
        ????????? v1.0 ?3??
          dist = |TV? ? TV.SL| ? 1.15???????????
          ?????????? 1.5?ATR / ???2 ????
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
        Spec v1.0 3.3: |TV.price - TV.stop_loss| x 1.15 -> frozen_hard_sl_px.
        TV-only, no ATR fallback (binance parity, c0f9275/b3b4afd).
        """
        cur = float(self._frozen_hard_px() or 0)
        if cur > 0:
            return cur
        hard = float(self._temp_hard_stop_from_tv(entry=entry, side=side) or 0)
        if hard <= 0:
            logger.error(
                f"[{self.symbol}] hard stop unavailable, missing TV.stop_loss | {source}"
            )
            return 0.0

        # binance c0f9275/b3b4afd parity: ATR-based widening fallback removed.
        # Hard stop is TV-only; a short TV distance is honored as-is instead of
        # being force-widened to 0.5xATR (that fallback caused cancel/replace
        # death spirals on the binance side and is banned by spec v1.0 3.3).

        self.frozen_hard_sl_px = float(hard)
        try:
            self._save_state()
        except Exception:
            pass
        logger.info(
            f"[{self.symbol}] ???????@{hard:.2f} "
            f"(TV.sl_ref={float(getattr(self, 'tv_sl_ref', 0) or 0):.2f} "
            f"buffer={float(self._defense_buffer_mult()):.2f}) | {source}"
        )
        return float(hard)

    def _frozen_hard_px(self):
        return round(float(getattr(self, "frozen_hard_sl_px", 0) or 0), 2)

    def _hard_stop_distance_meta(self, fill=None, tv_sl=None, tv_entry=None, atr=None):
        """??/???????????"""
        fill = float(fill if fill is not None else (self.watched_entry or 0))
        tv_entry = float(tv_entry if tv_entry is not None else (getattr(self, "tv_price", 0) or fill))
        tv_sl = float(tv_sl if tv_sl is not None else (getattr(self, "tv_sl_ref", 0) or 0))
        buf = float(self._defense_buffer_mult())
        return compute_hard_stop_distance(tv_entry, tv_sl, fill, 0.0, tv_mult=buf)

    def _apply_tv_sl_from_payload(self, payload, source=""):
        """
        ?? v1.0 ?3?TV.stop_loss ????????
        ?? = |TV.price ? TV.stop_loss| ? 1.15?
        ??????????%?? VPS ?????
        """
        tv_ref = payload.get("tv_sl")
        if tv_ref is None or tv_ref == "":
            return self._lock_frozen_hard_sl_from_tv(source=source or "??")
        ref_px = round(self._safe_float(tv_ref, 0), 2)
        if ref_px <= 0:
            return self._lock_frozen_hard_sl_from_tv(source=source or "TV??")
        entry = float(self.tv_price or self.watched_entry or 0)
        side = str(payload.get("action") or payload.get("side") or self.current_side or "").upper()
        if side not in ("LONG", "SHORT"):
            side = self.current_side
        self.tv_sl_ref = ref_px
        return self._lock_frozen_hard_sl_from_tv(entry=entry, side=side, source=source or "TV??")

    def _locked_initial_atr(self):
        """?? ATR ????? webhook open_atr??????"""
        atr = float(getattr(self, "open_atr", 0) or 0)
        if atr > 0:
            return atr
        atr = float(getattr(self, "current_atr", 0) or 0)
        return atr if atr > 0 else 0.0

    def _atr_1h_engine(self):
        """????ATR ?? TV webhook?"""
        return None

    def _refresh_breathing_coefficient(self, force=False):
        """?????? 1.0?ATR ?? TV ????"""
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
        ????? LATE_CLOSE_SUPPRESS_SEC ???? CLOSE ? ???
        ????????_close_open_chain_active?????
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
        if pos == "QUERY_FAILED":
            logger.warning("?? _should_ignore_late_close ??????? ? ??????")
            return False
        if not pos or float(pos.get("size") or 0) <= 0:
            return False
        return True

    def _apply_breath_stop_tick(self, curr_px=0.0):
        """??/?? tick????????? ADX??"""
        entry = float(self.watched_entry or 0)
        side = str(self.current_side or "").strip().upper()
        if entry <= 0 or side not in ("LONG", "SHORT"):
            return None
        atr = self._locked_initial_atr()
        profile = getattr(self, "breath_profile", None)
        # ?5.2/?5.3???????????
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
        """????????? TV ?????"""
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
        """?/?? TV ???????????????????"""
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
            reason=reason or f"TV??? @ {target:.2f}",
            force=True,
        )
        if ok:
            self._last_applied_tv_sl = target
            self._save_state()
            tv_floor = round(float(getattr(self, "tv_sl", 0) or 0), 2)
            logger.warning(
                f"??? [TV???] {reason or '????'} | {live_qty} ? @ {target:.2f} "
                f"| tv_sl={tv_floor or 'fallback'}"
            )
        return {"ok": ok, "skipped": False, "target": target}

    def _handle_tv_sl_update(self, payload):
        """UPDATE_SL????? tv_sl??????????????"""
        side = str(payload.get("side") or "").strip().upper()
        if not self._apply_tv_sl_from_payload(payload, source="UPDATE_SL"):
            logger.warning("UPDATE_SL ?????? tv_sl")
            return

        pos = self._get_active_position()
        if pos == "QUERY_FAILED":
            logger.warning("UPDATE_SL ?????????? ? ????? tv_sl")
            return
        if not pos or self._safe_qty(pos.get("size", 0)) <= 0:
            logger.info("UPDATE_SL ???????? ? ????? tv_sl")
            return
        pos_side = "LONG" if pos.get("posSide") == "long" else "SHORT"
        if side and side != pos_side:
            logger.warning(f"UPDATE_SL side={side} ??? {pos_side} ??????")
            return

        result = self._sync_tv_sl_stop(
            pos["size"],
            reason=f"TV UPDATE_SL @ {self.tv_sl:.2f}",
            force=True,
        )
        if result.get("skipped") and result.get("reason") == "idempotent":
            logger.info(f"UPDATE_SL ???? tv_sl={self.tv_sl:.2f} ????")
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
                + (f" | ?? {radar_sl:.2f}" if radar_sl else "")
                + f" | ?? {live_qty} ? @ {self.watched_entry:.2f}"
            )
            if not verified:
                verify_note += f" | {telegram_notify.VERIFY_DELAY_MARK}"
            self._call_telegram_notify(
                telegram_notify.report_tv_sl_updated,
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
            logger.warning(
                f"[?????] TV??????? | UPDATE_SL tv_sl={self.tv_sl:.2f} | ?????????????"
            )

    def _place_tp_levels_only(self, live_qty, retries=2):
        """
        ?? v1.0 ?8-9??? TP ????? newClientOrderId???????
        ??????????? ? ?????????
        ?? TP1+TP2?TP3 ??????
        """
        close_side = "sell" if self.current_side == "LONG" else "buy"
        pos_side = "long" if self.current_side == "LONG" else "short"
        live_qty = self._resolve_live_qty(live_qty)
        if live_qty <= 0:
            return 0

        # ????????????TP??????????????
        # ??????????????
        self._cancel_all_tp_limit_orders(max_rounds=4)
        time.sleep(1.0)  # ??????
        # ?????????
        existing_before = self._collect_tp_limit_orders()
        if existing_before:
            logger.warning(
                f"[{self.symbol}] ??TP????{len(existing_before)}??????????..."
            )
            time.sleep(2.0)
            existing_before = self._collect_tp_limit_orders()
            if existing_before:
                prices = [f"@{o['price']:.2f}" for o in existing_before]
                logger.error(
                    f"[{self.symbol}] ?TP???????????: {prices}"
                )
                return 0

        placed = 0
        for lv in self._expected_tp_levels(live_qty):
            level = int(lv.get("level") or 0)
            if level >= 3:
                continue  # TP3 ?????
            kind = f"TP{level}"
            q, px = float(lv["qty"] or 0), float(lv["price"] or 0)
            if q <= 0 or px <= 0:
                continue

            # ????????????????????????TP??
            existing_orders = self._collect_tp_limit_orders()
            at_px_existing = [o for o in existing_orders if abs(o.get("price", 0) - px) <= 1.0]
            if at_px_existing:
                logger.warning(
                    f"[{self.symbol}] ?????? TP@{px:.2f} ({len(at_px_existing)}?)???????"
                )
                # ???????????????
                existing = at_px_existing[0]
                existing_oid = str(existing.get("ordId") or existing.get("orderId") or "")
                if existing_oid:
                    tag = make_defense_client_order_id(self.symbol, kind, px)
                    self._register_pending_defense_tag(tag, kind, price=px, order_id=existing_oid)
                    self._save_state()
                continue

            # ????????? v1.0 ?8-9?
            # ????????????????????????????????
            blocked, tag0, meta0 = self._has_open_pending_defense_tag(kind)
            if blocked:
                logger.warning(
                    f"[{self.symbol}] ??????? tag={tag0} kind={kind} ? "
                    f"???????????? | px={px:.2f}"
                )
                # ? order_id ? ?????
                if meta0.get("order_id"):
                    if not self._confirm_stale_before_clear(tag0, meta0):
                        logger.warning(
                            f"[{self.symbol}] ?? {tag0} ????????? ? ?????????"
                        )
                        continue
                    self._complete_pending_defense_tag(tag=tag0)
                    self._save_state()
                    time.sleep(0.15)
                else:
                    # ? order_id????????
                    age = time.time() - float(meta0.get("ts", 0) or 0)
                    if age >= 45.0:
                        logger.warning(
                            f"[{self.symbol}] ?? {tag0} ? order_id ??? {age:.0f}s ? 45s ? ????"
                        )
                        self._complete_pending_defense_tag(tag=tag0)
                        self._save_state()
                        time.sleep(0.15)
                    else:
                        logger.warning(
                            f"[{self.symbol}] ?? {tag0} ? order_id ?? {age:.0f}s < 45s ? "
                            f"??????????????????"
                        )
                        continue

            tag = make_defense_client_order_id(self.symbol, kind, px)
            self._register_pending_defense_tag(tag, kind, price=px)
            try:
                self._save_state()
            except Exception:
                pass
            last = None
            # v16.11????????TP ????????? 3 ?
            max_retries = 3
            for attempt in range(max_retries):
                # v16.18 ????????
                allowed, guard_reason = self._check_tp_place_guard(level)
                if not allowed:
                    logger.warning(
                        f"? _place_tp_levels_only ???TP{level}@{px:.2f} | {guard_reason}"
                    )
                    break
                res = deepcoin_client.place_limit_order(
                    self.symbol, close_side, pos_side, px, q,
                    reduce_only=True, cl_ord_id=tag,
                )
                # ?????reduceOnly ????? ? ????? ? ?????
                if deepcoin_client.is_reduce_only_rejected(res):
                    logger.warning(
                        f"?? TP{level} reduceOnly ?? {close_side} {q}? ? "
                        f"force_rest ??????????"
                    )
                    time.sleep(0.3)
                    fresh = deepcoin_client.force_rest_get_all_positions(self.symbol)
                    live_qty = 0
                    if fresh:
                        for fp in fresh:
                            if fp.get("posSide", "").lower() == pos_side:
                                live_qty = self._safe_qty(fp.get("size", 0))
                                break
                    if live_qty <= 0:
                        logger.info(f"TP{level} ??????????")
                        break
                    # ???????????? level/px/pos_side/tag ???
                    new_q = min(q, live_qty)
                    logger.info(f"?? TP{level} ???? {q}?{new_q}????={live_qty}?")
                    q = new_q
                    # ???????????
                    res = deepcoin_client.place_limit_order(
                        self.symbol, close_side, pos_side, px, q,
                        reduce_only=True, cl_ord_id=tag,
                    )
                    if deepcoin_client.is_reduce_only_rejected(res):
                        logger.error(f"? TP{level} ????? ? ????")
                        break
                if res and deepcoin_client._is_success(res):
                    last = res
                    self._increment_tp_place_guard()
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
                    f"[{self.symbol}] UPDATE_TP ? TP{level} @ {px:.2f} "
                    f"?????????max_retries={max_retries}?"
                )
                continue
            oid = str(last.get("orderId") or last.get("algoId") or "")
            self._register_pending_defense_tag(tag, kind, price=px, order_id=oid)
            placed += 1
            logger.info(f"?? UPDATE_TP ? TP{level} {q} @ {px:.2f} tag={tag}")
            time.sleep(0.25)
        return placed

    def _handle_tv_tp_update(self, payload):
        """
        UPDATE_TP?v6.9.108 ????????
        ????? TP123???????? / ???
        """
        side = str(payload.get("side") or "").strip().upper()
        new_tps = self._sanitize_tp_prices([
            self._safe_float(payload.get("tv_tp1"), 0),
            self._safe_float(payload.get("tv_tp2"), 0),
            self._safe_float(payload.get("tv_tp3"), 0),
        ])
        if sum(1 for t in new_tps if t > 0) < 3:
            logger.warning(f"UPDATE_TP ???TP ?? {new_tps}")
            return

        pos = self._get_active_position()
        if pos == "QUERY_FAILED":
            logger.warning("UPDATE_TP ?????????? ? ??")
            return
        if not pos or self._safe_qty(pos.get("size", 0)) <= 0:
            logger.info("UPDATE_TP ???????? ? ??")
            return

        pos_side = self._pos_side_label(pos)
        if side and side != pos_side:
            logger.warning(f"UPDATE_TP side={side} ??? {pos_side} ??????")
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
                f"UPDATE_TP ?????? {pos_side} entry={entry:.2f} tps={new_tps} ? ??"
            )
            logger.warning(
                f"[?????] UPDATE_TP ??? | {pos_side} entry `{entry:.2f}` | ?TP {new_tps} ???????"
            )
            return

        tp1 = float(new_tps[0])
        if curr_px > 0:
            if pos_side == "LONG" and tp1 <= curr_px:
                logger.warning(
                    f"UPDATE_TP ?TP1={tp1:.2f} ? ?? {curr_px:.2f} ? ?????????"
                )
                return
            if pos_side == "SHORT" and tp1 >= curr_px:
                logger.warning(
                    f"UPDATE_TP ?TP1={tp1:.2f} ? ?? {curr_px:.2f} ? ?????????"
                )
                return

        old_tps = list(getattr(self, "_prev_tv_tps_before_update", None) or self.tv_tps or [])
        self.tv_tps = new_tps
        self._save_state()

        # TP???????????? ?5.1?????????TP1/TP2?
        self._unblock_radar_activation()

        same_ledger = (
            len(old_tps) >= 3
            and all(
                abs(float(old_tps[i] or 0) - float(new_tps[i] or 0)) <= 0.51
                for i in range(3)
            )
        )
        audit_before = self._audit_tp_levels(live_qty)
        if same_ledger and self._tp_audit_ok(audit_before):
            logger.info(f"UPDATE_TP ?????TP ?? {new_tps}")
            return

        cancelled = self._cancel_all_tp_limit_orders(max_rounds=4)
        time.sleep(1.5)  # ??????????????????
        leftover = self._collect_tp_limit_orders()
        if leftover:
            logger.warning(
                f"UPDATE_TP ?? TP ???? {len(leftover)}?? ??????"
            )
            # ??????????????????
            self._cancel_all_tp_limit_orders(max_rounds=3)
            time.sleep(1.5)
            leftover = self._collect_tp_limit_orders()
            if leftover:
                logger.error(
                    f"UPDATE_TP ?? TP ????? {len(leftover)}?? ??????????"
                )
                logger.warning(
                    f"[?????] UPDATE_TP ???? | ??? TP ??? {len(leftover)} ??????????/????"
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
            f"?? UPDATE_TP | {old_tps} ? {new_tps} | "
            f"?? {cancelled} | ?? {placed} | "
            f"?? {audit.get('matched_full', 0)}/{audit.get('expected', 0)} | "
            f"?? {live_qty} ? @ {entry:.2f} | "
            f"?? {curr_px:.2f} | ???/?????"
        )
        logger.info(f"?? UPDATE_TP ?? verified={verified} | {verify_note}")
        self._call_telegram_notify(
            telegram_notify.report_tv_tp_updated,
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
            logger.warning(
                f"[?????] UPDATE_TP ????? | {self._format_audit_summary(audit)} | ???????"
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
        """FAVORABLE=???/??? | SHIELD=??TV???"""
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
            f"?????? @ {new_sl:.2f} ???"
            + (f" | ?? TV??? @ {stop_px:.2f}" if stop_px else "")
            + f" | ?? {real_amt} ?"
        )
        if not sl_verified:
            verify_note += f" | {telegram_notify.VERIFY_DELAY_MARK}"
        self._call_telegram_notify(
            telegram_notify.report_shield_disarmed,
            side=self.current_side,
            live_qty=real_amt,
            entry=self.watched_entry,
            cancelled_count=max(cancelled_hint, 1),
            reason=reason or "???? ? ?????? tv_sl",
            radar_progress=progress,
            verify_note=verify_note,
        )
        self._shield_handoff_notified = True
        self._save_state()

    def _perform_radar_handoff(self, real_amt, curr_px, reason=""):
        """
        ??? v1.0 ? ?5.1 ???????
        ????TP1-TP2?????????TP2??????????????
        ???? TP1 ????
        """
        real_amt = float(self._resolve_live_qty(real_amt) or 0)
        if real_amt <= 0:
            return False
        if getattr(self, "_open_in_progress", False) or getattr(
            self, "_defense_align_in_progress", False
        ):
            logger.info(f"?? ?????????/????? | {reason or ''}")
            return False
        # ?? ?5.1???????TP1-TP2???????TP2????
        if not self._should_radar_trail(curr_px):
            gate = float(self._radar_activation_price() or 0)
            logger.info(
                f"?? ??????????????? | ??={float(curr_px or 0):.2f} "
                f"| ??={gate:.2f} | {reason or ''}"
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
                f"?? ????????? {safe_sl:.2f} ??? {curr_px:.2f} "
                f"?? {gap:.2f} USDT??? tv_sl ????"
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
                f"?? ????????? @ {safe_sl:.2f} ?????? tv_sl ???"
            )
            if had_tv_shield and old_tv:
                self._maintain_hard_shield(real_amt, curr_px, force=True)
            return False

        if had_tv_shield:
            # TODO(v13.81+): ?????????????? tv_sl/?????????????
            # Deepcoin ? _disarm_shield ? ????? STOP ????????????
            self._disarm_shield("???? ? ????", notify=False)

        logger.info(
            f"?? ????????? @ {safe_sl:.2f} | best={self.best_price:.2f} | "
            f"?? {curr_px:.2f}"
        )
        if had_tv_shield and not getattr(self, "_shield_handoff_notified", False):
            self._notify_shield_handoff_to_radar(
                real_amt, curr_px, safe_sl,
                reason=reason or "???? ? ?????? tv_sl",
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
            real_amt, curr_px, reason=reason or "????",
        )
        return 1 if ok else 0

    def _should_disarm_shield_for_favorable(self, curr_px):
        """TP1 ???????? ? ?? tv_sl ???????TP1 ????????
        TODO(v13.81+): ?? _should_disarm_shield_for_favorable ?? False????????
        ???? True ??? _perform_radar_handoff ? _disarm_shield?????????"""
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
        ????????????VPS?+ TV tv_sl ??????????????
        ??????? tv_sl?UPDATE_SL ???????????????
        """
        self._disarm_premature_radar(real_amt, curr_px, source="????")
        if self._resolve_defense_regime(curr_px) == "FAVORABLE":
            if self._should_radar_trail(curr_px) or self._is_radar_active():
                self._process_radar_trailing(real_amt, curr_px)
        self._maintain_hard_shield(real_amt, curr_px)

    def _should_activate_shield(self, curr_px):
        """???? TV ?????????????"""
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
        # v16.15??????????? set-position-sltp ???????? True
        # ????????????????????
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
            # v16.15????????????????????????
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

        # v16.15????????????? set-position-sltp ? order_id?
        # ??????????? ? ???????? Deepcoin ???????????????
        # ???order_id ?? + ?????? + ??5?????????????
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
                # v16.15???????????? set-position-sltp ?????????????
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
                # v16.15????? set-position-sltp ???????????????
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
        """? worker ??????????????????????/???"""
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
                        f"?? ???????? (?? {info} ???, {age:.0f}s ?)"
                    )
                    return False
                if age < RECOVER_LOCK_TTL_SEC and not holder_alive:
                    logger.info(
                        f"?? ??????? (? {info})?????????"
                    )
            with open(RECOVER_LOCK_FILE, "w", encoding="utf-8") as f:
                f.write(f"pid={os.getpid()} ts={datetime.now().isoformat()}")
            return True
        except Exception as e:
            logger.warning(f"recover singleton lock: {e}")
            return True

    def _build_recover_health_report(self, pos, curr_px, tp_audit, shield_audit=None):
        """??????????? + TV + TP123 + ??? + ??/??????"""
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
            pnl_label = f"?????? (?? {radar_progress:.0%})"
            defense_plan = "?????? + TV????? (??)"
        elif adverse > 0.001:
            pnl_label = f"?? {adverse:.1%}"
            defense_plan = "?? TP123 + TV?????"
        elif favorable > 0.001:
            pnl_label = f"?? {favorable:.1%}???????"
            defense_plan = "?? TP123 + TV??? (?TP1???)"
        else:
            pnl_label = "????"
            defense_plan = "?? TP123 + TV???"

        stop_px = self._shield_stop_price(entry)
        if should_radar or radar_active:
            radar_sl = (
                self._clamp_radar_to_tv_floor(self.current_sl)
                if self._is_radar_active() else None
            )
            shield_status = (
                f"TV?? @ {stop_px:.2f}" if stop_px else "TV?????"
            )
            if radar_sl:
                shield_status += f" | ?? @ {radar_sl:.2f}"
        elif shield_ok:
            shield_status = f"?? @ {stop_px:.2f}" if stop_px else "???"
        else:
            shield_status = (
                f"??? @ {stop_px:.2f}" if stop_px
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
        """????????TV tv_sl ??? + ????????????"""
        actions = []
        if health.get("should_radar") or health.get("radar_active"):
            if not self._is_radar_active():
                self._refresh_radar_state_on_recover(curr_px, self.watched_entry)
            sl = self._clamp_radar_to_tv_floor(self.current_sl) if self._is_radar_active() else None
            if sl and not self._has_trigger_sl_near(sl):
                if self._ensure_radar_sl(real_amt, sl):
                    actions.append(f"????@{sl:.2f}")
                else:
                    actions.append(f"??????@{sl:.2f}")
            elif sl:
                actions.append(f"??????@{sl:.2f}")

        ok = self._maintain_hard_shield(real_amt, curr_px, force=True)
        stop_px = self._shield_stop_price()
        tv_note = (
            "TV???"
            if getattr(self, "tv_sl", 0) > 0
            else "TV tv_sl ??"
        )
        tag = f"{tv_note}@{stop_px:.2f}" if stop_px else tv_note
        actions.append(f"{tag}??" if ok else f"{tag}??")
        return actions

    def _bootstrap_live_defenses_after_recover(self, real_amt, curr_px, audit=None):
        """
        ??/??????????? TP123+?? ? ??????? ? ?????????
        v16.21 ???TP ????????????? 5 ??????????????
        """
        if real_amt <= 0 or not self.current_side:
            return {"actions": [], "audit": audit or {}}

        curr_px = float(curr_px or deepcoin_client.get_current_price(self.symbol) or 0)
        actions = []

        # ?? v16.21?????????? 5 ? ?????????????????????????
        RECOVER_TP_RETRY_ROUNDS = 5
        RECOVER_TP_RETRY_GAP_SEC = 5.0
        final_audit = audit
        for retry_round in range(RECOVER_TP_RETRY_ROUNDS):
            try:
                if final_audit is None:
                    final_audit = self._audit_tp_levels(real_amt)

                if self._tp_audit_ok(final_audit):
                    logger.info(
                        f"?? [??????] TP ????? "
                        f"({final_audit.get('matched_full', 0)}/{final_audit.get('expected', 0)})???????"
                    )
                    break

                if retry_round > 0:
                    logger.warning(
                        f"?? [??????] TP ????? "
                        f"({final_audit.get('matched_full', 0)}/{final_audit.get('expected', 0)})?"
                        f"? {retry_round + 1}/{RECOVER_TP_RETRY_ROUNDS} ?????? {RECOVER_TP_RETRY_GAP_SEC:.0f}s"
                    )
                    time.sleep(RECOVER_TP_RETRY_GAP_SEC)

                repaired, n_actions = self._surgical_repair_tp_defenses(
                    real_amt, self.watched_entry,
                )
                if n_actions > 0:
                    actions.append(f"????TP({n_actions}?)")
                    final_audit = repaired
                else:
                    # ???? TP ??????????????
                    if not self._tp_audit_ok(final_audit):
                        patched = self._patch_missing_tp_levels(real_amt)
                        if patched > 0:
                            actions.append(f"????TP({patched}?)")
                        final_audit = self._audit_tp_levels(real_amt)
            except Exception as e:
                logger.error(f"??????? {retry_round + 1} ???: {e}")
                final_audit = final_audit or self._audit_tp_levels(real_amt)
        # ?? ???????? ???????????????????????????????????????

        try:
            self._refresh_radar_state_on_recover(curr_px, self.watched_entry)
            health = self._build_recover_health_report(
                {"side": self.current_side, "size": real_amt, "entry_price": self.watched_entry},
                curr_px, final_audit,
            )
            actions.extend(self._apply_recover_defense_policy(real_amt, curr_px, health))

            if curr_px > 0 and (health.get("should_radar") or health.get("radar_active")):
                self._process_radar_trailing(real_amt, curr_px)
                sl = self._radar_sl_to_pass()
                if sl and not self._has_trigger_sl_near(sl):
                    if self._ensure_radar_sl(real_amt, sl):
                        actions.append(f"??SL@{sl:.2f}")
                if self._is_radar_active() and not getattr(self, "_radar_activation_notified", False):
                    self._report_radar_first_activation(
                        real_amt, curr_px, self._clamp_radar_to_tv_floor(self.current_sl),
                        self._has_trigger_sl_near(self.current_sl),
                    )
                actions.append(f"???????{health.get('radar_progress', 0):.0%}")

            self._radar_guardian_audit(real_amt, curr_px)
        except Exception as e:
            logger.error(f"??????????(????): {e}")
            actions.append(f"????:{e}")
            final_audit = final_audit or self._audit_tp_levels(real_amt)
            health = {}

        self._post_recover_radar_pulse = True
        self._save_state()
        logger.info(
            f"?? [??????] {' ? '.join(actions) if actions else '?????????'} | "
            f"TP {final_audit.get('matched_full', 0)}/{final_audit.get('expected', 0)}"
        )
        return {"actions": actions, "audit": final_audit, "health": health}

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
                f"??? ????? TV?????"
                + (f" @ {stop_px:.2f}" if stop_px else "")
                + "?????"
            )
            self._save_state()
            return

        if audit["status"] == "duplicate":
            purged = self._purge_shield_stop_orders(audit["tier_prices"])
            self._record_shield_maintain(success=False)
            logger.warning(
                f"??? ?????????? {purged} ?????????????"
            )
            self.shield_active = True
            self._save_state()
            return

        if curr_px > 0 and self._should_activate_shield(curr_px):
            self.shield_active = True
            logger.info(
                "??? ???TV???????????????????"
            )
            self._save_state()

    def _disarm_shield(self, reason="", notify=False):
        # v16.15?????????? REST ???????? pending ????
        # ????????? ID + ??????????? REST ??
        all_pending = deepcoin_client.get_trigger_orders_pending(self.symbol)
        cancelled = 0

        # ???????? shield ??
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

        # ?????? ID ???????? shield?? exchange ??????????
        _local_ord = str(getattr(self, "_shield_sltp_ord_id", "") or "").strip()
        _local_set = float(getattr(self, "_shield_sltp_set_at", 0) or 0)
        still_have_local = (
            _local_ord
            and _local_ord not in self._shield_cancelled_ids
            and (time.time() - _local_set) < 300
        )

        # ?????????? shield ??
        still_on_exchange = False
        for t in all_pending:
            if not self._is_shield_trigger_order(t):
                continue
            oid = str(t.get("ordId") or "").strip()
            if oid and oid not in self._shield_cancelled_ids:
                still_on_exchange = True
                break

        if still_on_exchange or still_have_local:
            # ????????????????????
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
            logger.info(f"??? [?????] {reason} | ?? {cancelled} ? TV???")
        if notify and cancelled > 0 and live_qty > 0:
            progress = 0.0
            try:
                curr_px = deepcoin_client.get_current_price(self.symbol) or 0
                progress = self._radar_activation_progress(curr_px)
            except Exception:
                curr_px = 0
            self._call_telegram_notify(
                telegram_notify.report_shield_disarmed,
                side=self.current_side,
                live_qty=live_qty,
                entry=entry,
                cancelled_count=cancelled,
                reason=reason,
                radar_progress=progress,
                verify_note=(
                    f"? {cancelled} ? TV??? | "
                    + (
                        "????????????"
                        if self._is_radar_active()
                        else f"???? {progress:.0%}?TP1???????"
                    )
                ),
            )

    def _place_shield_stops(self, live_qty, entry=None, reason="", force=False,
                            recover_mode=False, suppress_alert=False):
        """
        ?? set-position-sltp ????????????????????
        ?? Deepcoin API ???set-position-sltp ??????????????????
        ???????slOrdPx=-1????????????????????
        """
        entry = float(entry or self.watched_entry or 0)
        live_qty = self._resolve_live_qty(live_qty)
        if live_qty <= 0 or entry <= 0 or not self.current_side:
            return False

        # ???????????????????????????
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
                        f"??? ???@{stop_px:.2f} ????@{curr_px:.2f}???"
                        f"???????????"
                    )
                    self.shield_active = True
                    self.shield_sized_qty = live_qty
                    self._save_state()
                    return True

        pos_side = "long" if self.current_side == "LONG" else "short"
        stop_px = self._shield_stop_price()
        if not stop_px or stop_px <= 0:
            logger.warning(f"??? ?????????????")
            return False

        # ???????????????
        exchange_sl = float(order_stop_price(
            self.current_side, stop_px,
            profile=getattr(self, "breath_profile", None)
        ) or stop_px)

        # v16.15?????????????????????15s???????
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
                f"??? [TV???] ???? | {live_qty} ? @ {exchange_sl:.2f} | "
                f"ordId={_local_ord} | {_last_set > 0 and (time.time() - _last_set):.1f}s????"
            )
            return True

        # v16.26 ????????????????????? _local_ord ?????????
        self._purge_shield_stop_orders(tier_prices=None)
        for t in deepcoin_client.get_trigger_orders_pending(self.symbol):
            oid = str(t.get("ordId") or "").strip()
            if not oid:
                continue
            t_pos_side = str(t.get("posSide", "")).strip()
            t_ord_type = str(t.get("ordType", "")).strip()
            is_shield = t_ord_type in ("stop", "trigger", "conditional") and t_pos_side == pos_side
            if is_shield and oid not in getattr(self, "_shield_cancelled_ids", set()):
                deepcoin_client.cancel_trigger_order(self.symbol, oid)
                self._shield_cancelled_ids = self._shield_cancelled_ids or set()
                self._shield_cancelled_ids.add(oid)
                logger.info(f"??? [TV???] ?????? {oid} @ {t.get('triggerPrice', '?')}")

        # v16.15???????????????????? Deepcoin ???????
        if _local_ord and _local_ord not in getattr(self, "_shield_cancelled_ids", set()):
            deepcoin_client.cancel_trigger_order(self.symbol, _local_ord)
            self._shield_cancelled_ids = self._shield_cancelled_ids or set()
            self._shield_cancelled_ids.add(_local_ord)
            logger.info(f"??? [TV???] ????? {_local_ord} ?????")
            time.sleep(0.3)

        # ?? set-position-sltp ??????????slOrdPx=-1?
        res = deepcoin_client.set_position_sltp(
            symbol=self.symbol,
            pos_side=pos_side,
            sl_trigger_px=exchange_sl,
            tp_trigger_px=None,  # ??? TP123 ????
            td_mode="cross",
            mrg_position="merge",
            trigger_px_type="last",
            sl_ord_px="-1",  # ????
            tp_ord_px="-1",
        )

        # ????????
        if res and str(res.get("code", "0")) in ("0", "00000", ""):
            self.shield_active = True
            self.shield_sized_qty = live_qty
            self._shield_fail_streak = 0
            # v16.15?????????????? order_id ???
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
                f"??? [TV???] ??? | {live_qty} ? @ {exchange_sl:.2f} | "
                f"??????? | ?????????"
            )
            if not getattr(self, "_shield_arm_notified", False):
                self._shield_arm_notified = True
                self._call_telegram_notify(
                    telegram_notify.report_adverse_shield_armed,
                    side=self.current_side,
                    entry=entry,
                    live_qty=live_qty,
                    adverse_pct=0,
                    tier_prices=[exchange_sl],
                    tier_pcts=SHIELD_TIER_PCTS,
                    verify_note=(
                        (reason or f"TV??? @ {exchange_sl:.2f}")
                        + f" | ?? {live_qty} ? | ???? | ?????"
                    ),
                )
            return True
        else:
            # ???????????
            err_msg = str(res.get("msg", "") if res else "????")
            self._shield_fail_streak = getattr(self, "_shield_fail_streak", 0) + 1
            logger.error(f"??? ???????: {err_msg} | ????: {self._shield_fail_streak}")

            if not suppress_alert and self._shield_fail_streak >= 3:
                logger.warning(
                    f"[?????] TV??????? | ???? {self._shield_fail_streak} ? | ??: {err_msg}"
                )
            return False

    def _maintain_hard_shield(self, real_amt, curr_px=None, force=False):
        """?? TV tv_sl ??????????????"""
        if real_amt <= 0 or not self.watched_entry:
            return False
        if getattr(self, "tv_sl", 0) > 0:
            if not force and not self._can_maintain_shield_now(force=force):
                return getattr(self, "shield_active", False)
            return self._sync_tv_sl_stop(
                real_amt,
                reason="??TV???",
                force=force,
            ).get("ok", False)

        # v16.23 ???tv_sl=0 ??? journal ???? LONG/SHORT ?????
        # v16.26 ???journal ? symbol ?? + ?????????? ETH ???? BNB
        if real_amt > 0 and getattr(self, "tv_sl", 0) <= 0:
            curr_px = curr_px or deepcoin_client.get_current_price(self.symbol)
            recovered_sl = 0.0
            for src in [self.last_tv_signal,
                        self._load_last_journal_entry(TV_JOURNAL, self.symbol),
                        self._load_last_tv_open_signal(),
                        self._load_last_journal_entry(OPEN_JOURNAL, self.symbol)]:
                if not src:
                    continue
                sl = float(src.get("tv_sl", 0) or 0)
                if sl <= 0:
                    continue
                # v16.26???????????? symbol ????
                # SHORT ??? > ????LONG ??? < ???
                # Bug fix (2026-08-02): SHORT stop-loss must be > entry price, not current price.
                # The 5% check must anchor on entry price per spec v1.0 ?3.
                # Previous check used current_price (584) which rejected valid SL (581.67)
                # because 581.67 < 584. Now using watched_entry (575.82) which is correct.
                anchor_px = self.watched_entry if self.watched_entry > 0 else curr_px
                if anchor_px and anchor_px > 0:
                    if self.current_side == "SHORT" and sl <= anchor_px * 1.05:
                        logger.warning(
                            f"?? [journal??] tv_sl={sl} ????SHORT ?>??{anchor_px:.2f}????? 5%????"
                        )
                        continue
                    if self.current_side == "LONG" and sl >= anchor_px * 0.95:
                        logger.warning(
                            f"?? [journal??] tv_sl={sl} ????LONG ?<??{anchor_px:.2f}????? 5%????"
                        )
                        continue
                recovered_sl = sl
                break
            if recovered_sl <= 0 and self.watched_entry > 0 and self.current_atr > 0:
                sl_m = {1: 0.9, 2: 1.05, 3: 1.10, 4: 1.25}.get(int(self.regime or 3), 1.10)
                if self.current_side == "LONG":
                    recovered_sl = round(self.watched_entry - self.current_atr * sl_m, 2)
                else:
                    recovered_sl = round(self.watched_entry + self.current_atr * sl_m, 2)
            if recovered_sl > 0:
                logger.info(f"?? ??????? journal ?? tv_sl={recovered_sl:.2f}")
                self.tv_sl = recovered_sl
                return self._sync_tv_sl_stop(
                    real_amt,
                    reason="journal??TV???",
                    force=True,
                ).get("ok", False)

        if real_amt > 0 and not getattr(self, "_tv_sl_missing_alerted", False):
            logger.error("??TV???????? tv_sl??? fallback ???")
            logger.warning(
                f"[?????] TV????? | ?? {real_amt} ? ???? tv_sl??????"
            )
            self._tv_sl_missing_alerted = True
        return False

    def _process_adverse_shield(self, real_amt, curr_px):
        """????? ? ?????"""
        return self._maintain_hard_shield(real_amt, curr_px)

    def _is_radar_active(self):
        """
        ??? v1.0 ? ?5.1?
        ????????????TP1-TP2?????????TP2???????
        ???????????current_sl ???/?? entry??
        ???? TP1 ????????
        """
        if not self.watched_entry or not self.current_sl:
            return False
        # ?????????????????????
        if getattr(self, "radar_activated", False):
            return True
        # ????????????
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
        # ???????????
        if not price_reached:
            return False
        if side == "LONG":
            return self.current_sl > self.watched_entry
        if side == "SHORT":
            return self.current_sl < self.watched_entry
        return False

    def _radar_is_dormant(self):
        """?????????????"""
        return not self._is_radar_active() and not getattr(self, "radar_activated", False)

    # _check_early_be_checkpoint (spec v1.0 5.0) removed for binance parity
    # (v16.22 + v16.24 v2.1): the pre-breakeven checkpoint is abolished. See
    # call-site comment in the monitor loop for rationale. _early_be_checkpoint_done
    # is still saved/restored as a harmless legacy state field.

    # ?? ?? v1.0 ?8-9???????? ????????????????????????????????????????

    def _clear_pending_tags_for_kind(self, kind_prefix, save=False):
        """????????????TP1/TP2/HARD/RADAR??"""
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
        v16.14????????VPS?????????????
        ???? VPS ???????????????/??/???????????
        ?????_pending_order_tags ??????????????
        """
        tags = dict(getattr(self, "_pending_order_tags", {}) or {})
        if not tags:
            return
        dropped = []
        for tag, meta in list(tags.items()):
            dropped.append(f"{tag}({meta.get('kind','?')}/{meta.get('status','?')}/{meta.get('order_id','no-oid')})")
        self._pending_order_tags = {}
        logger.warning(
            f"[{self.symbol}] v16.14 ??????? {len(dropped)} ?: {dropped[:8]}"
            f"{'?' if len(dropped) > 8 else ''}"
        )
        self._save_state()

    def _gc_stale_pending_defense_tags(self, max_pending_age_sec=45.0, save=True):
        """??????????????/?????"""
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
            # ?BUG???? order_id ?????????
            # ??????? order_id?????????????
            # ???????/???????????? _confirm_stale_before_clear ???????
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
            f"[{self.symbol}] ???????? {len(dropped)} ?: "
            f"{dropped[:6]}{'?' if len(dropped) > 6 else ''}"
        )
        if save:
            try:
                self._save_state()
            except Exception:
                pass
        return len(dropped)

    def _confirm_stale_before_clear(self, tag, meta):
        """
        v16.11??????????????????????????/???
        ???"??????"?????????????????????????????

        ?? True = ?????????False = ????????????????
        """
        if not tag or not meta:
            return False
        meta = meta or {}
        oid = str(meta.get("order_id") or "").strip()
        ts = float(meta.get("ts") or 0)
        age = (time.time() - ts) if ts > 0 else 0.0

        # ??1?? order_id ? ????????
        if oid:
            try:
                order_res = deepcoin_client.get_order(self.symbol, ord_id=oid)
                if order_res:
                    state = str(order_res.get("state", "") or order_res.get("status", "")).lower().strip()
                    # v16.14???????????/??????????
                    # ???????????????????????????????????
                    if state in ("filled", "cancelled", "canceled", "done", "????", "??", ""):
                        reason = "???" if state else "???????"
                        logger.info(
                            f"[{self.symbol}] ?? {tag} ??? {oid} ??=[{state or 'N/A'}]?{reason}??????"
                        )
                        return True
                    # ????????????
                    logger.warning(
                        f"[{self.symbol}] ?? {tag} ??? {oid} ?? {state}?????"
                    )
                    return False
                else:
                    # v16.14????????order_res ????????/?????? ?????
                    logger.warning(
                        f"[{self.symbol}] ?? {tag} ??? {oid} ???????????????????"
                    )
                    return True
            except Exception as e:
                logger.warning(f"[{self.symbol}] ???? {oid} ????: {e}?????")
                return False

        # ??2?? order_id???????? ????????
        STALE_THRESHOLD_SEC = 45.0
        if age >= STALE_THRESHOLD_SEC:
            logger.warning(
                f"[{self.symbol}] ?? {tag} ? order_id ???? {age:.0f}s >= {STALE_THRESHOLD_SEC}s?"
                f"?????"
            )
            return True
        # ???????????????????????
        logger.warning(
            f"[{self.symbol}] ?? {tag} ? order_id ?? {age:.0f}s < {STALE_THRESHOLD_SEC}s?"
            f"?????????????"
        )
        return False

    def _has_open_pending_defense_tag(self, kind=None):
        """??????????? ? ?????????"""
        self._gc_stale_pending_defense_tags(save=False)
        want = str(kind or "").upper()
        for tag, meta in dict(getattr(self, "_pending_order_tags", {}) or {}).items():
            st = str((meta or {}).get("status") or "open").lower()
            # ?BUG????????????????? order_id??????
            if st in ("done", "filled", "cancelled", "canceled", "acked"):
                continue
            k = str((meta or {}).get("kind") or "").upper()
            if want and k != want and not k.startswith(want):
                continue
            # ?????????????? pending ??? order_id ?????
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
        ??? v1.0 ? ?5.1?
        ?????????????
        ???? TP1 ?????????????????????
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

    def _cancel_all_tp_limit_orders(self, max_rounds=3):
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
                    time.sleep(0.10)
            logger.info(f"?? ????? ?{round_i + 1}?: {len(orders)} ?")
            time.sleep(0.8)  # v16.19 ???1.5s?0.8s?????1????
        if total:
            logger.info(f"?? ????????? {total} ?")
        return total

    def _scorched_earth_cancel_for_recover(self):
        for attempt in range(6):
            deepcoin_client.cancel_all_open_orders(self.symbol)
            time.sleep(0.8)
            self._cancel_all_tp_limit_orders(max_rounds=4)
            time.sleep(0.6)
            remaining = self._collect_tp_limit_orders()
            if not remaining:
                logger.info(f"?? ?????????????? (? {attempt + 1} ?)")
                return True
            remain_txt = ", ".join(f"{o['qty']}@{o['price']}" for o in remaining[:4])
            logger.warning(
                f"?? ????? {len(remaining)} ????? ({remain_txt}) "
                f"? ?? {attempt + 1}/6"
            )
        logger.error("? ????????? TP ???????????? APP ???????")
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
        ??? v1.0 ? ?5.1 ???????
        ????TP1-TP2?????????TP2????????????????
        ???? TP1 ????
        """
        if getattr(self, "_radar_activation_notified", False):
            return
        # ?? ?5.1????????????????TP1??
        # ???????????????????????
        if self.current_side == "LONG" and float(new_sl or 0) <= float(self.watched_entry or 0):
            logger.warning(
                f"?? ?????????LONG ?? {new_sl:.2f} ??? entry"
            )
            return
        if self.current_side == "SHORT" and float(new_sl or 0) >= float(self.watched_entry or 0):
            logger.warning(
                f"?? ?????????SHORT ?? {new_sl:.2f} ??? entry"
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
            f"???? {progress:.0%} | ???? @ {new_sl:.2f} | "
            f"TV?? tv_sl={tv_floor or 'fallback'} | "
            f"?? {real_amt} ? @ {self.watched_entry:.2f}"
        )
        if not verified and not sl_placed:
            logger.warning(f"????????????? @ {new_sl:.2f} ???")
            return
        if not verified:
            verify_note += f" | {telegram_notify.VERIFY_DELAY_MARK}"
        breath_meta = getattr(self, "_breath_coeff_meta", None) or {}
        trail_dist = float(
            (getattr(self, "open_atr", 0) or 0)
            * float(getattr(self, "breathing_coefficient", 1.0) or 1.0)
        )
        # ?? v1.0 ?5.1??????????
        reentry_attempt = int(getattr(self, "reentry_attempt", 0) or 0)
        if reentry_attempt >= 1:
            open_kind = "????"
            trigger_gate = "???TP2???"
        else:
            open_kind = "????"
            trigger_gate = "???TP1-TP2??"
        self._call_telegram_notify(
            telegram_notify.report_radar_activated,
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
        # P1 ????? TG ??????
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
                f"?? ???????TP1 ?????? @{tp1_px:.2f}"
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
        """TP1 ???????+????+??TP1?"""
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
        ??? v1.0 ? ?5.1?
        ???????????TP1-TP2?????????TP2?????????
        ????? tv_sl ????????????
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
                f"?? [{source or '??'}] ??? TP{consumed} ?? "
                f"(TP1 ?????)"
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
                f"?? [{source or '??'}] TP1??? | tv_sl={tv:.2f} | entry={entry:.2f}"
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
            f"?? [{source or '??'}] ??????/?TP{stale or '??'} "
            f"? ?? tv_sl={tv:.2f}"
        )
        logger.info(
            f"[?????] ??????????? | "
            f"{self.current_side} {live_qty}? @ {entry:.2f} | "
            f"???TP{stale or '??'} | tv_sl={tv:.2f} | "
            f"??????TP1-TP2???????"
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
        atr = float(self.current_atr or symbol_aware_atr_fallback(self.symbol))
        cushion = max(atr * TV_BOOT_SL_ATR, entry * RADAR_FEE_BUFFER_PCT)
        if self.current_side == "LONG":
            return round(entry + cushion, 2)
        if self.current_side == "SHORT":
            return round(entry - cushion, 2)
        return entry

    def _radar_trail_offset_price(self):
        return float(self.current_atr or symbol_aware_atr_fallback(self.symbol)) * self._radar_tv_trail_atr_mult()

    def _refresh_radar_state_on_recover(self, curr_px, entry):
        """???????? best_price?? TP1 ???????????"""
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
                f"?? ??????: ??????TP1-TP2??(??{gate:.2f}) "
                f"(?? {self._radar_activation_progress(curr_px):.0%})"
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
                f"?? ??????: TP1??? ?? {progress:.0%} | best={self.best_price:.2f} | "
                f"SL={self.current_sl:.2f} | ?? {self._radar_tv_trail_atr_mult():.2f}ATR"
            )
        elif self.current_sl == 0.0:
            self.current_sl = floor_px

    def _nuclear_realign_tp(self, live_qty, entry, dynamic_sl=None, rounds=3):
        """
        ???????????? TP ? ?? TP123 ? ???? tv_sl/???????
        """
        # ?????????????????????????
        now = time.time()
        if now - getattr(self, "_last_nuclear_realign_ts", 0) < 15.0:
            audit = self._audit_tp_levels(live_qty)
            if self._tp_audit_ok(audit):
                logger.warning(
                    f"?? ??????{now - getattr(self, '_last_nuclear_realign_ts', 0):.1f}s < 15s??TP??????"
                )
                return audit
            logger.warning(
                f"?? ??????{now - getattr(self, '_last_nuclear_realign_ts', 0):.1f}s < 15s?????????"
            )
            return audit
        self._last_nuclear_realign_ts = now

        # binance parity (129cd68): verify the exchange's real position before
        # tearing down and rebuilding TP orders. Without this, a stale
        # watched_qty (state residue after the exchange already went flat)
        # makes the system nuke-and-rebuild TP orders against a position that
        # no longer exists, which just accumulates orphan orders.
        real_pos = self._get_active_position()
        if real_pos == "QUERY_FAILED":
            real_pos = None
        real_qty = self._safe_qty(real_pos.get("size")) if real_pos else 0
        if real_qty <= 0:
            logger.info(
                f"?? [{self.symbol}] ??????????={real_qty} | ??={live_qty} | "
                f"???????????"
            )
            return {"expected": 0, "matched_full": 0, "missing": [], "orphans": [], "skipped_no_position": True}

        last_audit = self._audit_tp_levels(live_qty)
        for r in range(rounds):
            # v16.22 ??????????????????/?????????? TP ??
            fresh_pos = self._get_active_position()
            if fresh_pos == "QUERY_FAILED":
                logger.warning("?? ?????????????? live_qty ??")
                fresh_pos = None
            live_qty = self._safe_qty(fresh_pos.get("size")) if fresh_pos else live_qty
            logger.warning(
                f"?? ????????? {r + 1}/{rounds} | ?? {live_qty}? | "
                f"?? {last_audit['matched_full']}/{last_audit['expected']} | "
                f"{self._format_audit_summary(last_audit)}"
            )
            # v16.14???????????????????????
            # ? VPS ??/???????????????????????????
            # ??????????????"?????"????
            self._pending_order_tags = {}
            self._save_state()
            self._cancel_all_tp_limit_orders()
            time.sleep(1.0)
            # v16.22 ???????????????? qty ?? TP ??
            placed = self._rebuild_defenses(live_qty, entry, dynamic_sl=None)
            logger.info(f"?? ??? {r + 1} ?? {placed} ?????")
            curr_px = deepcoin_client.get_current_price(self.symbol)
            self._maintain_hard_shield(live_qty, curr_px, force=True)
            if dynamic_sl and not self._has_trigger_sl_near(dynamic_sl):
                self._ensure_radar_sl(live_qty, dynamic_sl)
            time.sleep(1.0)
            # v16.22 ??????????????
            last_audit = self._audit_tp_levels(live_qty)
            stop_px = self._resolve_defense_stop_for_audit(dynamic_sl)
            # v16.22 ????? matched < expected ?????matched >= expected ???????
            if last_audit["matched_full"] >= last_audit["expected"] and not last_audit.get("orphans"):
                logger.info(f"?? ??????: {self._format_audit_summary(last_audit)}")
                return last_audit
            logger.warning(
                f"?? ??? {r + 1} ????: {self._format_audit_summary(last_audit)}"
            )
            time.sleep(1.5)
        # binance parity (247e3c2): rounds exhausted -- if stale/orphan TP
        # orders are still sitting on the book, clean them up before giving
        # up, otherwise they persist as noise that interferes with the next
        # audit cycle.
        if last_audit.get("orphans"):
            self._cancel_orphan_tp_orders(live_qty)
            time.sleep(0.5)
            last_audit = self._audit_tp_levels(live_qty)
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
        ??/??????? ? ???? ? ???? ? ????????

        v16.18 ???
        - ??????? _verify_live_tp_completeness ???? live_qty vs saved_initial
        - ???????????????????????
        - ????? TP ????????
        - ??????????????
        """
        live_qty = self._resolve_live_qty(live_qty)
        saved_initial = self._safe_qty(getattr(self, "initial_qty", 0) or 0)

        # ============================================================
        # Step 0: v16.18 ???? - ? live_qty ?? initial?? saved ??
        # ============================================================
        qty_check = self._verify_live_tp_completeness(live_qty, saved_initial)

        conservative_mode = qty_check["needs_conservative_mode"]
        if conservative_mode:
            logger.warning(
                f"?? [TP??] ???={qty_check['confidence']} | "
                f"??={qty_check['discrepancy_pct']:.1%} | ??????"
            )
            # ??????????????
            self._reset_tp_place_guard()
            # ??????????????????? tp_levels_consumed
            result = self._conservative_tp_recover(
                live_qty, entry, dynamic_sl=dynamic_sl,
            )
            audit = self._audit_tp_levels(live_qty)
            matched = audit["matched_full"]
            expected = audit["expected"]
            if matched >= expected and not audit.get("orphans"):
                logger.info(
                    f"? [??] TP ???? | {matched}/{expected} | "
                    f"??={result['placed']} | ??={result['skipped']} | "
                    f"??={result['guarded']}"
                )
                return matched, audit["pending_prices"], expected, result["placed"] > 0
            # ???????????????????????
            logger.warning(
                f"?? [??] TP ??? ({matched}/{expected})???????"
            )

        # ============================================================
        # Step 1: ??????
        # ============================================================
        audit = self._audit_tp_levels(live_qty)
        expected = audit["expected"]
        matched = audit["matched_full"]
        pending_prices = audit["pending_prices"]
        logger.info(
            f"?? ????: ?? {live_qty}? | TP {matched}/{expected} | "
            f"{self._format_audit_summary(audit)}"
        )

        if self._audit_requires_nuclear(audit) or self._has_duplicate_tp_orders():
            logger.warning(
                f"?? ?????????: {len(self._collect_tp_limit_orders())} ??? | "
                f"{self._format_audit_summary(audit)}"
            )
            # v16.18 ???????????
            guard_ok, _ = self._check_tp_place_guard(level=0)
            if not guard_ok:
                logger.warning(
                    f"? ??? TP ?????????????="
                    f"{getattr(self, '_tp_place_guard_count', 0)}/{RECOVER_TP_PLACE_GUARD_MAX}"
                )
                # ???????????????????
            audit = self._nuclear_realign_tp(live_qty, entry, dynamic_sl=dynamic_sl, rounds=3)
            return audit["matched_full"], audit["pending_prices"], audit["expected"], True

        if self._defenses_fully_ok(live_qty, dynamic_sl):
            logger.info(
                f"? TP123 ???? ({matched}/{expected}) @ {pending_prices}?????"
            )
            if dynamic_sl and not self._has_trigger_sl_near(dynamic_sl):
                self._ensure_radar_sl(live_qty, dynamic_sl)
            return matched, pending_prices, expected, False

        self._cancel_orphan_tp_orders(live_qty)
        logger.info(f"?? ???? ({matched}/{expected})?????????????????")
        self._patch_missing_tp_levels(live_qty)
        time.sleep(0.8)
        matched, pending_prices = self._wait_tp_hung(
            self.tv_tps, live_qty=live_qty, retries=5, delay=1.0,
        )
        audit = self._audit_tp_levels(live_qty)
        matched = audit["matched_full"]

        if self._defenses_fully_ok(live_qty, dynamic_sl):
            logger.info(f"? ?????? ({matched}/{expected}) @ {audit['pending_prices']}")
            if dynamic_sl and not self._has_trigger_sl_near(dynamic_sl):
                self._ensure_radar_sl(live_qty, dynamic_sl)
            return matched, audit["pending_prices"], expected, True

        logger.warning(
            f"?? ??????? ({matched}/{expected}) {audit['issues']}????????"
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

        # ?????????TP ???????TP ????????
        now = time.time()
        if not recover_mode and not getattr(self, "_force_defense_realign", False):
            last_align = getattr(self, "_last_defense_align_ok_ts", 0) or 0
            audit = self._audit_tp_levels(live_qty)
            if now - last_align < DEFENSE_ALIGN_COOLDOWN_SEC:
                if self._tp_audit_ok(audit):
                    logger.info(
                        f"??? ??????????? {now - last_align:.0f}s < {DEFENSE_ALIGN_COOLDOWN_SEC}s?"
                        f"?TP ??????? | {self._format_audit_summary(audit)}"
                    )
                    return {
                        "matched": audit["matched_full"],
                        "expected": audit["expected"],
                        "pending_prices": audit["pending_prices"],
                        "rebuilt": False,
                        "audit": audit,
                        "nuclear": False,
                    }
                # ??????TP ????????????????
                logger.warning(
                    f"??? ??????????? {now - last_align:.0f}s < {DEFENSE_ALIGN_COOLDOWN_SEC}s?"
                    f"?? TP ??? ? ???? | {self._format_audit_summary(audit)}"
                )

        if reason:
            logger.info(f"??? ????: {reason} | ?? {live_qty}?")

        self._defense_align_in_progress = True
        try:
            # v16.22 ??????????????????
            curr_px = deepcoin_client.get_current_price(self.symbol)
            audit = self._audit_tp_levels(live_qty)

            if recover_mode and self._tp_audit_ok(audit):
                logger.info(
                    f"? ??????? TP ????????? | "
                    f"{self._format_audit_summary(audit)}"
                )
                # v16.22 ???TP ????????????????
                if curr_px is None:
                    curr_px = deepcoin_client.get_current_price(self.symbol)
                self._maintain_hard_shield(live_qty, curr_px or 0, force=True)
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
                        f"? ???????? ({n_actions} ?)????? | "
                        f"{self._format_audit_summary(audit)}"
                    )
                    # v16.22 ?????????????
                    if curr_px is None:
                        curr_px = deepcoin_client.get_current_price(self.symbol)
                    self._maintain_hard_shield(live_qty, curr_px or 0, force=True)
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
                    f"?? ?????????? ({n_actions} ?) ? ???? | "
                    f"{self._format_audit_summary(audit)}"
                )

            if not recover_mode and self._tp_audit_ok(audit):
                logger.info(f"? TP ???????: {self._format_audit_summary(audit)}")
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
                logger.info(f"? ??? TP ??: {self._format_audit_summary(audit)}")
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
                logger.warning("?? ?????????????")
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

        if audit.get("query_failed"):
            logger.warning(
                f"?? [????] ?????? ? ??????????????????"
            )
            return None

        if self._tp_audit_ok(audit):
            self._guardian_bad_streak = 0
            if sl and not self._has_trigger_sl_near(sl):
                self._ensure_radar_sl(real_amt, sl)
            return None

        self._guardian_bad_streak += 1
        now = time.time()
        severe = self._defense_needs_immediate_fix(audit)
        in_grace = now < getattr(self, "_sentinel_grace_until", 0)
        # binance parity (eb0dc4c): quiet window scales with bad_streak
        # instead of a flat 30s -- repeated audit misses under order
        # propagation delay/IP rate limit should back off *more*, not less.
        # The old "bypass once streak>=2" behavior was backwards: it made
        # protection weaker exactly when the audit was proving unreliable.
        streak = int(getattr(self, "_guardian_bad_streak", 0) or 0)
        since_ok = now - getattr(self, "_last_defense_align_ok_ts", 0)
        quiet_sec = DEFENSE_ALIGN_COOLDOWN_SEC * (1 + min(streak, 4))
        in_cooldown = since_ok < quiet_sec
        if (in_grace or in_cooldown) and not severe:
            logger.info(
                f"?? [????] TP ????????? "
                f"({'?????' if in_grace else f'???{since_ok:.0f}s/{quiet_sec}s streak={streak}'}) | "
                f"{self._format_audit_summary(audit)}"
            )
            return None

        logger.warning(
            f"?? [????] TP ??? ? ?????? | "
            f"{self._format_audit_summary(audit)}"
        )
        # binance parity (8a279a9): a cooldown-period bypass (severe/naked
        # case forced through despite in_cooldown) uses rounds=1, not the
        # normal 3 -- order-propagation lag during the cooldown window can
        # make a still-settling audit look wrong, and rounds=3 in that state
        # risks a cancel/place/cancel loop instead of one clean correction.
        rounds = 1 if in_cooldown else 3
        sl_preserve = sl if self._is_radar_active() else None
        result = self._enforce_defense_alignment(
            real_amt, self.watched_entry, dynamic_sl=sl_preserve,
            reason="????????", rounds=rounds,
        )
        new_audit = result["audit"]
        if new_audit["matched_full"] < new_audit["expected"]:
            self._call_telegram_notify(
                telegram_notify.report_system_alert,
                title="???????????",
                detail=(
                    f"{self.current_side} {real_amt}? | "
                    f"{self._format_audit_summary(new_audit)} | ????? Deepcoin ??"
                ),
            )
        elif self._defense_needs_immediate_fix(audit):
            logger.info(
                f"?? [????] ????: "
                f"{new_audit['matched_full']}/{new_audit['expected']} | "
                f"{self._format_audit_summary(new_audit)}"
            )
            if getattr(self, "_recover_tp_unconfirmed", False):
                self._recover_tp_unconfirmed = False
                self._call_telegram_notify(
                    telegram_notify.report_radar_guardian_realigned,
                    side=self.current_side,
                    qty=real_amt,
                    tp_audit=new_audit,
                    verify_note=(
                        f"???????????? | "
                        f"{new_audit['matched_full']}/{new_audit['expected']} | "
                        f"{self._format_audit_summary(new_audit)}"
                    ),
                )
            elif getattr(self, "_open_tp_unconfirmed", False):
                self._open_tp_unconfirmed = False
                self._call_telegram_notify(
                    telegram_notify.report_radar_guardian_realigned,
                    side=self.current_side,
                    qty=real_amt,
                    tp_audit=new_audit,
                    verify_note=(
                        f"???????? | "
                        f"{new_audit['matched_full']}/{new_audit['expected']} | "
                        f"{self._format_audit_summary(new_audit)}"
                    ),
                )
        return result

    def _full_rebuild_tp_loop(self, live_qty, entry, dynamic_sl=None):
        result = self._enforce_defense_alignment(
            live_qty, entry, dynamic_sl=dynamic_sl, reason="????", rounds=3,
        )
        audit = result["audit"]
        return audit["matched_full"], audit["pending_prices"], audit["expected"]

    def _smart_realign_defenses(self, live_qty, entry, dynamic_sl=None, reason=""):
        return self._enforce_defense_alignment(
            live_qty, entry, dynamic_sl=dynamic_sl, reason=reason or "??????", rounds=3,
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

    def _has_tp_limit_at_price(self, price, tolerance=0.1):
        # binance parity (129cd68): tightened from a loose 1.0 -- with a 0.01
        # tick size a $1 tolerance risks matching a stale orphan order at a
        # nearby-but-wrong price as if it were the real TP, which both hides
        # the orphan from cleanup and blocks a legitimate re-place.
        if price <= 0:
            return False
        orders = self._collect_tp_limit_orders()
        # binance parity (52d26bd): a failed pending-orders query must never
        # be read as "this TP is missing" -- that would trigger a re-place
        # against an order that may well still be live on the exchange,
        # which is exactly the stacked-duplicate-LIMIT-orders incident this
        # codebase's own README calls out as the historical worst case.
        # Conservatively assume "already exists" (blocks re-place for one
        # cycle) rather than "missing" (would stack); TP is reduceOnly so a
        # one-cycle placement delay is the safe side of this tradeoff.
        if not getattr(deepcoin_client, "_last_pending_orders_query_ok", True):
            logger.warning(
                f"[{self.symbol}] TP query failed @{price:.2f} -> "
                f"conservatively assume exists, block re-place"
            )
            return True
        for o in orders:
            if abs(o["price"] - price) <= tolerance:
                return True
        return False

    def _detect_tp_fills(self, old_qty, new_qty, curr_px=0.0):
        """????/????+??+???????????????"""
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

        # Deepcoin ????????? TP ?? + ?? + ????
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
                f"?? ??? TP{soft} ???????? "
                f"(?? {initial}?{new_qty} | ???TP+????+???)"
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
            logger.info(f"?? ????? TP ??? {cancelled} ?")
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
                        f"?? ??? TP{lv['level']} @{px:.2f}: "
                        f"?? {o['qty']} ? ? {target_q}?"
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
                    f"?? ?? TP{lv} @{px:.2f} "
                    f"(?? {initial_qty} ? ?? {live_qty}????????)"
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
            f"?? TP ???????: ?? {live_qty}? | "
            f"??? TP{consumed} | ??????"
        )
        self._cancel_tp_orders_at_levels(consumed)
        time.sleep(0.35)
        n_fix = self._cancel_mismatched_remaining_tps(live_qty)
        if n_fix:
            logger.info(f"?? TP ??????????? {n_fix} ?")
            time.sleep(0.35)
        placed = self._patch_missing_tp_levels(live_qty)
        time.sleep(0.5)
        audit = self._audit_tp_levels(live_qty)
        if dynamic_sl and not self._has_trigger_sl_near(dynamic_sl):
            self._ensure_radar_sl(live_qty, dynamic_sl)
        if placed == 0 and self._tp_audit_ok(audit):
            logger.info(
                f"? TP ??????? ({audit['matched_full']}/{audit['expected']})"
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
                    f"????????????????? TP{consumed}"
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
                f"???? TP{stale_levels} | ?? {initial_qty} ? ?? {live_qty}?"
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
                actions.append(f"????? TP{inferred}")
        if not consumed:
            return {"repaired": False, "actions": actions, "result": None, "consumed": []}

        if live_qty > DUST_ORPHAN_CONTRACTS and self._expected_tp_count() == 0:
            self._sanitize_tp_consumed(initial_qty, live_qty, curr_px)
            if self._expected_tp_count() == 0 and live_qty > DUST_ORPHAN_CONTRACTS:
                logger.warning(
                    f"?? ?? {live_qty}? ???? TP ? TP1+TP2 ???"
                    f"????????TP3???"
                )
                self.tp_levels_consumed = [1, 2]
                try:
                    self._cancel_tp_orders_at_levels([3])
                except Exception:
                    pass
                self._save_state()

        n_stale = self._cancel_tp_orders_at_levels(consumed)
        if n_stale:
            actions.append(f"??????? {n_stale} ?")
        n_mismatch = self._cancel_mismatched_remaining_tps(live_qty)
        if n_mismatch:
            actions.append(f"??? TP {n_mismatch} ?")
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
            live_qty, dynamic_sl=sl_to_pass, reason="????????",
        )
        rem_levels = self._expected_tp_levels(live_qty)
        rem_sum = sum(lv["qty"] for lv in rem_levels)
        audit = result.get("audit") or {}
        actions.append(
            f"?? TP ?? {rem_sum}/{live_qty}? | "
            f"?? {audit.get('matched_full', 0)}/{audit.get('expected', 0)} ?"
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
                "?? [????] ???TP1 ?????????0????"
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
        # binance parity (a62117a): TP1 touch alone is never sufficient to
        # advance the tracked stop to breakeven/radar level -- only the
        # (TP1+TP2)/2 midpoint (or TP2 on reentry) gate may do that, per spec
        # v1.0 5.1/5.2. Without this check, current_sl silently jumps to
        # breakeven the instant TP1 fills, before the real activation gate is
        # reached, which is the same "TP1触及即启动" bug binance's live ETH
        # LONG incident traced back to.
        if self._should_radar_trail(curr_px):
            new_sl = self._compute_radar_sl(curr_px)
            floor_px = self._radar_breakeven_floor()
            if new_sl is not None:
                if self.current_side == "LONG":
                    self.current_sl = max(self.current_sl or floor_px, new_sl, floor_px)
                else:
                    self.current_sl = min(self.current_sl or floor_px, new_sl, floor_px)
            elif max_level >= 1:
                self.current_sl = floor_px
        note = f"TP{max_level}??"
        if max_level >= 2 and tp3 > 0:
            note += f" ? ????? TP3({tp3:.2f}) ????"
        elif max_level == 1:
            note += " ? ?????????? TP2/TP3"
        logger.info(
            f"?? [????] {note} | SL={self.current_sl:.2f} | best={self.best_price:.2f}"
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
                reason="???????",
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
                    f"?? [????] {levels} ?????"
                    f"????TP1 / ???????? ???????? | "
                    f"{old_qty}?{new_qty}"
                )
                logger.warning(
                    f"[?????] ??????TP1?? | "
                    f"{self.current_side} {old_qty}?{new_qty}? | {levels} | "
                    f"?? {float(curr_px or 0):.2f} | "
                    f"???TP1????????????"
                )
                result = self._smart_realign_defenses(
                    new_qty, self.watched_entry, dynamic_sl=None,
                    reason="?TP????????",
                )
                if self._should_activate_shield(curr_px) or getattr(
                    self, "shield_active", False
                ):
                    self._maintain_hard_shield(new_qty, curr_px, force=True)
                change = {"kind": "reduce_unknown", "tp_fills": [], "shield_fills": []}
                self._save_state()
                return change, result
            logger.info(
                f"?? [????] {levels} ???? {old_qty} ? {new_qty} ? ???? + ???TP"
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
                reason=f"{levels} ??????",
            )
            if sl_to_pass and not getattr(self, "_radar_activation_notified", False):
                self._perform_radar_handoff(
                    new_qty, curr_px_safe, reason=f"{levels} ??????",
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
                f"??? [????] TV????? "
                f"{old_qty} ? {new_qty} @ {f['price']:.2f}"
            )
            if new_qty <= 0 or self._is_dust_qty(new_qty):
                flat_meta = self._build_close_meta(
                    "CLOSE_STOPLOSS",
                    self.current_side,
                    self._estimate_pnl_pct(curr_px),
                    "????????TV tv_sl?",
                )
                flat_meta["close_type"] = CLOSE_TYPE_VPS_SHIELD
                self._disarm_shield("TV?????", notify=False)
                self._handle_manual_flat_detected(
                    flat_meta["tv_reason"],
                    close_meta=flat_meta,
                    curr_px=curr_px,
                )
                self._save_state()
                return change, None
            self._disarm_shield("TV?????", notify=True)
            self.shield_tiers_consumed = []
            result = self._smart_realign_defenses(
                new_qty, self.watched_entry, dynamic_sl=None,
                reason="?????? TP ??",
            )
            self._call_telegram_notify(
                telegram_notify.report_shield_tier_fill,
                side=self.current_side,
                tier_pct=f["pct"],
                tier_price=f["price"],
                filled_qty=f["qty"],
                remain_qty=new_qty,
                entry_px=self.watched_entry,
                remaining_tiers=[],
                verify_note=(
                    f"??? -{f['pct']:.0%} @ {f['price']:.2f} ?? | "
                    f"?? {new_qty} ?"
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
                reason="???????",
            )
            if self._should_disarm_shield_for_favorable(curr_px):
                self._perform_radar_handoff(
                    new_qty, curr_px, reason="TP???????",
                )
            elif self._should_activate_shield(curr_px) or getattr(self, "shield_active", False):
                self._maintain_hard_shield(new_qty, curr_px, force=True)

        self._save_state()
        return change, result

    def _report_qty_change_dingtalk(self, old_qty, new_qty, realign_result, change=None):
        """TP ?? / ???REST ?????????"""
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
            f"?? {new_qty}? @ {entry_px:.2f} | "
            f"?? {realign_result['matched']}/{realign_result['expected']} ? | "
            f"{self._format_audit_summary(realign_result['audit'])}"
        )
        if not verified:
            verify_note += f" | {telegram_notify.VERIFY_DELAY_MARK}"

        fills = []
        if change and change.get("kind") == "tp_fill":
            fills = change.get("tp_fills") or []
        if not fills:
            fills = self._detect_tp_fills(old_qty, new_qty)
        if fills:
            for fill in fills:
                self._call_telegram_notify(
                    telegram_notify.report_tp_fill,
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
                    f"?? TP{fill['level']} ??????? @ {fill['price']:.2f} "
                    f"({fill['qty']}?)"
                )
        else:
            action_msg = (
                "????" if new_qty > old_qty else "?????? / ????"
            )
            self._call_telegram_notify(
                telegram_notify.report_manual_position_change,
                action_type=action_msg,
                old_qty=old_qty,
                new_qty=new_qty,
                new_entry_price=entry_px,
                verify_note=verify_note,
                tp_audit=realign_result["audit"],
                verified=verified,
            )

        if realign_result["expected"] > 0 and realign_result["matched"] < realign_result["expected"]:
            logger.warning(
                f"[?????] ?????????? | {self._format_audit_summary(realign_result['audit'])}"
            )

    def _report_radar_intervention(self, real_amt, new_sl, action_msg, sl_placed=True):
        """??????????????????"""
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
            f"???? @ {new_sl:.2f} | ?? {real_amt}? | ?? {SENTINEL_POLL_RADAR}s"
        )
        if not sl_placed and not verified:
            logger.warning(f"??????? @ {new_sl:.2f} ???????????")
            return
        if verified:
            verify_note = base_note
        else:
            verify_note = f"{base_note} | {telegram_notify.VERIFY_DELAY_MARK}"
            logger.info(f"????????? REST ?????? @{new_sl:.2f}")
        self._call_telegram_notify(
            telegram_notify.report_intervention,
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
        """?????TP ???????????"""
        new_sl = self._clamp_radar_to_tv_floor(new_sl)
        self._cancel_stop_orders(scope="radar")
        time.sleep(0.35)
        audit = self._audit_tp_levels(live_qty)
        if self._defense_needs_immediate_fix(audit):
            self._enforce_defense_alignment(
                live_qty, entry, dynamic_sl=new_sl,
                reason="????? TP ??", rounds=2,
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
        """???/??? REST ???????????????"""
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
        """???? 1 ??? + ?????qty1+qty2+qty3 ??? total_qty"""
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

        assert qty1 + qty2 + qty3 == total_qty, f"TP ?????: {qty1}+{qty2}+{qty3}!={total_qty}"
        return qty1, qty2, qty3

    def _resolve_live_qty(self, fallback_qty: int) -> int:
        """? reduceOnly ?????????????????/??????????"""
        pos = self._get_active_position()
        if pos == "QUERY_FAILED":
            logger.warning(f"?? ??????????? ? ?? fallback={fallback_qty}")
            return self._safe_qty(fallback_qty)
        if pos and self._safe_qty(pos.get("size")) > 0:
            live = self._safe_qty(pos["size"])
            if live != fallback_qty:
                logger.info(f"?? ??????: ?? {fallback_qty} ? ??? {live}")
            self._last_flat_qty_zero_ts = 0.0
            return live
        # ?? ?9.6 ?????????????0????fallback?????
        # 5?????????????????
        now = time.time()
        prev = float(getattr(self, "_last_flat_qty_zero_ts", 0.0) or 0.0)
        if now - prev > 5.0:
            logger.warning(f"?? ??????: ?? {fallback_qty} ? ??? 0")
        self._last_flat_qty_zero_ts = now
        return 0

    def handle_signal(self, payload):
        """???????"""
        payload = self._enrich_tv_payload(dict(payload or {}))
        self.enqueue_signal(payload)

    def _enrich_tv_payload(self, payload):
        """v6.9.75?TV ?? regime/atr/tp ????????????"""
        action = str(payload.get("action", "")).strip().upper()
        live_px = deepcoin_client.get_current_price(self.symbol) or self.tv_price or 0.0
        return enrich_signal_fields(
            payload,
            action,
            fallback_regime=self.regime or 3,
            fallback_atr=self.current_atr or symbol_aware_atr_fallback(self.symbol),
            fallback_price=live_px,
        )

    def _tv_field_source_note(self, payload):
        return format_tv_field_sources(payload or {})

    def _format_close_extra(self, close_side, pnl_pct, tv_price, regime=None, atr=None):
        parts = []
        if close_side:
            parts.append(f"TV?? {close_side}")
        if regime:
            parts.append(f"TV?? R{int(regime)}")
        if atr and float(atr) > 0:
            parts.append(f"TV ATR {float(atr):.2f}")
        if tv_price and float(tv_price) > 0:
            parts.append(f"TV? {float(tv_price):.2f}")
        if pnl_pct is not None and pnl_pct != "":
            parts.append(f"TV?? {self._safe_float(pnl_pct):+.2f}%")
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

    def _note_adverse_extreme(self, curr_px):
        """
        binance parity (0e943a7): passively track this cycle's worst-direction
        mark price (LONG: lowest seen, SHORT: highest seen) via the WS price
        feed that is already running regardless of order-fill events. Never
        feeds into any stop-loss/radar calculation -- purely evidence for
        close-attribution fallback below, for the case where price spikes
        through the hard stop and rebounds before the current-price
        comparison in _likely_exchange_stop_exit runs (so that comparison
        sees a price nowhere near the stop and misattributes the close as
        "unknown source").
        """
        px = float(curr_px or 0)
        if px <= 0:
            return
        side = str(self.current_side or "").strip().upper()
        wp = float(getattr(self, "_adverse_worst_px", 0) or 0)
        if side == "LONG":
            new_wp = min(wp, px) if wp > 0 else px
        elif side == "SHORT":
            new_wp = max(wp, px) if wp > 0 else px
        else:
            return
        if new_wp != wp:
            self._adverse_worst_px = new_wp
            self._adverse_worst_px_ts = time.time()

    def _build_adverse_extreme_hint(self):
        """
        binance parity (0e943a7): if the worst-direction mark price this
        cycle touched the frozen hard stop within the last 180s, treat that
        as evidence of a pin-through-stop even if no WS fill report latched
        and the current price has since rebounded away from the stop.
        """
        hard = float(self._frozen_hard_px() or 0)
        worst = float(getattr(self, "_adverse_worst_px", 0) or 0)
        worst_ts = float(getattr(self, "_adverse_worst_px_ts", 0) or 0)
        side = str(self.current_side or "").strip().upper()
        if hard <= 0 or worst <= 0 or worst_ts <= 0:
            return None
        if time.time() - worst_ts > 180.0:
            return None
        tol = max(3.0, hard * 0.003)
        if side == "LONG":
            touched = worst <= hard + tol
        elif side == "SHORT":
            touched = worst >= hard - tol
        else:
            touched = False
        if not touched:
            return None
        logger.info(
            f"[{self.symbol}] adverse-extreme fallback confirms hard-stop attribution "
            f"worst={worst:.2f} hard={hard:.2f}"
        )
        return {
            "sl": hard,
            "worst": worst,
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
                f"??????? @ {sl:.2f} (TP1????/???????) | {hint_reason}",
            )

        adverse_hint = self._build_adverse_extreme_hint()
        if adverse_hint and not getattr(self, "_radar_activation_notified", False):
            est = self._estimate_pnl_pct(curr_px)
            return self._build_close_meta(
                "CLOSE_STOPLOSS",
                self.current_side,
                est,
                f"pin-through hard stop @ {adverse_hint['sl']:.2f} "
                f"(worst={adverse_hint['worst']:.2f}, no WS fill report) | {hint_reason}",
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
                self._estimate_pnl_pct(curr_px), "TP3????",
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
                f"???????? @ {sl:.2f} | {hint_reason}",
            )
        if getattr(self, "shield_active", False):
            return self._build_close_meta(
                "CLOSE_STOPLOSS", self.current_side,
                self._estimate_pnl_pct(curr_px),
                "????????TV tv_sl?",
            )
        return self._build_close_meta("CLOSE", self.current_side, None, hint_reason or "????")

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

        # P0 ???tier / tier_label ? webhook ???TV ???????????
        tier_in = self._safe_int(payload.get("tier"), None)
        if tier_in is not None and tier_in in (0, 1, 2):
            self.adx_tier = tier_in
            self.radar_tier = tier_in
            tier_src = "tv"
        else:
            tier_src = "local"
        # tier_label ?????????????? payload ??????
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
        close_reason = str(payload.get("reason") or "??????/???????").strip()
        close_side = str(payload.get("side") or "").strip().upper()
        pnl_pct = payload.get("pnl_pct")
        # ?BUG???is_close ? _process_signal ???????? _process_close_action ?
        # ??? close ?? is_close ??? False??? side ?? CLOSE ??
        is_close = (
            raw_action in ("CLOSE", "CLOSE_PROTECT", "CLOSE_TP3", "CLOSE_STOPLOSS", "CLOSE_QUICK_EXIT", "CLOSE_RSI_EXIT")
            or str(raw_action or "").startswith("CLOSE")
        )

        # P0 ???CLOSE ???? ? ???????????
        if is_close and close_side in ("LONG", "SHORT"):
            live_pos = self._get_active_position()
            if live_pos == "QUERY_FAILED":
                logger.error(
                    f"?? CLOSE ???????? ? ????? [{self.symbol}] | {close_reason or raw_action}"
                )
                return
            if live_pos and self._safe_qty(live_pos.get("size", 0)) > 0:
                pos_side = "LONG" if str(live_pos.get("posSide") or "").lower() == "long" else "SHORT"
                if close_side != pos_side:
                    logger.warning(
                        f"?? CLOSE ????? ??? | "
                        f"TV side={close_side} ??={pos_side} | "
                        f"???? CLOSE???????"
                    )
                    try:
                        logger.warning(
                            f"[?????] CLOSE ????? [{self.symbol}] | "
                            f"TV={close_side} ??={pos_side} | ??? | {close_reason or raw_action}"
                        )
                    except Exception:
                        pass
                    return
        elif is_close and close_side not in ("LONG", "SHORT"):
            # ? side ????????????????
            live_pos = self._get_active_position()
            if live_pos == "QUERY_FAILED":
                logger.warning(f"?? CLOSE ??????? side ??????? [{self.symbol}]")
                live_pos = None
            if live_pos and self._safe_qty(live_pos.get("size", 0)) > 0:
                close_side = "LONG" if str(live_pos.get("posSide") or "").lower() == "long" else "SHORT"

        close_meta = self._build_close_meta(raw_action, close_side, pnl_pct, close_reason)
        close_extra = self._format_close_extra(
            close_side, pnl_pct, self.tv_price, self.regime, self.current_atr,
        )

        if not raw_action:
            logger.warning("TV ???? action????")
            return
        if raw_action in (
            "LONG", "SHORT", "CLOSE", "CLOSE_PROTECT", "CLOSE_TP3",
            "CLOSE_STOPLOSS", "UPDATE_SL", "UPDATE_TP",
        ) or raw_action.startswith("CLOSE"):
            self._record_tv_signal(payload, raw_action)

        if not self._lock.acquire(timeout=120.0):
            logger.error(f"?? ??? 120s ????? {raw_action} ????")
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
                    f"??? [{self.symbol}] ??????? | {raw_action} "
                    f"??? {age:.2f}s < {LATE_CLOSE_SUPPRESS_SEC}s ? ????"
                )
                try:
                    logger.warning(
                        f"[?????] ???????????? [{self.symbol}] | "
                        f"{raw_action} ?????? {age:.2f}s (?={LATE_CLOSE_SUPPRESS_SEC}s)"
                    )
                except Exception:
                    pass
                return
            if is_close:
                self.monitoring = False
            if raw_action in ("CLOSE_PROTECT", "CLOSE_QUICK_EXIT", "CLOSE_RSI_EXIT") or raw_action.startswith("CLOSE_PROTECT"):
                pos = self._get_active_position()
                if pos == "QUERY_FAILED":
                    logger.error(f"?? [{self.symbol}] {raw_action} ??????????? ? ??????")
                    return
                tv_reason = close_reason or ("?????" if "PROTECT" in raw_action else f"TV??:{raw_action}")
                if not pos or self._safe_qty(pos.get("size", 0)) <= 0:
                    logger.info(f"??? ????????????? ? ???? | {tv_reason}{close_extra}")
                    self._handle_manual_flat_detected(
                        tv_reason,
                        close_meta=close_meta,
                        curr_px=self.tv_price,
                    )
                else:
                    self._close_all(
                        f"??? ?????{tv_reason}{close_extra}",
                        close_meta=close_meta,
                    )
            elif raw_action == "CLOSE_TP3":
                pos = self._get_active_position()
                if pos == "QUERY_FAILED":
                    logger.error(f"?? [{self.symbol}] CLOSE_TP3 ??????????? ? ??????")
                    return
                tv_reason = close_reason or "TP3????"
                if not pos or self._safe_qty(pos.get("size", 0)) <= 0:
                    self._handle_manual_flat_detected(
                        tv_reason,
                        close_meta=close_meta,
                        curr_px=self.tv_price,
                    )
                else:
                    self._close_all(
                        f"?? TP3???{tv_reason}{close_extra}",
                        close_meta=close_meta,
                    )
            elif raw_action == "CLOSE_STOPLOSS":
                pos = self._get_active_position()
                if pos == "QUERY_FAILED":
                    logger.error(f"?? [{self.symbol}] CLOSE_STOPLOSS ??????????? ? ??????")
                    return
                tv_reason = close_reason or "????/??"
                if not pos or self._safe_qty(pos.get("size", 0)) <= 0:
                    self._handle_manual_flat_detected(
                        tv_reason,
                        close_meta=close_meta,
                        curr_px=self.tv_price,
                    )
                else:
                    tag = (
                        "?????"
                        if close_meta.get("close_type") == CLOSE_TYPE_BREAKEVEN
                        else "???"
                    )
                    self._close_all(
                        f"?? {tag}?{tv_reason}{close_extra}",
                        close_meta=close_meta,
                    )
            elif raw_action == "CLOSE":
                pos = self._get_active_position()
                if pos == "QUERY_FAILED":
                    logger.error(f"?? [{self.symbol}] CLOSE ??????????? ? ??????")
                    return
                tv_reason = close_reason or "TV??????"
                if not pos or self._safe_qty(pos.get("size", 0)) <= 0:
                    logger.info(
                        f"?? TV????????? ? ???????? | {tv_reason}{close_extra}"
                    )
                    self._handle_manual_flat_detected(
                        tv_reason,
                        close_meta=close_meta,
                        curr_px=self.tv_price,
                    )
                else:
                    self._close_all(
                        f"?? TV???????{tv_reason}{close_extra}",
                        close_meta=close_meta,
                    )
            elif raw_action == "UPDATE_SL":
                self._handle_tv_sl_update(payload)
            elif raw_action == "UPDATE_TP":
                self._handle_tv_tp_update(payload)
            elif raw_action in ["LONG", "SHORT"]:
                self._apply_tv_sl_from_payload(payload, source=f"{raw_action}??")
                self._apply_tv_sizing_params(payload)
                self.last_tv_side = raw_action
                self._save_state()
                self._handle_smart_entry(raw_action, payload)
            else:
                logger.warning(f"???? TV action: {raw_action}")
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
        """???????? ATR ? ? ?? ? ? ??????"""
        ref_px = curr_px or self.tv_price or pos["entry_price"]
        live_entry = pos["entry_price"]
        diff_pct = self._entry_price_diff_pct(live_entry, self.tv_price, ref_px)
        open_regime = int(getattr(self, "open_regime", self.regime) or self.regime)
        open_atr = float(getattr(self, "open_atr", self.current_atr) or self.current_atr)
        tv_atr = float(self.current_atr)

        if not self._is_similar_atr(open_atr, tv_atr):
            logger.info(
                f"?? ?? [{action}] ATR {open_atr:.2f}?{tv_atr:.2f} ?? "
                f"(>{ATR_SIMILAR_RATIO:.0%}) ? ??????"
            )
            return "FULL_REENTRY", diff_pct, "atr_changed", open_atr, tv_atr

        if int(self.regime) != open_regime:
            logger.info(
                f"?? ?? [{action}] ?? R{open_regime}?R{self.regime} ? ??????"
            )
            return "FULL_REENTRY", diff_pct, "regime_changed", open_atr, tv_atr

        if diff_pct >= SAME_DIR_MIN_SPREAD_PCT:
            logger.info(
                f"?? ?? [{action}] ?? {diff_pct:.3f}% ? {SAME_DIR_MIN_SPREAD_PCT}% ? ????"
            )
            return "FULL_REENTRY", diff_pct, "spread_ok", open_atr, tv_atr

        logger.info(
            f"?? ?? [{action}] ATR {tv_atr:.2f} ?? + ?? {diff_pct:.3f}% "
            f"< {SAME_DIR_MIN_SPREAD_PCT}% ? ??? TP123"
        )
        return "REFRESH_TP", diff_pct, "refresh_tp", open_atr, tv_atr

    def _report_smart_reentry(self, action, pos, diff_pct, reason, open_atr, tv_atr):
        live_entry = pos["entry_price"]
        real_qty = self._safe_qty(pos.get("size"))
        reason_txt = {
            "atr_changed": f"TV ATR `{tv_atr:.2f}` ? ?? ATR `{open_atr:.2f}` ? ????",
            "regime_changed": f"?? R{self.open_regime}?R{self.regime} ? ????",
            "spread_ok": f"???? {diff_pct:.3f}% ? {SAME_DIR_MIN_SPREAD_PCT}% ? ????",
        }.get(reason, "??????")
        self._call_telegram_notify(
            telegram_notify.report_smart_same_dir_decision,
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
                f"???? {real_qty}? @ {live_entry:.2f} | {reason_txt} | ??????"
            ),
        )

    def _same_direction_refresh_tp(self, action, pos, curr_px, diff_pct, open_atr, tv_atr):
        live_pos = self._get_active_position()
        if live_pos == "QUERY_FAILED":
            logger.warning("?? ????: ????????????? ? ??")
            return
        if not live_pos or self._safe_qty(live_pos.get("size", 0)) <= 0:
            logger.warning("?? ????: ?????????")
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
            reason="??TV??????",
        )
        self._ensure_sentinel_running()

        verify_note = (
            f"???? {real_qty}? @ {entry:.2f} | TV?? {self.tv_price:.2f} | "
            f"??ATR {open_atr:.2f} = TV ATR {tv_atr:.2f} | "
            f"?? {diff_pct:.3f}% (< {SAME_DIR_MIN_SPREAD_PCT}%) | "
            f"?? {result['matched']}/{result['expected']} ? | "
            f"{self._format_audit_summary(result['audit'])}"
        )
        self._call_telegram_notify(
            telegram_notify.report_smart_same_dir_decision,
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
        logger.info("?? ????????: ATR??+??????????TP123 ??? TV ???")

    def _ensure_sentinel_running(self):
        if self.monitoring and not self._sentinel_active:
            threading.Thread(
                target=self._sentinel_loop, daemon=True, name="sentinel",
            ).start()
        # ?6.2????? WS??????????
        if not getattr(self, "_deepcoin_private_ws_started", False):
            self._start_deepcoin_private_ws()

    def _start_deepcoin_private_ws(self):
        """?6.2??? Deepcoin ?? WebSocket ? ?????? ? ????????"""
        self._deepcoin_private_ws_started = True
        self._ws_hard_sl_fill_hint = None
        self._ws_tp1_fill_hint = False
        self._ws_tp_fill_levels = set()
        try:
            deepcoin_client.start_private_ws(on_message=self._on_deepcoin_ws_message)
        except Exception as e:
            logger.warning(f"[{self.symbol}] ??WS??????????: {e}")
            self._deepcoin_private_ws_started = False

    def _on_deepcoin_ws_message(self, data):
        """?6.2?Deepcoin ?? WS ?? ? TP ???? + ??????????"""
        if not isinstance(data, dict):
            return
        table = str(data.get("table") or "").lower()
        if table not in ("order", "position", "trade", "triggerorder"):
            return
        # ????
        self._ws_defense_pulse = True
        self._ws_fast_poll = True

        if table == "trade":
            self._handle_deepcoin_trade_event(data)
        elif table == "order":
            self._handle_deepcoin_order_event(data)

    def _handle_deepcoin_trade_event(self, data):
        """Trade ??????? TP1 ???????????????"""
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
            # ????? ? TP ??
            if px <= 0:
                continue
            # ???? TP ??
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
        """Order ???FILLED/PARTIALLY_FILLED ? ??????????"""
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
        """?12.3????????? ? ?????? + ?????"""
        try:
            sym = str(order.get("instrument_id") or order.get("symbol") or "").upper()
            if sym and sym != self.symbol.upper():
                return
            oid = order.get("orderId") or order.get("algoId") or ""
            status = str(order.get("status") or "").upper()
            logger.warning(
                f"?? [{self.symbol}] ???? event: id={oid} status={status} | "
                f"??????"
            )
            self._ws_defense_pulse = True
            self._ws_fast_poll = True
        except Exception as e:
            logger.debug(f"????????: {e}")

    def _purge_all_defense_orders_on_flat(self, reason="", max_rounds=6):
        """?6.2????????????TP + ?? STOP??"""
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
            logger.info(f"?? [{self.symbol}] ?????{cancelled} ? | {reason}")
        return cancelled

    def _schedule_partial_fill_resize(self, source=""):
        """?6.2?TP/?????????????/?????????????"""
        if getattr(self, "api_monitor_only", False):
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
                    f"? [{self.symbol}] ?????????????? | {source}"
                )
                return
            live = float((pos or {}).get("size") or 0)
            if live <= 0:
                logger.info(
                    f"?? [{self.symbol}] WS?????? ? ??? | {source}"
                )
                self._purge_all_defense_orders_on_flat(f"WS????|{source}")
                return
            if self._is_dust_qty(live):
                logger.warning(
                    f"?? [{self.symbol}] ???????={live} ? ???? | {source}"
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

    def _full_reentry(self, action, close_reason, payload=None):
        """???????? ? ??? ? ???????????
        v16.19 ????? `_close_all` ???????????2??? + ??????
        """
        reason = close_reason or "TV?????????"
        try:
            self._pipeline_pending_clear(note=reason)
        except Exception:
            pass
        deepcoin_client.cancel_all_open_orders(self.symbol)
        time.sleep(0.3)  # v16.19: 0.5?0.3s
        if not self._close_all(reason, reset_state=True):
            logger.error("? ???????????????????")
            try:
                self._pipeline_fail(Role.AUDITOR_POS, "CLEAR_FAIL")
            except Exception:
                pass
            logger.warning(
                f"[?????] ???????????? | ?????????????????????? Deepcoin ??"
            )
            self._close_open_chain_active = False
            return
        if not self._wait_verify(self._verify_flat, retries=6, delay=0.25):  # v16.19: 8??0.5?6??0.25
            logger.error("? ??????????????")
            try:
                self._pipeline_fail(Role.AUDITOR_POS, "CLEAR_VERIFY_FAIL")
            except Exception:
                pass
            logger.warning(
                f"[?????] ????????????? | ??????? REST ?????????????"
            )
            self._close_open_chain_active = False
            return
        try:
            self._pipeline_cleared(note="sterile_ok")
        except Exception:
            pass
        # v16.19????2??? + ??0.5s ???_close_all ??????????????
        curr_px = deepcoin_client.get_current_price(self.symbol) or self.tv_price
        if curr_px <= 0:
            logger.error("? ????????????")
            return
        try:
            logger.info(
                f"[?????] ??????? [{self.symbol}] | {reason} @ {float(curr_px):.2f} ? ? {action}"
            )
        except Exception:
            pass
        self._open_position(action, curr_px, payload=payload)
        self._close_open_chain_active = False

    def _handle_manual_flat_detected(self, reason, close_meta=None, curr_px=0.0):
        """???? / ???? / ??????????? + ???????"""
        meta = self._enrich_close_meta_live(
            close_meta or self._infer_flat_close_meta(curr_px, hint_reason=reason),
            curr_px,
        )
        logger.info(f"?? ????: {meta.get('tv_reason') or reason}")
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
            meta.get("tv_reason") or reason or "????",
            close_meta=meta,
            curr_px=curr_px,
        )

    def _realign_after_position_add(self, new_qty, new_entry, curr_px, entry_type):
        """
        ??????? TV TP123 ?? + ???????????? tv_sl / ???
        ????? TP?????????? open_regime ?????????
        """
        self._ensure_tp123_prices_from_tv(new_entry)
        tp_txt = "/".join(
            f"{float(p):.0f}" for p in (self.tv_tps or []) if float(p or 0) > 0
        ) or "?"
        ratios = self.regime_settings[self._tp_split_regime()]["ratios"]
        consumed = list(getattr(self, "tp_levels_consumed", []) or [])

        sl_to_pass = None
        radar_note = "????(TP1?)"
        if self._tp1_filled_verified(new_qty, curr_px):
            self._refresh_radar_state_on_recover(curr_px, new_entry)
            sl_to_pass = self._radar_sl_to_pass()
            radar_note = (
                f"????? SL={sl_to_pass:.2f}"
                if sl_to_pass else "?????"
            )

        logger.info(
            f"??? [{entry_type}] ??????? | ?? {new_qty} ? @ {new_entry:.2f} "
            f"| TV TP={tp_txt} | R{self._tp_split_regime()} ?? {ratios} "
            f"| ???? {consumed or '?'}"
        )
        self._cancel_all_tp_limit_orders()
        time.sleep(0.45)

        result = self._enforce_defense_alignment(
            new_qty, new_entry,
            dynamic_sl=sl_to_pass,
            reason=f"{entry_type}???TP123??",
            rounds=4,
        )
        audit = result.get("audit") or self._audit_tp_levels(new_qty)
        if not self._tp_audit_ok(audit):
            logger.warning(
                f"?? [{entry_type}] ??? TP ?? ? ???? | "
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
                radar_note = f"???? SL={sl:.2f}"

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
        """PYRAMID / PROFIT_ADD?base_qty ? TV qty_ratio ?????? TP123 + ????"""
        entry_type = normalize_entry_type(payload.get("entry_type"))
        max_add = self._max_add_times_for_regime()
        tv_ratio = float(getattr(self, "tv_qty_ratio", 0) or 0)
        pos = self._get_active_position()
        if pos == "QUERY_FAILED":
            logger.error(f"{entry_type} ??????????????? ? ??")
            return
        if not pos or self._safe_qty(pos.get("size", 0)) <= 0:
            logger.warning(f"{entry_type} ????????????")
            return
        if self._pos_side_label(pos) != action:
            logger.warning(
                f"[?????] {entry_type} ???? | TV {action} vs ?? {self._pos_side_label(pos)}??????"
            )
            return
        if tv_ratio <= 0:
            logger.warning(
                f"{entry_type} ???R{self.regime} TV????={tv_ratio:.2f}????????"
            )
            logger.warning(
                f"[?????] {entry_type} ???? | TV????={tv_ratio:.2f} ? 0 | base={getattr(self, 'base_qty', 0)} ?"
            )
            return
        if int(getattr(self, "add_count", 0) or 0) >= max_add:
            logger.warning(
                f"{entry_type} ????? R{self.regime} ?????? {max_add} "
                f"(base={getattr(self, 'base_qty', 0)})"
            )
            logger.warning(
                f"[?????] {entry_type} ???? | ?????? {max_add} ? | base={getattr(self, 'base_qty', 0)} | ?? {self._safe_qty(pos.get('size', 0))} ?"
            )
            return

        curr_px = deepcoin_client.get_current_price(self.symbol) or self.tv_price
        old_qty = self._safe_qty(pos.get("size", 0))
        old_entry = float(pos.get("entry_price", 0))
        add_qty, meta = self._calc_vps_add_qty(tv_ratio)
        if add_qty <= 0:
            logger.error(f"{entry_type} ?????????? {meta}")
            logger.warning(
                f"[?????] {entry_type} ???? | ??????"
            )
            return

        deepcoin_client.set_position_mode(self.symbol, mode="both")
        deepcoin_client.set_leverage(self.symbol, leverage=EXCHANGE_LEVERAGE)
        logger.info(
            f"? [{entry_type}] {action} ?? {add_qty} ? | "
            f"{self._tv_sizing_note(add_qty, meta, entry_type=entry_type)}"
        )
        open_side = "buy" if action == "LONG" else "sell"
        pos_side = "long" if action == "LONG" else "short"
        res = deepcoin_client.place_market_order(self.symbol, open_side, pos_side, add_qty)
        if not res or not deepcoin_client._is_success(res):
            logger.warning(
                f"[?????] {entry_type} ???? | {action} ?? {add_qty} ? ??????"
            )
            return
        time.sleep(1.5)

        new_pos = self._get_active_position()
        if new_pos == "QUERY_FAILED":
            logger.error(
                f"[?????] {entry_type} ??????? ? ??????????"
            )
            return
        if not new_pos or self._safe_qty(new_pos.get("size", 0)) <= old_qty:
            logger.warning(
                f"[?????] {entry_type} ???? | ?? {add_qty} ? ??????"
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
        type_label = "????" if entry_type == ENTRY_TYPE_PROFIT_ADD else "?????"
        tp_summary = self._format_audit_summary(audit)
        verify_note = (
            f"{type_label} | {self._tv_sizing_note(add_qty, meta, entry_type=entry_type)} "
            f"| base={getattr(self, 'base_qty', 0)} "
            f"| ???? {self.add_count}/{max_add} "
            f"| ?? {old_qty}?{new_qty} ? @ {new_entry:.2f} "
            f"| TV TP={realign.get('tp_prices', '?')} "
            f"| TP {audit.get('matched_full', 0)}/{audit.get('expected', 0)} "
            f"| {tp_summary} "
            f"| {realign.get('radar_note', '')} "
            f"| tv_sl={getattr(self, 'tv_sl', 0):.2f} "
            f"| {'?????' if sl_ok and self._tp_audit_ok(audit) else '?????'}"
        )
        self._call_telegram_notify(
            telegram_notify.report_tv_position_add,
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
        """??????????? OPEN ????????"""
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
        last_open = self._load_last_journal_entry(OPEN_JOURNAL, self.symbol)
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
        ??????? TV ? ?????????????? CLOSE* ?????
        """
        payload = payload or {}
        entry_type = normalize_entry_type(payload.get("entry_type"))

        if entry_type in (ENTRY_TYPE_PYRAMID, ENTRY_TYPE_PROFIT_ADD):
            self._add_to_position(action, payload)
            self._touch_entry_signal_signature(action)
            return

        curr_px = deepcoin_client.get_current_price(self.symbol) or self.tv_price
        if self._verify_flat() and self._is_duplicate_flat_entry(action, curr_px):
            logger.info(f"?? ???????? TV [{action}] ? ?????????")
            try:
                self._call_telegram_notify(
                    telegram_notify.report_smart_same_dir_decision,
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
                    verify_note="??????????? | ??????",
                )
            except Exception:
                pass
            self._touch_entry_signal_signature(action)
            return

        pos = self._get_active_position()
        if pos == "QUERY_FAILED":
            logger.warning("?? [????] ??????????????")
            live_sz = 0
            live_side = "UNKNOWN"
        else:
            live_sz = self._safe_qty((pos or {}).get("size", 0))
            live_side = self._pos_side_label(pos) if pos else None
        logger.info(
            f"? TV?? [{action}] entry={entry_type} ? ???????? "
            f"| ?? {live_side or 'FLAT'} {live_sz}?"
        )
        self._full_reentry(
            action,
            "TV???????????????????????????",
            payload=payload,
        )
        self._touch_entry_signal_signature(action)

    def _open_position(self, action, curr_px, payload=None):
        payload = payload or {}
        if self._open_in_progress:
            logger.error(f"??????????????????? [{action}]")
            return
        self._open_in_progress = True
        try:
            self._snapshot_sizing_principal(
                f"??? {normalize_entry_type(payload.get('entry_type'))} R{self.regime}"
            )
            qty, balance, margin_usdt, margin_pct, sizing_meta = self._calc_target_open_qty(
                curr_px, payload=payload,
            )
            if qty <= 0:
                logger.error(f"??????????? balance={balance:.2f} px={curr_px}")
                return

            deepcoin_client.set_position_mode(self.symbol, mode="both")
            deepcoin_client.set_leverage(self.symbol, leverage=EXCHANGE_LEVERAGE)
            notional = qty * self.face_value * curr_px
            budget_txt = format_vps_sizing_note(sizing_meta, qty=qty, entry_type=ENTRY_TYPE_OPEN)
            logger.info(f"?? ???? [{self.symbol}]: {budget_txt} (?? ~{notional:.0f}U)")

            cap_ok, _cap_meta = self._assert_notional_cap_or_reject(
                qty, curr_px, sizing_meta=sizing_meta,
            )
            if not cap_ok:
                return

            if not self._wait_verify(self._verify_flat, retries=4, delay=0.35):
                logger.error("???????????????")
                logger.warning(
                    f"[?????] ???????????? [{self.symbol}] | TV {action} ?? {qty}??REST ?????????"
                )
                return

            open_side = "buy" if action == "LONG" else "sell"
            pos_side = "long" if action == "LONG" else "short"
            logger.info(f"?? [????] ????: {open_side} {qty} ? | {self.symbol} | ?? {self.regime}")
            try:
                self._pipeline_entry_submitted(action, qty)
            except Exception:
                pass
            res = deepcoin_client.place_market_order(self.symbol, open_side, pos_side, qty)
            if not res or not deepcoin_client._is_success(res):
                logger.error("???????????")
                try:
                    self._pipeline_fail(Role.EXECUTION, "ENTRY_SUBMIT_FAIL")
                except Exception:
                    pass
                logger.warning(
                    f"[?????] ???? | TV {action} {qty} ? ?????"
                )
                return
            time.sleep(1.2)  # v16.19: 2.0?1.2s???????WS ?????

            pos = self._get_active_position()
            if pos == "QUERY_FAILED":
                logger.error("???????? REST ???????? ? ??????")
                try:
                    self._pipeline_fail(Role.EXECUTION, "ENTRY_CONFIRM_FAIL")
                except Exception:
                    pass
                return
            if not pos or self._safe_qty(pos.get("size", 0)) <= 0:
                logger.error("???????? REST ???")
                try:
                    self._pipeline_fail(Role.EXECUTION, "ENTRY_CONFIRM_FAIL")
                except Exception:
                    pass
                return

            real_qty = self._safe_qty(pos["size"])
            if real_qty > qty * OPEN_OVERSIZE_RATIO:
                logger.error(
                    f"?? ????: ?? {qty} ???? {real_qty} ? "
                    f"(>{qty * OPEN_OVERSIZE_RATIO:.3f})?????"
                )
                logger.warning(
                    f"[?????] ????????? | ?? {qty}???? {real_qty}???? reduceOnly ??"
                )
                real_qty = self._trim_position_to_target(qty, action)
                fresh_pos = self._get_active_position()
                if fresh_pos == "QUERY_FAILED":
                    logger.warning("?? ??????????? ? ??????????")
                elif fresh_pos:
                    pos = fresh_pos
                    pos["size"] = real_qty

            self.current_side = action
            self.open_regime = self.regime
            payload_atr = self._safe_float((payload or {}).get("atr"), 0.0)
            if payload_atr <= 0:
                try:
                    logger.warning(
                        f"[?????] ??????TV atr | {self.symbol} webhook ? atr ? ????"
                    )
                except Exception:
                    pass
                logger.error(f"????? TV atr [{self.symbol}]")
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

    def _sanitize_open_tps_vs_mark(self, curr_px=None):
        """
        binance parity (2a2e28f/0805f17/977135d): if a TV TP1/TP2 price is
        already marketable against the current mark price at open time, nudge
        it the minimum safe distance away and persist the nudged value into
        self.tv_tps. Without this, a marketable TP either fills instantly
        against intent or gets treated as "drifted" by later audit passes,
        which repeatedly cancels and re-places it (the cancel/place death
        spiral binance hit before this fix). TP3 is never a limit order so
        it is left untouched.
        """
        try:
            px = float(curr_px if curr_px is not None else (deepcoin_client.get_current_price(self.symbol) or 0))
        except Exception:
            px = 0.0
        side = str(self.current_side or "").upper()
        tps = list(getattr(self, "tv_tps", None) or [])
        if px <= 0 or side not in ("LONG", "SHORT") or not tps:
            return
        atr = float(getattr(self, "open_atr", None) or self.current_atr or 0)
        min_gap = max(px * 0.0015, atr * 0.15, 0.5)
        changed = False
        for idx in (0, 1):
            if idx >= len(tps):
                continue
            tp_px = float(tps[idx] or 0)
            if tp_px <= 0:
                continue
            if side == "LONG" and tp_px <= px + min_gap:
                tps[idx] = round(px + min_gap, 2)
                changed = True
            elif side == "SHORT" and tp_px >= px - min_gap:
                tps[idx] = round(px - min_gap, 2)
                changed = True
        if changed:
            logger.warning(
                f"[{self.symbol}] TP marketable at open, nudged away from mark "
                f"px={px:.2f} min_gap={min_gap:.4f} | before={self.tv_tps} after={tps}"
            )
            self.tv_tps = tps
            self._save_state()

    def _protect_and_monitor(self, qty, entry_price, budget_note="", target_qty=0, sizing_meta=None):
        """?? v1.0 ?3?????? = |TV.price ? TV.stop_loss| ? 1.15?"""
        self._lock_frozen_hard_sl_from_tv(
            entry=entry_price, side=self.current_side, source="???",
        )
        hard_sl = float(self._frozen_hard_px() or 0)
        self.current_sl = hard_sl if hard_sl > 0 else 0.0
        self.best_price = entry_price
        self._adverse_worst_px = 0.0
        self._adverse_worst_px_ts = 0.0
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

        # ?? ?3.7?? tv_stop_distance / atr ?? adx_tier??0/?1/?2?
        # tv_stop_distance = |TV.price ? TV.stop_loss|?1.3?ATR = ??????
        open_atr = float(getattr(self, "open_atr", None) or self.current_atr or 0)
        tv_sl_val = float(getattr(self, "tv_sl_ref", 0) or 0)
        tv_price_val = float(getattr(self, "tv_price", 0) or entry_price or 0)
        if open_atr > 0 and tv_sl_val > 0 and tv_price_val > 0:
            tv_stop_dist = abs(tv_price_val - tv_sl_val)
            tier_threshold = 1.3 * open_atr
            if tv_stop_dist > tier_threshold:
                derived_tier = 0  # ???
            else:
                derived_tier = 2  # ??????? tv_stop_dist <= tier_threshold?
            self.adx_tier = derived_tier
            self.radar_tier = derived_tier
            logger.info(
                f"[{self.symbol}] ?? ?3.7 adx_tier={derived_tier} "
                f"(tv_dist={tv_stop_dist:.2f} ATR={open_atr:.2f} "
                f"??={tier_threshold:.2f})"
            )

        if hasattr(self, "_init_reentry_runtime"):
            self._init_reentry_runtime()

        if hasattr(self, "_radar_stage_last"):
            self._radar_stage_last = 0
        self._radar_activation_notified = False

        self._sanitize_open_tps_vs_mark(entry_price)

        # ?? v1.0???????????? TP1/TP2 ?????
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
                vqty, verified["entry_price"], source="????",
            )
            self._enforce_defense_alignment(
                vqty, verified["entry_price"],
                dynamic_sl=None, reason="???????", rounds=4,
                recover_mode=True,
            )
            audit = self._wait_defense_settled(vqty)
            matched, expected = audit["matched_full"], audit["expected"]
            curr_px = deepcoin_client.get_current_price(self.symbol) or entry_price
            if expected > 0 and matched < expected:
                logger.warning(
                    f"?? ???? TP ? {matched}/{expected} ? ??????"
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
                f"?? {vqty}? @ {verified['entry_price']:.2f} | "
                f"???? {matched}/{expected} ? | {self._format_audit_summary(audit)} | "
                f"{self._tv_field_source_note(getattr(self, '_last_tv_field_sources', {}))}"
            )
            if target_qty > 0 and vqty > target_qty * OPEN_OVERSIZE_RATIO:
                verify_note += f" | ?? ???? {target_qty} ?"
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
                    tp1={"px": float((self.tv_tps or [0])[0] or 0), "qty": round(vqty * float(ratios[0]), 4), "filled": False},
                    tp2={"px": float((self.tv_tps or [0, 0])[1] or 0) if len(self.tv_tps or []) > 1 else 0.0, "qty": round(vqty * float(ratios[1]), 4), "filled": False},
                )
                self._pipeline_run_chief_audit(source="deepcoin_open")
            except Exception as e:
                logger.warning(f"[{self.symbol}] chief audit wire: {e}")
            self._call_telegram_notify(
                telegram_notify.report_supervisor_open,
                side=self.current_side,
                entry_price=verified['entry_price'],
                tv_price=self.tv_price,
                qty=vqty,
                tp_pxs=self.tv_tps,
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
                    "?? TP ??????? | ???????"
                    if dupes else "?? logs/deepcoin_brain.log"
                )
                logger.warning(
                    f"[?????] ???????????? | {self.current_side} {vqty}? | ? {matched}/{expected} ?"
                )
            if self._should_activate_shield(curr_px):
                self._maintain_hard_shield(
                    vqty, curr_px,
                    force=True,
                )
        else:
            logger.warning("????????????????")
        self._ensure_sentinel_running()

    def _ensure_price_ws(self):
        deepcoin_client.start_public_price_ws(self.symbol)

    def _tp1_distance(self):
        if self.tv_tps[0] > 0 and self.watched_entry:
            return abs(self.tv_tps[0] - self.watched_entry)
        return self.current_atr * 1.5

    def _radar_activation_ratio(self):
        """????? ADX ?????0.70~0.90??"""
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
        ??? v1.0 ? ???????
        ?????????? = (TP1 + TP2) / 2?TP1/TP2 ? webhook ???????
        ?????????? = TP2????????? TP2 ????
        ???? ADX ?? ? TP1 ???????
        """
        from reentry_profiles import radar_gate_price_from_tps

        frozen = float(getattr(self, "radar_activation_price", 0) or 0)
        activated = bool(getattr(self, "radar_activated", False))
        tps = list(getattr(self, "tv_tps", None) or [])
        tp1_px = float(tps[0] or 0) if len(tps) > 0 else 0.0
        tp2_px = float(tps[1] or 0) if len(tps) > 1 else 0.0
        attempt = int(getattr(self, "reentry_attempt", 0) or 0)

        # ?????????????
        if frozen > 0 and tp1_px > 0 and tp2_px > 0:
            if not activated:
                expect = radar_gate_price_from_tps(tp1_px, tp2_px, attempt)
                if expect > 0 and abs(frozen - expect) / max(expect, 1e-9) > 0.002:
                    self.radar_activation_price = expect
                    return expect
            return frozen

        # ????
        if tp1_px > 0 and tp2_px > 0:
            px = radar_gate_price_from_tps(tp1_px, tp2_px, attempt)
            if px > 0:
                self.radar_activation_price = px
                return px
        return 0.0

    def _should_radar_trail(self, curr_px):
        """
        ??? v1.0 ? ?5.1 ???????
        ????TP1-TP2?????????TP2???????????????
        ???? TP1 ????????
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
        """TP ?????????????????????????"""
        if not self._should_radar_trail(curr_px):
            return self.current_sl
        new_sl = self._compute_radar_sl(curr_px)
        if new_sl is None:
            return self.current_sl
        if self.current_side == "LONG" and new_sl > self.current_sl:
            logger.info(
                f"?? ????????: {self.current_sl:.2f} ? {new_sl:.2f} "
                f"(best={self.best_price:.2f})"
            )
            self.current_sl = new_sl
            self._save_state()
        elif self.current_side == "SHORT" and (
                self.current_sl >= self.watched_entry or new_sl < self.current_sl
        ):
            logger.info(
                f"?? ????????: {self.current_sl:.2f} ? {new_sl:.2f} "
                f"(best={self.best_price:.2f})"
            )
            self.current_sl = new_sl
            self._save_state()
        return self.current_sl

    def _bump_best_on_tp_fill(self, old_qty, new_qty, curr_px):
        """?????? best_price ?????? TP ????????"""
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
                    f"?? ?????? best_price: {self.best_price:.2f} ? {new_best:.2f} "
                    f"(qty {old_qty}?{new_qty})"
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
                    f"?? ?????? best_price: {self.best_price:.2f} ? {new_best:.2f} "
                    f"(qty {old_qty}?{new_qty})"
                )
                self.best_price = new_best

    def _radar_activation_progress(self, curr_px):
        """0~1?? ADX ??????????????????????"""
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
                real_amt, curr_px, reason="???? ? ????",
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
                    f"?? ??{self.regime} ????????????? {new_sl:.2f}",
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
                    f"?? ??{self.regime} ?????????????? {new_sl:.2f}",
                    sl_placed=sl_placed,
                )
                return True
        return False

    def _sentinel_loop(self):
        """?????/TP ?? + ???????????? 2~6 ??"""
        self._sentinel_active = True
        last_px = 0.0
        try:
            while self.monitoring:
                try:
                    if not self._lock.acquire(timeout=2.0):
                        continue
                    try:
                        pos = self._get_active_position()
                        if pos == "QUERY_FAILED":
                            logger.warning("?? [??] ??????????? ? ????")
                            continue
                        real_amt = self._safe_qty(pos.get("size")) if pos else 0
                        actual_side = "LONG" if pos and pos.get('posSide') == "long" else "SHORT"

                        if real_amt == 0:
                            if time.time() < getattr(self, "_sentinel_grace_until", 0):
                                logger.debug(
                                    "????????????????????"
                                )
                                continue
                            if self.watched_qty > 0:
                                if not self._confirm_position_flat():
                                    logger.warning(
                                        "?? [??] ??????????? ? ?????"
                                    )
                                    continue
                                flat_meta = self._infer_flat_close_meta(
                                    curr_px=last_px,
                                    hint_reason="???? (???? / ???? / ????)",
                                )
                                self._handle_manual_flat_detected(
                                    flat_meta.get("tv_reason", "????"),
                                    close_meta=flat_meta,
                                    curr_px=last_px,
                                )
                            break

                        if self.watched_qty > 0 and self._should_finalize_tp_victory(real_amt):
                            self._sweep_dust_and_finalize(
                                "???? (???? / ???? / TV ????)"
                            )
                            break

                        tv_opposite = self._strict_tv_opposite_side(actual_side)
                        if (
                            tv_opposite
                            and actual_side
                            and not self._live_aligns_with_credible_tv(actual_side)
                        ):
                            reason = (
                                f"?????????({actual_side}) vs "
                                f"??TV({tv_opposite}) [????]"
                            )
                            verify_note = (
                                f"???: ???? | ??TV {tv_opposite} | "
                                f"???? {actual_side}"
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
                            self._note_adverse_extreme(curr_px)

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
                                        f"?? [??] ???? {self.watched_qty}?{real_amt} ? "
                                        f"({drift:.2%}??? {QTY_ALIGN_MIN_PCT:.0%} ????)??????"
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
                            logger.info("?? [??] ???????????")
                        elif not qty_changed:
                            self._radar_guardian_audit(real_amt, curr_px)

                        if curr_px <= 0:
                            continue

                        # binance parity (v16.22+v16.24 v2.1): pre-breakeven checkpoint
                        # abolished. XAU/BNB volatility caused premature stop moves that
                        # got stopped out before the real move played out. Radar activation
                        # itself now starts at breakeven (see _perform_radar_handoff), so
                        # this early single-shot checkpoint is redundant and removed.

                        self._process_directional_defenses(real_amt, curr_px)
                        progress = self._radar_activation_progress(curr_px)
                        if (
                            progress >= 0.5
                            and not self._is_radar_active()
                            and self._scan_ticks % 5 == 0
                        ):
                            logger.info(
                                f"?? ????: ?? {progress:.0%} | ?? {curr_px:.2f} | "
                                f"?? {SENTINEL_POLL_ARMING}s"
                            )
                    finally:
                        self._lock.release()
                except Exception as e:
                    logger.error(f"????: {e}")
                if self.monitoring:
                    time.sleep(self._sentinel_poll_sec(last_px))
        finally:
            self._sentinel_active = False

    def _rebuild_defenses(self, qty, entry, dynamic_sl=None):
        close_side = "sell" if self.current_side == "LONG" else "buy"
        pos_side = "long" if self.current_side == "LONG" else "short"

        live_qty = self._resolve_live_qty(qty)
        if live_qty <= 0:
            logger.warning(f"??????????????? (?? {qty} ?)")
            return 0

        # ???????????????????_rebuild_defenses????
        now = time.time()
        if now - getattr(self, "_last_rebuild_attempt_ts", 0) < 10.0:
            logger.warning(
                f"[{self.symbol}] _rebuild_defenses ????{now - getattr(self, '_last_rebuild_attempt_ts', 0):.1f}s < 10s????"
            )
            return 0
        self._last_rebuild_attempt_ts = now

        self._cancel_all_tp_limit_orders()
        # ????????????????????0.35???2??
        time.sleep(2.0)
        # ????????????????????????????
        leftover = self._collect_tp_limit_orders()
        if leftover:
            prices = [f"@{o['price']:.2f}" for o in leftover]
            logger.warning(
                f"[{self.symbol}] ?????{len(leftover)}??TP??????..."
            )
            time.sleep(2.0)
            leftover = self._collect_tp_limit_orders()
            if leftover:
                prices = [f"@{o['price']:.2f}" for o in leftover]
                logger.error(
                    f"[{self.symbol}] ?TP???????????: {prices}"
                )
                return 0

        if live_qty != qty:
            self.watched_qty = live_qty
            self._save_state()

        consumed = getattr(self, "tp_levels_consumed", []) or []
        placed = 0

        # ?? v1.0 ?6 ???tv_tps ???? TP ???????????TV ????? / ????????
        # ? open_atr + open_regime ??? fallback????????
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
                    # v16.27: ??????? - TP ???????
                    tps_valid = True
                    if self.current_side == "SHORT":
                        for tp_px in tps:
                            if tp_px > entry * 0.95:
                                tps_valid = False
                                break
                    elif self.current_side == "LONG":
                        for tp_px in tps:
                            if tp_px < entry * 1.05:
                                tps_valid = False
                                break
                    if tps_valid:
                        self.tv_tps = self._sanitize_tp_prices(tps)
                        logger.warning(
                            f"?? tv_tps=?? ? ATR??Fallback TP123={self.tv_tps} "
                            f"| entry={entry:.2f} ATR={atr:.2f} R{regime}"
                        )
                    else:
                        logger.warning(
                            f"? ATR??Fallback???TP????????? | "
                            f"tps={tps} entry={entry:.2f} side={self.current_side}"
                        )
                    logger.warning(
                        f"[?????] TP?????ATR??Fallback | {self.current_side} {live_qty}? | ATR={atr:.2f} R{regime}"
                    )
                    self._save_state()

        logger.info(
            f"??? ?? TP: ? {live_qty}? | ??? TP{consumed or '?'} | "
            f"R{self._tp_split_regime()}"
        )

        for lv in self._expected_tp_levels(live_qty):
            q, px = lv["qty"], lv["price"]
            kind = f"TP{int(lv.get('level', 0))}"
            if q > 0 and px > 0:
                # v16.11?????????????????????????/?????
                blocked, tag0, meta0 = self._has_open_pending_defense_tag(kind)
                if blocked:
                    logger.warning(
                        f"[{self.symbol}] ??????? tag={tag0} kind={kind} ? "
                        f"???????????? | px={px:.2f}"
                    )
                    if not meta0.get("order_id"):
                        age = time.time() - float(meta0.get("ts", 0) or 0)
                        if age >= 45.0:
                            logger.warning(
                                f"[{self.symbol}] ?? {tag0} ? order_id ??? {age:.0f}s ? 45s ? ????"
                            )
                            self._complete_pending_defense_tag(tag=tag0)
                            self._save_state()
                            time.sleep(0.15)
                        else:
                            logger.warning(
                                f"[{self.symbol}] ?? {tag0} ? order_id ?? {age:.0f}s < 45s ? "
                                f"??????????????????"
                            )
                            continue
                    else:
                        if not self._confirm_stale_before_clear(tag0, meta0):
                            logger.warning(
                                f"[{self.symbol}] ?? {tag0} ????????? ? ?????????"
                            )
                            continue
                        self._complete_pending_defense_tag(tag=tag0)
                        self._save_state()
                        time.sleep(0.15)
                tag = make_defense_client_order_id(self.symbol, kind, px)
                # ???????????????????????TP??
                # ???????????????????????????
                existing_orders = self._collect_tp_limit_orders()
                # ??????????? px ??? price
                at_px_existing = [o for o in existing_orders if abs(o.get("px", o.get("price", 0)) - px) <= 1.0]
                if at_px_existing:
                    logger.warning(
                        f"[{self.symbol}] ?????? TP@{px:.2f} ({len(at_px_existing)}?)?????"
                    )
                    # ?????????
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
                # v16.11????????TP ???????????????
                max_retries = 3
                for attempt in range(max_retries):
                    # v16.18 ??????????????50???
                    allowed, guard_reason = self._check_tp_place_guard(int(lv.get("level", 0)))
                    if not allowed:
                        logger.warning(
                            f"? _rebuild???TP{lv['level']}@{px:.2f} | {guard_reason}"
                        )
                        break
                    res = deepcoin_client.place_limit_order(
                        self.symbol, close_side, pos_side, px, q,
                        reduce_only=True, cl_ord_id=tag,
                    )
                    # ?????reduceOnly ????? ? ????? ? ?????
                    if deepcoin_client.is_reduce_only_rejected(res):
                        logger.warning(
                            f"?? _rebuild TP{lv['level']} reduceOnly ?? {close_side} {q}? ? "
                            f"force_rest ??????????"
                        )
                        time.sleep(0.3)
                        fresh = deepcoin_client.force_rest_get_all_positions(self.symbol)
                        live_qty = 0
                        if fresh:
                            for fp in fresh:
                                if fp.get("posSide", "").lower() == pos_side:
                                    live_qty = self._safe_qty(fp.get("size", 0))
                                    break
                        if live_qty <= 0:
                            logger.info(f"_rebuild TP{lv['level']} ????????")
                            break
                        q = min(q, live_qty)
                        logger.info(f"?? _rebuild TP{lv['level']} ???? ? {q}????={live_qty}?")
                        res = deepcoin_client.place_limit_order(
                            self.symbol, close_side, pos_side, px, q,
                            reduce_only=True, cl_ord_id=tag,
                        )
                        if deepcoin_client.is_reduce_only_rejected(res):
                            logger.error(f"? _rebuild TP{lv['level']} ????? ? ??")
                            break
                    if res and deepcoin_client._is_success(res):
                        last = res
                        oid = str(last.get("orderId") or last.get("algoId") or "")
                        self._register_pending_defense_tag(tag, kind, price=px, order_id=oid)
                        placed += 1
                        self._increment_tp_place_guard()
                        logger.info(f"?? TP{int(lv.get('level', 0))} {q} @ {px:.2f} tag={tag}")
                        break
                    if attempt < max_retries - 1:
                        logger.warning(
                            f"[{self.symbol}] TP{int(lv.get('level', 0))} @ {px:.2f} "
                            f"??????? {attempt + 1}/{max_retries}"
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
                        f"?????????max_retries={max_retries}?"
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
        """???????????????????????
        ?? ?????reduceOnly ?????? ? ?? force_rest ????????
        ?? v16.19 ???????????????? WS + REST ?????????
        """
        deepcoin_client.cancel_all_open_orders(self.symbol)
        time.sleep(0.3)  # v16.19: 0.5?0.3s
        self._cancel_all_tp_limit_orders()
        time.sleep(0.2)  # v16.19: 0.3?0.2s?WS ???????????
        closed_successfully = False

        for round_i in range(6):
            # ?????????
            all_positions = self._get_all_positions()
            if not all_positions:
                closed_successfully = True
                break

            # ?????????
            for pos in all_positions:
                close_side = "sell" if pos["posSide"] == "long" else "buy"
                live_sz = self._safe_qty(pos["size"])
                logger.info(f"?? ??{round_i + 1}/6: {close_side} {live_sz}? {pos['posSide']} reduceOnly")
                res = deepcoin_client.place_market_order(
                    self.symbol, close_side, pos["posSide"], live_sz, reduce_only=True,
                )
                # ?? ?????reduceOnly ????????? force_rest ?? ??
                if deepcoin_client.is_reduce_only_rejected(res):
                    logger.warning(
                        f"?? reduceOnly ?? {close_side} {live_sz}? ? "
                        f"force_rest ????????"
                    )
                    time.sleep(0.4)
                    fresh_positions = deepcoin_client.force_rest_get_all_positions(self.symbol)
                    if fresh_positions:
                        for fp in fresh_positions:
                            fpsz = self._safe_qty(fp["size"])
                            if fpsz <= 0:
                                continue
                            fp_side = "sell" if fp["posSide"] == "long" else "buy"
                            logger.info(f"?? ?????: {fp_side} {fpsz}? {fp['posSide']}")
                            retry_res = deepcoin_client.place_market_order(
                                self.symbol, fp_side, fp["posSide"], fpsz, reduce_only=True,
                            )
                            if deepcoin_client.is_reduce_only_rejected(retry_res):
                                logger.error(
                                    f"? ??? reduceOnly ??? {fp_side} {fpsz}? "
                                    f"? ??????????"
                                )
                            time.sleep(0.4)
                    break
                # ?? ???????????? ??
                time.sleep(0.3)  # v16.19: 0.5?0.3s?WS ?????????
            # v16.19: ????????????????????
            inter_wait = 1.2 if round_i == 0 else 1.0  # ??1.2s???????????1.0s
            time.sleep(inter_wait)

        # ?????????????
        if not closed_successfully:
            all_residual = self._get_all_positions()
            if all_residual:
                total_residual = sum(self._safe_qty(p["size"]) for p in all_residual)
                # ????????
                for residual in all_residual:
                    residual_sz = self._safe_qty(residual["size"])
                    if residual_sz > 0:
                        if self._is_dust_qty(residual_sz):
                            close_side = "sell" if residual["posSide"] == "long" else "buy"
                            logger.warning(f"?? ?????: {close_side} {residual_sz}? {residual['posSide']}")
                            res = deepcoin_client.place_market_order(
                                self.symbol, close_side, residual["posSide"], residual_sz, reduce_only=True,
                            )
                            # ??? reduceOnly ?????
                            if deepcoin_client.is_reduce_only_rejected(res):
                                logger.error(
                                    f"? ??? reduceOnly ??: {close_side} {residual_sz}? "
                                    f"? ???????"
                                )
                            time.sleep(1.0)
                        else:
                            logger.warning(f"?? ??: {residual['posSide']} {residual_sz}? ?????")
                closed_successfully = self._verify_flat()

            if not closed_successfully:
                all_residual = self._get_all_positions()
                residual_info = ", ".join(
                    f"{p['posSide']}:{self._safe_qty(p['size'])}?" for p in all_residual
                ) if all_residual else "?"
                logger.error(f"? 6 ????????: {residual_info}")
                logger.warning(
                    f"[?????] ??????? | 6????????: {residual_info}?????? Deepcoin ??"
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
                # v16.22 ???????????????????????????? tv_sl/tv_tps
                self.tv_sl = 0.0
                self.tv_tps = [0.0, 0.0, 0.0]
                self._snapshot_sizing_principal("???????")
            else:
                # ?????? tv_sl ? tv_tps?????????????????
                residual = self._get_active_position()
                if residual == "QUERY_FAILED":
                    logger.warning("?? ??????????????????? ? ??????")
                elif residual:
                    self.watched_qty = self._safe_qty(residual["size"])
                    self.current_side = self._pos_side_label(residual)
                    self.watched_entry = residual["entry_price"]
                    logger.warning(
                        f"????????????: {self.current_side} {self.watched_qty} ?"
                    )
            self._save_state()

        deepcoin_client.cancel_all_open_orders(self.symbol)

        if reason and closed_successfully:
            if force_align:
                real_side, expected_side = force_align
                flat = self._wait_verify(self._verify_flat, retries=6, delay=0.5)
                verify_note = "????? | ????? | ????????"
                if not flat:
                    verify_note += " | REST ?????"
                self._call_telegram_notify(
                    telegram_notify.report_force_align,
                    real_side=real_side,
                    expected_side=expected_side,
                    verify_note=force_verify_note or verify_note,
                )
            else:
                self._report_flat_close(reason, close_meta=close_meta)

        return closed_successfully

    def recover_state_on_startup(self):
        """????????? TV/???? ? ???? ? ???? TP123 ? ????"""
        if not self._try_acquire_recover_singleton():
            return
        try:
            # v16.18?????? TP ????????????
            self._reset_tp_place_guard()
            # v16.21?????????????????
            self._clear_recover_confirmed_levels()
            # v16.22 ?????????????????? monitoring ?????
            # ?? monitoring=False?????????????????????????
            # ????????????????tv_sl/tv_tps??????
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r') as f:
                    s = json.load(f)
                    saved_monitoring = bool(s.get("monitoring"))
                    self.last_tv_side = s.get("last_tv_side")
                    self.current_side = s.get("current_side")
                    self.current_sl = s.get("current_sl", 0.0)
                    self.regime = s.get("regime", 3)
                    self.current_atr = s.get("current_atr", 30.0)
                    # v16.22 ???ATR ?????? open_journal ????? tv_sl fallback ????
                    if float(self.current_atr or 0) <= 0:
                        last_open = self._load_last_journal_entry(OPEN_JOURNAL, self.symbol)
                        if last_open and float(last_open.get("atr", 0) or 0) > 0:
                            self.current_atr = float(last_open.get("atr"))
                            logger.info(f"?? [??] ??????? ATR={self.current_atr:.2f}")
                    self.tv_tps = self._sanitize_tp_prices(s.get("tv_tps", [0.0, 0.0, 0.0]))
                    self.tv_sl = float(s.get("tv_sl", 0) or 0)
                    # v16.22 ???????????? last_tv_signal?
                    # ?? _hydrate_tv_defense_context ????????? TP123 ? tv_sl
                    self.last_tv_signal = s.get("last_tv_signal")
                    self.tv_price = float(s.get("tv_price", 0.0) or 0.0)
                    self.best_price = s.get("best_price", 0.0)
                    self.watched_qty = s.get("watched_qty", 0)
                    self.watched_entry = s.get("watched_entry", 0.0)
                    self.initial_qty = s.get("initial_qty", 0)
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
                    # ?? v1.0 ?3?????????
                    self.frozen_hard_sl_px = float(s.get("frozen_hard_sl_px", 0) or 0)
                    # ?? v1.0 ?5.0????????
                    self._early_be_checkpoint_done = bool(s.get("_early_be_checkpoint_done", False))
                    # ?? v1.0 ?8-9???????? + ?????
                    self.exit_ownership = str(s.get("exit_ownership", "NONE") or "NONE")
                    self.ownership_locked_at = float(s.get("ownership_locked_at", 0) or 0)
                    raw_tags = s.get("pending_order_tags") or {}
                    self._pending_order_tags = dict(raw_tags) if isinstance(raw_tags, dict) else {}
                    # v16.14??????????????????????????????
                    self._gc_stale_pending_defense_tags_on_startup()
                    self._mutex_leg = str(s.get("mutex_leg", "") or "")
                    if self.sizing_principal <= 0:
                        eq = deepcoin_client.get_principal_wallet_balance()
                        if eq > 0:
                            self.sizing_principal = eq

            if self.base_qty <= 0 and os.path.exists(self.state_file):
                last_open = self._load_last_journal_entry(OPEN_JOURNAL, self.symbol)
                if last_open:
                    jq = int(last_open.get("qty", 0) or 0)
                    if jq > 0:
                        self.base_qty = jq
                        logger.info(f"?? ?? base_qty ?????? {jq} ?")

            if self._scan_and_sweep_dust_on_startup(was_monitoring=saved_monitoring):
                return

            if self._recover_missed_flat_on_startup(was_monitoring=saved_monitoring):
                return

            pos = self._get_active_position()
            if pos == "QUERY_FAILED":
                logger.error(
                    "?? [??????] ???????? ? ??????????????????? ? ??????"
                )
                return
            if pos and self._safe_qty(pos.get("size", 0)) != 0:
                # v16.22????????? _run_idle_live_reconcile ?????????
                self._recover_in_progress = True
                recover_ok = False
                recover_err = ""
                radar_active = False
                sl_ok = False
                if not self._lock.acquire(timeout=120.0):
                    logger.error("? ????????????")
                    self._recover_in_progress = False
                    logger.warning(
                        f"[?????] ?????? | ????????120s???????????????????"
                    )
                    return
                try:
                    reconcile = self._reconcile_context_on_recover(pos)
                    reconcile_notes = reconcile["notes"]
                    side = "LONG" if pos.get("posSide") == "long" else "SHORT"

                    if self._live_aligns_with_credible_tv(side):
                        if reconcile.get("direction_mismatch"):
                            logger.warning(
                                f"?? [??] ????????????? {side} "
                                f"???TV???? ? ????"
                            )
                            self.last_tv_side = side
                            reconcile["direction_mismatch"] = False
                    elif self._enforce_tv_direction_or_flat(pos, source="VPS??"):
                        self._recover_in_progress = False
                        return

                    if reconcile.get("manual_open") or self._safe_qty(self.watched_qty) <= 0:
                        logger.info(
                            f"?? [??] ??/????? {side} "
                            f"{self._safe_qty(pos.get('size'))}? ? ???? TP123+??+??"
                        )
                        self._perform_live_takeover(
                            pos,
                            source="VPS??",
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
                        source="VPS??", manual_fresh=bool(reconcile.get("manual_open")),
                        recover_mode=True,
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
                        f"?? [??????] ??????? {self.current_side} {real_amt}? @ "
                        f"{self.watched_entry:.2f} | ?? {saved_initial}? | "
                        f"??? TP{getattr(self, 'tp_levels_consumed', []) or '?'} | "
                        f"??={'???' if radar_active else '??(TP1?)'} | "
                        f"TV?? {self.last_tv_side} | ?? {len(reconcile_notes)} ?"
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
                        self._call_telegram_notify(
                            telegram_notify.report_manual_position_change,
                            action_type="???? ? ????",
                            old_qty=0,
                            new_qty=real_amt,
                            new_entry_price=entry_px,
                            verify_note=(
                                f"TV?? {self.last_tv_side} | TP123 {self.tv_tps} | "
                                f"tv_sl={getattr(self, 'tv_sl', 0):.2f}"
                            ),
                            tp_audit=audit,
                            verified=bool(verified),
                        )

                    tv_note = ""
                    if self.last_tv_signal:
                        tv_note = (
                            f" | ??TV: {self.last_tv_signal.get('action')} "
                            f"@{self.last_tv_signal.get('ts', '')}"
                        )
                    reconcile_txt = (" | " + " ; ".join(reconcile_notes)) if reconcile_notes else ""
                    skip_note = " | ???????????" if not _rebuilt else ""
                    verify_note = (
                        f"?? {real_amt}? @ {entry_px:.2f} | "
                        f"?? {saved_initial}? | "
                        f"??? TP{getattr(self, 'tp_levels_consumed', []) or '?'} | "
                        f"TV?? {self.last_tv_side} | "
                        f"tv_sl={float(getattr(self, 'tv_sl', 0) or 0):.2f} | "
                        f"?? {matched}/{expected} ? | "
                        f"{self._format_audit_summary(audit)}{skip_note}{tv_note}{reconcile_txt}"
                    )
                    if not verified:
                        verify_note += f" | {telegram_notify.VERIFY_DELAY_MARK}"
                    if qty_change:
                        old_q, new_q, action_msg = qty_change
                        self._call_telegram_notify(
                            telegram_notify.report_manual_position_change,
                            action_type=action_msg,
                            old_qty=old_q,
                            new_qty=new_q,
                            new_entry_price=entry_px,
                            verify_note=f"?????? | {verify_note}",
                            tp_audit=audit,
                            verified=bool(verified),
                        )
                    if expected > 0 and matched < expected:
                        dupes = [lv for lv in audit.get("levels", []) if lv.get("status") == "duplicate"]
                        hint = (
                            "?? TP ????????TP3 ??? | ? API ????"
                            if dupes else "?? logs/deepcoin_brain.log ?????/????"
                        )
                        self._recover_tp_unconfirmed = True
                        logger.warning(
                            f"[?????] ???????????? | {self.current_side} {real_amt}? | ? {matched}/{expected}?"
                        )

                    health_txt = (
                        f" | ??? {health.get('pnl_label', '??')} | "
                        f"??? {health.get('shield_status', '???')} | "
                        f"?? {health.get('defense_plan', 'TP123+???')}"
                    )
                    verify_note = verify_note + health_txt

                    self._sentinel_grace_until = time.time() + SENTINEL_GRACE_AFTER_RECOVER_SEC

                    self._call_telegram_notify(
                        telegram_notify.report_recover_takeover,
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
                        f"  -> ?? ???????? | {health.get('pnl_label', '')} | "
                        f"?? {' ? '.join(policy_actions) if policy_actions else '???'}"
                    )
                    recover_ok = True
                except Exception as e:
                    import traceback
                    recover_err = f"{e}\n{traceback.format_exc()[-800:]}"
                    logger.error(f"? ????????: {recover_err}")
                    self.monitoring = True
                    self._save_state()
                    logger.warning(
                        f"[?????] ???????? | ??????????????? | {recover_err}"
                    )
                finally:
                    self._recover_in_progress = False
                    self._lock.release()

                if recover_ok and radar_active:
                    logger.info(
                        f"?? [??] ??????? | SL={self.current_sl:.2f} | "
                        f"??={'??/???' if sl_ok else '?????'}"
                    )

                if not self._sentinel_active:
                    threading.Thread(
                        target=self._sentinel_loop, daemon=True, name="sentinel",
                    ).start()
                elif recover_err:
                    self._post_recover_radar_pulse = True
            else:
                deepcoin_client.cancel_all_open_orders(self.symbol)
                logger.info("?? [??????] ??????????????????")
                self.monitoring = False
                self.watched_qty = 0
                self.initial_qty = 0
                self.base_qty = 0
                self.add_count = 0
                self.current_side = None
                self._save_state()
                flat_ok = self._wait_verify(self._verify_flat, retries=6, delay=0.5)
                standby_note = (
                    f"???? | ????? | ????? | "
                    f"{DEEPCOIN_SUPERVISOR_VERSION}"
                )
                if not flat_ok:
                    standby_note += f" | {telegram_notify.VERIFY_DELAY_MARK}"
                telegram_notify.report_recover_standby(
                    verify_note=standby_note,
                    version=DEEPCOIN_SUPERVISOR_VERSION,
                )
        except Exception as e:
            logger.error(f"? ??????: {e}")
            logger.warning(
                f"[?????] ?????? | {e}"
            )


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
                logger.info(f"?? ???? [{sym}] ?")
                sup.recover_state_on_startup()
            except Exception as e:
                logger.error(f"?????? [{sym}]: {e}")
    return SUPERVISORS


bootstrap_supervisors()
