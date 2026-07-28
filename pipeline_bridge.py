#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
岗位边界桥接：挂到现有 supervisor，不大拆交易所调用。

用法：class PositionSupervisorBinance(PipelineBridgeMixin, ...)
      self._pipeline_boot(exchange="binance")
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

from pipeline_ledger import Phase, Role, get_pipeline, soft_gates_enabled
from chief_auditor import audit_open_bundle, should_hard_pause
from api_throttle import get_throttle

logger = logging.getLogger(__name__)


class PipelineBridgeMixin:
    """轻量 mixin：状态推进 / 督察官 / 节流快照。"""

    def _pipeline_boot(self, exchange: str = "binance") -> None:
        ex = str(exchange or "binance")
        self._pipeline_exchange = ex
        self._pipeline = get_pipeline(getattr(self, "symbol", ""), ex)
        self._pipeline_throttle = get_throttle(ex)
        # 若 state 已 hydrate 过 pipeline，外层会 load_dict

    def _pipeline_obj(self):
        pl = getattr(self, "_pipeline", None)
        if pl is None:
            self._pipeline_boot(getattr(self, "_pipeline_exchange", "binance"))
            pl = self._pipeline
        return pl

    def _pipeline_state_blob(self) -> Dict[str, Any]:
        try:
            return self._pipeline_obj().to_dict()
        except Exception:
            return {}

    def _pipeline_load_blob(self, blob) -> None:
        try:
            self._pipeline_obj().load_dict(blob if isinstance(blob, dict) else None)
        except Exception as e:
            logger.debug(f"[{getattr(self, 'symbol', '')}] pipeline load skip: {e}")

    def _pipeline_advance(self, phase: Phase, role: Role, note: str = "", **fields):
        try:
            ok, msg = self._pipeline_obj().advance(phase, role, note=note, **fields)
            if not ok:
                logger.warning(
                    f"📋 [{getattr(self, 'symbol', '')}] pipeline "
                    f"{role.value}→{phase.value}: {msg}"
                )
            else:
                logger.info(
                    f"📋 [{getattr(self, 'symbol', '')}] "
                    f"{role.value} → {phase.value}"
                    + (f" | {note}" if note else "")
                )
            return ok
        except Exception as e:
            logger.warning(f"[{getattr(self, 'symbol', '')}] pipeline advance: {e}")
            return False

    def _pipeline_fail(self, role: Role, reason: str) -> None:
        try:
            self._pipeline_obj().mark_failed(role, reason)
            logger.error(
                f"📋 [{getattr(self, 'symbol', '')}] FAILED by {role.value}: {reason}"
            )
        except Exception as e:
            logger.warning(f"pipeline fail mark: {e}")

    def _pipeline_reset_flat(self, note: str = "flat") -> None:
        try:
            self._pipeline_obj().reset_idle(note=note)
        except Exception:
            pass

    def _pipeline_sync_qty(
        self,
        side: str = "",
        qty: float = 0.0,
        entry: float = 0.0,
        *,
        allow_initial: bool = False,
    ) -> None:
        try:
            self._pipeline_obj().sync_position(
                side=side, qty=qty, entry=entry, allow_initial=allow_initial,
            )
        except Exception:
            pass

    def _pipeline_signal_received(self, payload: Optional[dict] = None) -> None:
        p = dict(payload or {})
        action = str(p.get("action") or "").upper()
        if action not in ("LONG", "SHORT"):
            return
        tier = str(p.get("tier") or p.get("regime") or "")
        self._pipeline_advance(
            Phase.SIGNAL_RECEIVED,
            Role.SIGNAL,
            note=f"TV {action}",
            side=action,
            tier=tier,
            hard_sl_px=0.0,
        )

    def _pipeline_pending_clear(self, note: str = "") -> None:
        self._pipeline_advance(Phase.PENDING_CLEAR, Role.AUDITOR_POS, note=note)

    def _pipeline_cleared(self, note: str = "sterile") -> None:
        self._pipeline_advance(Phase.CLEARED, Role.AUDITOR_POS, note=note)

    def _pipeline_entry_submitted(self, side: str, qty: float) -> None:
        self._pipeline_advance(
            Phase.ENTRY_SUBMITTED,
            Role.EXECUTION,
            note="market_submit",
            side=str(side or "").upper(),
            qty=float(qty or 0),
        )

    def _pipeline_entry_confirmed(
        self, side: str, qty: float, entry: float,
    ) -> None:
        self._pipeline_advance(
            Phase.ENTRY_CONFIRMED,
            Role.EXECUTION,
            note="fill_confirmed",
            side=str(side or "").upper(),
            qty=float(qty or 0),
            entry=float(entry or 0),
            initial_qty=float(qty or 0),
            open_ts=time.time(),
        )
        self._pipeline_sync_qty(side, qty, entry, allow_initial=True)

    def _pipeline_orders_placed(
        self,
        *,
        hard_sl_px: float = 0.0,
        hard_sl_live: bool = False,
        tp1: Optional[dict] = None,
        tp2: Optional[dict] = None,
    ) -> None:
        fields: Dict[str, Any] = {
            "hard_sl_px": float(hard_sl_px or 0),
            "hard_sl_live": bool(hard_sl_live),
        }
        if tp1:
            fields["tp1"] = tp1
        if tp2:
            fields["tp2"] = tp2
        self._pipeline_advance(
            Phase.ORDERS_PLACED, Role.EXECUTION, note="hard+tp12", **fields,
        )

    def _pipeline_facts_for_audit(self) -> Dict[str, Any]:
        """从 supervisor 现有字段拼督察事实（不额外打 REST）。"""
        sym = str(getattr(self, "symbol", "") or "").upper()
        pl = self._pipeline_obj().to_dict()
        th = get_throttle(getattr(self, "_pipeline_exchange", "binance")).snapshot()
        tp1 = dict(pl.get("tp1") or {})
        tp2 = dict(pl.get("tp2") or {})
        # 优先用账本切片；缺则从 supervisor 推算意图量
        tq1 = float(tp1.get("qty") or getattr(self, "tv_qty1", 0) or 0)
        tq2 = float(tp2.get("qty") or getattr(self, "tv_qty2", 0) or 0)
        init_q = float(
            pl.get("initial_qty")
            or getattr(self, "initial_qty", 0)
            or getattr(self, "watched_qty", 0)
            or 0
        )
        if (tq1 <= 0 or tq2 <= 0) and init_q > 0:
            try:
                from webhook_parser import LEG_TP_RATIOS
                ratios = list(LEG_TP_RATIOS)
            except Exception:
                ratios = [0.10, 0.20, 0.70]
            if tq1 <= 0:
                tq1 = round(init_q * float(ratios[0]), 3)
            if tq2 <= 0:
                tq2 = round(init_q * float(ratios[1]), 3)
        hard_px = float(
            pl.get("hard_sl_px")
            or getattr(self, "frozen_hard_sl_px", 0)
            or getattr(self, "initial_stop", 0)
            or 0
        )
        risk = 0.0
        try:
            from webhook_parser import get_runtime_risk_pct, get_runtime_leverage
            risk_cfg = float(get_runtime_risk_pct())
            lev_cap = float(get_runtime_leverage())
        except Exception:
            risk_cfg = 0.2
            lev_cap = 5.0
        try:
            risk = float(getattr(self, "tv_risk_pct", 0) or risk_cfg)
            if risk > 1.0:
                risk = risk / 100.0
        except Exception:
            risk = risk_cfg
        lev = float(
            getattr(self, "tv_sizing_leverage", 0)
            or getattr(self, "leverage", 0)
            or lev_cap
        )
        # 雷达是否该激活：有激活价且现价已过 → should True（无价则跳过）
        should_act = None
        try:
            act_px = float(self._radar_activation_price() or 0)  # type: ignore[attr-defined]
            mark = float(getattr(self, "best_price", 0) or getattr(self, "tv_price", 0) or 0)
            side = str(getattr(self, "current_side", "") or "").upper()
            if act_px > 0 and mark > 0 and side in ("LONG", "SHORT"):
                if side == "LONG":
                    should_act = mark >= act_px
                else:
                    should_act = mark <= act_px
        except Exception:
            should_act = None
        return {
            "symbol": sym,
            "ledger_symbol": str(pl.get("symbol") or sym).upper(),
            "signal_side": str(pl.get("side") or getattr(self, "last_tv_side", "") or "").upper(),
            "live_side": str(getattr(self, "current_side", "") or pl.get("side") or "").upper(),
            "live_qty": float(getattr(self, "watched_qty", 0) or pl.get("qty") or 0),
            "initial_qty": init_q,
            "entry": float(getattr(self, "watched_entry", 0) or pl.get("entry") or 0),
            "leverage": lev,
            "leverage_cap": lev_cap,
            "risk_pct": risk,
            "risk_pct_cfg": risk_cfg,
            "tp1_qty": tq1,
            "tp2_qty": tq2,
            "hard_sl_px": hard_px,
            "hard_sl_expected": hard_px,
            "hard_sl_live": bool(pl.get("hard_sl_live")) or hard_px > 0,
            "radar_activated": bool(getattr(self, "radar_activated", False)),
            "radar_should_active": should_act,
            "api_recent": int(th.get("recent") or 0),
            "api_budget": int(th.get("budget") or 48),
            "place_levels": 2,
        }

    def _pipeline_run_chief_audit(self, *, source: str = "open") -> Any:
        """督察官：硬失败可暂停；软警告只记日志/告警。"""
        try:
            facts = self._pipeline_facts_for_audit()
            # 若刚挂完单，用对账函数刷新切片事实
            try:
                live_qty = float(facts.get("live_qty") or 0)
                if live_qty > 0 and hasattr(self, "_split_remaining_tp_quantities"):
                    qm = self._split_remaining_tp_quantities(live_qty)  # type: ignore[attr-defined]
                    if isinstance(qm, dict):
                        if float(qm.get(1) or 0) > 0:
                            facts["tp1_qty"] = float(qm.get(1) or 0)
                        if float(qm.get(2) or 0) > 0:
                            facts["tp2_qty"] = float(qm.get(2) or 0)
            except Exception:
                pass
            result = audit_open_bundle(facts)
            self._pipeline_obj().set_audit(
                result.ok, result.hard_fails, result.warns,
            )
            if result.ok:
                self._pipeline_advance(
                    Phase.VERIFIED, Role.CHIEF, note=f"audit_ok:{source}",
                )
                logger.info(
                    f"✅ [{getattr(self, 'symbol', '')}] 督察官通过 "
                    f"({source}) warns={result.warns[:3]}"
                )
            else:
                logger.error(
                    f"🚨 [{getattr(self, 'symbol', '')}] 督察官未通过 "
                    f"hard={result.hard_fails} warn={result.warns}"
                )
                if should_hard_pause(result):
                    self._pipeline_fail(
                        Role.CHIEF,
                        ";".join(result.hard_fails)[:240],
                    )
                    if hasattr(self, "_pause_symbol_trading"):
                        try:
                            self._pause_symbol_trading(  # type: ignore[attr-defined]
                                f"chief_auditor:{';'.join(result.hard_fails)[:160]}",
                                title=f"督察官拦截 [{getattr(self, 'symbol', '')}]",
                                detail=(
                                    f"source={source} | hard={result.hard_fails} | "
                                    f"warn={result.warns}"
                                ),
                            )
                        except Exception as e:
                            logger.warning(f"chief pause failed: {e}")
                else:
                    # 软模式：仍标 VERIFIED 以免卡死通讯，但记警告
                    self._pipeline_advance(
                        Phase.VERIFIED,
                        Role.CHIEF,
                        note=f"audit_soft_fail:{source}",
                        force=True,
                    )
            return result
        except Exception as e:
            logger.warning(
                f"[{getattr(self, 'symbol', '')}] chief audit error: {e}"
            )
            return None

    def _pipeline_reported(self, note: str = "notify") -> None:
        self._pipeline_advance(Phase.REPORTED, Role.COMMS, note=note)
        self._pipeline_advance(Phase.MONITORING, Role.SYSTEM, note="hold_loop")

    def _pipeline_radar_update(
        self,
        *,
        activated: Optional[bool] = None,
        sl_px: float = 0.0,
        oid: str = "",
        qty: Optional[float] = None,
    ) -> None:
        """雷达值守：只写账本；数量以调用方传入的实盘头寸为准。"""
        try:
            pl = self._pipeline_obj()
            if not soft_gates_enabled() and not pl.role_allowed(Role.RADAR):
                logger.warning(
                    f"📋 radar blocked at {pl.phase.value}"
                )
                return
            radar = dict(pl.to_dict().get("radar") or {})
            if activated is not None:
                radar["activated"] = bool(activated)
            if float(sl_px or 0) > 0:
                radar["sl_px"] = float(sl_px)
            if oid:
                radar["oid"] = str(oid)
            pl.advance(
                pl.phase,
                Role.RADAR,
                note="radar_tick",
                radar=radar,
            )
            if qty is not None:
                pl.sync_position(
                    side=str(getattr(self, "current_side", "") or ""),
                    qty=float(qty),
                    entry=float(getattr(self, "watched_entry", 0) or 0),
                )
        except Exception as e:
            logger.debug(f"radar pipeline update: {e}")

    def _pipeline_stale_check(self) -> None:
        try:
            msg = self._pipeline_obj().stale()
            if msg:
                logger.error(
                    f"⏰ [{getattr(self, 'symbol', '')}] {msg}"
                )
        except Exception:
            pass
