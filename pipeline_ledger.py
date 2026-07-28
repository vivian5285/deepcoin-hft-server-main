#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
总账本 + 状态机（生产流水线编制 v1）。

币安单账户 / Deepcoin 共用同一套阶段名与交接规则；实现独立、键空间独立。
软闸默认开启：错误阶段只记日志，不阻断现有实盘路径（PIPELINE_SOFT_GATES=0 才硬拦）。
"""
from __future__ import annotations

import os
import threading
import time
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


class Phase(str, Enum):
    IDLE = "IDLE"
    SIGNAL_RECEIVED = "SIGNAL_RECEIVED"
    PENDING_CLEAR = "PENDING_CLEAR"
    CLEARED = "CLEARED"
    ENTRY_SUBMITTED = "ENTRY_SUBMITTED"
    ENTRY_CONFIRMED = "ENTRY_CONFIRMED"
    ORDERS_PLACED = "ORDERS_PLACED"
    VERIFIED = "VERIFIED"
    REPORTED = "REPORTED"
    MONITORING = "MONITORING"
    FAILED = "FAILED"


class Role(str, Enum):
    SIGNAL = "signal_officer"
    AUDITOR_POS = "position_auditor"
    EXECUTION = "execution_officer"
    RADAR = "radar_sentinel"
    CHIEF = "chief_auditor"
    COMMS = "communications_officer"
    SYSTEM = "system"


# 岗位 → 允许动手的阶段
ROLE_PHASES = {
    Role.SIGNAL: {Phase.IDLE, Phase.MONITORING, Phase.FAILED, Phase.REPORTED},
    Role.AUDITOR_POS: {Phase.SIGNAL_RECEIVED, Phase.PENDING_CLEAR},
    Role.EXECUTION: {
        Phase.CLEARED,
        Phase.ENTRY_SUBMITTED,
        Phase.ENTRY_CONFIRMED,
    },
    Role.RADAR: {
        Phase.ORDERS_PLACED,
        Phase.VERIFIED,
        Phase.REPORTED,
        Phase.MONITORING,
    },
    Role.CHIEF: {
        Phase.ORDERS_PLACED,
        Phase.VERIFIED,
        Phase.REPORTED,
        Phase.MONITORING,
        Phase.FAILED,
    },
    Role.COMMS: {Phase.VERIFIED, Phase.REPORTED, Phase.MONITORING},
    Role.SYSTEM: set(Phase),
}

# 合法前进边（FAILED 可由任意角色标记；复位到 IDLE/SIGNAL 由 SYSTEM）
_FORWARD: Dict[Phase, Tuple[Phase, ...]] = {
    Phase.IDLE: (Phase.SIGNAL_RECEIVED,),
    Phase.SIGNAL_RECEIVED: (Phase.PENDING_CLEAR, Phase.FAILED),
    Phase.PENDING_CLEAR: (Phase.CLEARED, Phase.FAILED),
    Phase.CLEARED: (Phase.ENTRY_SUBMITTED, Phase.FAILED),
    Phase.ENTRY_SUBMITTED: (Phase.ENTRY_CONFIRMED, Phase.FAILED),
    Phase.ENTRY_CONFIRMED: (Phase.ORDERS_PLACED, Phase.FAILED),
    Phase.ORDERS_PLACED: (Phase.VERIFIED, Phase.FAILED),
    Phase.VERIFIED: (Phase.REPORTED, Phase.FAILED),
    Phase.REPORTED: (Phase.MONITORING, Phase.FAILED),
    Phase.MONITORING: (Phase.SIGNAL_RECEIVED, Phase.IDLE, Phase.FAILED),
    Phase.FAILED: (Phase.IDLE, Phase.SIGNAL_RECEIVED, Phase.MONITORING),
}

PHASE_STALE_SEC = {
    Phase.SIGNAL_RECEIVED: 60.0,
    Phase.PENDING_CLEAR: 90.0,
    Phase.CLEARED: 60.0,
    Phase.ENTRY_SUBMITTED: 30.0,
    Phase.ENTRY_CONFIRMED: 90.0,
    Phase.ORDERS_PLACED: 120.0,
    Phase.VERIFIED: 60.0,
}


def soft_gates_enabled() -> bool:
    return str(os.getenv("PIPELINE_SOFT_GATES", "1")).strip() not in (
        "0", "false", "False", "no", "OFF",
    )


def blank_pipeline(symbol: str = "", exchange: str = "binance") -> Dict[str, Any]:
    return {
        "schema": "pipeline_v1",
        "exchange": str(exchange or "binance"),
        "symbol": str(symbol or "").upper(),
        "phase": Phase.IDLE.value,
        "phase_ts": 0.0,
        "last_role": "",
        "side": "",
        "qty": 0.0,
        "entry": 0.0,
        "open_ts": 0.0,
        "initial_qty": 0.0,  # 仅 ENTRY_CONFIRMED 写入，其他岗位不得覆写
        "tier": "",
        "hard_sl_px": 0.0,
        "hard_sl_oid": "",
        "hard_sl_live": False,
        "tp1": {"px": 0.0, "qty": 0.0, "oid": "", "filled": False},
        "tp2": {"px": 0.0, "qty": 0.0, "oid": "", "filled": False},
        "radar": {
            "activated": False,
            "sl_px": 0.0,
            "oid": "",
        },
        "audit": {
            "ok": None,
            "ts": 0.0,
            "fails": [],
            "warns": [],
        },
        "api_ts": [],  # 最近 REST 时间戳（账号维度另有节流阀）
        "fail_reason": "",
        "updated_ts": 0.0,
        "history": [],  # 最近交接 [{ts, from, to, role, note}]
    }


class PipelineLedger:
    """内存总账本；由 supervisor 持久化到 state JSON 的 pipeline 字段。"""

    def __init__(self, symbol: str, exchange: str = "binance"):
        self._lock = threading.RLock()
        self.data = blank_pipeline(symbol, exchange)

    # ── 序列化 ──────────────────────────────────────────────
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self.data)

    def load_dict(self, raw: Optional[Dict[str, Any]]) -> None:
        with self._lock:
            base = blank_pipeline(
                (raw or {}).get("symbol") or self.data.get("symbol"),
                (raw or {}).get("exchange") or self.data.get("exchange"),
            )
            if not isinstance(raw, dict):
                self.data = base
                return
            base.update({k: v for k, v in raw.items() if k in base or k == "schema"})
            # 嵌套字段合并
            for nest in ("tp1", "tp2", "radar", "audit"):
                if isinstance(raw.get(nest), dict):
                    merged = dict(base.get(nest) or {})
                    merged.update(raw[nest])
                    base[nest] = merged
            if isinstance(raw.get("api_ts"), list):
                base["api_ts"] = list(raw["api_ts"][-64:])
            if isinstance(raw.get("history"), list):
                base["history"] = list(raw["history"][-40:])
            try:
                Phase(str(base.get("phase") or Phase.IDLE.value))
            except Exception:
                base["phase"] = Phase.IDLE.value
            self.data = base

    @property
    def phase(self) -> Phase:
        with self._lock:
            try:
                return Phase(str(self.data.get("phase") or Phase.IDLE.value))
            except Exception:
                return Phase.IDLE

    def role_allowed(self, role: Role) -> bool:
        allowed = ROLE_PHASES.get(role) or set()
        return self.phase in allowed or role == Role.SYSTEM

    def can_advance(self, to: Phase) -> bool:
        cur = self.phase
        if to == Phase.FAILED:
            return True
        if cur == to:
            return True
        return to in _FORWARD.get(cur, ())

    def advance(
        self,
        to: Phase,
        role: Role,
        *,
        note: str = "",
        force: bool = False,
        **fields: Any,
    ) -> Tuple[bool, str]:
        """
        推进阶段。返回 (ok, message)。
        soft gates：非法推进记 warning 但仍写字段（不改 phase），返回 False。
        """
        with self._lock:
            cur = self.phase
            if not force and role != Role.SYSTEM and not self.role_allowed(role):
                msg = f"role={role.value} blocked at phase={cur.value}"
                if soft_gates_enabled():
                    self._touch_history(cur, cur, role, f"SOFT_BLOCK {msg} | {note}")
                    self._apply_fields(fields, role=role)
                    return False, msg
                return False, msg
            if not force and not self.can_advance(to):
                msg = f"illegal {cur.value} → {to.value} by {role.value}"
                if soft_gates_enabled():
                    self._touch_history(cur, cur, role, f"SOFT_SKIP {msg} | {note}")
                    self._apply_fields(fields, role=role)
                    return False, msg
                return False, msg
            self._apply_fields(fields, role=role)
            if cur != to:
                self.data["phase"] = to.value
                self.data["phase_ts"] = time.time()
                self.data["last_role"] = role.value
                if to == Phase.FAILED:
                    self.data["fail_reason"] = str(note or fields.get("fail_reason") or "")
                elif to not in (Phase.FAILED,):
                    if to == Phase.SIGNAL_RECEIVED:
                        self.data["fail_reason"] = ""
                self._touch_history(cur, to, role, note)
            self.data["updated_ts"] = time.time()
            return True, "ok"

    def mark_failed(self, role: Role, reason: str) -> None:
        self.advance(Phase.FAILED, role, note=str(reason or "failed"), force=True)

    def reset_idle(self, note: str = "flat") -> None:
        with self._lock:
            sym = self.data.get("symbol")
            ex = self.data.get("exchange")
            self.data = blank_pipeline(sym, ex)
            self.data["phase"] = Phase.IDLE.value
            self.data["phase_ts"] = time.time()
            self.data["updated_ts"] = time.time()
            self._touch_history(Phase.MONITORING, Phase.IDLE, Role.SYSTEM, note)

    def set_audit(self, ok: Optional[bool], fails: List[str], warns: List[str]) -> None:
        with self._lock:
            self.data["audit"] = {
                "ok": ok,
                "ts": time.time(),
                "fails": list(fails or [])[:32],
                "warns": list(warns or [])[:32],
            }
            self.data["updated_ts"] = time.time()

    def note_api(self, ts: Optional[float] = None) -> None:
        with self._lock:
            t = float(ts if ts is not None else time.time())
            arr = list(self.data.get("api_ts") or [])
            arr.append(t)
            self.data["api_ts"] = arr[-64:]
            self.data["updated_ts"] = t

    def stale(self) -> Optional[str]:
        """阶段超时则返回告警文案。"""
        with self._lock:
            ph = self.phase
            limit = PHASE_STALE_SEC.get(ph)
            if not limit:
                return None
            age = time.time() - float(self.data.get("phase_ts") or 0)
            if float(self.data.get("phase_ts") or 0) <= 0:
                return None
            if age > limit:
                return f"phase_stale {ph.value} age={age:.0f}s>{limit:.0f}s"
            return None

    def sync_position(
        self,
        *,
        side: str = "",
        qty: float = 0.0,
        entry: float = 0.0,
        allow_initial: bool = False,
    ) -> None:
        """雷达/哨兵更新头寸；initial_qty 仅允许在开仓确认时写入。"""
        with self._lock:
            if side:
                self.data["side"] = str(side).upper()
            self.data["qty"] = float(qty or 0)
            if float(entry or 0) > 0:
                self.data["entry"] = float(entry)
            if allow_initial and float(qty or 0) > 0:
                # 只写一次：已有 initial 且 >0 则保留
                if float(self.data.get("initial_qty") or 0) <= 0:
                    self.data["initial_qty"] = float(qty)
                    self.data["open_ts"] = time.time()
            self.data["updated_ts"] = time.time()

    # ── 内部 ────────────────────────────────────────────────
    def _apply_fields(self, fields: Dict[str, Any], role: Role) -> None:
        if not fields:
            return
        for k, v in fields.items():
            if k in ("phase", "history", "schema"):
                continue
            if k == "initial_qty":
                # 铁律：仅 execution 在确认阶段可写；已有值不可被非 SYSTEM 覆写变小/改掉
                cur_i = float(self.data.get("initial_qty") or 0)
                if cur_i > 0 and role != Role.SYSTEM:
                    continue
                self.data["initial_qty"] = float(v or 0)
                continue
            if k in ("tp1", "tp2", "radar", "audit") and isinstance(v, dict):
                merged = dict(self.data.get(k) or {})
                merged.update(v)
                self.data[k] = merged
                continue
            self.data[k] = v

    def _touch_history(self, frm: Phase, to: Phase, role: Role, note: str) -> None:
        hist = list(self.data.get("history") or [])
        hist.append({
            "ts": time.time(),
            "from": frm.value if isinstance(frm, Phase) else str(frm),
            "to": to.value if isinstance(to, Phase) else str(to),
            "role": role.value if isinstance(role, Role) else str(role),
            "note": str(note or "")[:200],
        })
        self.data["history"] = hist[-40:]


_LEDGERS: Dict[str, PipelineLedger] = {}
_LEDGER_LOCK = threading.Lock()


def get_pipeline(symbol: str, exchange: str = "binance") -> PipelineLedger:
    key = f"{exchange}:{str(symbol or '').upper()}"
    with _LEDGER_LOCK:
        if key not in _LEDGERS:
            _LEDGERS[key] = PipelineLedger(symbol, exchange)
        return _LEDGERS[key]
