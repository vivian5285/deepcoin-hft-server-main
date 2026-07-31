#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
督察官复查清单（与架构方案 3.5 对齐）。

硬失败（默认触发品种暂停）：方向、TP切片≈30%、硬止损缺失/明显偏离。
软警告（只告警不暂停）：杠杆、权重、雷达候命、API频率——避免误伤正常实盘。
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class AuditItem:
    key: str
    ok: bool
    hard: bool
    detail: str = ""


@dataclass
class AuditResult:
    ok: bool
    hard_fails: List[str] = field(default_factory=list)
    warns: List[str] = field(default_factory=list)
    items: List[AuditItem] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "hard_fails": list(self.hard_fails),
            "warns": list(self.warns),
            "items": [
                {
                    "key": i.key,
                    "ok": i.ok,
                    "hard": i.hard,
                    "detail": i.detail,
                }
                for i in self.items
            ],
        }


def _f(v, default=0.0) -> float:
    try:
        return float(v if v is not None else default)
    except Exception:
        return float(default)


def _hard_pause_enabled() -> bool:
    return str(os.getenv("PIPELINE_AUDITOR_HARD_PAUSE", "0")).strip() not in (
        "0", "false", "False", "no", "OFF",
    )


def check_tp_slice_budget(
    initial_qty: float,
    tp1_qty: float,
    tp2_qty: float,
    *,
    place_levels: int = 2,
    ratios: Optional[List[float]] = None,
    tol_ratio: float = 0.03,
) -> AuditItem:
    """TP1+TP2 之和应 ≈ initial × 30%（place=2）。
    
    特殊处理：当 initial_qty <= 2 时（最小仓位），tp1+tp2 允许等于 initial_qty
    （因为 10%+20% 比例在 1 张时无法满足：min(tp1)=1, min(tp2)=1, min(和)=2 > 1×0.30）
    """
    ratios = list(ratios or [0.10, 0.20, 0.70])
    place_n = max(1, min(2, int(place_levels or 2)))
    initial = _f(initial_qty)
    if initial <= 0:
        return AuditItem("tp_slice", False, True, "initial_qty<=0")
    expected = initial * sum(_f(r) for r in ratios[:place_n])
    got = _f(tp1_qty) + _f(tp2_qty)
    drift = abs(got - expected)
    tol = max(0.001, initial * float(tol_ratio), expected * float(tol_ratio))
    
    # 特殊处理：对于 1-2 张的最小仓位，tp1+tp2 允许等于 initial（全部给 TP1）
    # 因为 1 张时 min(tp1)=1, min(tp2)=1，导致 tp1+tp2 >= 2 > 1×0.30
    if initial <= 2:
        ok = got == initial  # 全部给 TP1 是唯一合理选择
    else:
        ok = drift <= tol and got <= initial * 0.35 + 1e-9
    
    return AuditItem(
        "tp_slice",
        ok,
        True,
        f"tp1+tp2={got:.4f} expected≈{expected:.4f} drift={drift:.4f} init={initial:.4f}",
    )


def audit_open_bundle(facts: Dict[str, Any]) -> AuditResult:
    """
    facts 建议字段：
      symbol, signal_side, live_side, live_qty, initial_qty, entry,
      leverage, leverage_cap, risk_pct, risk_pct_cfg,
      tp1_qty, tp2_qty, tp1_px, tp2_px,
      hard_sl_px, hard_sl_expected, hard_sl_live,
      radar_activated, radar_should_active,
      api_recent, api_budget
    """
    facts = dict(facts or {})
    items: List[AuditItem] = []

    # 1) TV 方向
    sig = str(facts.get("signal_side") or "").upper()
    live = str(facts.get("live_side") or "").upper()
    dir_ok = bool(sig and live and sig == live)
    items.append(AuditItem(
        "direction", dir_ok, True,
        f"signal={sig or '?'} live={live or '?'}",
    ))

    # 2) 下单金额权重（软）
    risk = _f(facts.get("risk_pct"))
    risk_cfg = _f(facts.get("risk_pct_cfg"), risk or 0.2)
    if risk_cfg <= 0:
        risk_cfg = 0.2
    risk_ok = abs(risk - risk_cfg) <= max(0.05, risk_cfg * 0.5) if risk > 0 else True
    items.append(AuditItem(
        "sizing_weight", risk_ok, False,
        f"risk={risk:.4f} cfg={risk_cfg:.4f}",
    ))

    # 3) 杠杆（软）
    lev = _f(facts.get("leverage"))
    lev_cap = _f(facts.get("leverage_cap"), lev or 5)
    if lev_cap <= 0:
        lev_cap = 5.0
    lev_ok = lev <= lev_cap + 1e-9 if lev > 0 else True
    items.append(AuditItem(
        "leverage", lev_ok, False,
        f"lev={lev:.1f} cap={lev_cap:.1f}",
    ))

    # 4) 币种
    sym = str(facts.get("symbol") or "").upper()
    key_sym = str(facts.get("ledger_symbol") or sym).upper()
    sym_ok = bool(sym) and sym == key_sym
    items.append(AuditItem("symbol", sym_ok, True, f"{sym} vs {key_sym}"))

    # 5) TP 切片
    items.append(check_tp_slice_budget(
        _f(facts.get("initial_qty") or facts.get("live_qty")),
        _f(facts.get("tp1_qty")),
        _f(facts.get("tp2_qty")),
        place_levels=int(facts.get("place_levels") or 2),
        ratios=facts.get("ratios"),
    ))

    # 6) 硬止损
    hard_px = _f(facts.get("hard_sl_px"))
    hard_exp = _f(facts.get("hard_sl_expected") or hard_px)
    hard_live = bool(facts.get("hard_sl_live"))
    if hard_px <= 0 and hard_exp > 0:
        hard_px = hard_exp
    px_ok = hard_px > 0 and (
        hard_exp <= 0 or abs(hard_px - hard_exp) / max(hard_exp, 1e-9) <= 0.02
    )
    hard_ok = px_ok and hard_live
    items.append(AuditItem(
        "hard_sl",
        hard_ok,
        True,
        f"px={hard_px:.4f} expected={hard_exp:.4f} live={hard_live}",
    ))

    # 7) 雷达候命（软）
    act = bool(facts.get("radar_activated"))
    should = facts.get("radar_should_active")
    if should is None:
        radar_ok = True
        detail = f"activated={act} (no gate price)"
    else:
        radar_ok = bool(act) == bool(should)
        detail = f"activated={act} should={bool(should)}"
    items.append(AuditItem("radar_standby", radar_ok, False, detail))

    # 8) API 频率（软）
    recent = int(_f(facts.get("api_recent")))
    budget = int(_f(facts.get("api_budget"), 48))
    api_ok = recent <= max(1, int(budget * 1.15))
    items.append(AuditItem(
        "api_freq", api_ok, False, f"recent={recent} budget={budget}",
    ))

    hard_fails = [f"{i.key}:{i.detail}" for i in items if i.hard and not i.ok]
    warns = [f"{i.key}:{i.detail}" for i in items if (not i.hard) and not i.ok]
    # 总体 ok：无硬失败
    ok = len(hard_fails) == 0
    return AuditResult(ok=ok, hard_fails=hard_fails, warns=warns, items=items)


def should_hard_pause(result: AuditResult) -> bool:
    if not _hard_pause_enabled():
        return False
    return bool(result and result.hard_fails)
