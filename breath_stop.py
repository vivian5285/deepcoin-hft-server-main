#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
呼吸止损引擎（开仓即工作 · TV initial_atr 基准 · 1h ATR 连续插值呼吸系数）

两阶段：
  阶段一：早保本 + initial_stop 基准阶梯（步长/跟进 = 固定 ×initial_atr，不乘系数）
  阶段二：追踪距离 = initial_atr × trailDistanceMultiplier(smoothedRatio)

呼吸系数 = 连续线性插值（见 breath_profiles.trail_distance_multiplier）；
XAU 不再额外 ×0.8。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from breath_profiles import (
    BREATH_ETH,
    cold_start_multiplier,
    default_breath_profile,
    trail_distance_multiplier,
)

# ── ETH 默认常量（兼容旧 import / 测试）──────────────────────────────────────
INITIAL_SL_ATR = float(BREATH_ETH["initial_sl_atr"])
STEP_TRIGGER_ATR = float(BREATH_ETH["step_trigger_atr"])
STEP_ADVANCE_ATR = float(BREATH_ETH["step_advance_atr"])
BREAKEVEN_TRIGGER_ATR = float(BREATH_ETH["phase_switch_atr"])  # 阶段切换（非早保本）
TP1_ATR = float(BREATH_ETH["tp1_atr"])
TP1_FLOOR_ATR = float(BREATH_ETH["tp1_floor_atr"])
TP2_ATR = float(BREATH_ETH["tp2_atr"])
TP2_FLOOR_ATR = float(BREATH_ETH["tp2_floor_atr"])
STOP_EXEC_BUFFER_USD = float(BREATH_ETH["stop_exec_buffer"])

# 兼容旧 import（阶段二已改呼吸系数，不再用 ADX 追踪）
ADX_WEAK_BOUND = 15.0
ADX_STRONG_BOUND = 35.0
TRAIL_DIST_WEAK_ATR = 1.2
TRAIL_DIST_STRONG_ATR = 2.5
ADX_FALLBACK = 25.0


def _profile(profile: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if isinstance(profile, dict) and profile:
        return profile
    return default_breath_profile()


def trail_distance_by_adx(adx_val: float) -> float:
    """已废弃：阶段二改用呼吸系数。保留空壳供旧测试/静态检查。"""
    try:
        adx = float(adx_val)
    except (TypeError, ValueError):
        adx = ADX_FALLBACK
    if adx <= ADX_WEAK_BOUND:
        return TRAIL_DIST_WEAK_ATR
    if adx >= ADX_STRONG_BOUND:
        return TRAIL_DIST_STRONG_ATR
    ratio = (adx - ADX_WEAK_BOUND) / (ADX_STRONG_BOUND - ADX_WEAK_BOUND)
    return TRAIL_DIST_WEAK_ATR + ratio * (TRAIL_DIST_STRONG_ATR - TRAIL_DIST_WEAK_ATR)


def get_breathing_coefficient(
    current_atr_1h: float,
    initial_atr: float,
    ratio_history: Optional[List[float]] = None,
    profile: Optional[Dict[str, Any]] = None,
) -> Tuple[float, float, List[float]]:
    """
    连续插值呼吸系数（trailDistanceMultiplier）：
      1) 采样 ratio = current_1h / initial_atr
      2) 近 3 次 ratio 算术平均 → smoothedRatio
      3) trail_distance_multiplier(smoothedRatio)
    冷启动（0 次有效采样 / cur<=0 且 hist 空）：ratio=1.0 → 公式中间值。
    返回 (coefficient, smooth_ratio, updated_history)
    """
    p = _profile(profile)
    init = float(initial_atr or 0)
    cur = float(current_atr_1h or 0)
    hist = list(ratio_history or [])

    if init <= 0:
        cold = cold_start_multiplier(p)
        return float(cold), 1.0, hist

    if cur <= 0:
        if not hist:
            cold = cold_start_multiplier(p)
            return float(cold), 1.0, hist
        smooth = sum(hist) / len(hist)
        return float(trail_distance_multiplier(smooth, p)), float(smooth), hist

    ratio = cur / init
    hist.append(ratio)
    if len(hist) > 3:
        hist = hist[-3:]
    smooth = sum(hist) / len(hist)
    coeff = trail_distance_multiplier(smooth, p)
    return float(coeff), float(smooth), hist


def initial_stop_price(
    side: str,
    entry_price: float,
    initial_atr: float,
    profile: Optional[Dict[str, Any]] = None,
) -> float:
    """理论 initialStop：多=entry-1.5ATR，空=entry+1.5ATR（不含执行缓冲）。"""
    p = _profile(profile)
    entry = float(entry_price or 0)
    atr = float(initial_atr or 0)
    mult = float(p.get("initial_sl_atr") or INITIAL_SL_ATR)
    if entry <= 0 or atr <= 0:
        return 0.0
    side = str(side or "").strip().upper()
    if side == "SHORT":
        return round(entry + mult * atr, 2)
    return round(entry - mult * atr, 2)


def order_stop_price(
    side: str,
    initial_stop: float,
    buffer_usd: Optional[float] = None,
    profile: Optional[Dict[str, Any]] = None,
) -> float:
    """
    盘口挂单止损 = initialStop ± buffer 执行缓冲（向外扩）。
    多单再减；空单再加。buffer 缺省取 profile.stop_exec_buffer。
    """
    p = _profile(profile)
    stop = float(initial_stop or 0)
    if buffer_usd is None:
        buf = abs(float(p.get("stop_exec_buffer") or STOP_EXEC_BUFFER_USD))
    else:
        buf = abs(float(buffer_usd or 0))
    if stop <= 0:
        return 0.0
    side = str(side or "").strip().upper()
    if side == "SHORT":
        return round(stop + buf, 2)
    return round(stop - buf, 2)


def _tick_size(profile: Dict[str, Any]) -> float:
    try:
        t = float(profile.get("tick_size") or 0.01)
    except (TypeError, ValueError):
        t = 0.01
    return t if t > 0 else 0.01


def calculate_stop_long(
    price: float,
    entry_price: float,
    initial_atr: float,
    initial_stop: float,
    current_stop: float,
    highest_price: float,
    breakeven_phase: bool,
    breathing_coefficient: float = 1.0,
    adx_val: float = ADX_FALLBACK,
    profile: Optional[Dict[str, Any]] = None,
    early_be_done: bool = False,
) -> Tuple[float, float, bool, int, bool]:
    """多单。返回：(新止损, 新最高, 新阶段, step_count, early_be_done)"""
    p = _profile(profile)
    price = float(price or 0)
    entry_price = float(entry_price or 0)
    initial_atr = float(initial_atr or 0)
    initial_stop = float(initial_stop or 0)
    current_stop = float(current_stop or 0)
    highest_price = float(highest_price or entry_price or 0)
    breakeven_phase = bool(breakeven_phase)
    early_be_done = bool(early_be_done)
    coeff = float(breathing_coefficient or 1.0)
    if coeff <= 0:
        coeff = cold_start_multiplier(p)

    step_trig = float(p.get("step_trigger_atr") or STEP_TRIGGER_ATR)
    step_adv = float(p.get("step_advance_atr") or STEP_ADVANCE_ATR)
    phase_sw = float(p.get("phase_switch_atr") or BREAKEVEN_TRIGGER_ATR)
    early_be = float(p.get("early_be_atr") or 0)
    tp1_a = float(p.get("tp1_atr") or TP1_ATR)
    tp1_f = float(p.get("tp1_floor_atr") or TP1_FLOOR_ATR)
    tp2_a = float(p.get("tp2_atr") or TP2_ATR)
    tp2_f = float(p.get("tp2_floor_atr") or TP2_FLOOR_ATR)
    tick = _tick_size(p)

    new_highest = max(highest_price, price) if price > 0 else highest_price
    new_stop = current_stop
    new_phase = breakeven_phase
    step_count = 0

    if entry_price <= 0 or initial_atr <= 0 or price <= 0:
        return new_stop, new_highest, new_phase, step_count, early_be_done

    # 阶段二：trail = initial_atr × trailDistanceMultiplier（无额外 ×0.8）
    trail_dist = initial_atr * coeff

    # 早保本：价达 entry+early_be×ATR → stop ≥ entry+1tick
    if early_be > 0 and price >= entry_price + early_be * initial_atr:
        early_be_done = True
        be_stop = round(entry_price + tick, 2)
        new_stop = max(float(new_stop or 0), be_stop)

    if not new_phase:
        # 阶梯：固定 ×initial_atr（文档总表；不乘呼吸系数）
        step_trigger = step_trig * initial_atr
        step_count = max(0, int((price - entry_price) / step_trigger)) if step_trigger > 0 else 0
        step_stop = initial_stop + step_count * step_adv * initial_atr
        candidate = max(float(new_stop or 0), float(current_stop or 0), step_stop)

        if price >= entry_price + tp1_a * initial_atr:
            candidate = max(candidate, entry_price + tp1_f * initial_atr)
        if price >= entry_price + tp2_a * initial_atr:
            candidate = max(candidate, entry_price + tp2_f * initial_atr)

        new_stop = candidate

        if price >= entry_price + phase_sw * initial_atr:
            new_phase = True
            new_stop = max(new_stop, new_highest - trail_dist)
    else:
        candidate = new_highest - trail_dist
        new_stop = max(float(current_stop or 0), float(new_stop or 0), candidate)

    return (
        round(float(new_stop), 2),
        round(float(new_highest), 2),
        bool(new_phase),
        int(step_count),
        bool(early_be_done),
    )


def calculate_stop_short(
    price: float,
    entry_price: float,
    initial_atr: float,
    initial_stop: float,
    current_stop: float,
    lowest_price: float,
    breakeven_phase: bool,
    breathing_coefficient: float = 1.0,
    adx_val: float = ADX_FALLBACK,
    profile: Optional[Dict[str, Any]] = None,
    early_be_done: bool = False,
) -> Tuple[float, float, bool, int, bool]:
    """空单对称。返回：(新止损, 新最低, 新阶段, step_count, early_be_done)"""
    p = _profile(profile)
    price = float(price or 0)
    entry_price = float(entry_price or 0)
    initial_atr = float(initial_atr or 0)
    initial_stop = float(initial_stop or 0)
    current_stop = float(current_stop or 0)
    lowest_price = float(lowest_price or entry_price or 0)
    breakeven_phase = bool(breakeven_phase)
    early_be_done = bool(early_be_done)
    coeff = float(breathing_coefficient or 1.0)
    if coeff <= 0:
        coeff = cold_start_multiplier(p)

    step_trig = float(p.get("step_trigger_atr") or STEP_TRIGGER_ATR)
    step_adv = float(p.get("step_advance_atr") or STEP_ADVANCE_ATR)
    phase_sw = float(p.get("phase_switch_atr") or BREAKEVEN_TRIGGER_ATR)
    early_be = float(p.get("early_be_atr") or 0)
    tp1_a = float(p.get("tp1_atr") or TP1_ATR)
    tp1_f = float(p.get("tp1_floor_atr") or TP1_FLOOR_ATR)
    tp2_a = float(p.get("tp2_atr") or TP2_ATR)
    tp2_f = float(p.get("tp2_floor_atr") or TP2_FLOOR_ATR)
    tick = _tick_size(p)

    new_lowest = min(lowest_price, price) if (lowest_price > 0 and price > 0) else (
        price if price > 0 else lowest_price
    )
    if lowest_price <= 0 and price > 0:
        new_lowest = price
    new_stop = current_stop
    new_phase = breakeven_phase
    step_count = 0

    if entry_price <= 0 or initial_atr <= 0 or price <= 0:
        return new_stop, new_lowest, new_phase, step_count, early_be_done

    trail_dist = initial_atr * coeff

    if early_be > 0 and price <= entry_price - early_be * initial_atr:
        early_be_done = True
        be_stop = round(entry_price - tick, 2)
        if new_stop <= 0:
            new_stop = be_stop
        else:
            new_stop = min(new_stop, be_stop)

    if not new_phase:
        step_trigger = step_trig * initial_atr
        step_count = max(0, int((entry_price - price) / step_trigger)) if step_trigger > 0 else 0
        step_stop = initial_stop - step_count * step_adv * initial_atr
        if current_stop > 0 or new_stop > 0:
            candidate = min(x for x in (current_stop, new_stop, step_stop) if x > 0)
        else:
            candidate = step_stop

        if price <= entry_price - tp1_a * initial_atr:
            candidate = min(candidate, entry_price - tp1_f * initial_atr)
        if price <= entry_price - tp2_a * initial_atr:
            candidate = min(candidate, entry_price - tp2_f * initial_atr)

        new_stop = candidate

        if price <= entry_price - phase_sw * initial_atr:
            new_phase = True
            new_stop = min(new_stop, new_lowest + trail_dist)
    else:
        candidate = new_lowest + trail_dist
        refs = [x for x in (current_stop, new_stop, candidate) if x > 0]
        new_stop = min(refs) if refs else candidate

    return (
        round(float(new_stop), 2),
        round(float(new_lowest), 2),
        bool(new_phase),
        int(step_count),
        bool(early_be_done),
    )


def calculate_breath_stop(
    side: str,
    price: float,
    entry_price: float,
    initial_atr: float,
    initial_stop: float,
    current_stop: float,
    best_price: float,
    breakeven_phase: bool,
    breathing_coefficient: float = 1.0,
    adx_val: float = ADX_FALLBACK,
    profile: Optional[Dict[str, Any]] = None,
    early_be_done: bool = False,
    **_kw,
):
    """
    统一入口。best_price = 多单 highest / 空单 lowest。
    返回 dict: stop, best, breakeven_phase, early_be_done, meta
    """
    p = _profile(profile)
    side = str(side or "").strip().upper()
    atr = float(initial_atr or 0)
    entry = float(entry_price or 0)
    px = float(price or 0)
    coeff = float(breathing_coefficient or 1.0)
    if coeff <= 0:
        coeff = cold_start_multiplier(p)
    meta = {
        "trail_atr": coeff,
        "breathing_coefficient": coeff,
        "phase2_trail_mult": 1.0,
        "profile": p.get("name") or "ETH",
        "adx": float(adx_val or 0),
        "phase": "breakeven" if breakeven_phase else "ladder",
        "step_count": 0,
        "early_be_done": bool(early_be_done),
    }
    if side == "SHORT":
        stop, best, phase, step_count, early = calculate_stop_short(
            px, entry, atr, initial_stop, current_stop, best_price,
            breakeven_phase, breathing_coefficient=coeff, profile=p,
            early_be_done=early_be_done,
        )
    else:
        stop, best, phase, step_count, early = calculate_stop_long(
            px, entry, atr, initial_stop, current_stop, best_price,
            breakeven_phase, breathing_coefficient=coeff, profile=p,
            early_be_done=early_be_done,
        )
    meta["step_count"] = int(step_count)
    meta["phase"] = "breakeven" if phase else "ladder"
    meta["early_be_done"] = bool(early)
    meta["trail_distance"] = round(atr * coeff, 4) if atr > 0 else 0.0
    return {
        "stop": stop,
        "best": best,
        "breakeven_phase": phase,
        "early_be_done": bool(early),
        "meta": meta,
    }
