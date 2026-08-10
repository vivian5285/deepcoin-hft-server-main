#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
呼吸止损引擎（马拉松雷达 · 被动跟随）

边界：
  · 雷达激活前不调用本引擎推进止损
  · 激活臂 = 保本起步 entry ± tick ± fee_cover（禁止跳到 TP1 底线）
  · 阶梯：step_trigger / step_advance × initial_atr（按 ADX 档）
  · 分区呼吸：TP1–TP2 / TP2–TP3 / TP3+ 使用 breath_tp12 / breath_tp23 / trail(min~max)
  · 取消 TP1/TP2 强制底线；浮盈≥phase_switch_atr(默认3) 切入阶段二
  · 禁用早保本抢跑（early_be_atr=0）
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from breath_profiles import (
    BREATH_ETH,
    cold_start_multiplier,
    default_breath_profile,
    trail_distance_multiplier,
)

INITIAL_SL_ATR = float(BREATH_ETH.get("initial_sl_atr") or 0.0)
STEP_TRIGGER_ATR = float(BREATH_ETH["step_trigger_atr"])
STEP_ADVANCE_ATR = float(BREATH_ETH["step_advance_atr"])
BREAKEVEN_TRIGGER_ATR = float(BREATH_ETH["phase_switch_atr"])
TP1_ATR = float(BREATH_ETH["tp1_atr"])
TP1_FLOOR_ATR = float(BREATH_ETH.get("tp1_floor_atr") or 0.0)
TP2_ATR = float(BREATH_ETH["tp2_atr"])
TP2_FLOOR_ATR = float(BREATH_ETH.get("tp2_floor_atr") or 0.0)
STOP_EXEC_BUFFER_USD = float(BREATH_ETH["stop_exec_buffer"])
FEE_COVER_PCT = float(BREATH_ETH.get("fee_cover_pct") or 0.0008)

# 兼容旧 import
ADX_WEAK_BOUND = 20.0
ADX_STRONG_BOUND = 30.0
TRAIL_DIST_WEAK_ATR = 1.2
TRAIL_DIST_STRONG_ATR = 2.5
ADX_FALLBACK = 25.0


def _profile(profile: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if isinstance(profile, dict) and profile:
        return profile
    return default_breath_profile()


def trail_distance_by_adx(adx_val: float) -> float:
    """兼容旧测试：弱/强边界按白皮书 20/30。"""
    try:
        adx = float(adx_val)
    except (TypeError, ValueError):
        adx = ADX_FALLBACK
    if adx < ADX_WEAK_BOUND:
        return TRAIL_DIST_WEAK_ATR
    if adx > ADX_STRONG_BOUND:
        return TRAIL_DIST_STRONG_ATR
    ratio = (adx - ADX_WEAK_BOUND) / (ADX_STRONG_BOUND - ADX_WEAK_BOUND)
    return TRAIL_DIST_WEAK_ATR + ratio * (TRAIL_DIST_STRONG_ATR - TRAIL_DIST_WEAK_ATR)


def get_breathing_coefficient(
    current_atr_1h: float,
    initial_atr: float,
    ratio_history: Optional[List[float]] = None,
    profile: Optional[Dict[str, Any]] = None,
) -> Tuple[float, float, List[float]]:
    """TP3+ 动态追踪带宽系数（min_mult~max_mult 插值）。"""
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
    initial_atr: float = 0.0,
    profile: Optional[Dict[str, Any]] = None,
) -> float:
    """
    雷达激活臂（马拉松保本起步）：
      多 = entry + tick + fee_cover
      空 = entry − tick − fee_cover
    initial_atr 保留签名兼容；不再用于跳到 ±0.5ATR。
    """
    _ = initial_atr
    p = _profile(profile)
    entry = float(entry_price or 0)
    if entry <= 0:
        return 0.0
    side = str(side or "").strip().upper()
    # 旧路径：若 profile 仍带正 initial_sl_atr，保留兼容
    mult = float(p.get("initial_sl_atr") or 0)
    atr = float(initial_atr or 0)
    if mult > 0 and atr > 0:
        if side == "SHORT":
            return round(entry + mult * atr, 2)
        return round(entry - mult * atr, 2)
    tick = _tick_size(p)
    try:
        fee_pct = abs(float(p.get("fee_cover_pct") or FEE_COVER_PCT))
    except (TypeError, ValueError):
        fee_pct = FEE_COVER_PCT
    fee = entry * fee_pct
    if side == "SHORT":
        return round(entry - tick - fee, 2)
    if side == "LONG":
        return round(entry + tick + fee, 2)
    return 0.0


def order_stop_price(
    side: str,
    initial_stop: float,
    buffer_usd: Optional[float] = None,
    profile: Optional[Dict[str, Any]] = None,
) -> float:
    """盘口挂单止损 = initialStop ± buffer 执行缓冲（向外扩）。"""
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


TP3_CONFIRM_ATR = 1.0  # 刚过TP3但还没走出这么远之前，呼吸空间暂不放宽到tp3_plus


def _zone_trail_atr(
    *,
    side: str,
    price: float,
    entry: float,
    atr: float,
    profile: Dict[str, Any],
    coeff: float,
    tp1_px: float = 0.0,
    tp2_px: float = 0.0,
    tp3_px: float = 0.0,
) -> Tuple[float, str]:
    """
    按价格相对 TP 进度返回呼吸空间（×ATR）与区名。
    TP1–TP2 → breath_tp12；TP2–TP3 → breath_tp23；
    刚过TP3但还没走出 tp3_confirm_atr(默认1×ATR) → tp3_confirm，仍用 breath_tp23
    这么紧（防"一冲到TP3就立刻回落"的假突破，不另挂TP3限价单跟雷达抢单——
    两条腿同时在场有竞态成交风险，binance v15.9.0 踩过、v16.4.0 已删除）；
    确认走出这段缓冲、真延续了 → tp3_plus，放开到 coeff(min~max)。
    """
    side_u = str(side or "").upper()
    b12 = float(profile.get("breath_tp12") or 1.2)
    b23 = float(profile.get("breath_tp23") or 1.6)
    tp1_a = float(profile.get("tp1_atr") or TP1_ATR)
    tp2_a = float(profile.get("tp2_atr") or TP2_ATR)
    confirm_atr = float(
        profile.get("tp3_confirm_atr")
        if profile.get("tp3_confirm_atr") is not None
        else TP3_CONFIRM_ATR
    )

    # 优先 TV 价；否则用 ATR 倍数估进度
    if side_u == "LONG":
        tp3_ref = tp3_px if tp3_px > 0 else entry + (tp2_a + 1.0) * atr
        past_tp3 = price >= tp3_ref
        past_tp3_confirmed = price >= tp3_ref + confirm_atr * atr
        past_tp2 = (tp2_px > 0 and price >= tp2_px) or (
            tp2_px <= 0 and price >= entry + tp2_a * atr
        )
        past_tp1 = (tp1_px > 0 and price >= tp1_px) or (
            tp1_px <= 0 and price >= entry + tp1_a * atr
        )
    else:
        tp3_ref = tp3_px if tp3_px > 0 else entry - (tp2_a + 1.0) * atr
        past_tp3 = price <= tp3_ref
        past_tp3_confirmed = price <= tp3_ref - confirm_atr * atr
        past_tp2 = (tp2_px > 0 and price <= tp2_px) or (
            tp2_px <= 0 and price <= entry - tp2_a * atr
        )
        past_tp1 = (tp1_px > 0 and price <= tp1_px) or (
            tp1_px <= 0 and price <= entry - tp1_a * atr
        )

    if past_tp3_confirmed:
        return float(coeff), "tp3_plus"
    if past_tp3:
        return b23, "tp3_confirm"
    if past_tp2:
        return b23, "tp2_tp3"
    if past_tp1:
        return b12, "tp1_tp2"
    # 激活后尚未到 TP1：用较宽的 tp12 空间，避免过紧
    return b12, "pre_tp1"


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
    tp1_px: float = 0.0,
    tp2_px: float = 0.0,
    tp3_px: float = 0.0,
) -> Tuple[float, float, bool, int, bool]:
    """多单。返回：(新止损, 新最高, 新阶段, step_count, early_be_done)"""
    p = _profile(profile)
    price = float(price or 0)
    entry_price = float(entry_price or 0)
    initial_atr = float(initial_atr or 0)
    initial_stop = float(initial_stop or 0)
    current_stop = float(current_stop or 0)
    highest_price = float(highest_price or entry_price or 0)
    early_be_done = bool(early_be_done)
    coeff = float(breathing_coefficient or 1.0)
    if coeff <= 0:
        coeff = cold_start_multiplier(p)

    step_trig = float(p.get("step_trigger_atr") or STEP_TRIGGER_ATR)
    step_adv = float(p.get("step_advance_atr") or STEP_ADVANCE_ATR)

    new_highest = max(highest_price, price) if price > 0 else highest_price
    new_stop = current_stop
    step_count = 0

    if entry_price <= 0 or initial_atr <= 0 or price <= 0:
        return new_stop, new_highest, bool(breakeven_phase), step_count, early_be_done

    trail_mult, zone = _zone_trail_atr(
        side="LONG", price=price, entry=entry_price, atr=initial_atr,
        profile=p, coeff=coeff,
        tp1_px=float(tp1_px or 0), tp2_px=float(tp2_px or 0), tp3_px=float(tp3_px or 0),
    )
    trail_dist = trail_mult * initial_atr
    # 浮盈≥phase_switch → 阶段二；或已过 TP3
    phase_sw = float(p.get("phase_switch_atr") or BREAKEVEN_TRIGGER_ATR or 3.0)
    mfe_atr = (new_highest - entry_price) / initial_atr if initial_atr > 0 else 0.0
    new_phase = zone == "tp3_plus" or (phase_sw > 0 and mfe_atr >= phase_sw)

    # 阶梯跟进：价格每涨 step_trigger，止损上移 step_advance（从保本位推进）
    step_trigger = step_trig * initial_atr
    step_count = max(0, int((price - entry_price) / step_trigger)) if step_trigger > 0 else 0
    step_stop = initial_stop + step_count * step_adv * initial_atr
    candidate = max(float(new_stop or 0), float(current_stop or 0), float(step_stop or 0))

    # 分区呼吸：止损不得远落后于最高价超过 trail_dist
    trail_floor = new_highest - trail_dist
    if new_phase:
        # 阶段二：连续追踪为主，阶梯不打断
        candidate = max(candidate, trail_floor)
    else:
        candidate = max(candidate, trail_floor)

    # 马拉松：禁止 TP1/TP2 强制底线抬升（floor_atr<=0 跳过）
    f1 = float(p.get("tp1_floor_atr") or 0.0)
    f2 = float(p.get("tp2_floor_atr") or 0.0)
    if f2 > 0 and zone in ("tp2_tp3", "tp3_confirm", "tp3_plus"):
        candidate = max(candidate, entry_price + f2 * initial_atr)
    elif f1 > 0 and zone == "tp1_tp2":
        candidate = max(candidate, entry_price + f1 * initial_atr)

    new_stop = candidate
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
    tp1_px: float = 0.0,
    tp2_px: float = 0.0,
    tp3_px: float = 0.0,
) -> Tuple[float, float, bool, int, bool]:
    """空单对称。"""
    p = _profile(profile)
    price = float(price or 0)
    entry_price = float(entry_price or 0)
    initial_atr = float(initial_atr or 0)
    initial_stop = float(initial_stop or 0)
    current_stop = float(current_stop or 0)
    lowest_price = float(lowest_price or entry_price or 0)
    early_be_done = bool(early_be_done)
    coeff = float(breathing_coefficient or 1.0)
    if coeff <= 0:
        coeff = cold_start_multiplier(p)

    step_trig = float(p.get("step_trigger_atr") or STEP_TRIGGER_ATR)
    step_adv = float(p.get("step_advance_atr") or STEP_ADVANCE_ATR)

    new_lowest = min(lowest_price, price) if (lowest_price > 0 and price > 0) else (
        price if price > 0 else lowest_price
    )
    if lowest_price <= 0 and price > 0:
        new_lowest = price
    new_stop = current_stop
    step_count = 0

    if entry_price <= 0 or initial_atr <= 0 or price <= 0:
        return new_stop, new_lowest, bool(breakeven_phase), step_count, early_be_done

    trail_mult, zone = _zone_trail_atr(
        side="SHORT", price=price, entry=entry_price, atr=initial_atr,
        profile=p, coeff=coeff,
        tp1_px=float(tp1_px or 0), tp2_px=float(tp2_px or 0), tp3_px=float(tp3_px or 0),
    )
    trail_dist = trail_mult * initial_atr
    phase_sw = float(p.get("phase_switch_atr") or BREAKEVEN_TRIGGER_ATR or 3.0)
    mfe_atr = (entry_price - new_lowest) / initial_atr if initial_atr > 0 else 0.0
    new_phase = zone == "tp3_plus" or (phase_sw > 0 and mfe_atr >= phase_sw)

    step_trigger = step_trig * initial_atr
    step_count = max(0, int((entry_price - price) / step_trigger)) if step_trigger > 0 else 0
    step_stop = initial_stop - step_count * step_adv * initial_atr
    refs = [x for x in (current_stop, new_stop, step_stop) if x > 0]
    candidate = min(refs) if refs else step_stop

    trail_ceil = new_lowest + trail_dist
    if candidate <= 0:
        candidate = trail_ceil
    else:
        candidate = min(candidate, trail_ceil)

    # 马拉松：禁止 TP1/TP2 强制底线（floor_atr<=0 跳过）
    f1 = float(p.get("tp1_floor_atr") or 0.0)
    f2 = float(p.get("tp2_floor_atr") or 0.0)
    if f2 > 0 and zone in ("tp2_tp3", "tp3_confirm", "tp3_plus"):
        floor = entry_price - f2 * initial_atr
        candidate = min(candidate, floor) if candidate > 0 else floor
    elif f1 > 0 and zone == "tp1_tp2":
        floor = entry_price - f1 * initial_atr
        candidate = min(candidate, floor) if candidate > 0 else floor

    new_stop = candidate
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
    tp1_px: float = 0.0,
    tp2_px: float = 0.0,
    tp3_px: float = 0.0,
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
    trail_mult, zone = _zone_trail_atr(
        side=side, price=px, entry=entry, atr=atr, profile=p, coeff=coeff,
        tp1_px=float(tp1_px or 0), tp2_px=float(tp2_px or 0), tp3_px=float(tp3_px or 0),
    )
    meta = {
        "trail_atr": trail_mult,
        "breathing_coefficient": coeff,
        "phase2_trail_mult": 1.0,
        "profile": p.get("name") or "ETH",
        "adx": float(adx_val or 0),
        "zone": zone,
        "phase": "trail" if zone == "tp3_plus" else "ladder",
        "step_count": 0,
        "early_be_done": bool(early_be_done),
    }
    if side == "SHORT":
        stop, best, phase, step_count, early = calculate_stop_short(
            px, entry, atr, initial_stop, current_stop, best_price,
            breakeven_phase, breathing_coefficient=coeff, profile=p,
            early_be_done=early_be_done,
            tp1_px=tp1_px, tp2_px=tp2_px, tp3_px=tp3_px,
        )
    else:
        stop, best, phase, step_count, early = calculate_stop_long(
            px, entry, atr, initial_stop, current_stop, best_price,
            breakeven_phase, breathing_coefficient=coeff, profile=p,
            early_be_done=early_be_done,
            tp1_px=tp1_px, tp2_px=tp2_px, tp3_px=tp3_px,
        )
    meta["step_count"] = int(step_count)
    meta["phase"] = "trail" if phase else "ladder"
    meta["early_be_done"] = bool(early)
    meta["trail_distance"] = round(atr * trail_mult, 4) if atr > 0 else 0.0
    return {
        "stop": stop,
        "best": best,
        "breakeven_phase": phase,
        "early_be_done": bool(early),
        "meta": meta,
    }
