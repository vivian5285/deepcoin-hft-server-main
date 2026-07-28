#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
硬止损纯函数（规格 v1.0 对齐币安 v16.4）：

  tv_stop_distance = |TV.price − TV.stop_loss|
  actual_stop_distance = tv_stop_distance × 1.15（统一系数，不分档）
  多：硬止损 = 成交价 − actual_stop_distance
  空：硬止损 = 成交价 + actual_stop_distance

雷达 ATR 一律用 TV webhook.atr。
TP 限价仅 TP1+TP2；TP3 不挂限价。
"""
from __future__ import annotations

from typing import Dict, Optional

HARD_SL_BUFFER_MULT = 1.15
HARD_SL_RADAR_ATR_MULT = 0.0
HARD_SL_RADAR_PAD = 0.0
HARD_SL_SLIPPAGE_MULT = 0.0
TEMP_STOP_BUFFER_MULT = HARD_SL_BUFFER_MULT


def compute_hard_stop_distance(
    tv_entry: float,
    tv_stop_loss: float,
    fill_entry: float = 0.0,
    initial_atr: float = 0.0,
    *,
    tv_mult: float = HARD_SL_BUFFER_MULT,
    radar_atr_mult: float = 0.0,
    radar_pad: float = 0.0,
    slip_mult: float = 0.0,
) -> Dict[str, float]:
    """
    硬止损距离拆解（不含方向）。
    仅 |TV价−TV.SL|×buffer；radar_floor/slip 恒为 0（旧字段保留兼容日志）。
    """
    tv_e = float(tv_entry or 0)
    tv_sl = float(tv_stop_loss or 0)
    tv_m = float(tv_mult or HARD_SL_BUFFER_MULT)

    tv_implied = abs(tv_e - tv_sl) * tv_m if tv_e > 0 and tv_sl > 0 and tv_m > 0 else 0.0
    radar_floor = 0.0
    slip = 0.0
    base = float(tv_implied)
    final = base if base > 0 else 0.0
    return {
        "tv_implied": float(tv_implied),
        "radar_floor": float(radar_floor),
        "base": float(base),
        "slip": float(slip),
        "final": float(final),
    }


def hard_stop_price(
    side: str,
    entry: float,
    tv_stop_loss: float,
    buffer_mult: float = HARD_SL_BUFFER_MULT,
    *,
    tv_entry: Optional[float] = None,
    initial_atr: float = 0.0,
    fill_entry: Optional[float] = None,
    slip_mult: float = 0.0,
) -> float:
    """
    永久硬止损价（唯一路径）。

    - fill = fill_entry 或 entry（交易所成交价）
    - 距离 = |tv_entry − tv_stop_loss| × buffer（tv_entry 缺省=fill）
    - 无有效 stop_loss → 0（上层必须拒开，禁止 1.5×ATR 兜底）
    """
    side_u = str(side or "").strip().upper()
    fill = float(fill_entry if fill_entry is not None else (entry or 0))
    sl = float(tv_stop_loss or 0)
    mult = float(buffer_mult or HARD_SL_BUFFER_MULT)
    tv_e = float(tv_entry) if tv_entry is not None else fill
    if tv_e <= 0:
        tv_e = fill

    if fill <= 0 or side_u not in ("LONG", "SHORT"):
        return 0.0
    if sl <= 0:
        return 0.0

    parts = compute_hard_stop_distance(
        tv_e, sl, fill, 0.0, tv_mult=mult, slip_mult=0.0,
    )
    dist = float(parts["final"])
    if dist <= 0:
        return 0.0
    if side_u == "LONG":
        return round(fill - dist, 2)
    return round(fill + dist, 2)


def temp_hard_stop_price(side: str, entry: float, tv_stop_loss: float,
                         buffer_mult: float = HARD_SL_BUFFER_MULT, **kwargs) -> float:
    """兼容旧调用名 → 永久硬止损（同一公式）。"""
    return hard_stop_price(
        side, entry, tv_stop_loss, buffer_mult=buffer_mult, **kwargs
    )
