#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
按品种呼吸参数档（ETH / XAU）。执行引擎共用，只在配置层区分。

连续插值版（2026-07-22 定稿）：
  ratioFloor=0.6 / ratioCeiling=2.2（共用）
  trailDistanceMultiplier(ratio) = 线性插值 minMult→maxMult
  平滑：先对 ratio 做近 3 次均值，再代入公式（无离散档跳变）
  冷启动（0 次采样）：ratio=1.0 → 公式自然中间值
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

# 共用边界
RATIO_FLOOR = 0.6
RATIO_CEILING = 2.2

# ETH：追踪距离 1.2~2.5×ATR
BREATH_ETH: Dict[str, Any] = {
    "name": "ETH",
    "initial_sl_atr": 1.5,
    "stop_exec_buffer": 0.3,
    "early_be_atr": 0.5,
    "step_trigger_atr": 0.75,
    "step_advance_atr": 0.4,
    "phase_switch_atr": 3.0,
    "tp1_atr": 1.35,
    "tp1_floor_atr": 0.5,
    "tp2_atr": 2.5,
    "tp2_floor_atr": 1.5,
    "phase2_trail_mult": 1.0,  # 已废弃额外收紧；保留键兼容，恒为 1.0
    "min_mult": 1.2,
    "max_mult": 2.5,
    "ratio_floor": RATIO_FLOOR,
    "ratio_ceiling": RATIO_CEILING,
    "tick_size": 0.01,
    "entry_score": 3,
    "exit_score": 2,
}

# XAU：更紧，min/max 直接体现收紧（不再 ×0.8）
BREATH_XAU: Dict[str, Any] = {
    "name": "XAU",
    "initial_sl_atr": 1.5,
    "stop_exec_buffer": 0.5,
    "early_be_atr": 0.3,
    "step_trigger_atr": 0.4,
    "step_advance_atr": 0.35,
    "phase_switch_atr": 3.0,
    "tp1_atr": 1.35,
    "tp1_floor_atr": 0.5,
    "tp2_atr": 2.5,
    "tp2_floor_atr": 1.5,
    "phase2_trail_mult": 1.0,
    "min_mult": 0.8,
    "max_mult": 1.8,
    "ratio_floor": RATIO_FLOOR,
    "ratio_ceiling": RATIO_CEILING,
    "tick_size": 0.01,
    "entry_score": 1,
    "exit_score": 1,
}

_BY_BINANCE = {
    "ETHUSDT": BREATH_ETH,
    "XAUUSDT": BREATH_XAU,
}

_BY_DEEPCOIN = {
    "ETH-USDT-SWAP": BREATH_ETH,
    "XAU-USDT-SWAP": BREATH_XAU,
}


def get_breath_profile(symbol: str, exchange: str = "binance") -> Dict[str, Any]:
    sym = str(symbol or "").strip().upper()
    if exchange == "deepcoin":
        return dict(_BY_DEEPCOIN.get(sym) or BREATH_ETH)
    return dict(_BY_BINANCE.get(sym) or BREATH_ETH)


def trail_distance_multiplier(ratio: float, profile: Optional[Dict[str, Any]] = None) -> float:
    """
    连续线性插值：
      ratio<=floor → minMult
      ratio>=ceiling → maxMult
      否则线性
    """
    p = profile if isinstance(profile, dict) and profile else BREATH_ETH
    lo = float(p.get("ratio_floor") if p.get("ratio_floor") is not None else RATIO_FLOOR)
    hi = float(p.get("ratio_ceiling") if p.get("ratio_ceiling") is not None else RATIO_CEILING)
    mn = float(p.get("min_mult") if p.get("min_mult") is not None else 1.2)
    mx = float(p.get("max_mult") if p.get("max_mult") is not None else 2.5)
    r = float(ratio or 0.0)
    if r <= lo:
        return mn
    if r >= hi:
        return mx
    if hi <= lo:
        return mx
    t = (r - lo) / (hi - lo)
    return mn + (mx - mn) * t


def cold_start_multiplier(profile: Optional[Dict[str, Any]] = None) -> float:
    """0 次采样：ratio=1.0 代入公式。"""
    return trail_distance_multiplier(1.0, profile)


def map_coeff_from_tiers(smooth_ratio: float, tiers: Optional[List] = None) -> float:
    """
    兼容旧名：现改为连续插值。
    tiers 参数忽略；若传入 profile dict 作第二参则用之（旧调用可能传 tiers list）。
    """
    # 旧签名 map_coeff_from_tiers(smooth, tiers_list) — tiers_list 忽略，用 ETH 默认
    profile = None
    if isinstance(tiers, dict):
        profile = tiers
    return trail_distance_multiplier(float(smooth_ratio or 0), profile)


def default_breath_profile() -> Dict[str, Any]:
    return dict(BREATH_ETH)


class LockedInitialAtr:
    """
    initial_atr 开仓写入后锁定；仅 clear_on_flat 可清零。
    非开仓路径赋值 raise / 忽略（strict 模式 raise）。
    """

    def __init__(self, strict: bool = True):
        self._value = 0.0
        self._locked = False
        self._strict = bool(strict)

    @property
    def value(self) -> float:
        return float(self._value or 0.0)

    @property
    def locked(self) -> bool:
        return bool(self._locked)

    def set_on_open(self, atr: float) -> float:
        v = float(atr or 0)
        if v <= 0:
            raise ValueError("set_on_open requires atr>0")
        self._value = v
        self._locked = True
        return self._value

    def clear_on_flat(self) -> None:
        self._value = 0.0
        self._locked = False

    def try_set(self, atr: float, *, allow_while_locked: bool = False) -> float:
        """持仓期禁止写入；allow_while_locked 仅测试/迁移用。"""
        if self._locked and not allow_while_locked:
            if self._strict:
                raise RuntimeError(
                    f"initial_atr locked at {self._value}; refuse write {atr}"
                )
            return self._value
        v = float(atr or 0)
        if v > 0:
            self._value = v
        return self._value
