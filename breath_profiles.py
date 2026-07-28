#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
按品种呼吸参数档（ETH / XAU）。执行引擎共用，只在配置层区分。

v16.8.0 / 马拉松雷达：
  - 雷达激活臂 = 保本起步（entry±tick±fee），不再用 initial_sl_atr=0.5 跳价
  - 取消 TP1/TP2 强制底线（tp1_floor_atr=tp2_floor_atr=0）
  - 浮盈≥3×ATR 切入阶段二连续追踪（phase_switch_atr=3）
  - 禁用早保本抢跑（early_be_atr=0）；ADX 三档步进/呼吸由 reentry_profiles overlay
  - 硬止损呼吸垫已迁至 defense_profiles 统一 1.15（本文件不管 buffer）
  - 本文件存基线；运行时 apply_tier_to_breath_profile 覆盖 step/breath/trail
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

# 共用边界（ATR 比插值；TP3+ 实际 min/max 由档位 overlay）
RATIO_FLOOR = 0.6
RATIO_CEILING = 2.2

# ETH 基线（中趋势档会 overlay）
BREATH_ETH: Dict[str, Any] = {
    "name": "ETH",
    "initial_sl_atr": 0.0,  # 马拉松：激活用保本位，不用 ATR 臂
    "fee_cover_pct": 0.0008,
    "stop_exec_buffer": 0.3,
    "early_be_atr": 0.0,
    "step_trigger_atr": 0.50,
    "step_advance_atr": 0.35,
    "phase_switch_atr": 3.0,  # 浮盈≥3×ATR → 阶段二连续追踪
    "tp1_atr": 1.35,
    "tp1_floor_atr": 0.0,  # 取消强制底线
    "tp2_atr": 2.5,
    "tp2_floor_atr": 0.0,
    "breath_tp12": 1.20,
    "breath_tp23": 1.60,
    "phase2_trail_mult": 1.0,
    "min_mult": 1.8,
    "max_mult": 2.5,
    "ratio_floor": RATIO_FLOOR,
    "ratio_ceiling": RATIO_CEILING,
    "tick_size": 0.01,
    "entry_score": 3,
    "exit_score": 2,
}

# XAU 基线
BREATH_XAU: Dict[str, Any] = {
    "name": "XAU",
    "initial_sl_atr": 0.0,
    "fee_cover_pct": 0.0008,
    "stop_exec_buffer": 0.5,
    "early_be_atr": 0.0,
    "step_trigger_atr": 0.40,
    "step_advance_atr": 0.30,
    "phase_switch_atr": 3.0,
    "tp1_atr": 1.35,
    "tp1_floor_atr": 0.0,
    "tp2_atr": 2.5,
    "tp2_floor_atr": 0.0,
    "breath_tp12": 1.00,
    "breath_tp23": 1.40,
    "phase2_trail_mult": 1.0,
    "min_mult": 1.5,
    "max_mult": 2.0,
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
    连续线性插值（TP3+ 动态追踪带宽）：
      ratio<=floor → minMult
      ratio>=ceiling → maxMult
      否则线性
    """
    p = profile if isinstance(profile, dict) and profile else BREATH_ETH
    lo = float(p.get("ratio_floor") if p.get("ratio_floor") is not None else RATIO_FLOOR)
    hi = float(p.get("ratio_ceiling") if p.get("ratio_ceiling") is not None else RATIO_CEILING)
    mn = float(p.get("min_mult") if p.get("min_mult") is not None else 2.0)
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
    """兼容旧名：现改为连续插值。"""
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

    def upgrade_to_vps(self, atr: float) -> float:
        """已废除：VPS 不得覆盖 TV 锁定 atr；保留方法以免旧调用崩，直接返回现值。"""
        return self._value
