#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
双币种雷达 + 智能再入场（v16.8.1 · 马拉松激活比例修复版）。

- 档位 0/1/2：ADX <20 / 20–30 / >30（弱/中/强趋势）— 步进/呼吸 + 启动比例
- 硬止损呼吸垫：统一 1.15（不分档）；硬止损独立于雷达，始终并存
- 雷达启动（第一层）：ADX 三档离散 × 1.35×initial_atr
  · 弱 ADX<20 → 68%（早激活保护微利）；中 20–30 → 78%；强 >30 → 88%（晚激活留呼吸）
- 激活瞬间：保本起步 = entry ± tick ± fee_cover（禁止跳到 TP1 底线）
- 取消 TP1/TP2 强制底线；TP 成交只缩量不改价
- 重入最多 1 次；窗口 = K线根数（ETH 2×90m）
- 重入成功后雷达系数放宽一档（looser_tier）；不影响 TP 价量
- 第二层 trail（ATR 比插值）见 breath_profiles，本文件不改
- 双保险限价：多 min(5m低+tick, TV×0.997)；空 max(5m高−tick, TV×1.003)
- 硬止损 / 亏损出局禁止重入；本地订单标签防狂挂
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

# ── 默认（config/reentry_tiers.json 可覆盖）────────────────────────────────
_DEFAULT_ARM_SL_ATR = 0.0  # 马拉松：激活臂不再用 ATR 跳价
_DEFAULT_FEE_COVER_PCT = 0.0008  # 双边约 0.08% 手续费覆盖
_DEFAULT_HARD_SL_BUFFER = 1.15
_DEFAULT_ADX_WEAK_LT = 20.0
_DEFAULT_ADX_STRONG_GT = 30.0
# 第一层雷达启动（与档位 20/30 对齐 · 离散三档）
_DEFAULT_RADAR_ACT_ADX_LO = 20.0
_DEFAULT_RADAR_ACT_ADX_HI = 30.0
# 带宽：弱早(低比例) … 强晚(高比例)
_DEFAULT_RADAR_ACT_RATIO_LO = 0.68
_DEFAULT_RADAR_ACT_RATIO_HI = 0.88
_DEFAULT_ACT_RATIO_WEAK = 0.68   # 65%–70% 中值：弱趋势早激活
_DEFAULT_ACT_RATIO_MID = 0.78    # 75%–80% 中值
_DEFAULT_ACT_RATIO_STRONG = 0.88  # 85%–90% 中值：强趋势晚激活
# v4.0 写反的离散值 → normalize 时若与 ADX 档不符则重算
_INVERTED_LEGACY_RATIOS = frozenset({0.70, 0.85})

_DEFAULT_ETH_TIERS: List[Dict[str, float]] = [
    # binance parity v2.1 (498757d): step_trigger/step_advance widened ~40-60%,
    # breath_tp12/breath_tp23 carried over from the already-validated v2.0
    # values (market runs ahead, radar trails at a safe distance behind it).
    {"step_trigger_atr": 0.70, "step_advance_atr": 0.42,
     "breath_tp12": 1.50, "breath_tp23": 2.00, "min_mult": 2.5, "max_mult": 3.5},
    {"step_trigger_atr": 0.85, "step_advance_atr": 0.55,
     "breath_tp12": 2.00, "breath_tp23": 2.80, "min_mult": 3.0, "max_mult": 4.5},
    {"step_trigger_atr": 1.00, "step_advance_atr": 0.65,
     "breath_tp12": 2.50, "breath_tp23": 3.50, "min_mult": 4.0, "max_mult": 6.0},
]
_DEFAULT_XAU_TIERS: List[Dict[str, float]] = [
    # binance parity v2.1 (498757d). XAU previously had no dedicated tier
    # table in this file and silently fell back to the ETH table via
    # get_reentry_profile()'s default -- XAU's price scale/volatility profile
    # is different enough (see breath_profiles.BREATH_XAU) that it should
    # use its own tuned tiers, matching binance.
    {"step_trigger_atr": 0.70, "step_advance_atr": 0.50,
     "breath_tp12": 2.00, "breath_tp23": 2.80, "min_mult": 3.0, "max_mult": 4.5},
    {"step_trigger_atr": 0.85, "step_advance_atr": 0.55,
     "breath_tp12": 2.50, "breath_tp23": 3.50, "min_mult": 3.5, "max_mult": 5.5},
    {"step_trigger_atr": 1.00, "step_advance_atr": 0.65,
     "breath_tp12": 3.00, "breath_tp23": 4.00, "min_mult": 5.0, "max_mult": 7.0},
]

REENTRY_TIERS_JSON = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "config", "reentry_tiers.json",
)


def _load_tiers_file() -> Dict[str, Any]:
    path = REENTRY_TIERS_JSON
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


_CFG = _load_tiers_file()
# 默认启动比例（ADX 中档附近）；真实值开仓时按 ADX 冻结
ACTIVATION_TP1_FRAC = float(
    _CFG.get("radar_act_ratio_lo")
    if _CFG.get("radar_act_ratio_lo") is not None
    else (
        _CFG.get("activation_tp1_frac")
        if _CFG.get("activation_tp1_frac") is not None
        else _DEFAULT_RADAR_ACT_RATIO_LO
    )
)
# 兼容旧字段名（不再表示重入 TP2）
ACTIVATION_TP1_FRAC_REENTRY = float(
    _CFG.get("radar_act_ratio_hi")
    if _CFG.get("radar_act_ratio_hi") is not None
    else (
        _CFG.get("activation_tp1_frac_reentry")
        if _CFG.get("activation_tp1_frac_reentry") is not None
        else _DEFAULT_RADAR_ACT_RATIO_HI
    )
)
ACTIVATION_FRACS: List[float] = [ACTIVATION_TP1_FRAC, ACTIVATION_TP1_FRAC_REENTRY]
# 兼容旧常量名（语义已改为 adx_ratio）
ACTIVATION_MODE_FIRST = "adx_ratio"
ACTIVATION_MODE_REENTRY = "adx_ratio"
ARM_SL_ATR = float(
    _CFG.get("arm_sl_atr")
    if _CFG.get("arm_sl_atr") is not None
    else _DEFAULT_ARM_SL_ATR
)
FEE_COVER_PCT = float(
    _CFG.get("fee_cover_pct")
    if _CFG.get("fee_cover_pct") is not None
    else _DEFAULT_FEE_COVER_PCT
)
PHASE_SWITCH_ATR = float(
    _CFG.get("phase_switch_atr")
    if _CFG.get("phase_switch_atr") is not None
    else 3.0
)
_ACT_RATIOS = _CFG.get("activation_ratios") or {}
ACT_RATIO_WEAK = float(
    _ACT_RATIOS.get("weak")
    if _ACT_RATIOS.get("weak") is not None
    else _DEFAULT_ACT_RATIO_WEAK
)
ACT_RATIO_MID = float(
    _ACT_RATIOS.get("mid")
    if _ACT_RATIOS.get("mid") is not None
    else _DEFAULT_ACT_RATIO_MID
)
ACT_RATIO_STRONG = float(
    _ACT_RATIOS.get("strong")
    if _ACT_RATIOS.get("strong") is not None
    else _DEFAULT_ACT_RATIO_STRONG
)
ARM_MODE = str(_CFG.get("arm_mode") or "breakeven_fee").strip().lower()
LIMIT_DISCOUNT = float(_CFG.get("limit_discount") or 0.003)
LIMIT_TTL_SEC = int(_CFG.get("limit_ttl_sec") or 300)
MAX_REENTRIES = int(_CFG.get("max_reentries") or 1)
MAX_TIER_INDEX = 2  # ADX 档 0..2
MAX_UNFILLED_REFRESHES = int(_CFG.get("max_unfilled_refreshes") or 5)
DEFAULT_TICK = 0.01
STERILE_MAX_RETRY = 3
TP1_ATR_MULT = 1.35  # 启动距离 = 1.35 × initial_atr
HARD_SL_BUFFER_MULT = float(
    _CFG.get("hard_sl_buffer_mult")
    if _CFG.get("hard_sl_buffer_mult") is not None
    else _DEFAULT_HARD_SL_BUFFER
)

_bounds = _CFG.get("adx_bounds") or {}
ADX_WEAK_LT = float(_bounds.get("weak_lt") if _bounds.get("weak_lt") is not None else _DEFAULT_ADX_WEAK_LT)
ADX_STRONG_GT = float(
    _bounds.get("strong_gt") if _bounds.get("strong_gt") is not None else _DEFAULT_ADX_STRONG_GT
)
RADAR_ACT_ADX_LO = float(
    _CFG.get("radar_act_adx_lo")
    if _CFG.get("radar_act_adx_lo") is not None
    else _DEFAULT_RADAR_ACT_ADX_LO
)
RADAR_ACT_ADX_HI = float(
    _CFG.get("radar_act_adx_hi")
    if _CFG.get("radar_act_adx_hi") is not None
    else _DEFAULT_RADAR_ACT_ADX_HI
)
RADAR_ACT_RATIO_LO = float(
    _CFG.get("radar_act_ratio_lo")
    if _CFG.get("radar_act_ratio_lo") is not None
    else _DEFAULT_RADAR_ACT_RATIO_LO
)
RADAR_ACT_RATIO_HI = float(
    _CFG.get("radar_act_ratio_hi")
    if _CFG.get("radar_act_ratio_hi") is not None
    else _DEFAULT_RADAR_ACT_RATIO_HI
)
# 兼容旧名：三档同值 1.15（白皮书 v3 废止分档）
BUFFER_BY_TIER: List[float] = [HARD_SL_BUFFER_MULT, HARD_SL_BUFFER_MULT, HARD_SL_BUFFER_MULT]

ETH_TIERS: List[Dict[str, float]] = list(
    ((_CFG.get("ETH") or {}).get("tiers") or _DEFAULT_ETH_TIERS)
)
_ETH_ZONE = float((_CFG.get("ETH") or {}).get("reentry_zone_atr") or 0.5)
_ETH_WINDOW_BARS = int((_CFG.get("ETH") or {}).get("reentry_window_bars") or 2)
_ETH_TF_SEC = int((_CFG.get("ETH") or {}).get("tv_tf_sec") or 5400)

XAU_TIERS: List[Dict[str, float]] = list(
    ((_CFG.get("XAU") or {}).get("tiers") or _DEFAULT_XAU_TIERS)
)
_XAU_ZONE = float((_CFG.get("XAU") or {}).get("reentry_zone_atr") or 0.3)
_XAU_WINDOW_BARS = int((_CFG.get("XAU") or {}).get("reentry_window_bars") or 3)
_XAU_TF_SEC = int((_CFG.get("XAU") or {}).get("tv_tf_sec") or 2700)


def make_reentry_client_order_id(
    symbol: str, side: str, price: float, ts: Optional[float] = None,
) -> str:
    """交易所 newClientOrderId（≤36）：SHA-256 订单标签，幂等防狂挂。"""
    sym_u = str(symbol or "").upper()
    sym = "E" if "ETH" in sym_u else "S"
    sd = "L" if str(side or "").upper() in ("LONG", "BUY", "L") else "S"
    px = abs(int(round(float(price or 0) * 100))) % 1_000_000
    t = abs(int(float(ts if ts is not None else time.time()))) % 100_000
    raw = f"{sym_u}|RE|{sd}|{px}|{t}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:8]
    return f"RE{sym}{sd}{digest}{t % 10000}"[:36]


REENTRY_ETH: Dict[str, Any] = {
    "name": "ETH",
    "tv_tf": "90m",
    "tv_tf_sec": _ETH_TF_SEC,
    "enabled": True,
    "activation_tp1_frac": ACTIVATION_TP1_FRAC,
    "activation_tp1_frac_reentry": ACTIVATION_TP1_FRAC_REENTRY,
    "radar_act_adx_lo": RADAR_ACT_ADX_LO,
    "radar_act_adx_hi": RADAR_ACT_ADX_HI,
    "radar_act_ratio_lo": RADAR_ACT_RATIO_LO,
    "radar_act_ratio_hi": RADAR_ACT_RATIO_HI,
    "arm_sl_atr": ARM_SL_ATR,
    "fee_cover_pct": FEE_COVER_PCT,
    "arm_mode": ARM_MODE,
    "tiers": ETH_TIERS,
    "reentry_zone_atr": _ETH_ZONE,
    "reentry_window_bars": _ETH_WINDOW_BARS,
    "limit_discount": LIMIT_DISCOUNT,
    "limit_ttl_sec": LIMIT_TTL_SEC,
    "max_reentries": MAX_REENTRIES,
    "max_unfilled_refreshes": MAX_UNFILLED_REFRESHES,
    "tick_size": 0.01,
}

# BNB 重入配置（跟随 ETH 逻辑）
REENTRY_BNB: Dict[str, Any] = {
    "name": "BNB",
    "tv_tf": "90m",
    "tv_tf_sec": _ETH_TF_SEC,
    "enabled": True,
    "activation_tp1_frac": ACTIVATION_TP1_FRAC,
    "activation_tp1_frac_reentry": ACTIVATION_TP1_FRAC_REENTRY,
    "radar_act_adx_lo": RADAR_ACT_ADX_LO,
    "radar_act_adx_hi": RADAR_ACT_ADX_HI,
    "radar_act_ratio_lo": RADAR_ACT_RATIO_LO,
    "radar_act_ratio_hi": RADAR_ACT_RATIO_HI,
    "arm_sl_atr": ARM_SL_ATR,
    "fee_cover_pct": FEE_COVER_PCT,
    "arm_mode": ARM_MODE,
    "tiers": ETH_TIERS,
    "reentry_zone_atr": _ETH_ZONE,
    "reentry_window_bars": _ETH_WINDOW_BARS,
    "limit_discount": LIMIT_DISCOUNT,
    "limit_ttl_sec": LIMIT_TTL_SEC,
    "max_reentries": MAX_REENTRIES,
    "max_unfilled_refreshes": MAX_UNFILLED_REFRESHES,
    "tick_size": 0.01,
}

REENTRY_XAU: Dict[str, Any] = {
    "name": "XAU",
    "tv_tf": "45m",
    "tv_tf_sec": _XAU_TF_SEC,
    "enabled": True,
    "activation_tp1_frac": ACTIVATION_TP1_FRAC,
    "activation_tp1_frac_reentry": ACTIVATION_TP1_FRAC_REENTRY,
    "radar_act_adx_lo": RADAR_ACT_ADX_LO,
    "radar_act_adx_hi": RADAR_ACT_ADX_HI,
    "radar_act_ratio_lo": RADAR_ACT_RATIO_LO,
    "radar_act_ratio_hi": RADAR_ACT_RATIO_HI,
    "arm_sl_atr": ARM_SL_ATR,
    "fee_cover_pct": FEE_COVER_PCT,
    "arm_mode": ARM_MODE,
    "tiers": XAU_TIERS,
    "reentry_zone_atr": _XAU_ZONE,
    "reentry_window_bars": _XAU_WINDOW_BARS,
    "limit_discount": LIMIT_DISCOUNT,
    "limit_ttl_sec": LIMIT_TTL_SEC,
    "max_reentries": MAX_REENTRIES,
    "max_unfilled_refreshes": MAX_UNFILLED_REFRESHES,
    "tick_size": 0.01,
}

_BY_SYMBOL = {
    "ETHUSDT": REENTRY_ETH,
    "ETH-USDT-SWAP": REENTRY_ETH,
    "BNBUSDT": REENTRY_BNB,
    "BNB-USDT-SWAP": REENTRY_BNB,
    "XAUUSDT": REENTRY_XAU,
    "XAU-USDT-SWAP": REENTRY_XAU,
}


def get_reentry_profile(symbol: str) -> Dict[str, Any]:
    sym = str(symbol or "").strip().upper()
    return dict(_BY_SYMBOL.get(sym) or REENTRY_ETH)


def reentry_enabled(symbol: str) -> bool:
    return bool(get_reentry_profile(symbol).get("enabled", True))


def adx_to_tier(adx: float) -> int:
    """ADX → 档位 0弱 / 1中 / 2强。"""
    try:
        a = float(adx)
    except (TypeError, ValueError):
        a = 25.0  # 缺省按中趋势
    if a < ADX_WEAK_LT:
        return 0
    if a > ADX_STRONG_GT:
        return 2
    return 1


def clamp_tier(tier: int) -> int:
    t = int(tier or 0)
    if t < 0:
        return 0
    if t > MAX_TIER_INDEX:
        return MAX_TIER_INDEX
    return t


def looser_tier(tier: int) -> int:
    """重入成功后放宽一档（封顶强趋势）。"""
    return clamp_tier(int(tier or 0) + 1)


def tier_label(tier: int) -> str:
    """档位显示名：弱/中/强。"""
    names = ("弱趋势", "中趋势", "强趋势")
    return names[clamp_tier(tier)]


def tier_label_short(tier: int) -> str:
    return f"T{clamp_tier(tier)}"


def format_tier_notify_line(
    tier: int,
    adx: Optional[float] = None,
    *,
    source: str = "",
) -> str:
    """钉钉/TG 档位一行：弱/中/强 + ADX + 作用说明（硬止损不分档）。"""
    t = clamp_tier(tier)
    parts = [f"{tier_label(t)}（T{t}）"]
    try:
        a = float(adx or 0)
    except (TypeError, ValueError):
        a = 0.0
    if a > 0:
        parts.append(f"ADX={a:.1f}")
    parts.append("仅影响雷达步进/呼吸 · 硬止损垫恒1.15")
    src = str(source or "").strip()
    if src:
        parts.append(f"来源={src}")
    return " · ".join(parts)


def buffer_for_tier(tier: int = 0) -> float:
    """白皮书 v3.0：统一 1.15，tier 忽略。"""
    return float(HARD_SL_BUFFER_MULT)


def buffer_for_adx(adx: float = 0.0) -> float:
    return float(HARD_SL_BUFFER_MULT)


def activation_mode_for_attempt(attempt: int = 0) -> str:
    """首次/重入均 ADX 比例启动（兼容旧名）。"""
    _ = attempt
    return ACTIVATION_MODE_FIRST


def is_legacy_activation_frac(frac: Optional[float]) -> bool:
    """旧中点/TP2 模式标记 0.0/1.0，或越界值 → 需按 ADX 重算。
    合法区间放宽到 0.65~0.95，兼容旧连续插值冻结的 0.70~0.90。
    """
    try:
        f = float(frac)
    except (TypeError, ValueError):
        return True
    if f <= 0.0 or f >= 0.999:
        return True
    return not (0.65 <= f <= 0.95)


def radar_activation_ratio_from_adx(adx: Optional[float] = None) -> float:
    """
    第一层：ADX → 启动比例（马拉松离散三档 · 修复版）。
    ADX<20 → 68%（早）；20≤ADX≤30 → 78%；ADX>30 → 88%（晚）。
    弱趋势早激活防微利回吐；强趋势晚激活给深度回踩留呼吸空间。
    """
    try:
        a = float(adx)
    except (TypeError, ValueError):
        a = 25.0
    if a != a:  # NaN
        a = 25.0
    weak_lt = float(ADX_WEAK_LT)
    strong_gt = float(ADX_STRONG_GT)
    if a < weak_lt:
        return round(float(ACT_RATIO_WEAK), 6)
    if a > strong_gt:
        return round(float(ACT_RATIO_STRONG), 6)
    return round(float(ACT_RATIO_MID), 6)


def normalize_activation_ratio(
    frac: Optional[float] = None,
    adx: Optional[float] = None,
) -> float:
    """账本 frac 合法则沿用；旧标记 / v4.0 翻转离散值按 ADX 重算。"""
    expected = radar_activation_ratio_from_adx(adx)
    if is_legacy_activation_frac(frac):
        return expected
    try:
        f = round(float(frac), 6)
    except (TypeError, ValueError):
        return expected
    # v4.0 写反：弱85%/强70% — 与当前 ADX 档偏差大则迁移
    if round(f, 2) in _INVERTED_LEGACY_RATIOS and abs(f - expected) > 0.029:
        return expected
    return f


def activation_frac_for_attempt(
    attempt: int = 0, profile: Optional[Dict[str, Any]] = None,
    *,
    adx: Optional[float] = None,
) -> float:
    """返回 ADX 启动比例（首次/重入同一公式；attempt 忽略）。"""
    _ = attempt
    _ = profile
    return radar_activation_ratio_from_adx(adx)


def activation_frac_fixed(profile: Optional[Dict[str, Any]] = None) -> float:
    """兼容旧名：缺省 ADX=25 → 中间比例。"""
    return activation_frac_for_attempt(0, profile, adx=25.0)


def radar_activation_price_adx(
    side: str,
    entry: float,
    initial_atr: float,
    *,
    adx: Optional[float] = None,
    ratio: Optional[float] = None,
    tp1_atr_mult: float = TP1_ATR_MULT,
    tp1_dist: Optional[float] = None,
) -> float:
    """
    启动价 = entry ± ratio × TP1距离。
    TP1距离优先用真实 TV TP1（tp1_dist）；缺失时回退 1.35 × initial_atr。
    ratio 优先；否则由 adx 计算。
    """
    side_u = str(side or "").strip().upper()
    entry_f = float(entry or 0)
    atr = float(initial_atr or 0)
    if ratio is not None and not is_legacy_activation_frac(ratio):
        r = float(ratio)
    else:
        r = radar_activation_ratio_from_adx(adx)
    try:
        base = abs(float(tp1_dist or 0))
    except (TypeError, ValueError):
        base = 0.0
    if base <= 0:
        base = tp1_distance(atr, tp1_atr_mult)
    dist = base * float(r)
    if entry_f <= 0 or dist <= 0 or side_u not in ("LONG", "SHORT"):
        return 0.0
    if side_u == "LONG":
        return round(entry_f + dist, 4)
    return round(entry_f - dist, 4)


RADAR_GATE_TP1_PROGRESS = 0.8  # 首次开仓：entry→TP1 走完 80%（剩 20%）即激活
RADAR_GATE_ATR_MULT = 1.5  # 首次开仓：顺向浮盈达到 1.5×ATR 即激活（双触发的另一条腿，对齐binance f162ede数值）


def radar_gate_price_from_tps(
    tp1: float,
    tp2: float,
    reentry_attempt: int = 0,
    entry: float = 0.0,
    atr: float = 0.0,
    **kwargs,
) -> float:
    """
    【规格 v2.1 · TP1临近/ATR双触发（首次，谁先到用谁）· TP2激活（重入）】
    对齐币安同款修复（binance commit c3c6522 + f162ede）：
    - 首次开仓 reentry_attempt=0：雷达激活价 = entry 沿盈利方向推进
      min(0.8×|TP1-entry|, 1.5×ATR) 的距离——即"距TP1剩20%"和"顺向浮盈满
      1.5×ATR"两条线谁先到就用谁。
      单纯按 TP1 距离的老公式有个漏洞：强趋势档 TP1 定得更远，连带触发
      点也被动拉远，导致强趋势单反而更容易在触发前把浮盈吐回去。加一条
      独立的 ATR 触发线兜底，同时保留 TP1 这条线。
      ATR 倍数直接对齐 binance 数值（1.5），未按 deepcoin 自己 TV 策略
      （深币策略.txt）实际分档保本触发（弱0.41×ATR~强1.47×ATR，regime
      相关）逐档精确校准——2026-08-10 已确认差异并跟用户核实过，用户
      选择先用统一数值对齐，未来若要精确对齐需解析 webhook 的 tp1_m
      字段按 regime 动态计算。
      entry/atr 缺失时（旧调用点未传参）回退到中点公式，保持向后兼容。
    - 重入开仓 reentry_attempt>=1：雷达激活价 = TP2（不变）
    """
    t1 = float(tp1 or 0)
    t2 = float(tp2 or 0)
    attempt = int(reentry_attempt or 0)
    if t1 <= 0 or t2 <= 0:
        return 0.0
    if attempt >= 1:
        # 重入开仓：TP2 绝对价格
        return round(t2, 4)
    e = float(entry or 0)
    a = float(atr or 0)
    if e <= 0:
        # 兼容旧调用签名（未传 entry）：退回 TP1-TP2 区间中点
        return round((t1 + t2) / 2.0, 4)
    direction = 1.0 if t1 >= e else -1.0
    dist_tp1 = RADAR_GATE_TP1_PROGRESS * abs(t1 - e)
    dist_atr = RADAR_GATE_ATR_MULT * a if a > 0 else dist_tp1
    dist = min(dist_tp1, dist_atr)
    gate = e + direction * dist
    return round(gate, 4)


def radar_gate_label(reentry_attempt: int = 0, ratio: Optional[float] = None) -> str:
    """规格 v2.0：TP1临近/ATR双触发标签（首次=双触发，重入=TP2绝对价）。
    ratio 参数已废弃（旧 ADX 插值比例公式不再驱动激活判断），保留仅为
    兼容旧调用签名。"""
    _ = ratio
    return "TP2绝对价" if int(reentry_attempt or 0) >= 1 else "TP1临近/ATR双触发"


def tier_coeffs(tier: int, profile: Optional[Dict[str, Any]] = None) -> Dict[str, float]:
    p = profile if isinstance(profile, dict) else REENTRY_ETH
    tiers = list(p.get("tiers") or ETH_TIERS)
    idx = clamp_tier(tier)
    if idx >= len(tiers):
        idx = len(tiers) - 1
    row = dict(tiers[idx] if tiers else {})
    return {
        "step_trigger_atr": float(row.get("step_trigger_atr") or 0.5),
        "step_advance_atr": float(row.get("step_advance_atr") or 0.35),
        "breath_tp12": float(row.get("breath_tp12") or 1.2),
        "breath_tp23": float(row.get("breath_tp23") or 1.6),
        "min_mult": float(row.get("min_mult") or 2.0),
        "max_mult": float(row.get("max_mult") or 2.5),
        # 兼容旧 breath overlay 字段名（禁用早保本）
        "early_be_atr": 0.0,
    }


def apply_tier_to_breath_profile(
    breath_profile: Dict[str, Any],
    tier: int,
    reentry_profile: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Copy breath profile and overlay ADX-tier step/breath/trail band."""
    out = dict(breath_profile or {})
    rp = reentry_profile if isinstance(reentry_profile, dict) else None
    if rp is None:
        rp = REENTRY_ETH
    coeffs = tier_coeffs(tier, rp)
    out["step_trigger_atr"] = coeffs["step_trigger_atr"]
    out["step_advance_atr"] = coeffs["step_advance_atr"]
    out["breath_tp12"] = coeffs["breath_tp12"]
    out["breath_tp23"] = coeffs["breath_tp23"]
    out["min_mult"] = coeffs["min_mult"]
    out["max_mult"] = coeffs["max_mult"]
    out["early_be_atr"] = 0.0
    out["tp1_floor_atr"] = 0.0  # 马拉松：取消强制底线
    out["tp2_floor_atr"] = 0.0
    out["phase_switch_atr"] = float(PHASE_SWITCH_ATR)
    # 激活臂：保本起步，不再用 ATR 跳价
    out["initial_sl_atr"] = 0.0
    out["fee_cover_pct"] = float(
        rp.get("fee_cover_pct") if rp.get("fee_cover_pct") is not None else FEE_COVER_PCT
    )
    out["adx_tier"] = clamp_tier(tier)
    return out


def breakeven_arm_price(
    side: str,
    entry: float,
    *,
    tick_size: float = DEFAULT_TICK,
    fee_cover_pct: Optional[float] = None,
) -> float:
    """
    马拉松激活瞬间保本位：
      多 = entry + tick + fee_cover
      空 = entry − tick − fee_cover
    即使被扫也不亏（覆盖往返手续费）。
    """
    side_u = str(side or "").strip().upper()
    e = float(entry or 0)
    if e <= 0 or side_u not in ("LONG", "SHORT"):
        return 0.0
    try:
        tick = abs(float(tick_size or 0))
    except (TypeError, ValueError):
        tick = DEFAULT_TICK
    if tick <= 0:
        tick = DEFAULT_TICK
    try:
        fee_pct = abs(float(
            fee_cover_pct if fee_cover_pct is not None else FEE_COVER_PCT
        ))
    except (TypeError, ValueError):
        fee_pct = _DEFAULT_FEE_COVER_PCT
    fee = e * fee_pct
    if side_u == "LONG":
        return round(e + tick + fee, 2)
    return round(e - tick - fee, 2)


def arm_stop_price(
    side: str,
    entry: float,
    initial_atr: float = 0.0,
    arm_atr: float = None,
    *,
    tick_size: float = DEFAULT_TICK,
    fee_cover_pct: Optional[float] = None,
) -> float:
    """
    雷达激活时初始止损（马拉松保本起步）。
    默认：entry ± tick ± fee；旧 arm_atr>0 路径仅兼容测试。
    """
    _ = initial_atr
    if arm_atr is not None and float(arm_atr or 0) > 0:
        # 兼容旧测试/调用：显式传入正 arm_atr 时仍走 ATR 臂
        side_u = str(side or "").strip().upper()
        e = float(entry or 0)
        atr = float(initial_atr or 0)
        mult = abs(float(arm_atr))
        if e <= 0 or atr <= 0 or side_u not in ("LONG", "SHORT"):
            return 0.0
        if side_u == "LONG":
            return round(e - mult * atr, 2)
        return round(e + mult * atr, 2)
    return breakeven_arm_price(
        side, entry, tick_size=tick_size, fee_cover_pct=fee_cover_pct,
    )


def tp1_distance(initial_atr: float, tp1_atr_mult: float = TP1_ATR_MULT) -> float:
    return abs(float(initial_atr or 0)) * float(tp1_atr_mult or TP1_ATR_MULT)


def activation_price(
    side: str,
    entry: float,
    initial_atr: float,
    frac: float = None,
    tp1_atr_mult: float = TP1_ATR_MULT,
) -> float:
    """雷达启动价：多 = entry + ratio×1.35ATR；空对称。"""
    r = frac
    if r is None or is_legacy_activation_frac(r):
        r = RADAR_ACT_RATIO_LO
    return radar_activation_price_adx(
        side, entry, initial_atr, ratio=float(r), tp1_atr_mult=tp1_atr_mult,
    )


def activation_price_from_tp1(
    side: str,
    entry: float,
    tp1: float,
    frac: float = None,
    *,
    tv_price: Optional[float] = None,
    initial_atr: float = 0.0,
    adx: Optional[float] = None,
) -> float:
    """
    兼容旧签名：优先走 ADX比例 × 真实TP1距离；无 TP1 时回退 1.35ATR。
    """
    side_u = str(side or "").strip().upper()
    e = float(entry or 0)
    t = float(tp1 or 0)
    ref = float(tv_price or 0) or e
    real_dist = abs(t - ref) if t > 0 and ref > 0 else 0.0
    atr = float(initial_atr or 0)
    if atr > 0 or real_dist > 0:
        return radar_activation_price_adx(
            side, entry, atr, adx=adx, ratio=frac, tp1_dist=real_dist,
        )
    r = normalize_activation_ratio(frac, adx)
    if e <= 0 or t <= 0 or side_u not in ("LONG", "SHORT"):
        return 0.0
    dist = abs(t - e) * r
    if dist <= 0:
        return 0.0
    if side_u == "LONG":
        return round(e + dist, 4)
    return round(e - dist, 4)


def next_activation_frac(
    current_frac: float, attempt_after_bump: int,
    profile: Optional[Dict[str, Any]] = None,
    *,
    adx: Optional[float] = None,
) -> float:
    """重入后沿用已冻结比例（合法）或按 ADX 重算。"""
    _ = attempt_after_bump
    _ = profile
    return normalize_activation_ratio(current_frac, adx)


def reentry_window_sec(symbol: str) -> float:
    p = get_reentry_profile(symbol)
    bars = int(p.get("reentry_window_bars") or 2)
    tf = int(p.get("tv_tf_sec") or 5400)
    return float(max(1, bars) * max(60, tf))


def reentry_window_deadline(symbol: str, from_ts: Optional[float] = None) -> float:
    ts = float(from_ts if from_ts is not None else time.time())
    return ts + reentry_window_sec(symbol)


def reentry_limit_price_fallback(
    side: str, tv_price: float, discount: float = LIMIT_DISCOUNT,
) -> float:
    side_u = str(side or "").strip().upper()
    px = float(tv_price or 0)
    d = abs(float(discount if discount is not None else LIMIT_DISCOUNT))
    if px <= 0 or side_u not in ("LONG", "SHORT"):
        return 0.0
    if side_u == "LONG":
        return round(px * (1.0 - d), 2)
    return round(px * (1.0 + d), 2)


def reentry_limit_price(side: str, ref_price: float, discount: float = LIMIT_DISCOUNT) -> float:
    return reentry_limit_price_fallback(side, ref_price, discount)


def parse_kline_extreme(klines: Any) -> Tuple[float, float]:
    """取最近一根已收盘 K 线的 low/high（规格 8.3；跳过正在形成的 [-1]）。"""
    if not klines:
        return 0.0, 0.0
    try:
        import time as _time
        rows = list(klines)
        now_ms = int(_time.time() * 1000)
        row = None
        for cand in reversed(rows):
            try:
                close_t = int(cand[6])
            except (TypeError, ValueError, IndexError):
                close_t = 0
            if close_t > 0 and close_t < now_ms:
                row = cand
                break
        if row is None:
            # 无 close_time 时：≥2 根则 [-2] 通常已收盘，[-1] 为形成中
            row = rows[-2] if len(rows) >= 2 else rows[-1]
        hi = float(row[2])
        lo = float(row[3])
        if lo > 0 and hi > 0:
            return lo, hi
    except (TypeError, ValueError, IndexError):
        pass
    return 0.0, 0.0


def reentry_limit_from_extreme(
    side: str,
    low: float,
    high: float,
    tick: float = DEFAULT_TICK,
) -> float:
    side_u = str(side or "").strip().upper()
    t = abs(float(tick or DEFAULT_TICK))
    lo = float(low or 0)
    hi = float(high or 0)
    if side_u == "LONG":
        if lo <= 0:
            return 0.0
        return round(lo + t, 2)
    if side_u == "SHORT":
        if hi <= 0:
            return 0.0
        return round(hi - t, 2)
    return 0.0


def is_better_than_tv(side: str, limit_px: float, tv_price: float) -> bool:
    side_u = str(side or "").strip().upper()
    lim = float(limit_px or 0)
    tv = float(tv_price or 0)
    if lim <= 0 or tv <= 0 or side_u not in ("LONG", "SHORT"):
        return False
    if side_u == "LONG":
        return lim < tv - 1e-9
    return lim > tv + 1e-9


def is_better_than_entry(side: str, limit_px: float, entry: float) -> bool:
    """白皮书：重入价必须优于上一次开仓价。"""
    side_u = str(side or "").strip().upper()
    lim = float(limit_px or 0)
    e = float(entry or 0)
    if lim <= 0 or e <= 0 or side_u not in ("LONG", "SHORT"):
        return False
    if side_u == "LONG":
        return lim < e - 1e-9
    return lim > e + 1e-9


def pick_dual_insurance(
    side: str,
    extreme_px: float,
    tv_discount_px: float,
) -> Tuple[float, str]:
    side_u = str(side or "").strip().upper()
    ex = float(extreme_px or 0)
    tv = float(tv_discount_px or 0)
    if ex <= 0 and tv <= 0:
        return 0.0, "none"
    if ex <= 0:
        return tv, "tv_discount"
    if tv <= 0:
        return ex, "kline_extreme"
    if side_u == "LONG":
        if ex <= tv:
            return ex, "dual_min_kline"
        return tv, "dual_min_tv"
    if side_u == "SHORT":
        if ex >= tv:
            return ex, "dual_max_kline"
        return tv, "dual_max_tv"
    return 0.0, "bad_side"


def compute_reentry_limit_px(
    *,
    side: str,
    tv_price: float,
    low5: float = 0.0,
    high5: float = 0.0,
    low3: float = 0.0,
    high3: float = 0.0,
    tick: float = DEFAULT_TICK,
    discount: float = LIMIT_DISCOUNT,
    prev_entry: float = 0.0,
) -> Tuple[float, str]:
    """
    双保险：极值候选（5m→3m）与 TV 折扣取更优；必须优于 TV；
    若给 prev_entry，还必须优于上次开仓价。
    """
    side_u = str(side or "").strip().upper()
    tv = float(tv_price or 0)
    if side_u not in ("LONG", "SHORT") or tv <= 0:
        return 0.0, "bad_args"

    extreme = 0.0
    extreme_src = ""
    px5 = reentry_limit_from_extreme(side_u, low5, high5, tick)
    if px5 > 0:
        extreme, extreme_src = px5, "kline_5m"
    else:
        px3 = reentry_limit_from_extreme(side_u, low3, high3, tick)
        if px3 > 0:
            extreme, extreme_src = px3, "kline_3m"

    fb = reentry_limit_price_fallback(side_u, tv, discount)
    lim, pick = pick_dual_insurance(side_u, extreme, fb)
    if lim <= 0:
        return 0.0, "no_candidate"
    if not is_better_than_tv(side_u, lim, tv):
        return 0.0, "not_better_than_tv"
    pe = float(prev_entry or 0)
    if pe > 0 and not is_better_than_entry(side_u, lim, pe):
        return 0.0, "not_better_than_entry"
    src = pick
    if extreme_src and pick.startswith("dual_") and "kline" in pick:
        src = f"dual_{extreme_src}"
    elif pick == "kline_extreme" and extreme_src:
        src = extreme_src
    return lim, src


def exit_in_reentry_zone(
    side: str,
    entry: float,
    exit_px: float,
    initial_atr: float,
    zone_atr: float,
) -> bool:
    """保本/微赚区间：多 [entry, entry+zone×ATR]；空对称。亏损 → False。"""
    side_u = str(side or "").strip().upper()
    e = float(entry or 0)
    x = float(exit_px or 0)
    atr = float(initial_atr or 0)
    z = abs(float(zone_atr or 0))
    if e <= 0 or x <= 0 or atr <= 0 or z <= 0 or side_u not in ("LONG", "SHORT"):
        return False
    band = z * atr
    if side_u == "LONG":
        return e <= x <= e + band + 1e-9
    return e - band - 1e-9 <= x <= e


def can_smart_reenter(
    *,
    exit_source: str,
    side: str,
    entry: float,
    exit_px: float,
    initial_atr: float,
    reentry_attempt: int,
    profile: Optional[Dict[str, Any]] = None,
    window_deadline_ts: float = 0.0,
    now: Optional[float] = None,
    tp1_ever_filled: bool = False,
    adx_tier: Optional[int] = None,
) -> Tuple[bool, str]:
    """
    返回 (ok, reason)。硬止损 / 亏损 / 已重入过 / 窗口过期 / 区间外 /
    TP1已成交 / 非强趋势 → 拒绝。
    """
    p = profile if isinstance(profile, dict) else REENTRY_ETH
    if not bool(p.get("enabled", True)):
        return False, "reentry_disabled"
    src = str(exit_source or "").strip().lower()
    max_n = int(p.get("max_reentries") or MAX_REENTRIES)
    attempt = int(reentry_attempt or 0)
    if src in ("vps_hard_sl", "hard_sl"):
        return False, "hard_sl_no_reentry"
    if attempt >= max_n:
        return False, "max_reentries"
    if src in ("tv_close", "tv_protect", "quick_exit", "rsi_exit"):
        return False, "tv_close_no_reentry"
    if src not in (
        "radar_be", "sl_breakeven", "sl_initial", "breakeven", "radar", "radar_sl",
    ):
        return False, f"exit_source={src}"
    # 规格 8.1.5：TP1 已成交过 → 禁止重入
    if bool(tp1_ever_filled):
        return False, "tp1_already_filled"
    # 规格 8.1.6：仅强趋势 tier=2 允许重入
    if adx_tier is not None:
        try:
            tier_i = int(adx_tier)
        except (TypeError, ValueError):
            tier_i = -1
        if tier_i >= 0 and tier_i != 2:
            return False, "tier_not_strong"
    deadline = float(window_deadline_ts or 0)
    if deadline > 0:
        ts = float(now if now is not None else time.time())
        if ts > deadline + 1e-6:
            return False, "window_expired"
    zone = float(p.get("reentry_zone_atr") or 0.5)
    if not exit_in_reentry_zone(side, entry, exit_px, initial_atr, zone):
        return False, "outside_reentry_zone"
    return True, "ok"
