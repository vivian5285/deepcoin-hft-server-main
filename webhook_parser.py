#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""TradingView webhook parser — v6.9.85 比例传递版 compatible."""
import json
import logging
import math
import re

logger = logging.getLogger(__name__)

TV_STRATEGY_VERSION = "v6.9.86"

# VPS 自主风控（与 TV risk_pct / qty_ratio 完全脱钩）
VPS_RISK_PCT = 3.0
VPS_GLOBAL_SCALE = 1.0
VPS_REGIME_SCALE = {
    1: 0.55,   # 极弱
    2: 0.75,   # 弱
    3: 0.95,   # 中势
    4: 1.33,   # 强势（开发清单最终版）
}
MAX_RISK_PCT = 4.0
MIN_RISK_PCT = 0.5
MAX_POSITION_SIZE = 9999.0
MIN_QTY_DEFAULT = 0.001

# 加仓：固定首仓 50%，最多 2 次（PYRAMID / PROFIT_ADD 一视同仁）
ADD_QTY_RATIO = 0.5
MAX_ADD_TIMES = 2

# 兼容旧引用
MAX_RISK_PCT_LIMIT = MAX_RISK_PCT
VPS_REGIME_RISK_MULTIPLIERS = VPS_REGIME_SCALE

ENTRY_TYPE_OPEN = "OPEN"
ENTRY_TYPE_PYRAMID = "PYRAMID"
ENTRY_TYPE_PROFIT_ADD = "PROFIT_ADD"
VALID_ENTRY_TYPES = frozenset({
    ENTRY_TYPE_OPEN, ENTRY_TYPE_PYRAMID, ENTRY_TYPE_PROFIT_ADD,
})

VALID_ACTIONS = frozenset({
    "LONG", "SHORT", "CLOSE", "CLOSE_PROTECT", "CLOSE_TP3", "CLOSE_STOPLOSS",
    "UPDATE_SL", "PING",
})

ACTION_ALIASES = {
    "BUY": "LONG",
    "SELL": "SHORT",
    "CLOSE_LONG": "CLOSE",
    "CLOSE_SHORT": "CLOSE",
    "PROTECT": "CLOSE_PROTECT",
    "CLOSE_PROTECTIVE": "CLOSE_PROTECT",
    "TP3": "CLOSE_TP3",
    "CLOSE_TP": "CLOSE_TP3",
    "STOPLOSS": "CLOSE_STOPLOSS",
    "CLOSE_SL": "CLOSE_STOPLOSS",
    "STOP": "CLOSE_STOPLOSS",
    "CLOSE_STOP": "CLOSE_STOPLOSS",
}

CLOSE_TYPE_TP3 = "tp3"
CLOSE_TYPE_PROTECT = "protect"
CLOSE_TYPE_BREAKEVEN = "breakeven"
CLOSE_TYPE_HARD_SL = "hard_sl"
CLOSE_TYPE_VPS_SHIELD = "vps_shield"
CLOSE_TYPE_GENERIC = "generic"

CLOSE_TYPE_LABELS = {
    CLOSE_TYPE_TP3: "TP3止盈",
    CLOSE_TYPE_PROTECT: "风控拦截",
    CLOSE_TYPE_BREAKEVEN: "防回吐保本",
    CLOSE_TYPE_HARD_SL: "硬止损",
    CLOSE_TYPE_VPS_SHIELD: "TV硬止损",
    CLOSE_TYPE_GENERIC: "常规清场",
}


def classify_tv_close(action="", reason="", pnl_pct=None):
    action = str(action or "").strip().upper()
    reason = str(reason or "").strip()

    if action == "CLOSE_TP3" or ("TP3" in reason and ("完美" in reason or "收网" in reason)):
        return CLOSE_TYPE_TP3
    if action in ("CLOSE_PROTECT",) or action.startswith("CLOSE_PROTECT"):
        return CLOSE_TYPE_PROTECT
    if action == "CLOSE_STOPLOSS" or action.startswith("CLOSE_STOP"):
        if "防回吐" in reason:
            return CLOSE_TYPE_BREAKEVEN
        if "触碰硬止损" in reason or reason == "硬止损":
            return CLOSE_TYPE_HARD_SL
        if "VPS" in reason or "10%" in reason:
            return CLOSE_TYPE_VPS_SHIELD
        if "保本" in reason and "硬止损" not in reason:
            return CLOSE_TYPE_BREAKEVEN
        pnl = _to_float(pnl_pct, None)
        if pnl is not None and pnl > -0.1:
            return CLOSE_TYPE_BREAKEVEN
        if pnl is not None:
            return CLOSE_TYPE_HARD_SL
        return CLOSE_TYPE_HARD_SL
    if "防回吐" in reason or ("保本" in reason and "雷达" in reason):
        return CLOSE_TYPE_BREAKEVEN
    if "VPS" in reason and "硬止损" in reason:
        return CLOSE_TYPE_VPS_SHIELD
    if "触碰硬止损" in reason or ("硬止损" in reason and "10%" not in reason):
        return CLOSE_TYPE_HARD_SL
    if "保护" in reason or "拦截" in reason or "风控" in reason:
        return CLOSE_TYPE_PROTECT
    if "TP3" in reason or "完美胜利" in reason or "完美收网" in reason:
        return CLOSE_TYPE_TP3
    return CLOSE_TYPE_GENERIC


def close_type_display_label(close_type, fallback_reason=""):
    label = CLOSE_TYPE_LABELS.get(close_type or CLOSE_TYPE_GENERIC, "常规清场")
    if close_type == CLOSE_TYPE_GENERIC and fallback_reason:
        return fallback_reason[:48]
    return label

TV_REGIME_TP_MULT = {
    1: (0.75, 1.4, 2.0),
    2: (1.10, 2.0, 2.8),
    3: (1.30, 2.6, 3.8),
    4: (1.55, 3.0, 4.8),
}


def _strip_bom(text):
    if not text:
        return ""
    if text.startswith("\ufeff"):
        return text[1:]
    return text


def _to_float(val, default=None):
    if val is None or val == "":
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _to_int(val, default=None):
    f = _to_float(val, None)
    if f is None:
        return default
    try:
        return int(f)
    except (TypeError, ValueError):
        return default


def normalize_entry_type(val, default=ENTRY_TYPE_OPEN):
    raw = str(val or default).strip().upper()
    aliases = {
        "ADD": ENTRY_TYPE_PYRAMID,
        "PYRAMID_ADD": ENTRY_TYPE_PYRAMID,
        "RECHARGE": ENTRY_TYPE_PYRAMID,
        "PROFIT": ENTRY_TYPE_PROFIT_ADD,
        "PROFITADD": ENTRY_TYPE_PROFIT_ADD,
        "PROFIT_ADD": ENTRY_TYPE_PROFIT_ADD,
    }
    if raw in aliases:
        return aliases[raw]
    if raw in VALID_ENTRY_TYPES:
        return raw
    return default


def compute_vps_effective_risk(regime, global_scale=None):
    """VPS 有效风险% = clamp(VPS_RISK_PCT × REGIME_SCALE × GLOBAL_SCALE)"""
    regime = int(regime or 3)
    regime_scale = float(VPS_REGIME_SCALE.get(regime, VPS_REGIME_SCALE.get(3, 0.95)))
    gs = float(global_scale if global_scale is not None else VPS_GLOBAL_SCALE)
    raw = VPS_RISK_PCT * regime_scale * gs
    capped = raw > MAX_RISK_PCT
    final = min(max(raw, MIN_RISK_PCT), MAX_RISK_PCT)
    return final, {
        "regime": regime,
        "vps_risk_pct": VPS_RISK_PCT,
        "regime_scale": regime_scale,
        "global_scale": gs,
        "effective_risk_pct": round(final, 4),
        "raw_risk_pct": round(raw, 4),
        "risk_capped": capped,
    }


def _normalize_stop_dist(price, tv_sl):
    price = float(price or 0)
    tv_sl = float(tv_sl or 0)
    stop_dist = abs(price - tv_sl)
    if stop_dist <= 0:
        stop_dist = max(price * 0.001, 0.01)
    elif stop_dist < price * 0.0005:
        stop_dist = max(price * 0.001, 0.01)
    return stop_dist


def compute_vps_open_qty(principal, price, tv_sl, regime, leverage=5,
                         global_scale=None, qty_step=0.001, min_qty=None,
                         face_value=None, max_position=None):
    """
    首次开仓 OPEN（开发清单最终版）：
    保证金 = 本金 × VPS_RISK_PCT% × LEVERAGE × REGIME_SCALE
    头寸价值 = 保证金 × LEVERAGE
    张数 = 头寸价值 / price / face_value
    tv_sl 仅用于挂止损，不参与张数计算。
    """
    principal = float(principal or 0)
    price = float(price or 0)
    leverage = float(leverage or 1)
    min_qty = float(min_qty if min_qty is not None else MIN_QTY_DEFAULT)
    max_position = float(max_position if max_position is not None else MAX_POSITION_SIZE)

    effective_risk, risk_meta = compute_vps_effective_risk(regime, global_scale)
    regime_scale = float(risk_meta.get("regime_scale", 1.0))
    meta = {
        "principal": principal,
        "price": price,
        "tv_sl": float(tv_sl or 0),
        "leverage": leverage,
        "sizing_mode": "VPS_OPEN",
        **risk_meta,
    }
    if principal <= 0 or price <= 0 or leverage <= 0:
        meta["error"] = "invalid_inputs"
        return 0.0, meta

    margin = principal * (VPS_RISK_PCT / 100.0) * leverage * regime_scale
    position_value = margin * leverage
    meta["margin"] = round(margin, 2)
    meta["order_amount"] = round(position_value, 2)
    meta["position_value"] = round(position_value, 2)
    meta["numerator_usdt"] = round(position_value, 2)
    if tv_sl and price:
        meta["stop_dist"] = round(_normalize_stop_dist(price, tv_sl), 2)

    if face_value and float(face_value) > 0:
        fv = float(face_value)
        raw_qty = position_value / price / fv
        max_qty = (principal * leverage) / (fv * price)
        qty = max(1, int(raw_qty))
        qty = min(qty, max(1, int(max_qty)), int(max_position))
        meta["max_qty"] = int(max_qty)
        meta["raw_qty"] = round(raw_qty, 4)
        meta["base_qty"] = float(qty)
        meta["capped"] = qty >= int(max_qty)
        return float(qty), meta

    raw_qty = position_value / price
    max_qty = min((principal * leverage) / price, max_position)
    qty = math.floor(raw_qty / qty_step) * qty_step
    qty = max(min_qty, qty)
    qty = min(qty, math.floor(max_qty / qty_step) * qty_step)
    qty = round(qty, 3)
    meta["max_qty"] = round(max_qty, 3)
    meta["raw_qty"] = round(raw_qty, 4)
    meta["base_qty"] = qty
    meta["capped"] = qty >= round(max_qty, 3) - qty_step
    return qty, meta


def compute_vps_add_qty(base_qty, qty_ratio=None, qty_step=0.001, min_qty=None,
                        face_value=None, max_position=None):
    """加仓 PYRAMID/PROFIT_ADD：add_qty = base_qty × ADD_QTY_RATIO（固定，忽略 TV qty_ratio）"""
    base_qty = float(base_qty or 0)
    qty_ratio = float(ADD_QTY_RATIO if qty_ratio is None else qty_ratio)
    min_qty = float(min_qty if min_qty is not None else MIN_QTY_DEFAULT)
    max_position = float(max_position if max_position is not None else MAX_POSITION_SIZE)
    meta = {
        "base_qty": base_qty,
        "qty_ratio": qty_ratio,
        "add_qty_ratio_fixed": ADD_QTY_RATIO,
        "sizing_mode": "VPS_ADD",
    }
    if base_qty <= 0 or qty_ratio <= 0:
        meta["error"] = "invalid_base_or_ratio"
        return 0.0, meta

    raw = base_qty * qty_ratio
    if face_value and float(face_value) > 0:
        qty = max(1, int(raw))
        qty = min(qty, int(max_position))
        meta["raw_qty"] = round(raw, 4)
        return float(qty), meta

    qty = math.floor(raw / qty_step) * qty_step
    qty = max(min_qty, qty)
    qty = min(qty, math.floor(max_position / qty_step) * qty_step)
    qty = round(qty, 3)
    meta["raw_qty"] = round(raw, 4)
    return qty, meta


def apply_vps_regime_risk(risk_pct, regime):
    """兼容旧调用：转为 VPS 自主 effective_risk"""
    return compute_vps_effective_risk(regime)


def compute_tv_order_qty(principal, risk_pct, leverage, qty_ratio, price, tv_sl,
                         qty_step=0.001, min_qty=0.001, face_value=None, regime=None):
    """兼容别名 → VPS OPEN（忽略 TV risk_pct / qty_ratio≠1 时仍走 OPEN 公式）"""
    return compute_vps_open_qty(
        principal, price, tv_sl, regime, leverage=leverage,
        qty_step=qty_step, min_qty=min_qty, face_value=face_value,
    )


def format_vps_sizing_note(meta=None, qty=None, entry_type="OPEN"):
    meta = meta or {}
    mode = meta.get("sizing_mode", "VPS_OPEN")
    if mode == "VPS_ADD":
        return (
            f"base={float(meta.get('base_qty', 0)):.3f} "
            f"× ratio={float(meta.get('qty_ratio', 0)):.2f} "
            f"→ add={float(qty or meta.get('raw_qty', 0)):.3f}"
        )
    eff = float(meta.get("effective_risk_pct", VPS_RISK_PCT))
    parts = [
        f"VPS风险={eff:.3f}%",
        f"R{int(meta.get('regime', 3))}×{float(meta.get('regime_scale', 1)):.2f}",
        f"lev={int(round(float(meta.get('leverage', 5))))}x",
    ]
    if meta.get("margin"):
        parts.append(f"保证金={float(meta['margin']):.1f}U")
    if meta.get("position_value") or meta.get("order_amount"):
        pv = float(meta.get("position_value") or meta.get("order_amount") or 0)
        parts.append(f"头寸={pv:.1f}U")
    if qty is not None and float(qty) > 0:
        parts.append(f"qty={float(qty)}")
    if meta.get("base_qty"):
        parts.append(f"base={float(meta['base_qty'])}")
    return " · ".join(parts)


def format_tv_sizing_note(risk_pct=None, leverage=None, qty_ratio=None, principal=None, qty=None,
                          regime=None, final_risk_pct=None, meta=None, entry_type="OPEN"):
    if meta:
        return format_vps_sizing_note(meta, qty=qty, entry_type=entry_type)
    eff_meta, _ = compute_vps_effective_risk(regime or 3)
    parts = [
        f"VPS风险={eff_meta:.3f}%",
        f"lev={int(round(float(leverage or 5)))}x",
    ]
    if qty_ratio is not None:
        parts.append(f"ratio={float(qty_ratio):.2f}")
    if principal and principal > 0:
        parts.append(f"本金={float(principal):.0f}U")
    if qty is not None and float(qty) > 0:
        parts.append(f"qty={float(qty)}")
    return " · ".join(parts)


def _unwrap_payload(obj):
    if not isinstance(obj, dict):
        return obj
    for key in ("message", "alert", "payload", "data", "body"):
        inner = obj.get(key)
        if isinstance(inner, dict):
            merged = dict(obj)
            merged.update(inner)
            return merged
        if isinstance(inner, str):
            nested = _extract_json_object(inner)
            if isinstance(nested, dict):
                merged = dict(obj)
                merged.update(nested)
                return merged
    return obj


def _extract_json_object(text):
    text = _strip_bom(str(text or "").strip())
    if not text:
        return None

    try:
        obj = json.loads(text)
    except json.JSONDecodeError:
        obj = None

    if isinstance(obj, dict):
        return obj
    if isinstance(obj, str):
        try:
            inner = json.loads(obj)
            if isinstance(inner, dict):
                return inner
        except json.JSONDecodeError:
            pass

    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass
    return None


def parse_webhook_request(raw_bytes, content_type="", as_json=None):
    raw_text = ""
    if raw_bytes:
        try:
            raw_text = raw_bytes.decode("utf-8", errors="replace")
        except Exception:
            raw_text = str(raw_bytes)

    data = None
    if isinstance(as_json, dict):
        data = as_json
    elif raw_text:
        data = _extract_json_object(raw_text)

    if data is None:
        raise ValueError("Invalid JSON payload")

    data = _unwrap_payload(data)
    if not isinstance(data, dict):
        raise ValueError("Payload must be a JSON object")

    return raw_text, data


def normalize_tv_payload(data):
    if not isinstance(data, dict):
        raise ValueError("Payload must be a JSON object")

    src = _unwrap_payload(data)
    out = dict(src)

    action_raw = str(
        src.get("action")
        or src.get("side_action")
        or src.get("signal")
        or src.get("order")
        or ""
    ).strip().upper()
    action = ACTION_ALIASES.get(action_raw, action_raw)

    side_raw = str(src.get("side") or src.get("position_side") or "").strip().upper()
    if side_raw in ("BUY", "LONG"):
        side = "LONG"
    elif side_raw in ("SELL", "SHORT"):
        side = "SHORT"
    elif side_raw == "NONE":
        side = ""
    else:
        side = side_raw if side_raw in ("LONG", "SHORT") else ""

    if action in ("LONG", "SHORT") and not side:
        side = action

    price = _to_float(
        src.get("price") or src.get("close") or src.get("entry") or src.get("entry_price")
    )
    pnl_pct = _to_float(src.get("pnl_pct") or src.get("pnl") or src.get("pnlPercent"))
    regime = _to_int(src.get("regime") or src.get("adx_regime"))
    atr = _to_float(src.get("atr") or src.get("ATR"))

    tv_tp1 = _to_float(src.get("tv_tp1") or src.get("tp1") or src.get("TP1"))
    tv_tp2 = _to_float(src.get("tv_tp2") or src.get("tp2") or src.get("TP2"))
    tv_tp3 = _to_float(src.get("tv_tp3") or src.get("tp3") or src.get("TP3"))

    reason = str(
        src.get("reason")
        or src.get("exit_reason")
        or src.get("comment")
        or ""
    ).strip()

    secret = str(src.get("secret") or src.get("token") or src.get("key") or "").strip()
    tv_sl = _to_float(src.get("tv_sl") or src.get("stop") or src.get("sl"))
    entry_type = normalize_entry_type(src.get("entry_type") or src.get("entryType"))
    risk_pct = _to_float(src.get("risk_pct") or src.get("riskPct") or src.get("risk"))
    leverage = _to_float(src.get("leverage") or src.get("lev"))
    qty_ratio = _to_float(src.get("qty_ratio") or src.get("qtyRatio"))

    out["action"] = action
    out["side"] = side
    if price is not None:
        out["price"] = price
    if pnl_pct is not None:
        out["pnl_pct"] = pnl_pct
    if regime is not None:
        out["regime"] = regime
    if atr is not None:
        out["atr"] = atr
    if tv_tp1 is not None:
        out["tv_tp1"] = tv_tp1
    if tv_tp2 is not None:
        out["tv_tp2"] = tv_tp2
    if tv_tp3 is not None:
        out["tv_tp3"] = tv_tp3
    if reason:
        out["reason"] = reason
    if secret:
        out["secret"] = secret
    if tv_sl is not None and tv_sl > 0:
        out["tv_sl"] = round(tv_sl, 2)
    if action in ("LONG", "SHORT"):
        out["entry_type"] = entry_type
        if risk_pct is not None and risk_pct > 0:
            out["risk_pct"] = round(risk_pct, 4)
        if leverage is not None and leverage > 0:
            out["leverage"] = round(leverage, 2)
        if qty_ratio is not None and qty_ratio > 0:
            out["qty_ratio"] = round(qty_ratio, 4)
        elif entry_type == ENTRY_TYPE_OPEN:
            out["qty_ratio"] = 1.0

    out["_normalized"] = True
    out["_schema"] = TV_STRATEGY_VERSION
    out["_parse_ok"] = bool(action) and (
        action in VALID_ACTIONS or action.startswith("CLOSE")
    )
    return out


def compute_atr_from_klines(klines, period=14):
    if not klines or len(klines) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(klines)):
        try:
            high = float(klines[i][2])
            low = float(klines[i][3])
            prev_close = float(klines[i - 1][4])
        except (IndexError, TypeError, ValueError):
            continue
        tr = max(high - low, abs(high - prev_close), abs(l - prev_close))
        trs.append(tr)
    if len(trs) < period:
        return 0.0
    return sum(trs[-period:]) / period


def fetch_eth_atr_14_public(period=14):
    try:
        import requests
        resp = requests.get(
            "https://fapi.binance.com/fapi/v1/klines",
            params={"symbol": "ETHUSDT", "interval": "15m", "limit": period + 20},
            timeout=8,
        )
        resp.raise_for_status()
        return compute_atr_from_klines(resp.json(), period)
    except Exception as e:
        logger.warning(f"Public ETH ATR fetch failed: {e}")
        return 0.0


def _field_present(val):
    return val is not None and val != ""


def _has_positive_float(val):
    f = _to_float(val)
    return f is not None and f > 0


def enrich_entry_tp_prices(action, price, atr, regime, payload=None):
    payload = dict(payload or {})
    tps = {
        1: _to_float(payload.get("tv_tp1")),
        2: _to_float(payload.get("tv_tp2")),
        3: _to_float(payload.get("tv_tp3")),
    }
    if all(tps[i] and tps[i] > 0 for i in (1, 2, 3)):
        payload["_tp_source"] = "tv"
        return payload

    regime = int(regime or 3)
    if regime not in TV_REGIME_TP_MULT:
        regime = 3
    mults = TV_REGIME_TP_MULT[regime]
    sign = 1.0 if action == "LONG" else -1.0
    px = float(price or 0)
    a = float(atr or 0)
    if px <= 0 or a <= 0:
        return payload

    filled = 0
    for i, mult in enumerate(mults, start=1):
        key = f"tv_tp{i}"
        if not _has_positive_float(payload.get(key)):
            payload[key] = round(px + sign * a * mult, 2)
            filled += 1

    if filled == 3:
        payload["_tp_source"] = "local"
    elif filled > 0:
        payload["_tp_source"] = "tv+local"
    else:
        payload["_tp_source"] = "tv"
    return payload


def enrich_signal_fields(payload, action, fetch_atr=None, fallback_regime=3, fallback_atr=30.0,
                         fallback_price=0.0):
    out = dict(payload or {})
    action = str(action or "").strip().upper()

    if not _has_positive_float(out.get("price")) and fallback_price > 0:
        out["price"] = fallback_price
        out["_price_source"] = "local"
    elif _has_positive_float(out.get("price")):
        out["_price_source"] = "tv"

    is_entry = action in ("LONG", "SHORT")
    is_close = action.startswith("CLOSE") or action in (VALID_ACTIONS - {"LONG", "SHORT"})

    if is_entry or is_close:
        if not _field_present(out.get("regime")):
            out["regime"] = int(fallback_regime or 3)
            out["_regime_source"] = "local"
        else:
            out["regime"] = _to_int(out.get("regime"), fallback_regime)
            out["_regime_source"] = "tv"

        if not _has_positive_float(out.get("atr")):
            atr = 0.0
            if callable(fetch_atr):
                atr = float(fetch_atr() or 0)
            out["atr"] = atr or float(fallback_atr or 30.0)
            out["_atr_source"] = "local"
        else:
            out["_atr_source"] = "tv"

    if is_entry:
        out = enrich_entry_tp_prices(
            action, out.get("price"), out.get("atr"), out.get("regime"), out,
        )
    return out


def format_tv_field_sources(data):
    if not data:
        return "TV透传"

    label_map = {"regime": "档位", "atr": "ATR", "tp": "TP", "price": "价格"}
    source_vals = {"tv", "local", "tv+local"}

    def _tag(src):
        if src == "tv":
            return "TV透传"
        if src == "local":
            return "本地补全"
        if src == "tv+local":
            return "TV+补全"
        return str(src)

    if any(k in data for k in label_map) and all(
        (not data.get(k)) or str(data.get(k)) in source_vals for k in label_map
    ):
        parts = []
        for key, label in label_map.items():
            src = data.get(key)
            if src:
                parts.append(f"{label}={_tag(src)}")
        return " · ".join(parts) if parts else "TV透传"

    parts = []
    for key, label in (("regime", "档位"), ("atr", "ATR"), ("price", "价格")):
        src = data.get(f"_{key}_source")
        if src:
            parts.append(f"{label}={_tag(src)}")
    tp_src = data.get("_tp_source")
    if tp_src:
        parts.append(f"TP={_tag(tp_src)}")
    return " · ".join(parts) if parts else "TV透传"


def format_webhook_log(data):
    action = data.get("action", "?")
    parts = [f"📥 TV {TV_STRATEGY_VERSION} → 【{action}】"]
    if action.startswith("CLOSE") and data.get("reason"):
        parts.append(f"reason={str(data['reason'])[:56]}")
    if data.get("side"):
        parts.append(f"side={data['side']}")
    if data.get("price"):
        px_src = data.get("_price_source", "tv")
        parts.append(f"price={float(data['price']):.2f}({px_src})")
    if data.get("pnl_pct") is not None:
        parts.append(f"pnl={float(data['pnl_pct']):+.2f}%")
    if data.get("reason"):
        parts.append(f"reason={str(data['reason'])[:48]}")
    if data.get("regime") is not None:
        r_src = data.get("_regime_source", "tv")
        parts.append(f"R{data['regime']}({r_src})")
    if data.get("atr"):
        a_src = data.get("_atr_source", "tv")
        parts.append(f"ATR={float(data['atr']):.2f}({a_src})")
    tps = [data.get(f"tv_tp{i}") for i in (1, 2, 3)]
    if any(_has_positive_float(t) for t in tps):
        tp_txt = "/".join(
            f"{float(t):.0f}" if _has_positive_float(t) else "-"
            for t in tps
        )
        tp_src = data.get("_tp_source", "tv")
        parts.append(f"TP={tp_txt}({tp_src})")
    if data.get("tv_sl"):
        parts.append(f"tv_sl={float(data['tv_sl']):.2f}")
    if data.get("entry_type"):
        parts.append(f"type={data['entry_type']}")
    if data.get("risk_pct"):
        parts.append(f"risk={float(data['risk_pct']):.2f}%")
    if data.get("leverage"):
        parts.append(f"lev={float(data['leverage']):.0f}x")
    if data.get("qty_ratio") is not None:
        parts.append(f"ratio={float(data['qty_ratio']):.2f}")
    return " | ".join(parts)
