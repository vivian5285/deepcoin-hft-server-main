#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""TradingView webhook parser — v6.9.75 终极强化版 compatible."""
import json
import logging
import re

logger = logging.getLogger(__name__)

TV_STRATEGY_VERSION = "v6.9.75"

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
    CLOSE_TYPE_VPS_SHIELD: "VPS 10%硬止损",
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
    return " | ".join(parts)
