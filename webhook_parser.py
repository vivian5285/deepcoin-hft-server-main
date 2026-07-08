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


def enrich_entry_tp_prices(action, price, atr, regime, payload=None):
    payload = payload or {}
    if any(_to_float(payload.get(k)) for k in ("tv_tp1", "tv_tp2", "tv_tp3")):
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

    payload = dict(payload)
    payload["tv_tp1"] = round(px + sign * a * mults[0], 2)
    payload["tv_tp2"] = round(px + sign * a * mults[1], 2)
    payload["tv_tp3"] = round(px + sign * a * mults[2], 2)
    payload["_tp_enriched"] = True
    return payload


def format_webhook_log(data):
    action = data.get("action", "?")
    parts = [f"📥 TV {TV_STRATEGY_VERSION} → 【{action}】"]
    if data.get("side"):
        parts.append(f"side={data['side']}")
    if data.get("price"):
        parts.append(f"price={float(data['price']):.2f}")
    if data.get("pnl_pct") is not None:
        parts.append(f"pnl={float(data['pnl_pct']):+.2f}%")
    if data.get("reason"):
        parts.append(f"reason={str(data['reason'])[:48]}")
    if data.get("regime"):
        parts.append(f"R{data['regime']}")
    if data.get("_tp_enriched"):
        parts.append("TP=本地推算")
    return " | ".join(parts)
