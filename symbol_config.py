#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""双品种（ETH / XAU）元数据与 TV ticker 归一化 — 币安 / 深币共用逻辑。"""
import os
import re

# 币安 USDT 永续
# 注：本仓库不直接对接币安，此表仅作 resolve_deepcoin_symbol() 内部的别名
# 中转（TV ticker → 币安式 symbol → 深币 symbol）。之前只有 ETHUSDT 一项，
# 导致任何 XAU/BNB 的 TV ticker 都在中转时查不到，最终静默 fallback 到
# ETH-USDT-SWAP —— 一笔 XAU 信号会被当成 ETH 来开仓，这是发现的活跃 bug。
BINANCE_SYMBOL_META = {
    "ETHUSDT": {
        "symbol": "ETHUSDT",
        "unit": "ETH",
        "tag": "ETH",
        "qty_step": 0.001,
        "min_qty": 0.001,
        "dust_qty": 0.004,
        "price_precision": 2,
        "breath": "ETH",
    },
    "XAUUSDT": {
        "symbol": "XAUUSDT",
        "unit": "XAU",
        "tag": "XAU",
        "qty_step": 0.01,
        "min_qty": 0.01,
        "dust_qty": 0.01,
        "price_precision": 2,
        "breath": "XAU",
    },
    "BNBUSDT": {
        "symbol": "BNBUSDT",
        "unit": "BNB",
        "tag": "BNB",
        "qty_step": 0.001,
        "min_qty": 0.001,
        "dust_qty": 0.004,
        "price_precision": 2,
        "breath": "BNB",
    },
}

# 深币 SWAP
DEEPCOIN_SYMBOL_META = {
    "ETH-USDT-SWAP": {
        "symbol": "ETH-USDT-SWAP",
        "binance_mark": "ETHUSDT",
        "unit": "张",
        "tag": "ETH",
        "breath": "ETH",
        "face_value": 0.1,
        "qty_step": 1,
        "min_qty": 1,
        "dust_qty": 1,
        "price_precision": 2,
    },
    "BNB-USDT-SWAP": {
        "symbol": "BNB-USDT-SWAP",
        "binance_mark": "BNBUSDT",
        "unit": "张",
        "tag": "BNB",
        "breath": "BNB",
        "face_value": 0.1,
        "qty_step": 1,
        "min_qty": 1,
        "dust_qty": 1,
        "price_precision": 2,
    },
    "XAU-USDT-SWAP": {
        # 修复：此前完全缺失，XAU 信号会静默 fallback 成 ETH-USDT-SWAP 开仓。
        # face_value 为 API 确认前的占位默认值，__init__ 里 get_instrument_info
        # 查到真实 ctVal 后会覆盖；qty_step/min_qty/dust_qty/price_precision
        # 沿用张数合约的既有约定，tick=0.01 与 deepcoin_client.SYMBOL_TICK_FALLBACK
        # 一致。
        "symbol": "XAU-USDT-SWAP",
        "binance_mark": "XAUUSDT",
        "unit": "张",
        "tag": "XAU",
        "breath": "XAU",
        "face_value": 0.1,
        "qty_step": 1,
        "min_qty": 1,
        "dust_qty": 1,
        "price_precision": 2,
    },
}

_BINANCE_ALIASES = {
    "ETH": "ETHUSDT",
    "ETHUSDT": "ETHUSDT",
    "ETHUSD": "ETHUSDT",
    "ETHUSDT.P": "ETHUSDT",
    "BINANCE:ETHUSDT": "ETHUSDT",
    "BINANCE:ETHUSDT.P": "ETHUSDT",
    "XAU": "XAUUSDT",
    "XAUUSDT": "XAUUSDT",
    "XAUUSD": "XAUUSDT",
    "XAUUSDT.P": "XAUUSDT",
    "BINANCE:XAUUSDT": "XAUUSDT",
    "BINANCE:XAUUSDT.P": "XAUUSDT",
    "BNB": "BNBUSDT",
    "BNBUSDT": "BNBUSDT",
    "BNBUSD": "BNBUSDT",
    "BNBUSDT.P": "BNBUSDT",
    "BINANCE:BNBUSDT": "BNBUSDT",
    "BINANCE:BNBUSDT.P": "BNBUSDT",
}

_DEEPCOIN_ALIASES = {
    "ETH": "ETH-USDT-SWAP",
    "ETHUSDT": "ETH-USDT-SWAP",
    "ETHUSD": "ETH-USDT-SWAP",
    "ETH-USDT": "ETH-USDT-SWAP",
    "ETH-USDT-SWAP": "ETH-USDT-SWAP",
    "BNB": "BNB-USDT-SWAP",
    "BNBUSDT": "BNB-USDT-SWAP",
    "BNBUSD": "BNB-USDT-SWAP",
    "BNB-USDT": "BNB-USDT-SWAP",
    "BNB-USDT-SWAP": "BNB-USDT-SWAP",
    "XAU": "XAU-USDT-SWAP",
    "XAUUSDT": "XAU-USDT-SWAP",
    "XAUUSD": "XAU-USDT-SWAP",
    "XAU-USDT": "XAU-USDT-SWAP",
    "XAU-USDT-SWAP": "XAU-USDT-SWAP",
}


def _clean_ticker(raw):
    s = str(raw or "").strip().upper()
    if not s:
        return ""
    s = s.replace(" ", "")
    # TradingView: BINANCE:ETHUSDT.P / EXCHANGE:SYMBOL
    if ":" in s:
        s = s.split(":")[-1]
    s = s.replace(".P", "")
    return s


def resolve_binance_symbol(raw, default="ETHUSDT"):
    """
    归一化 TV ticker → 币安合约。
    default=\"\" 时未识别返回 symbol=\"\"（禁止静默落到 ETH）。
    """
    key = _clean_ticker(raw)
    sym = _BINANCE_ALIASES.get(key) or _BINANCE_ALIASES.get(
        re.sub(r"[^A-Z0-9]", "", key), None
    )
    if not sym and key.endswith("USDT") and key in BINANCE_SYMBOL_META:
        sym = key
    if not sym:
        if default == "" or default is None:
            return {"symbol": "", "unit": "?", "qty_step": 0.001, "min_qty": 0.001}
        sym = default
    meta = dict(BINANCE_SYMBOL_META.get(sym, BINANCE_SYMBOL_META["ETHUSDT"]))
    try:
        from breath_profiles import get_breath_profile
        meta["breath_profile"] = get_breath_profile(meta.get("symbol") or sym, "binance")
    except Exception:
        meta["breath_profile"] = None
    return meta


def resolve_deepcoin_symbol(raw, default="ETH-USDT-SWAP"):
    key = _clean_ticker(raw)
    sym = _DEEPCOIN_ALIASES.get(key)
    if not sym and key.endswith("-USDT-SWAP") and key in DEEPCOIN_SYMBOL_META:
        sym = key
    if not sym:
        # map binance-style
        b = resolve_binance_symbol(key, default="")
        if b.get("symbol") == "ETHUSDT":
            sym = "ETH-USDT-SWAP"
        elif b.get("symbol") == "XAUUSDT":
            sym = "XAU-USDT-SWAP"
        elif b.get("symbol") == "BNBUSDT":
            sym = "BNB-USDT-SWAP"
        else:
            sym = default
    meta = dict(DEEPCOIN_SYMBOL_META.get(sym, DEEPCOIN_SYMBOL_META["ETH-USDT-SWAP"]))
    try:
        from breath_profiles import get_breath_profile
        meta["breath_profile"] = get_breath_profile(meta.get("symbol") or sym, "deepcoin")
    except Exception:
        meta["breath_profile"] = None
    return meta


def active_binance_symbols():
    # binance parity (71372fe): env var missing must not silently drop
    # symbols with no rejection log -- default to the full active set.
    raw = os.getenv("BINANCE_SYMBOLS", "ETHUSDT,XAUUSDT,BNBUSDT")
    out = []
    for part in str(raw).split(","):
        meta = resolve_binance_symbol(part.strip(), default="")
        sym = meta.get("symbol")
        if sym and sym not in out and sym in BINANCE_SYMBOL_META:
            out.append(sym)
    return out or ["ETHUSDT", "XAUUSDT", "BNBUSDT"]


def active_deepcoin_symbols():
    # binance parity (71372fe): same fallback-default gap, deepcoin side.
    raw = os.getenv("DEEPCOIN_SYMBOLS", "ETH-USDT-SWAP,XAU-USDT-SWAP,BNB-USDT-SWAP")
    out = []
    for part in str(raw).split(","):
        meta = resolve_deepcoin_symbol(part.strip(), default="")
        sym = meta.get("symbol")
        if sym and sym not in out and sym in DEEPCOIN_SYMBOL_META:
            out.append(sym)
    return out or ["ETH-USDT-SWAP", "XAU-USDT-SWAP", "BNB-USDT-SWAP"]


def extract_symbol_from_payload(data):
    """从 TV / webhook 载荷提取 ticker（字段优先，全文扫描兜底）。"""
    if not isinstance(data, dict):
        return ""
    for key in (
        "symbol", "ticker", "Ticker", "sym", "pair", "market",
        "instrument", "instId", "inst_id",
    ):
        val = data.get(key)
        if val:
            return str(val).strip()
    # 兜底：扫描 JSON 文本中的已知合约（仅 ETH）
    try:
        import json
        blob = json.dumps(data, ensure_ascii=False).upper()
    except Exception:
        blob = str(data).upper()
    for token in (
        "ETHUSDT.P", "BINANCE:ETHUSDT", "ETHUSDT", "ETH-USDT-SWAP",
        "BNBUSDT.P", "BINANCE:BNBUSDT", "BNBUSDT", "BNB-USDT-SWAP",
    ):
        if token in blob:
            return token
    return ""
