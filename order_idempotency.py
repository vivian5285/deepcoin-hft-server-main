#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
挂单幂等 + 数量硬上限（规格 v1.0 对齐币安 v15.9.1）。

- 每笔防御限价必须带 newClientOrderId（订单标签）
- 本地未完成标签存在时禁止再挂（即使交易所查单为空）
- 单品种未成交挂单总数硬上限 MAX_OPEN_ORDERS_HARD_CAP=5
"""
from __future__ import annotations

import hashlib
import time
from typing import Optional

# 规格第八/九部分：未成交挂单硬上限（含 LIMIT + STOP）
MAX_OPEN_ORDERS_HARD_CAP = 5
MAX_OPEN_LIMIT_HARD_CAP = 5


def make_defense_client_order_id(
    symbol: str,
    kind: str,
    price: float = 0.0,
    ts: Optional[float] = None,
) -> str:
    """
    防御单 newClientOrderId（≤36）。
    kind 例：TP1 / TP2 / RE / HARD / RADAR（TP3 限价已废除）
    """
    sym_u = str(symbol or "").upper()
    sym = "E" if "ETH" in sym_u else ("X" if "XAU" in sym_u else "S")
    k = "".join(ch for ch in str(kind or "X").upper() if ch.isalnum())[:6] or "X"
    t = abs(int(float(ts if ts is not None else time.time()))) % 1_000_000
    px = abs(int(round(float(price or 0) * 100))) % 1_000_000
    raw = f"{sym_u}|{k}|{px}|{t}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:8]
    return f"D{sym}{k}{digest}{t % 10000}"[:36]


def blank_ownership_state() -> dict:
    return {
        "exit_ownership": "NONE",
        "ownership_locked_at": 0.0,
        "pending_order_tags": {},
    }
