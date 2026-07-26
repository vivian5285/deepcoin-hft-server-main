#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
币安原生 1h ATR(14) 拉取：用于呼吸系数（current_1h / initial_atr）。

- GET /fapi/v1/klines interval=1h（无需合成）
- 默认每 5 分钟刷新一次
- 与 breath_stop.get_breathing_coefficient 配合做 3 次采样平滑
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Callable, Dict, List, Optional, Tuple

from market_engine import atr_series, wilder_atr

logger = logging.getLogger(__name__)

ATR_PERIOD = 14
FETCH_LIMIT = 100
REFRESH_MIN_SEC = 300.0  # 5 分钟


class Atr1hEngine:
    def __init__(self, symbol: str, kline_fetcher: Callable):
        self.symbol = str(symbol or "").upper()
        self._fetcher = kline_fetcher
        self.atr = 0.0
        self.last_refresh_ts = 0.0
        self.last_error = ""
        self.ratio_history: List[float] = []  # 供呼吸系数平滑
        self._lock = threading.Lock()

    def snapshot(self) -> dict:
        return {
            "symbol": self.symbol,
            "atr_1h": float(self.atr or 0),
            "interval": "1h",
            "last_refresh_ts": float(self.last_refresh_ts or 0),
            "ratio_history": list(self.ratio_history),
            "last_error": str(self.last_error or ""),
        }

    def refresh(self, force: bool = False) -> float:
        now = time.time()
        with self._lock:
            if (
                not force
                and self.atr > 0
                and (now - float(self.last_refresh_ts or 0)) < REFRESH_MIN_SEC
            ):
                return float(self.atr)
            try:
                kl = self._fetcher(self.symbol, "1h", FETCH_LIMIT) or []
                # 统一为 [t,o,h,l,c,v]
                bars = []
                for r in kl:
                    try:
                        bars.append([
                            int(r[0]), float(r[1]), float(r[2]),
                            float(r[3]), float(r[4]), float(r[5] if len(r) > 5 else 0),
                        ])
                    except (TypeError, ValueError, IndexError):
                        continue
                atr = float(wilder_atr(bars, ATR_PERIOD) or 0)
                if atr <= 0:
                    series = atr_series(bars, ATR_PERIOD)
                    atr = float(series[-1]) if series else 0.0
                if atr > 0:
                    self.atr = atr
                    self.last_error = ""
                else:
                    self.last_error = "atr_1h_zero"
                self.last_refresh_ts = now
                return float(self.atr or 0)
            except Exception as e:
                self.last_error = str(e)
                logger.warning(f"[atr_1h] {self.symbol} refresh failed: {e}")
                return float(self.atr or 0)

    def breathing_coefficient(self, initial_atr: float, force_refresh: bool = False,
                              profile=None) -> Tuple[float, dict]:
        from breath_stop import get_breathing_coefficient
        atr_1h = self.refresh(force=force_refresh)
        coeff, smooth, hist = get_breathing_coefficient(
            atr_1h, initial_atr, self.ratio_history, profile=profile,
        )
        self.ratio_history = hist
        meta = {
            "atr_1h": float(atr_1h or 0),
            "initial_atr": float(initial_atr or 0),
            "smooth_ratio": float(smooth or 0),
            "breathing_coefficient": float(coeff or 1.0),
            "ratio_history": list(hist),
            "profile": (profile or {}).get("name") if isinstance(profile, dict) else None,
        }
        return float(coeff or 1.0), meta

    def reset_ratio_history(self):
        self.ratio_history = []


_ENGINES: Dict[str, Atr1hEngine] = {}
_REG_LOCK = threading.Lock()


def get_atr_1h_engine(symbol: str, kline_fetcher: Optional[Callable] = None) -> Atr1hEngine:
    sym = str(symbol or "").upper()
    with _REG_LOCK:
        eng = _ENGINES.get(sym)
        if eng is None:
            if kline_fetcher is None:
                from binance_client import binance_client
                kline_fetcher = binance_client.fetch_klines
            eng = Atr1hEngine(sym, kline_fetcher)
            _ENGINES[sym] = eng
        return eng
