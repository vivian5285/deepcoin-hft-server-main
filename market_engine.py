#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VPS 行情引擎：30m×3 合成 90m → ATR(14) / ADX(14)

币安无原生 90m；拉 30m K 线后按 **UTC epoch 90m 边界** 合并，
再算 Wilder ATR/ADX（与 TV RMA 对齐）。

合成锚点（与 TradingView 90 分钟图一致）：
  PERIOD_90M_MS = 90 * 60 * 1000
  bucket_open = open_time - (open_time % PERIOD_90M_MS)
仅当某 bucket 凑齐 3 根完整 30m（bucket / +30m / +60m）才产出一根已闭合 90m。
禁止「从进程启动时刻随意起算」的滑动三元组。

止损决策只认本模块数值；webhook 不传 ATR/ADX。
"""
from __future__ import annotations

import logging
import statistics
import threading
time_mod = __import__("time")
from typing import List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

KLINE_INTERVAL = "30m"  # 拉取周期；合成后等效 90m
SYNTH_INTERVAL = "90m"
PERIOD_30M_MS = 30 * 60 * 1000
PERIOD_90M_MS = 90 * 60 * 1000  # UTC epoch 对齐锚
ATR_PERIOD = 14
ADX_PERIOD = 14
FETCH_LIMIT = 220
REFRESH_MIN_SEC = 60.0
ATR_COMPARE_ALERT_PCT = 0.20
# TV 策略硬止损常见约 1.0×ATR（与 VPS initialStop=1.5×ATR 不同）。
# 用 stop_loss 反推「TV ATR」时必须除以该倍数；若误用 1.5，会系统性报出 ~33% 假偏差。
TV_HARD_SL_ATR_MULT = 1.0
# 开仓 ATR 合理性：低于近 N 根 ATR 中位数的该比例 → 异常（可触发应急降级）
ATR_MEDIAN_LOOKBACK = 50
ATR_ANOMALY_RATIO = 0.30
# 应急降级：VPS vs TV隐含 连续超阈值的开仓信号次数
ATR_DEGRADE_DIV_PCT = 0.20
ATR_DEGRADE_STREAK_N = 3


def _f(x, default=0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return float(default)


def bucket_90m_open_ms(open_time_ms: int) -> int:
    """将任意时间戳对齐到 UTC epoch 90m 桶开盘时间。"""
    t = int(open_time_ms or 0)
    if t <= 0:
        return 0
    return t - (t % PERIOD_90M_MS)


def merge_30m_to_90m(klines_30m: Sequence) -> List[list]:
    """
    按 UTC epoch 90m 边界合成：
      仅输出 bucket 内恰好具备 3 根完整 30m
      (t0, t0+30m, t0+60m) 的已闭合 90m K。
    返回 [open_time, o, h, l, c, volume]。
    """
    rows = []
    for r in (klines_30m or []):
        try:
            t = int(r[0])
        except (TypeError, ValueError, IndexError):
            continue
        if t <= 0:
            continue
        rows.append(r)
    if len(rows) < 3:
        return []

    rows.sort(key=lambda r: int(r[0]))
    by_t = {}
    for r in rows:
        by_t[int(r[0])] = r

    # 从数据中出现过的 90m 桶起算（已按 epoch 对齐，非进程启动偏移）
    buckets = sorted({bucket_90m_open_ms(int(r[0])) for r in rows})
    out = []
    for bucket in buckets:
        if bucket <= 0:
            continue
        expected = (
            bucket,
            bucket + PERIOD_30M_MS,
            bucket + 2 * PERIOD_30M_MS,
        )
        if not all(t in by_t for t in expected):
            continue
        a, b, c = by_t[expected[0]], by_t[expected[1]], by_t[expected[2]]
        out.append([
            bucket,
            a[1],
            max(_f(a[2]), _f(b[2]), _f(c[2])),
            min(_f(a[3]), _f(b[3]), _f(c[3])),
            c[4],
            _f(a[5]) + _f(b[5]) + _f(c[5]),
        ])
    return out


def atr_series(bars: Sequence, period: int = ATR_PERIOD) -> List[float]:
    """逐根闭合后的 Wilder ATR 序列（与最终 atr 同算法，便于中位数）。"""
    if not bars or len(bars) < period + 1:
        return []
    trs = _true_ranges(bars)
    if len(trs) < period:
        return []
    atr = sum(trs[:period]) / period
    series = [float(atr)]
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
        series.append(float(atr))
    return series


def _true_ranges(bars: Sequence) -> List[float]:
    trs = []
    for i in range(1, len(bars)):
        h = _f(bars[i][2])
        l = _f(bars[i][3])
        pc = _f(bars[i - 1][4])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return trs


def wilder_atr(bars: Sequence, period: int = ATR_PERIOD) -> float:
    series = atr_series(bars, period)
    return float(series[-1]) if series else 0.0


def wilder_adx(bars: Sequence, period: int = ADX_PERIOD) -> float:
    n = len(bars or [])
    if n < period * 2 + 2:
        return 0.0

    plus_dm, minus_dm, trs = [], [], []
    for i in range(1, n):
        h = _f(bars[i][2])
        l = _f(bars[i][3])
        ph = _f(bars[i - 1][2])
        pl = _f(bars[i - 1][3])
        pc = _f(bars[i - 1][4])
        up = h - ph
        down = pl - l
        plus_dm.append(up if up > down and up > 0 else 0.0)
        minus_dm.append(down if down > up and down > 0 else 0.0)
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))

    if len(trs) < period:
        return 0.0

    sm_tr = sum(trs[:period])
    sm_plus = sum(plus_dm[:period])
    sm_minus = sum(minus_dm[:period])

    def _di(sp, sm, st):
        if st <= 0:
            return 0.0, 0.0
        return 100.0 * sp / st, 100.0 * sm / st

    dx_list = []
    pdi, mdi = _di(sm_plus, sm_minus, sm_tr)
    denom = pdi + mdi
    dx_list.append(100.0 * abs(pdi - mdi) / denom if denom > 0 else 0.0)

    for i in range(period, len(trs)):
        sm_tr = sm_tr - sm_tr / period + trs[i]
        sm_plus = sm_plus - sm_plus / period + plus_dm[i]
        sm_minus = sm_minus - sm_minus / period + minus_dm[i]
        pdi, mdi = _di(sm_plus, sm_minus, sm_tr)
        denom = pdi + mdi
        dx_list.append(100.0 * abs(pdi - mdi) / denom if denom > 0 else 0.0)

    if len(dx_list) < period:
        return 0.0
    adx = sum(dx_list[:period]) / period
    for dx in dx_list[period:]:
        adx = (adx * (period - 1) + dx) / period
    return float(adx)


def implied_atr_from_stop(
    entry: float, stop_loss: float, mult: float = TV_HARD_SL_ATR_MULT
) -> float:
    """由 |entry−stop| / mult 反推 ATR。默认 mult=TV 硬止损倍数(1.0)，非 VPS 1.5。"""
    entry = _f(entry)
    sl = _f(stop_loss)
    mult = _f(mult, TV_HARD_SL_ATR_MULT) or TV_HARD_SL_ATR_MULT
    if entry <= 0 or sl <= 0 or mult <= 0:
        return 0.0
    return abs(entry - sl) / mult


def atr_divergence_pct(vps_atr: float, tv_implied_atr: float) -> float:
    v = _f(vps_atr)
    t = _f(tv_implied_atr)
    if v <= 0 or t <= 0:
        return 0.0
    return abs(v - t) / v


def resolve_tv_atr_for_compare(
    vps_atr: float,
    tv_atr: float = 0.0,
    entry: float = 0.0,
    stop_loss: float = 0.0,
    tv_sl_mult: float = TV_HARD_SL_ATR_MULT,
) -> Tuple[float, str]:
    """
    调试比对用 TV 侧 ATR：优先 webhook 显式 atr；否则按 TV 硬止损倍数反推。
    返回 (tv_ref_atr, source_label)。无效时 atr=0。
    """
    direct = _f(tv_atr)
    if direct > 0:
        return direct, "TV.atr"
    implied = implied_atr_from_stop(entry, stop_loss, tv_sl_mult)
    if implied > 0:
        return implied, f"stop÷{float(tv_sl_mult):.2g}"
    return 0.0, ""


def tv_implied_atr_for_degrade(
    entry: float, stop_loss: float, mult: float = TV_HARD_SL_ATR_MULT
) -> float:
    """
    应急降级用 TV 隐含 ATR：
      |price − stop_loss| / atrMultiplierSL（当前 1.0）
    """
    return implied_atr_from_stop(entry, stop_loss, mult)


def evaluate_atr_emergency_degrade(
    vps_atr: float,
    atr_history: Sequence[float],
    entry: float,
    stop_loss: float,
    div_streak: int = 0,
    klines_ok: bool = True,
    lookback: int = ATR_MEDIAN_LOOKBACK,
    anomaly_ratio: float = ATR_ANOMALY_RATIO,
    div_pct: float = ATR_DEGRADE_DIV_PCT,
    streak_n: int = ATR_DEGRADE_STREAK_N,
) -> Tuple[bool, dict]:
    """
    判断是否触发 ATR 应急降级（临时用 TV 隐含 ATR，非静默容错）。
    返回 (should_degrade, meta)。
    """
    vps = _f(vps_atr)
    tv_imp = tv_implied_atr_for_degrade(entry, stop_loss)
    meta = {
        "vps_atr": round(vps, 6),
        "tv_implied_atr": round(tv_imp, 6),
        "div_pct": 0.0,
        "div_streak": int(div_streak or 0),
        "div_streak_next": int(div_streak or 0),
        "reason": "",
        "klines_ok": bool(klines_ok),
        "tv_sl_mult": float(TV_HARD_SL_ATR_MULT),
    }
    if not klines_ok or vps <= 0:
        if tv_imp <= 0:
            meta["reason"] = "vps_atr_unavailable_and_no_tv_implied"
            return False, meta
        meta["reason"] = "vps_atr_unavailable"
        return True, meta

    is_anom, anom = check_atr_anomaly(vps, atr_history, lookback, anomaly_ratio)
    meta["anomaly"] = anom
    if is_anom and anom.get("reason") in ("atr_zero_or_missing", "atr_below_median_ratio"):
        if tv_imp <= 0:
            meta["reason"] = f"{anom.get('reason')}_no_tv_implied"
            return False, meta
        meta["reason"] = str(anom.get("reason") or "atr_anomaly")
        return True, meta

    # 连续偏差：仅当双边 ATR 都有效时累计
    if tv_imp > 0 and vps > 0:
        div = atr_divergence_pct(vps, tv_imp)
        meta["div_pct"] = round(div, 6)
        if div >= float(div_pct):
            nxt = int(div_streak or 0) + 1
            meta["div_streak_next"] = nxt
            if nxt >= int(streak_n):
                meta["reason"] = f"atr_div_streak_{nxt}"
                return True, meta
            meta["reason"] = f"atr_div_streak_pending_{nxt}"
            return False, meta
        meta["div_streak_next"] = 0
        meta["reason"] = "ok"
        return False, meta

    meta["reason"] = "ok"
    meta["div_streak_next"] = 0
    return False, meta


def check_atr_anomaly(
    atr: float,
    atr_history: Sequence[float],
    lookback: int = ATR_MEDIAN_LOOKBACK,
    ratio: float = ATR_ANOMALY_RATIO,
) -> Tuple[bool, dict]:
    """
    返回 (is_anomaly, meta)。
    atr<=0 或空值 → 无条件异常（高优先级）。
    atr < median(近 lookback 根) * ratio → 异常（拒绝本次开仓）。
    """
    atr = _f(atr)
    meta = {
        "atr": atr,
        "lookback": int(lookback),
        "ratio": float(ratio),
        "median": 0.0,
        "threshold": 0.0,
        "reason": "",
    }
    if atr <= 0:
        meta["reason"] = "atr_zero_or_missing"
        return True, meta
    hist = [float(x) for x in (atr_history or []) if float(x or 0) > 0]
    if len(hist) < max(5, int(lookback * 0.2)):
        # 历史不足时仅拦截 atr<=0；有少量样本仍用中位数
        if len(hist) < 3:
            meta["reason"] = "history_insufficient_skip"
            return False, meta
    window = hist[-int(lookback):] if lookback > 0 else hist
    try:
        med = float(statistics.median(window))
    except statistics.StatisticsError:
        meta["reason"] = "median_unavailable"
        return False, meta
    meta["median"] = round(med, 6)
    thr = med * float(ratio)
    meta["threshold"] = round(thr, 6)
    if med > 0 and atr < thr:
        meta["reason"] = "atr_below_median_ratio"
        return True, meta
    meta["reason"] = "ok"
    return False, meta


class MarketEngine:
    def __init__(self, symbol: str, fetch_klines=None):
        self.symbol = str(symbol or "").upper()
        self._fetch_klines = fetch_klines
        self._lock = threading.RLock()
        self.atr = 0.0
        self.adx = 0.0
        self.last_bar_open_ms = 0
        self.last_refresh_ts = 0.0
        self.last_error = ""
        self.bars_count = 0
        self.atr_history: List[float] = []
        self.last_bars_90m: List[list] = []

    def bind_fetcher(self, fetch_klines):
        self._fetch_klines = fetch_klines

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "symbol": self.symbol,
                "atr": float(self.atr),
                "adx": float(self.adx),
                "interval": SYNTH_INTERVAL,
                "source_interval": KLINE_INTERVAL,
                "align": "utc_epoch_90m",
                "period_90m_ms": PERIOD_90M_MS,
                "last_bar_open_ms": int(self.last_bar_open_ms),
                "bars": int(self.bars_count),
                "atr_history_n": len(self.atr_history),
                "updated_at": float(self.last_refresh_ts),
                "error": self.last_error,
            }

    def get_atr_median(self, lookback: int = ATR_MEDIAN_LOOKBACK) -> float:
        with self._lock:
            hist = [x for x in self.atr_history if x > 0]
        if not hist:
            return 0.0
        window = hist[-int(lookback):] if lookback > 0 else hist
        try:
            return float(statistics.median(window))
        except statistics.StatisticsError:
            return 0.0

    def check_open_atr(self, atr: Optional[float] = None) -> Tuple[bool, dict]:
        """开仓前调用：True=异常应拒绝。"""
        with self._lock:
            cur = float(atr if atr is not None else self.atr)
            hist = list(self.atr_history)
        return check_atr_anomaly(cur, hist)

    def refresh(self, force: bool = False) -> Tuple[float, float]:
        now = time_mod.time()
        with self._lock:
            if (
                not force
                and self.last_refresh_ts > 0
                and (now - self.last_refresh_ts) < REFRESH_MIN_SEC
                and self.atr > 0
            ):
                return float(self.atr), float(self.adx)

        if not callable(self._fetch_klines):
            self.last_error = "no_fetcher"
            return float(self.atr), float(self.adx)

        try:
            raw = self._fetch_klines(self.symbol, KLINE_INTERVAL, FETCH_LIMIT)
        except Exception as e:
            self.last_error = str(e)
            logger.warning(f"[行情引擎] {self.symbol} 拉K失败: {e}")
            return float(self.atr), float(self.adx)

        bars = merge_30m_to_90m(raw or [])
        series = atr_series(bars, ATR_PERIOD)
        atr = float(series[-1]) if series else 0.0
        adx = wilder_adx(bars, ADX_PERIOD)
        bar_open = int(bars[-1][0]) if bars else 0

        with self._lock:
            if atr > 0:
                self.atr = atr
            if adx > 0:
                self.adx = adx
            if bar_open > 0:
                self.last_bar_open_ms = bar_open
            self.bars_count = len(bars)
            self.last_bars_90m = list(bars)
            if series:
                self.atr_history = [float(x) for x in series if float(x) > 0]
            self.last_refresh_ts = now
            self.last_error = "" if atr > 0 else "atr_zero"
            logger.info(
                f"[行情引擎] {self.symbol} 90m={len(bars)}根(UTC epoch对齐←30m) | "
                f"last_open={bar_open} | "
                f"ATR({ATR_PERIOD})={self.atr:.4f} ADX({ADX_PERIOD})={self.adx:.2f} | "
                f"ATR中位(近{min(len(self.atr_history), ATR_MEDIAN_LOOKBACK)})="
                f"{self.get_atr_median():.4f}"
            )
            return float(self.atr), float(self.adx)


_ENGINES = {}
_ENGINES_LOCK = threading.Lock()


def get_market_engine(symbol: str, fetch_klines=None) -> MarketEngine:
    sym = str(symbol or "").upper()
    with _ENGINES_LOCK:
        eng = _ENGINES.get(sym)
        if eng is None:
            eng = MarketEngine(sym, fetch_klines=fetch_klines)
            _ENGINES[sym] = eng
        elif fetch_klines is not None:
            eng.bind_fetcher(fetch_klines)
        return eng
