#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import hmac
import hashlib
import base64
import json
import logging
import requests
import time
import threading
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from urllib.parse import urlencode
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))
logger = logging.getLogger(__name__)

WS_PUBLIC_SWAP = "wss://stream.deepcoin.com/streamlet/trade/public/swap?platform=api&version=v2"
WS_PRIVATE = "wss://stream.deepcoin.com/v1/private"

CLIENT_VERSION = "v16.10.1-query-fix"
# 公开 instruments 接口失败时的硬编码兜底
SYMBOL_TICK_FALLBACK = {
    "ETH-USDT-SWAP": "0.01",
    "XAU-USDT-SWAP": "0.01",
    "BTC-USDT-SWAP": "0.1",
}


class DeepcoinClient:
    def __init__(self):
        self.api_key = os.getenv("DEEPCOIN_API_KEY", "")
        self.secret_key = os.getenv("DEEPCOIN_API_SECRET", "")
        self.passphrase = os.getenv("DEEPCOIN_PASSPHRASE", "")
        self.base_url = "https://api.deepcoin.com"
        self._price_cache = {}
        self._price_cache_ts = {}
        self._price_lock = threading.Lock()
        self._pub_price_ws_running = False
        self._pub_price_ws_symbol = None
        self._rest_price_min_interval = 30
        self._last_rest_price_fetch = 0.0
        self._listen_key = None
        self._listen_key_expire = 0
        self._ws_thread = None
        self._ws_running = False
        self._ws_callbacks = {}
        self._instrument_cache = {}
        self._rest_lock = threading.Lock()
        self._rest_last_ts = 0.0
        # v16.10+：REST 硬间隔缩短到 0.3s（配合预算放宽）
        # Deepcoin 限流比 Binance 宽松很多，0.3s 足以避免触发限制
        try:
            self._rest_min_gap = float(os.getenv("DEEPCOIN_REST_MIN_GAP_SEC", "0.3"))
        except Exception:
            self._rest_min_gap = 1.5

    def _pace_rest(self):
        """进程内硬间隔，防止并发打穿 Deepcoin 限流。"""
        gap = float(getattr(self, "_rest_min_gap", 1.5) or 1.5)
        with getattr(self, "_rest_lock", threading.Lock()):
            now = time.time()
            wait = max(0.0, gap - (now - float(self._rest_last_ts or 0)))
            if wait > 0:
                time.sleep(wait)
            self._rest_last_ts = time.time()

    # ── 签名与请求 ──────────────────────────────────────────────

    def _get_timestamp(self):
        """官方要求 UTC ISO8601，如 2020-12-08T09:08:57.715Z（与 VPS 系统时区无关）"""
        now = datetime.now(timezone.utc)
        ms = int(now.microsecond / 1000)
        return now.strftime("%Y-%m-%dT%H:%M:%S") + f".{ms:03d}Z"

    def _build_query_string(self, params: dict) -> str:
        if not params:
            return ""
        return urlencode(params)

    def _build_request_path(self, endpoint: str, params: dict = None, method: str = "GET") -> str:
        if method.upper() == "GET" and params:
            qs = self._build_query_string(params)
            return f"{endpoint}?{qs}" if qs else endpoint
        return endpoint

    def _sign(self, timestamp: str, method: str, request_path: str, body: str = ""):
        message = (str(timestamp) + str(method.upper()) + str(request_path) + str(body)).encode('utf-8')
        h = hmac.new(self.secret_key.encode('utf-8'), message, hashlib.sha256)
        return base64.b64encode(h.digest()).decode('utf-8')

    def _normalize_endpoint(self, endpoint: str) -> str:
        if not endpoint.startswith("/deepcoin/"):
            endpoint = "/deepcoin" + (endpoint if endpoint.startswith("/") else "/" + endpoint)
        return endpoint

    def _request(self, method: str, endpoint: str, params: dict = None, _retry: int = 0,
                 _throttle_kind: str = "rest", _throttle_force: bool = False):
        if not self.api_key or not self.secret_key:
            logger.error("Deepcoin API Key/Secret 未配置，请检查 .env")
            return None
        # 账号级节流阀（交易操作用 force=True 绕过预算限制，但仍受静默期约束）
        try:
            from api_throttle import get_throttle
            ok, detail = get_throttle("deepcoin").acquire(
                _throttle_kind, symbol="", force=_throttle_force,
            )
            if not ok:
                logger.warning(f"🧊 [Deepcoin节流阀] 拒绝 REST ({detail})")
                return None
        except Exception as e:
            logger.debug(f"deepcoin throttle skip: {e}")
        self._pace_rest()
        endpoint = self._normalize_endpoint(endpoint)
        timestamp = self._get_timestamp()
        body_str = json.dumps(params, separators=(',', ':')) if params and method.upper() != "GET" else ""
        request_path = self._build_request_path(endpoint, params, method)
        signature = self._sign(timestamp, method, request_path, body_str)
        headers = {
            "Content-Type": "application/json",
            "DC-ACCESS-KEY": self.api_key,
            "DC-ACCESS-SIGN": signature,
            "DC-ACCESS-TIMESTAMP": timestamp,
            "DC-ACCESS-PASSPHRASE": self.passphrase,
        }
        try:
            resp = requests.request(
                method.upper(), f"{self.base_url}{request_path}",
                data=body_str if body_str else None, headers=headers, timeout=15,
            )
            data = resp.json()
            if isinstance(data, dict) and str(data.get("code", "")) != "0":
                msg = str(data.get("msg", ""))
                logger.error(f"Deepcoin API 错误 {method} {request_path} | code={data.get('code')} msg={msg}")
                low = msg.lower()
                # rate_limit 触发时静默 60s，避免长时间无法查询盘口导致开仓失败
                if any(k in low for k in ("rate", "too many", "429", "limit", "频繁")):
                    try:
                        from api_throttle import get_throttle
                        get_throttle("deepcoin").enter_silence(
                            seconds=get_throttle("deepcoin").rate_limit_silence_sec,
                            reason="deepcoin_rate_limit"
                        )
                    except Exception:
                        pass
                # 签名/时间戳类错误自动重试一次
                if _retry == 0 and any(k in msg.lower() for k in ("timestamp", "sign", "time", "expired")):
                    time.sleep(0.3)
                    return self._request(method, endpoint, params, _retry=1)
            return data
        except Exception as e:
            logger.error(f"Deepcoin 请求失败 {endpoint}: {e}")
            return None

    def _public_request(self, endpoint: str, params: dict = None):
        try:
            from api_throttle import get_throttle
            ok, detail = get_throttle("deepcoin").acquire("rest_public", symbol="")
            if not ok:
                logger.warning(f"🧊 [Deepcoin节流阀] 拒绝公开 REST ({detail})")
                return None
        except Exception as e:
            logger.debug(f"deepcoin public throttle skip: {e}")
        self._pace_rest()
        endpoint = self._normalize_endpoint(endpoint)
        qs = f"?{'&'.join(f'{k}={v}' for k, v in params.items())}" if params else ""
        try:
            resp = requests.get(f"{self.base_url}{endpoint}{qs}", timeout=10)
            data = resp.json()
            if resp.status_code == 429 or (
                isinstance(data, dict)
                and any(
                    k in str(data.get("msg", "")).lower()
                    for k in ("rate", "too many", "limit", "频繁")
                )
            ):
                try:
                    from api_throttle import get_throttle
                    get_throttle("deepcoin").enter_silence(
                        seconds=get_throttle("deepcoin").rate_limit_silence_sec,
                        reason="deepcoin_public_rate_limit"
                    )
                except Exception:
                    pass
            return data
        except Exception as e:
            logger.error(f"Deepcoin 公开接口失败 {endpoint}: {e}")
            return None

    @staticmethod
    def _is_success(res) -> bool:
        if not isinstance(res, dict):
            return False
        if str(res.get("code", "")) != "0":
            return False
        data = res.get("data")
        if isinstance(data, dict) and str(data.get("sCode", "0")) not in ("0", ""):
            return False
        return True

    @staticmethod
    def inst_id_to_instrument_id(inst_id: str) -> str:
        """BTC-USDT-SWAP -> BTCUSDT"""
        return inst_id.replace("-SWAP", "").replace("-", "")

    @staticmethod
    def swap_product_group(inst_id: str) -> str:
        """U本位 SwapU，币本位 Swap"""
        parts = inst_id.replace("-SWAP", "").split("-")
        return "SwapU" if len(parts) >= 2 and parts[-1] == "USDT" else "Swap"

    # ── 账户与行情 ──────────────────────────────────────────────

    def get_account_summary(self, ccy="USDT"):
        """合约账户概览：用于本金锚点，禁止用 depleted availBal 算档位额度"""
        out = {
            "cash_bal": 0.0,
            "eq": 0.0,
            "avail_bal": 0.0,
            "frozen_bal": 0.0,
        }
        res = self._request("GET", "/account/balances", {"instType": "SWAP"}, _throttle_kind="rest_query")
        if isinstance(res, dict) and "data" in res:
            for item in res["data"]:
                if item.get("ccy") != ccy:
                    continue
                out["cash_bal"] = float(item.get("cashBal", 0) or 0)
                out["eq"] = float(item.get("eq", 0) or 0)
                out["avail_bal"] = float(item.get("availBal", 0) or 0)
                out["frozen_bal"] = float(item.get("frozenBal", 0) or 0)
                break
        return out

    def get_principal_wallet_balance(self, ccy="USDT"):
        """
        USDT 合约本金余额（cashBal）— 唯一合法的档位额度基数。
        禁止用 availBal / eq(含浮盈) / 剩余保证金参与开仓与超标核查。
        """
        summary = self.get_account_summary(ccy)
        cash = float(summary.get("cash_bal", 0) or 0)
        if cash > 0:
            return cash
        return 0.0

    def get_cap_equity_balance(self, ccy="USDT"):
        """档位额度基数 = 本金 cashBal（兼容旧名）"""
        return self.get_principal_wallet_balance(ccy)

    def get_sizing_balance(self, ccy="USDT"):
        """本金口径（cashBal），用于 regime 仓位预算"""
        return self.get_principal_wallet_balance(ccy)

    def get_available_balance(self, ccy="USDT"):
        """仅诊断用，禁止用于开仓/档位额度计算"""
        summary = self.get_account_summary(ccy)
        eq = float(summary.get("eq", 0) or 0)
        if eq > 0:
            return eq
        return float(summary.get("avail_bal", 0) or 0)

    def inst_id_to_ws_symbol(self, symbol="ETH-USDT-SWAP"):
        """ETH-USDT-SWAP → ETHUSDT（深币 WS v2 合约格式）"""
        return symbol.replace("-SWAP", "").replace("-", "")

    @staticmethod
    def _extract_last_price(payload):
        if isinstance(payload, dict):
            for key in ("last", "LastPrice", "lastPx", "LastPx", "close", "price", "p", "Last"):
                val = payload.get(key)
                if val is not None and str(val).strip() not in ("", "0"):
                    try:
                        px = float(val)
                        if px > 0:
                            return px
                    except (TypeError, ValueError):
                        pass
            for val in payload.values():
                px = DeepcoinClient._extract_last_price(val)
                if px:
                    return px
        elif isinstance(payload, list):
            for item in payload:
                px = DeepcoinClient._extract_last_price(item)
                if px:
                    return px
        return None

    def _set_ws_price(self, symbol, price):
        with self._price_lock:
            self._price_cache[symbol] = price
            self._price_cache_ts[symbol] = time.time()

    def _get_ws_price(self, symbol, max_age=30.0):
        with self._price_lock:
            px = self._price_cache.get(symbol)
            ts = self._price_cache_ts.get(symbol, 0.0)
        if px and (time.time() - ts) <= max_age:
            return px
        return None

    def start_public_price_ws(self, symbol="ETH-USDT-SWAP"):
        """订阅 market-latest — 雷达用 WS 推价，避免 REST 轮询限频"""
        if self._pub_price_ws_running and self._pub_price_ws_symbol == symbol:
            return
        self._pub_price_ws_symbol = symbol
        if not self._pub_price_ws_running:
            self._pub_price_ws_running = True
            threading.Thread(
                target=self._public_price_ws_loop, args=(symbol,), daemon=True,
            ).start()
            logger.info(f"📡 深币公开 WS 启动: {self.inst_id_to_ws_symbol(symbol)} market-latest")

    def _public_price_ws_loop(self, symbol):
        try:
            import websocket
        except ImportError:
            logger.warning("未安装 websocket-client，雷达将回退 REST 慢速兜底")
            self._pub_price_ws_running = False
            return

        ws_symbol = self.inst_id_to_ws_symbol(symbol)

        def on_message(ws, message):
            if message == "pong":
                return
            try:
                data = json.loads(message)
                px = self._extract_last_price(data)
                if px:
                    self._set_ws_price(symbol, px)
            except Exception as e:
                logger.debug(f"WS 行情解析: {e}")

        def on_error(ws, error):
            logger.warning(f"深币公开 WS 错误: {error}")

        def on_close(ws, code, msg):
            logger.warning(f"深币公开 WS 断开: {code} {msg}")

        def on_open(ws):
            sub = {
                "SendTopicAction": {
                    "Action": "1",
                    "Symbol": ws_symbol,
                    "Topic": "market-latest",
                    "LocalNo": 101,
                    "ResumeNo": -1,
                }
            }
            ws.send(json.dumps(sub))
            logger.info(f"深币公开 WS 已订阅 {ws_symbol} market-latest")

            def ping_loop():
                while self._pub_price_ws_running:
                    try:
                        ws.send("ping")
                    except Exception:
                        break
                    time.sleep(15)

            threading.Thread(target=ping_loop, daemon=True).start()

        while self._pub_price_ws_running:
            try:
                ws = websocket.WebSocketApp(
                    WS_PUBLIC_SWAP, on_open=on_open, on_message=on_message,
                    on_error=on_error, on_close=on_close,
                )
                ws.run_forever(ping_interval=0)
            except Exception as e:
                logger.error(f"深币公开 WS 异常: {e}")
            if self._pub_price_ws_running:
                time.sleep(3)

    def get_current_price(self, symbol="ETH-USDT-SWAP", prefer_ws=True):
        """优先 WS 缓存；REST tickers 仅兜底且限频"""
        if prefer_ws:
            ws_px = self._get_ws_price(symbol)
            if ws_px:
                return ws_px
        now = time.time()
        min_gap = self._rest_price_min_interval if self._pub_price_ws_running else 2
        cached = self._get_ws_price(symbol, max_age=min_gap)
        if cached:
            return cached
        if now - self._last_rest_price_fetch < min_gap:
            stale = self._get_ws_price(symbol, max_age=120)
            return stale or 0.0
        self._last_rest_price_fetch = now
        res = self._public_request("/market/tickers", {"instType": "SWAP"})
        if res and str(res.get("code")) == "0":
            for item in res.get("data", []):
                last = float(item.get("last", 0) or 0)
                inst = item.get("instId", "")
                if last > 0:
                    self._set_ws_price(inst, last)
            if symbol in self._price_cache:
                return self._price_cache[symbol]
        stale = self._get_ws_price(symbol, max_age=120)
        return stale or 0.0

    def get_instrument_info(self, symbol="ETH-USDT-SWAP"):
        if symbol in self._instrument_cache:
            return self._instrument_cache[symbol]
        res = self._public_request("/market/instruments", {"instType": "SWAP", "instId": symbol})
        if res and str(res.get("code")) == "0" and res.get("data"):
            info = res["data"][0]
            self._instrument_cache[symbol] = info
            logger.info(
                f"[合约规格] {symbol} tickSz={info.get('tickSz')} lotSz={info.get('lotSz')} "
                f"minSz={info.get('minSz')}"
            )
            return info
        fallback_tick = SYMBOL_TICK_FALLBACK.get(symbol, "0.01")
        logger.warning(f"[合约规格] 无法拉取 {symbol} instruments，兜底 tickSz={fallback_tick}")
        return {"tickSz": fallback_tick, "instId": symbol}

    def fetch_klines(self, symbol="ETH-USDT-SWAP", interval="5m", limit=100):
        """
        获取 K 线数据（用于重入时取最近已收盘极值）。
        interval: 5m, 3m, 15m 等
        返回格式兼容 binance: [timestamp, open, high, low, close, volume]
        """
        interval_map = {
            "1m": "1m", "3m": "3m", "5m": "5m", "15m": "15m",
            "30m": "30m", "1h": "1h", "2h": "2h", "4h": "4h",
        }
        dc_interval = interval_map.get(interval, "5m")
        params = {
            "instId": symbol,
            "interval": dc_interval,
            "limit": str(min(int(limit), 100)),
        }
        res = self._public_request("/market/candles", params)
        if not res or str(res.get("code", "")) != "0":
            logger.warning(f"[{symbol}] 拉K线失败: {res}")
            return None
        data = res.get("data") or []
        if not isinstance(data, list) or len(data) == 0:
            return []
        out = []
        for row in reversed(data):
            try:
                if not isinstance(row, (list, tuple)) or len(row) < 6:
                    continue
                ts_str = str(row[0])
                try:
                    open_time_ms = int(float(ts_str))
                except (ValueError, TypeError):
                    open_time_ms = 0
                out.append([
                    open_time_ms,
                    float(row[1]),
                    float(row[2]),
                    float(row[3]),
                    float(row[4]),
                    float(row[5]),
                ])
            except (ValueError, TypeError, IndexError):
                continue
        return out

    def get_tick_size(self, symbol="ETH-USDT-SWAP") -> str:
        info = self.get_instrument_info(symbol)
        tick = str(info.get("tickSz", "") or "").strip()
        if not tick or tick == "0":
            tick = SYMBOL_TICK_FALLBACK.get(symbol, "0.01")
        return tick

    @staticmethod
    def _tick_decimal_places(tick_str: str) -> int:
        """tickSz='0.01' → 2 位小数（保留 tick 定义中的尾零）"""
        tick_str = str(tick_str).strip()
        if not tick_str or tick_str == "0":
            return 2
        if "." not in tick_str:
            return 0
        return len(tick_str.split(".", 1)[1])

    def format_price(self, px, symbol="ETH-USDT-SWAP"):
        """将价格对齐到 tickSz 整数倍；1517.4 → '1517.40'，避免 sCode=48 PriceNotOnTick"""
        tick_str = self.get_tick_size(symbol)
        try:
            tick = Decimal(tick_str)
        except InvalidOperation:
            tick = Decimal("0.01")
            tick_str = "0.01"
        if tick <= 0:
            tick = Decimal("0.01")
            tick_str = "0.01"

        try:
            price = Decimal(str(px))
        except InvalidOperation:
            price = Decimal(str(float(px)))

        units = (price / tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        snapped = units * tick

        decimals = self._tick_decimal_places(tick_str)
        if decimals <= 0:
            result = str(int(snapped))
        else:
            result = format(snapped, f".{decimals}f")

        raw = str(px).strip()
        if result != raw:
            logger.info(f"[tick对齐] {symbol} {raw} → {result} (tickSz={tick_str})")
        return result

    def _price_submit_variants(self, px, symbol="ETH-USDT-SWAP"):
        """PriceNotOnTick 时依次尝试多种合法字符串格式"""
        primary = self.format_price(px, symbol)
        seen = set()
        variants = []
        for candidate in (primary, primary.rstrip("0").rstrip(".") if "." in primary else primary,
                          f"{float(primary):.2f}", f"{float(primary):.1f}", str(int(round(float(primary))))):
            if candidate and candidate not in seen:
                seen.add(candidate)
                variants.append(candidate)
        return variants

    def get_position_info(self, symbol="ETH-USDT-SWAP"):
        """持仓查询：静默期等待，不阻塞开仓流程（v16.10.1修复）"""
        return self._request("GET", "/account/positions", {"instType": "SWAP", "instId": symbol}, _throttle_kind="rest_query")

    def get_all_swap_position_notionals(self):
        """
        账户全部 SWAP 名义敞口（|张数|×ctVal×mark）。
        用于双品种 Σnotional ≤ equity×9 硬顶。
        """
        from symbol_config import DEEPCOIN_SYMBOL_META
        out = {}
        total = 0.0
        try:
            res = self._request("GET", "/account/positions", {"instType": "SWAP"})
        except Exception as e:
            logger.error(f"[全仓名义查询失败] {e}")
            return out, 0.0
        rows = (res or {}).get("data") or []
        for p in rows:
            inst = str(p.get("instId") or "")
            try:
                pos = abs(float(p.get("pos") or 0))
            except (TypeError, ValueError):
                continue
            if pos <= 0 or not inst:
                continue
            meta = DEEPCOIN_SYMBOL_META.get(inst, {})
            fv = float(meta.get("face_value") or 0.1)
            try:
                info = self.get_instrument_info(inst)
                ct = float(info.get("ctVal") or 0)
                if ct > 0:
                    fv = ct
            except Exception:
                pass
            try:
                mark = float(p.get("markPx") or p.get("last") or 0)
            except (TypeError, ValueError):
                mark = 0.0
            if mark <= 0:
                mark = float(self.get_current_price(inst) or 0)
            notional = pos * fv * mark
            out[inst] = round(notional, 2)
            total += notional
        return out, round(total, 2)

    def set_leverage(self, symbol="ETH-USDT-SWAP", leverage=20, mgn_mode="cross", mrg_position="merge"):
        """POST /deepcoin/account/set-leverage"""
        # v16.10+：设置杠杆用 force=True（开仓前必要操作）
        res = self._request("POST", "/account/set-leverage", {
            "instId": symbol,
            "lever": str(int(leverage)),
            "mgnMode": mgn_mode,
            "mrgPosition": mrg_position,
        }, _throttle_kind="rest_trade", _throttle_force=True)
        if res and self._is_success(res):
            logger.info(f"[设置杠杆成功] {symbol} → {leverage}x")
        elif res:
            logger.warning(f"[设置杠杆失败] {symbol} → {leverage}x | {res}")
        return res

    # ── 下单 / 撤单 ──────────────────────────────────────────────

    def place_order(self, params: dict):
        """POST /deepcoin/trade/order"""
        # v16.10+：交易下单用 force=True 绕过预算限制（仍受静默期约束）
        res = self._request("POST", "/trade/order", params, _throttle_kind="rest_trade", _throttle_force=True)
        if res and not self._is_success(res):
            data = res.get("data") or {}
            logger.error(
                f"下单失败: instId={params.get('instId')} side={params.get('side')} "
                f"px={params.get('px')} sz={params.get('sz')} "
                f"sCode={data.get('sCode')} sMsg={data.get('sMsg')} msg={res.get('msg')}"
            )
        return res

    @staticmethod
    def format_contract_sz(qty):
        """API 数量可能是 int、float 或 '1.000000' 字符串"""
        if qty is None or qty == "":
            return "0"
        return str(int(float(qty)))

    def place_market_order(self, symbol, side, pos_side, qty, reduce_only=False, td_mode="cross", mrg_position="merge"):
        params = {
            "instId": symbol, "tdMode": td_mode, "side": side, "posSide": pos_side,
            "ordType": "market", "sz": self.format_contract_sz(qty), "mrgPosition": mrg_position,
        }
        if reduce_only:
            params["reduceOnly"] = True
        return self.place_order(params)

    def place_limit_order(self, symbol, side, pos_side, px, qty, reduce_only=False, td_mode="cross", mrg_position="merge", cl_ord_id=None):
        px_variants = self._price_submit_variants(px, symbol)
        last_res = None
        for px_str in px_variants:
            params = {
                "instId": symbol, "tdMode": td_mode, "side": side, "posSide": pos_side,
                "ordType": "limit", "sz": self.format_contract_sz(qty), "px": px_str, "mrgPosition": mrg_position,
            }
            if reduce_only:
                params["reduceOnly"] = True
            if cl_ord_id:
                params["clOrdId"] = str(cl_ord_id)
            logger.info(f"[限价单提交] {side} {pos_side} {qty}张 px={px_str} (原始={px})")
            for attempt in range(2):
                res = self.place_order(params)
                last_res = res
                if res and self._is_success(res):
                    ord_id = (res.get("data") or {}).get("ordId", "")
                    logger.info(f"[限价单成功] {side} {pos_side} {qty}张 @ {px_str} ordId={ord_id}")
                    return res
                data = (res or {}).get("data") or {}
                smsg = str(data.get("sMsg", ""))
                if smsg and "PriceNotOnTick" not in smsg and "tick" not in smsg.lower():
                    return res
                if attempt == 0:
                    time.sleep(0.3)
        return last_res

    def place_trigger_order(self, symbol, side, pos_side, sz, trigger_price, order_type="market",
                            td_mode="cross", mrg_position="merge", is_cross_margin="1",
                            trigger_px_type="last", price=None, product_group=None):
        """POST /deepcoin/trade/trigger-order — 条件单（含移动止损）"""
        if product_group is None:
            product_group = self.swap_product_group(symbol)
            if product_group == "SwapU":
                product_group = "Swap"
        params = {
            "instId": symbol, "productGroup": product_group, "sz": self.format_contract_sz(sz),
            "side": side, "posSide": pos_side, "isCrossMargin": str(is_cross_margin),
            "orderType": order_type,
            "triggerPrice": self.format_price(trigger_price, symbol),
            "mrgPosition": mrg_position, "tdMode": td_mode, "triggerPxType": trigger_px_type,
        }
        if order_type == "limit" and price is not None:
            params["price"] = self.format_price(price, symbol)
        # v16.10+：止损/止盈条件单用 force=True（关键防御操作）
        return self._request("POST", "/trade/trigger-order", params, _throttle_kind="rest_trade", _throttle_force=True)

    def set_position_sltp(self, symbol, pos_side, sl_trigger_px=None, tp_trigger_px=None,
                          td_mode="cross", mrg_position="merge", trigger_px_type="last",
                          sl_ord_px="-1", tp_ord_px="-1"):
        """POST /deepcoin/trade/set-position-sltp — 为已有持仓设置止盈止损
        sl_ord_px/tp_ord_px: -1 表示市价
        """
        params = {
            "instType": "SWAP", "instId": symbol, "posSide": pos_side,
            "mrgPosition": mrg_position, "tdMode": td_mode,
            "tpTriggerPxType": trigger_px_type, "slTriggerPxType": trigger_px_type,
            "tpOrdPx": tp_ord_px, "slOrdPx": sl_ord_px,
        }
        if tp_trigger_px is not None:
            params["tpTriggerPx"] = str(tp_trigger_px)
        if sl_trigger_px is not None:
            params["slTriggerPx"] = str(sl_trigger_px)
        # v16.10+：止盈止损设置用 force=True
        return self._request("POST", "/trade/set-position-sltp", params, _throttle_kind="rest_trade", _throttle_force=True)

    def cancel_order(self, symbol, ord_id=None, cl_ord_id=None):
        """POST /deepcoin/trade/cancel-order"""
        params = {"instId": symbol}
        if ord_id:
            params["ordId"] = ord_id
        elif cl_ord_id:
            params["clOrdId"] = cl_ord_id
        else:
            return None
        return self._safe_cancel("/trade/cancel-order", params)

    def get_order(self, symbol, ord_id=None, cl_ord_id=None):
        """GET /deepcoin/trade/order — 查询单笔订单"""
        params = {"instId": symbol}
        if ord_id:
            params["ordId"] = ord_id
        elif cl_ord_id:
            params["clOrdId"] = cl_ord_id
        else:
            return None
        return self._request("GET", "/trade/order", params, _throttle_kind="rest_query")

    def batch_close_position(self, symbol):
        """POST /deepcoin/trade/batch-close-position — 批量平仓指定产品所有仓位"""
        return self._request("POST", "/trade/batch-close-position", {
            "productGroup": self.swap_product_group(symbol),
            "instId": symbol,
        })

    def get_pending_orders(self, symbol="ETH-USDT-SWAP"):
        """GET /deepcoin/trade/v2/orders-pending — 未成交限价单（支持按品种或全账户查询）"""
        seen = set()
        merged = []
        for params in (
            {"instId": symbol, "index": 1, "limit": 100},
            {"index": 1, "limit": 100},
        ):
            res = self._request("GET", "/trade/v2/orders-pending", params)
            if not res or str(res.get("code", "")) != "0":
                if res:
                    logger.warning(
                        f"挂单查询失败 params={params} code={res.get('code')} msg={res.get('msg')}"
                    )
                continue
            for o in res.get("data") or []:
                if symbol and o.get("instId") != symbol:
                    continue
                oid = o.get("ordId")
                if oid and oid not in seen:
                    seen.add(oid)
                    merged.append(o)
        return merged

    def get_trigger_orders_pending(self, symbol="ETH-USDT-SWAP"):
        """GET /deepcoin/trade/trigger-orders-pending — 未触发条件单"""
        res = self._request("GET", "/trade/trigger-orders-pending", {
            "instType": "SWAP", "instId": symbol, "limit": 100,
        }, _throttle_kind="rest_query")
        if res and isinstance(res.get("data"), list):
            return res["data"]
        return []

    def _safe_cancel(self, endpoint, params):
        # v16.10+：撤单用 force=True（关键防御操作）
        res = self._request("POST", endpoint, params, _throttle_kind="rest_trade", _throttle_force=True)
        if res and str(res.get("code", "")) != "0":
            msg = str(res.get("msg", "")).lower() + str(res.get("sMsg", "")).lower()
            data = res.get("data") or {}
            if isinstance(data, dict):
                msg += str(data.get("sMsg", "")).lower()
            if "too many" in msg or "limit" in msg or "frequent" in msg:
                logger.warning(f"⚠️ [频率限制] 退避休眠 1.5 秒... | {msg}")
                time.sleep(1.5)
                res = self._request("POST", endpoint, params, _throttle_kind="rest_trade", _throttle_force=True)
            elif "not exist" in msg or "not found" in msg or "already" in msg or "no order" in msg:
                pass
            else:
                logger.warning(f"❌ [异常撤单] Endpoint: {endpoint} | Resp: {res}")
        return res

    def cancel_trigger_order(self, symbol, ord_id):
        """POST /deepcoin/trade/cancel-trigger-order — 撤销单笔条件单"""
        if not ord_id:
            return None
        return self._safe_cancel("/trade/cancel-trigger-order", {
            "instId": symbol, "ordId": ord_id,
        })

    def cancel_all_open_orders(self, symbol="ETH-USDT-SWAP"):
        """一键撤单 + 条件单一键撤单 + 兜底逐笔撤销"""
        try:
            instrument_id = self.inst_id_to_instrument_id(symbol)
            product_group = self.swap_product_group(symbol)
            self._safe_cancel("/trade/swap/cancel-all", {
                "InstrumentID": instrument_id,
                "ProductGroup": product_group,
                "IsCrossMargin": 1,
                "IsMergeMode": 1,
            })
            self._safe_cancel("/trade/swap/cancel-trigger-all", {
                "ProductGroup": product_group,
                "InstrumentID": instrument_id,
                "IsCrossMargin": -1,
                "IsMergeMode": -1,
            })
            time.sleep(0.4)

            pending = self._request("GET", "/trade/v2/orders-pending", {
                "instId": symbol, "index": 1, "limit": 100,
            })
            if pending and isinstance(pending.get("data"), list):
                for ord_item in pending["data"]:
                    if ord_item.get("ordId"):
                        self._safe_cancel("/trade/cancel-order", {
                            "instId": symbol, "ordId": ord_item["ordId"],
                        })

            trigger_pending = self._request("GET", "/trade/trigger-orders-pending", {
                "instType": "SWAP", "instId": symbol, "limit": 100,
            })
            if trigger_pending and isinstance(trigger_pending.get("data"), list):
                for t_ord in trigger_pending["data"]:
                    if t_ord.get("ordId"):
                        self._safe_cancel("/trade/cancel-trigger-order", {
                            "instId": symbol, "ordId": t_ord["ordId"],
                        })
        except Exception as e:
            logger.error(f"撤单巡检异常: {e}")

    # ── ListenKey 与私有 WebSocket ────────────────────────────────

    def acquire_listen_key(self):
        """GET /deepcoin/listenkey/acquire"""
        res = self._request("GET", "/listenkey/acquire")
        if self._is_success(res) and isinstance(res.get("data"), dict):
            self._listen_key = res["data"].get("listenkey")
            self._listen_key_expire = int(res["data"].get("expire_time", 0))
        return res

    def extend_listen_key(self, listenkey=None):
        """GET /deepcoin/listenkey/extend — 滑动续期 1 小时"""
        key = listenkey or self._listen_key
        if not key:
            return None
        res = self._request("GET", "/listenkey/extend", {"listenkey": key})
        if self._is_success(res) and isinstance(res.get("data"), dict):
            self._listen_key = res["data"].get("listenkey", key)
            self._listen_key_expire = int(res["data"].get("expire_time", 0))
        return res

    def start_private_ws(self, tables=None, on_message=None):
        """订阅私有 WebSocket：Order / Position / Trade 等频道"""
        if self._ws_running:
            return
        if not self._listen_key:
            self.acquire_listen_key()
        if not self._listen_key:
            logger.error("无法获取 listenKey，私有 WebSocket 启动失败")
            return

        tables = tables or ["Order", "Position", "Trade", "TriggerOrder"]
        if on_message:
            self._ws_callbacks["default"] = on_message

        self._ws_running = True
        self._ws_thread = threading.Thread(
            target=self._private_ws_loop, args=(tables,), daemon=True,
        )
        self._ws_thread.start()
        logger.info(f"私有 WebSocket 已启动，订阅频道: {tables}")

    def stop_private_ws(self):
        self._ws_running = False

    def _private_ws_loop(self, tables):
        try:
            import websocket
        except ImportError:
            logger.warning("未安装 websocket-client，跳过私有 WebSocket（pip install websocket-client）")
            self._ws_running = False
            return

        url = f"{WS_PRIVATE}?listenKey={self._listen_key}"
        last_extend = time.time()

        def on_message(ws, message):
            if message == "pong":
                return
            try:
                data = json.loads(message)
                cb = self._ws_callbacks.get("default")
                if cb:
                    cb(data)
            except Exception as e:
                logger.debug(f"WS 消息解析: {e}")

        def on_error(ws, error):
            logger.error(f"私有 WebSocket 错误: {error}")

        def on_close(ws, close_status_code, close_msg):
            logger.warning(f"私有 WebSocket 断开: {close_status_code} {close_msg}")
            self._ws_running = False

        def on_open(ws):
            sub = {"action": "subscribe", "tables": tables}
            ws.send(json.dumps(sub))
            logger.info("私有 WebSocket 订阅消息已发送")

        while self._ws_running:
            try:
                if time.time() - last_extend > 1800:
                    self.extend_listen_key()
                    last_extend = time.time()

                ws = websocket.WebSocketApp(
                    url, on_open=on_open, on_message=on_message,
                    on_error=on_error, on_close=on_close,
                )
                ws.run_forever(ping_interval=15, ping_payload="ping")
            except Exception as e:
                logger.error(f"私有 WebSocket 重连异常: {e}")
            if self._ws_running:
                time.sleep(3)


deepcoin_client = DeepcoinClient()
logger.info(f"DeepcoinClient {CLIENT_VERSION} 已加载")
try:
    deepcoin_client.get_instrument_info("ETH-USDT-SWAP")
except Exception as _e:
    logger.warning(f"启动预加载合约规格失败: {_e}")
