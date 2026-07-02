#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# position_supervisor_deepcoin.py — 与币安 VPS 逻辑完全对齐（深币张数/20x 适配）
import logging
import time
import threading
import os
import json
import queue
from datetime import datetime
from logging.handlers import RotatingFileHandler
from deepcoin_client import deepcoin_client, CLIENT_VERSION
import dingtalk

if not os.path.exists('logs'):
    os.makedirs('logs')
handler = RotatingFileHandler('logs/deepcoin_brain.log', maxBytes=5 * 1024 * 1024, backupCount=3)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] Brain: %(message)s',
    handlers=[handler, logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

DEEPCOIN_SUPERVISOR_VERSION = "v13.4.10-recover-dingtalk"
SENTINEL_POLL_NORMAL = 6
SENTINEL_POLL_ARMING = 3
SENTINEL_POLL_RADAR = 2
DUST_ORPHAN_CONTRACTS = 1
TP_COMPLETE_RESIDUAL_RATIO = 0.12
TV_JOURNAL = "logs/deepcoin_tv_journal.jsonl"
OPEN_JOURNAL = "logs/deepcoin_open_journal.jsonl"


class PositionSupervisor:
    def __init__(self):
        self.symbol = "ETH-USDT-SWAP"
        self.monitoring = False
        self._lock = threading.Lock()

        # 与币安完全一致的四档矩阵：activation=启动雷达的 TP1 距离比例，trail_offset=ATR 追踪倍数
        self.regime_settings = {
            1: {"margin": 0.15, "ratios": [0.25, 0.35, 0.40], "activation": 0.40, "trail_offset": 0.40},
            2: {"margin": 0.25, "ratios": [0.20, 0.35, 0.45], "activation": 0.50, "trail_offset": 0.60},
            3: {"margin": 0.35, "ratios": [0.18, 0.32, 0.50], "activation": 0.60, "trail_offset": 0.90},
            4: {"margin": 0.50, "ratios": [0.05, 0.20, 0.75], "activation": 0.70, "trail_offset": 1.30},
        }
        self.leverage = 20
        self.face_value = 0.1

        self.regime = 3
        self.current_atr = 30.0
        self.best_price = 0.0
        self.current_sl = 0.0
        self.tv_price = 0.0
        self.tv_tps = [0.0, 0.0, 0.0]

        self.initial_qty = 0
        self.watched_qty = 0
        self.watched_entry = 0.0
        self.current_side = None
        self.last_tv_side = None
        self.last_tv_signal = None
        self._scan_ticks = 0
        self._signal_queue = queue.Queue()
        self._signal_worker_started = False

        self.state_file = 'deepcoin_vps_state.json'
        logger.info(
            f"🧠 深币 VPS [{DEEPCOIN_SUPERVISOR_VERSION}/{CLIENT_VERSION}] "
            f"军师托管版已加载：双轨智慧雷达部署完毕！"
        )
        self._start_signal_worker()
        self._start_idle_flat_patrol()

    def _start_idle_flat_patrol(self):
        """空仓待命时后台巡检：发现孤立残张 → 自动扫尾 + 钉钉"""
        def loop():
            while True:
                time.sleep(30)
                if self.monitoring:
                    continue
                if not self._lock.acquire(timeout=2.0):
                    continue
                try:
                    if self.monitoring:
                        continue
                    pos = self._get_active_position()
                    if not pos or self._safe_qty(pos.get("size")) <= 0:
                        continue
                    real_amt = self._safe_qty(pos["size"])
                    if not self._is_dust_qty(real_amt) and not self._should_finalize_tp_victory(real_amt):
                        continue
                    if not self.current_side:
                        self.current_side = "LONG" if pos.get("posSide") == "long" else "SHORT"
                    logger.warning(
                        f"🐜 [空闲巡检] 发现残量 {self.current_side} {real_amt}张 → 扫尾"
                    )
                    self._sweep_dust_and_finalize("重启扫描：盘口蚂蚁仓自动扫平")
                except Exception as e:
                    logger.error(f"空闲巡检异常: {e}")
                finally:
                    self._lock.release()

        threading.Thread(target=loop, daemon=True, name="idle-flat-patrol").start()

    @staticmethod
    def _call_dingtalk(fn, **kwargs):
        """兼容 VPS 旧版 dingtalk.py（缺少 verified / swept_dust 等新参数）"""
        try:
            fn(**kwargs)
        except TypeError as exc:
            if "unexpected keyword argument" not in str(exc):
                raise
            legacy = {
                k: v for k, v in kwargs.items()
                if k not in ("verified", "swept_dust", "radar_sl_ok", "action_type")
            }
            logger.warning(f"钉钉旧版降级播报 {getattr(fn, '__name__', 'dingtalk')}: {exc}")
            fn(**legacy)

    def _start_signal_worker(self):
        if self._signal_worker_started:
            return
        self._signal_worker_started = True
        threading.Thread(target=self._signal_worker_loop, daemon=True, name="tv-signal-worker").start()

    def _signal_worker_loop(self):
        while True:
            payload = self._signal_queue.get()
            try:
                self._process_signal(payload)
            except Exception as e:
                logger.error(f"❌ 信号处理异常: {e}", exc_info=True)
            finally:
                self._signal_queue.task_done()

    def enqueue_signal(self, payload):
        depth = self._signal_queue.qsize()
        action = (payload.get("action") or "?").upper()
        self._signal_queue.put(payload)
        logger.info(f"📬 TV信号入队: {action} | 队列深度 {depth + 1}")

    def signal_queue_depth(self):
        return self._signal_queue.qsize()

    def _append_journal(self, path, record):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        record = dict(record)
        record["ts"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    def _load_last_journal_entry(self, path):
        if not os.path.exists(path):
            return None
        last = None
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        last = json.loads(line)
                    except json.JSONDecodeError:
                        continue
        return last

    def _record_tv_signal(self, payload, raw_action):
        entry = {
            "action": raw_action,
            "regime": self.regime,
            "atr": self.current_atr,
            "price": self.tv_price,
            "tv_tps": self.tv_tps,
            "reason": payload.get("reason", ""),
        }
        self.last_tv_signal = entry
        self._append_journal(TV_JOURNAL, entry)
        logger.info(
            f"📡 TV日志: {raw_action} R{self.regime} @ {self.tv_price:.2f} "
            f"TP={self.tv_tps}"
        )

    def _record_open_log(self, side, qty, entry, source="open"):
        self._append_journal(OPEN_JOURNAL, {
            "source": source,
            "side": side,
            "qty": qty,
            "entry": entry,
            "regime": self.regime,
            "tv_tps": self.tv_tps,
            "tv_price": self.tv_price,
            "last_tv_side": self.last_tv_side,
        })

    def _load_last_tv_open_signal(self):
        """TV 日志中最近一条 LONG/SHORT（CLOSE 之后仍可用于方向对账）"""
        if not os.path.exists(TV_JOURNAL):
            return None
        last_open = None
        with open(TV_JOURNAL, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                action = (entry.get("action") or "").upper()
                if action in ("LONG", "SHORT"):
                    last_open = entry
        return last_open

    def _reconcile_context_on_recover(self, pos):
        """重启对账：实盘头寸 vs 账本 vs 最新 TV / 开仓日志"""
        notes = []
        reconcile = {
            "notes": notes,
            "tv_close": False,
            "direction_mismatch": False,
            "qty_manual_change": None,
        }
        side = "LONG" if pos.get("posSide") == "long" else "SHORT"
        real_amt = self._safe_qty(pos.get("size"))
        saved_watched = self._safe_qty(self.watched_qty)
        saved_initial = self._safe_qty(self.initial_qty)

        last_tv = self._load_last_journal_entry(TV_JOURNAL)
        last_open = self._load_last_journal_entry(OPEN_JOURNAL)
        last_open_tv = self._load_last_tv_open_signal()

        if last_tv:
            self.last_tv_signal = last_tv
            tv_action = (last_tv.get("action") or "").upper()
            tv_tps_saved = self._sanitize_tp_prices(last_tv.get("tv_tps", []))
            tv_tp_count = sum(1 for t in tv_tps_saved if t > 0)

            if last_tv.get("regime"):
                self.regime = int(last_tv["regime"])
            if last_tv.get("atr"):
                self.current_atr = float(last_tv["atr"])
            if self.tv_price <= 0 and float(last_tv.get("price", 0) or 0) > 0:
                self.tv_price = float(last_tv["price"])

            if tv_action in ("LONG", "SHORT"):
                self.last_tv_side = tv_action
                if tv_tp_count > 0:
                    self.tv_tps = tv_tps_saved
                    notes.append(f"TV日志同步止盈价 {self.tv_tps}")
                if side != tv_action:
                    reconcile["direction_mismatch"] = True
                    notes.append(
                        f"方向背离: 实盘{side} vs TV最新{tv_action} ({last_tv.get('ts', '')})"
                    )
            elif tv_action.startswith("CLOSE"):
                reconcile["tv_close"] = True
                notes.append(
                    f"TV最新为{tv_action} ({last_tv.get('ts', '')})，实盘仍有仓 → 应清场"
                )
                if last_open_tv:
                    self.last_tv_side = (last_open_tv.get("action") or "").upper()
                    open_tps = self._sanitize_tp_prices(last_open_tv.get("tv_tps", []))
                    if sum(1 for t in open_tps if t > 0) > 0:
                        self.tv_tps = open_tps

        if not self.last_tv_side and last_open_tv:
            self.last_tv_side = (last_open_tv.get("action") or "").upper()

        if last_open:
            open_side = last_open.get("side")
            if open_side and side != open_side:
                notes.append(f"开仓日志方向 {open_side} ≠ 实盘 {side}")
            open_entry = float(last_open.get("entry", 0) or 0)
            entry = float(pos.get("entry_price", 0) or 0)
            if open_entry > 0 and abs(entry - open_entry) > 3.0:
                notes.append(f"入场偏差: 开仓日志 {open_entry:.2f} vs 实盘 {entry:.2f}")

        if saved_watched > 0 and saved_watched != real_amt:
            action_msg = (
                "手动加仓" if real_amt > saved_watched
                else "部分止盈吃单 / 手动减仓"
            )
            reconcile["qty_manual_change"] = (saved_watched, real_amt, action_msg)
            notes.append(f"人工异动(重启): {saved_watched}张 → {real_amt}张 ({action_msg})")

        if not self.last_tv_side:
            self.last_tv_side = side
        elif side != self.last_tv_side and not reconcile["tv_close"]:
            reconcile["direction_mismatch"] = True
            if not any("方向背离" in n for n in notes):
                notes.append(f"方向背离: 实盘{side} vs TV指令{self.last_tv_side}")

        if saved_initial <= 0 and real_amt > 0:
            self.initial_qty = real_amt

        for n in notes:
            logger.warning(f"🔎 重启对账: {n}")
        return reconcile

    @staticmethod
    def _sanitize_tp_prices(tp_list):
        """TV/状态文件里的浮点价统一规整到 2 位小数，避免 1517.4 触发 PriceNotOnTick"""
        out = []
        for t in tp_list:
            try:
                out.append(round(float(t), 2) if float(t) > 0 else 0.0)
            except (TypeError, ValueError):
                out.append(0.0)
        return out

    def _save_state(self):
        try:
            with open(self.state_file, 'w') as f:
                json.dump({
                    "last_tv_side": self.last_tv_side,
                    "current_side": self.current_side,
                    "watched_qty": self.watched_qty,
                    "watched_entry": self.watched_entry,
                    "current_sl": self.current_sl,
                    "monitoring": self.monitoring,
                    "regime": self.regime,
                    "current_atr": self.current_atr,
                    "tv_tps": self.tv_tps,
                    "tv_price": self.tv_price,
                    "best_price": self.best_price,
                    "initial_qty": self.initial_qty,
                    "last_tv_signal": self.last_tv_signal,
                }, f)
        except Exception as e:
            logger.error(f"保存状态失败: {e}")

    @staticmethod
    def _safe_qty(val, default=0):
        """Deepcoin API 常返回 '1.000000' 字符串，须先 float 再 int"""
        if val is None or val == "":
            return default
        try:
            return int(float(val))
        except (TypeError, ValueError):
            return default

    def _get_active_position(self):
        res = deepcoin_client.get_position_info(self.symbol)
        if res and 'data' in res:
            for p in res['data']:
                if self._safe_qty(p.get("pos")) > 0:
                    return {
                        "size": self._safe_qty(p.get("pos")),
                        "entry_price": round(float(p.get("avgPx", p.get("price", 0)) or 0), 2),
                        "posSide": p.get("posSide", "long").lower(),
                    }
        return None

    def _verify_flat(self):
        pos = self._get_active_position()
        return pos is None or self._safe_qty(pos.get("size")) == 0

    def _is_dust_qty(self, qty):
        """深币最小 1 张；无主仓账本时的孤立 1 张视为蚂蚁仓"""
        q = self._safe_qty(qty)
        if q <= 0:
            return False
        ref = self._safe_qty(self.initial_qty) + self._safe_qty(self.watched_qty)
        return q == DUST_ORPHAN_CONTRACTS and ref == 0

    def _should_finalize_tp_victory(self, real_amt):
        """止盈网格已吃完、盘口无 TP 限价单，但可能残留张数 → 扫尾收网"""
        real_amt = self._safe_qty(real_amt)
        if real_amt <= 0:
            return False
        if self._is_dust_qty(real_amt):
            return True
        if self._collect_limit_tp_prices():
            return False
        ref = self._safe_qty(self.initial_qty or self.watched_qty)
        if ref > 0:
            threshold = max(DUST_ORPHAN_CONTRACTS, int(ref * TP_COMPLETE_RESIDUAL_RATIO))
            if real_amt <= threshold:
                return True
        return False

    def _verify_position_qty(self, expected_qty, expected_side=None):
        pos = self._verify_position(expected_side)
        if not pos or self._safe_qty(pos.get("size")) != expected_qty:
            return None
        return pos

    def _report_flat_close(self, reason, swept_dust=False):
        """平仓/止盈收网钉钉：REST 核查重试"""
        flat = self._wait_verify(self._verify_flat, retries=6, delay=0.5)
        base_note = "盘口无持仓 | 挂单已清空 | 智慧大脑复位待命"
        if swept_dust:
            base_note = f"蚂蚁仓已市价扫尾 | {base_note}"
        if flat:
            verify_note = base_note
        else:
            pos = self._get_active_position()
            residual = self._safe_qty(pos["size"]) if pos else 0
            if residual > 0 and not self._is_dust_qty(residual):
                logger.warning(
                    f"平仓钉钉跳过：空仓核查未通过 | 残留 {residual}张 | reason={reason}"
                )
                return
            verify_note = f"{base_note} | REST 同步略延迟"
            logger.info(f"平仓钉钉：REST 延迟，仍推送收网播报 | reason={reason}")
        self._call_dingtalk(
            dingtalk.report_supervisor_close,
            reason=reason or "仓位归零 (人工全平 / 止盈吃满)",
            verify_note=verify_note,
            verified=flat,
            swept_dust=swept_dust,
        )

    def _sweep_dust_and_finalize(self, reason):
        """哨兵检测：止盈后蚂蚁仓/无 TP 残张 → 撤单 + reduceOnly 扫尾 + 收网钉钉"""
        logger.warning(f"🐜 止盈扫尾：检测到残量，启动蚂蚁仓强平 → {reason}")
        self.monitoring = False
        deepcoin_client.cancel_all_open_orders(self.symbol)
        time.sleep(0.4)
        for round_i in range(4):
            pos = self._get_active_position()
            if not pos or self._safe_qty(pos.get("size")) <= 0:
                break
            close_side = "sell" if pos["posSide"] == "long" else "buy"
            live_sz = self._safe_qty(pos["size"])
            logger.info(f"🐜 扫尾第 {round_i + 1}/4: {close_side} {live_sz}张 reduceOnly")
            deepcoin_client.place_market_order(
                self.symbol, close_side, pos["posSide"], live_sz, reduce_only=True,
            )
            time.sleep(1.0)
        self.watched_qty = 0
        self.initial_qty = 0
        self.current_side = None
        self._save_state()
        deepcoin_client.cancel_all_open_orders(self.symbol)
        self._report_flat_close(reason, swept_dust=True)

    def _apply_recover_live_alignment(self, side, reconcile):
        """重启以实盘为准：不回放 TV 平仓，不因日志方向差异核武全平（仅 live webhook 才平仓）"""
        extra_notes = []
        if reconcile.get("tv_close"):
            action = (self.last_tv_signal or {}).get("action", "CLOSE")
            msg = (
                f"TV日志末条为 {action}，重启不回放平仓 → 以实盘 {side} 继续闪电接管"
            )
            logger.warning(f"🔄 [重启] {msg}")
            extra_notes.append(msg)
            last_open_tv = self._load_last_tv_open_signal()
            if last_open_tv:
                self.last_tv_side = (last_open_tv.get("action") or side).upper()
                open_tps = self._sanitize_tp_prices(last_open_tv.get("tv_tps", []))
                if sum(1 for t in open_tps if t > 0) > 0:
                    self.tv_tps = open_tps
        if reconcile.get("direction_mismatch") or (
                self.last_tv_side and side != self.last_tv_side
        ):
            old_tv = self.last_tv_side
            self.last_tv_side = side
            msg = f"方向以实盘为准: {side} (TV日志={old_tv})"
            logger.warning(f"🔄 [重启] {msg}")
            extra_notes.append(msg)
        elif not self.last_tv_side:
            self.last_tv_side = side
        return extra_notes

    def _scan_and_sweep_dust_on_startup(self, was_monitoring=False):
        """重启首检：发现蚂蚁仓/止盈残张 → 扫尾收网，避免误接管为正常持仓"""
        pos = self._get_active_position()
        if not pos or self._safe_qty(pos.get("size")) <= 0:
            return False
        if not self.current_side:
            self.current_side = "LONG" if pos.get("posSide") == "long" else "SHORT"
        real_amt = self._safe_qty(pos["size"])
        ref = max(self._safe_qty(self.initial_qty), self._safe_qty(self.watched_qty))
        if was_monitoring and not self._is_dust_qty(real_amt):
            if ref <= 0 or real_amt > max(
                DUST_ORPHAN_CONTRACTS, int(ref * TP_COMPLETE_RESIDUAL_RATIO)
            ):
                logger.info(
                    f"🔄 [重启扫描] 活跃主仓 {real_amt}张 (ref={ref})，跳过蚂蚁扫尾"
                )
                return False
        if not self._is_dust_qty(real_amt) and not self._should_finalize_tp_victory(real_amt):
            return False
        if self._safe_qty(self.initial_qty) > 0 or self._safe_qty(self.watched_qty) > 0:
            reason = "仓位归零 (止盈吃单 / 人工全平 / TV 强制平仓)"
        else:
            reason = "重启扫描：盘口蚂蚁仓自动扫平"
        logger.warning(
            f"🐜 [重启扫描] {self.current_side} 残量 {real_amt}张 "
            f"(initial={self.initial_qty}, watched={self.watched_qty}) → 扫尾强平"
        )
        self._sweep_dust_and_finalize(reason)
        return True

    def _recover_missed_flat_on_startup(self, was_monitoring=False):
        """重启对账：服务宕机期间已全平，但账本仍有仓 → 补发收网钉钉"""
        pos = self._get_active_position()
        if pos and self._safe_qty(pos.get("size")) > 0:
            return False

        prev_watched = self._safe_qty(self.watched_qty)
        prev_initial = self._safe_qty(self.initial_qty)
        prev_side = self.current_side

        had_active_book = (
            prev_watched > 0
            or prev_initial > 0
            or prev_side in ("LONG", "SHORT")
            or was_monitoring
        )
        if not had_active_book:
            last_open = self._load_last_journal_entry(OPEN_JOURNAL)
            if last_open and last_open.get("source") in ("open", "recover"):
                had_active_book = True
                prev_watched = prev_watched or self._safe_qty(last_open.get("qty", 0))
                prev_side = prev_side or last_open.get("side")

        if not had_active_book:
            return False

        logger.warning(
            f"📭 [重启对账] 账本/日志曾有仓 (watched={prev_watched}, side={prev_side}, "
            f"monitoring={was_monitoring}) 但盘口已全平 → 补发收网播报"
        )
        deepcoin_client.cancel_all_open_orders(self.symbol)
        self.monitoring = False
        self.watched_qty = 0
        self.initial_qty = 0
        self.current_side = None
        self._save_state()

        verify_note = (
            f"重启对账补发 | 原账本 {prev_watched}张 {prev_side or ''} | "
            f"盘口无持仓 | 挂单已清空 | 智慧大脑复位待命"
        )
        self._call_dingtalk(
            dingtalk.report_supervisor_close,
            reason="仓位归零 (止盈吃单 / 人工全平 / TV 强制平仓)",
            verify_note=verify_note,
            verified=True,
            swept_dust=False,
        )
        return True

    def _verify_position(self, expected_side=None):
        pos = self._get_active_position()
        if not pos or self._safe_qty(pos.get("size")) <= 0:
            return None
        side = "LONG" if pos["posSide"] == "long" else "SHORT"
        if expected_side and side != expected_side:
            return None
        return pos

    def _is_tp_limit_order(self, o):
        if o.get("ordType") not in ("limit", "post_only", None):
            return False
        val = o.get("reduceOnly")
        if val is True or str(val).lower() in ("true", "1"):
            return True
        if not self.current_side:
            return False
        close_side = "sell" if self.current_side == "LONG" else "buy"
        return str(o.get("side", "")).lower() == close_side

    def _collect_limit_tp_prices(self):
        prices = []
        for o in deepcoin_client.get_pending_orders(self.symbol):
            if not self._is_tp_limit_order(o):
                continue
            px = float(o.get("px", 0) or 0)
            if px > 0:
                prices.append(round(px, 2))
        return sorted(prices)

    def _collect_tp_limit_orders(self):
        orders = []
        for o in deepcoin_client.get_pending_orders(self.symbol):
            if not self._is_tp_limit_order(o):
                continue
            px = float(o.get("px", 0) or 0)
            if px <= 0:
                continue
            orders.append({
                "orderId": o.get("ordId"),
                "price": round(px, 2),
                "qty": self._safe_qty(o.get("sz")),
            })
        return orders

    def _expected_tp_count(self, tp_pxs=None):
        tp_pxs = tp_pxs if tp_pxs is not None else self.tv_tps
        return sum(1 for t in tp_pxs if t > 0)

    def _expected_tp_levels(self, live_qty):
        ratios = self.regime_settings[self.regime]["ratios"]
        q1, q2, q3 = self._calculate_tp_quantities(live_qty, ratios)
        return [
            {"level": 1, "qty": q1, "price": self.tv_tps[0]},
            {"level": 2, "qty": q2, "price": self.tv_tps[1]},
            {"level": 3, "qty": q3, "price": self.tv_tps[2]},
        ]

    def _audit_tp_levels(self, live_qty, tolerance=1.0):
        """严格审计：每档价位唯一 + 张数符合 regime 比例 + 无孤儿单"""
        live_qty = self._resolve_live_qty(live_qty)
        orders = self._collect_tp_limit_orders()
        levels = []
        matched_full = 0
        issues = []

        for lv in self._expected_tp_levels(live_qty):
            if lv["qty"] <= 0 or lv["price"] <= 0:
                continue
            at_px = [o for o in orders if abs(o["price"] - lv["price"]) <= tolerance]
            status = "ok"
            actual_qty = 0
            if len(at_px) == 0:
                status = "missing"
                issues.append(f"TP{lv['level']} @{lv['price']:.2f} 缺失")
            elif len(at_px) > 1:
                status = "duplicate"
                actual_qty = sum(o["qty"] for o in at_px)
                issues.append(f"TP{lv['level']} @{lv['price']:.2f} 重复 {len(at_px)} 张")
            elif at_px[0]["qty"] != lv["qty"]:
                status = "qty_mismatch"
                actual_qty = at_px[0]["qty"]
                issues.append(
                    f"TP{lv['level']} {actual_qty}张 ≠ 期望 {lv['qty']}张 "
                    f"({self.regime_settings[self.regime]['ratios']})"
                )
            else:
                matched_full += 1
                actual_qty = at_px[0]["qty"]
            levels.append({**lv, "status": status, "actual_qty": actual_qty})

        expected_prices = [lv["price"] for lv in levels]
        orphans = [
            o for o in orders
            if not any(abs(o["price"] - p) <= tolerance for p in expected_prices)
        ]
        for o in orphans:
            issues.append(f"孤儿止盈 @{o['price']:.2f} {o['qty']}张")

        expected = self._expected_tp_count()
        pending_prices = sorted({o["price"] for o in orders})
        return {
            "matched_full": matched_full,
            "expected": expected,
            "levels": levels,
            "issues": issues,
            "orphans": orphans,
            "pending_prices": pending_prices,
            "live_qty": live_qty,
        }

    def _format_audit_summary(self, audit):
        parts = []
        for lv in audit.get("levels", []):
            if lv["price"] <= 0:
                continue
            icon = "✅" if lv["status"] == "ok" else "❌"
            line = f"{icon}TP{lv['level']} {lv['qty']}张@{lv['price']:.2f}"
            if lv["status"] != "ok":
                line += f"({lv['status']})"
            parts.append(line)
        if audit.get("issues"):
            parts.append("问题:" + "; ".join(audit["issues"][:3]))
        return " | ".join(parts) if parts else "无有效 TP"

    def _count_matched_tp_orders(self, tp_pxs, tolerance=1.0, live_qty=None):
        if live_qty is not None and live_qty > 0:
            audit = self._audit_tp_levels(live_qty, tolerance)
            return audit["matched_full"], audit["pending_prices"]
        pending_prices = self._collect_limit_tp_prices()
        matched = 0
        for tp in tp_pxs:
            if tp <= 0:
                continue
            if any(abs(p - tp) <= tolerance for p in pending_prices):
                matched += 1
        return matched, pending_prices

    def _has_duplicate_tp_orders(self, tolerance=1.0):
        orders = self._collect_tp_limit_orders()
        expected = self._expected_tp_count()
        if expected <= 0:
            return False
        if len(orders) > expected:
            return True
        for tp in self.tv_tps:
            if tp <= 0:
                continue
            at_px = [o for o in orders if abs(o["price"] - tp) <= tolerance]
            if len(at_px) > 1:
                return True
        return False

    def _defenses_fully_ok(self, live_qty, dynamic_sl=None, tolerance=1.0):
        tp_pxs = self.tv_tps
        expected = self._expected_tp_count(tp_pxs)
        if expected == 0:
            return dynamic_sl is None or self._has_trigger_sl_near(dynamic_sl, tolerance)

        audit = self._audit_tp_levels(live_qty, tolerance)
        if audit["matched_full"] < expected:
            return False
        if audit["orphans"]:
            return False
        if dynamic_sl and not self._has_trigger_sl_near(dynamic_sl, tolerance):
            return False
        return True

    def _patch_missing_tp_levels(self, live_qty, tolerance=1.0):
        close_side = "sell" if self.current_side == "LONG" else "buy"
        pos_side = "long" if self.current_side == "LONG" else "short"
        live_qty = self._resolve_live_qty(live_qty)
        ratios = self.regime_settings[self.regime]["ratios"]
        qty1, qty2, qty3 = self._calculate_tp_quantities(live_qty, ratios)
        levels = [(qty1, self.tv_tps[0]), (qty2, self.tv_tps[1]), (qty3, self.tv_tps[2])]
        placed = 0

        for q, px in levels:
            if q <= 0 or px <= 0:
                continue
            orders = self._collect_tp_limit_orders()
            at_px = [o for o in orders if abs(o["price"] - px) <= tolerance]
            if len(at_px) == 1 and at_px[0]["qty"] == q:
                logger.info(f"  ✓ TP @ {px:.2f} 已存在 {at_px[0]['qty']}张，跳过")
                continue
            for o in at_px:
                if o.get("orderId"):
                    deepcoin_client.cancel_order(self.symbol, ord_id=o["orderId"])
                    time.sleep(0.25)
            logger.info(f"  + 补挂 TP @ {px:.2f} qty={q}张")
            res = deepcoin_client.place_limit_order(
                self.symbol, close_side, pos_side, px, q, reduce_only=True,
            )
            if res and deepcoin_client._is_success(res):
                placed += 1
            time.sleep(0.4)
        return placed

    def _cancel_orphan_tp_orders(self, live_qty, tolerance=1.0):
        audit = self._audit_tp_levels(live_qty, tolerance)
        cancelled = 0
        for o in audit["orphans"]:
            if o.get("orderId"):
                deepcoin_client.cancel_order(self.symbol, ord_id=o["orderId"])
                cancelled += 1
                time.sleep(0.2)
        if cancelled:
            logger.info(f"🧹 撤销 {cancelled} 张孤儿止盈单")
        return cancelled

    def _cancel_stop_orders(self):
        cancelled = 0
        for t in deepcoin_client.get_trigger_orders_pending(self.symbol):
            oid = t.get("ordId")
            if oid:
                deepcoin_client.cancel_trigger_order(self.symbol, oid)
                cancelled += 1
                time.sleep(0.2)
        return cancelled

    def _is_radar_active(self):
        if not self.watched_entry or not self.current_sl:
            return False
        if self.current_side == "LONG":
            return self.current_sl > self.watched_entry
        if self.current_side == "SHORT":
            return self.current_sl < self.watched_entry
        return False

    def _radar_sl_to_pass(self):
        return self.current_sl if self._is_radar_active() else None

    def _audit_requires_nuclear(self, audit):
        expected = audit.get("expected", 0)
        if expected <= 0:
            return False
        if audit.get("matched_full", 0) >= expected and not audit.get("orphans"):
            return False
        orders = self._collect_tp_limit_orders()
        if len(orders) > expected:
            return True
        if audit.get("matched_full", 0) == 0 and audit.get("issues"):
            return True
        bad = [lv for lv in audit.get("levels", []) if lv.get("status") in ("duplicate", "qty_mismatch")]
        if bad:
            return True
        missing = sum(1 for lv in audit.get("levels", []) if lv.get("status") == "missing")
        if missing >= 2:
            return True
        if audit.get("orphans"):
            return True
        return False

    def _cancel_all_tp_limit_orders(self):
        cancelled = 0
        for o in deepcoin_client.get_pending_orders(self.symbol):
            if not self._is_tp_limit_order(o):
                continue
            oid = o.get("ordId")
            if oid:
                deepcoin_client.cancel_order(self.symbol, ord_id=oid)
                cancelled += 1
                time.sleep(0.15)
        if cancelled:
            logger.info(f"🧹 已撤销全部限价止盈 {cancelled} 张")
        return cancelled

    def _ensure_radar_sl(self, live_qty, sl_price):
        if not sl_price:
            return False
        if self._has_trigger_sl_near(sl_price):
            return True
        self._place_radar_sl(live_qty, sl_price)
        time.sleep(0.35)
        return self._has_trigger_sl_near(sl_price)

    def _refresh_radar_state_on_recover(self, curr_px, entry):
        """重启：按现价恢复 best_price / 雷达激活 / 追踪止损位"""
        if curr_px <= 0 or not entry:
            return
        fee_buffer = entry * 0.0015
        trail_offset = self.current_atr * self.regime_settings[self.regime]["trail_offset"]

        if self.best_price == 0.0:
            self.best_price = entry
        if self.current_side == "LONG":
            self.best_price = max(self.best_price, curr_px)
        else:
            self.best_price = min(self.best_price, curr_px)

        progress = self._radar_activation_progress(curr_px)
        if progress >= 1.0:
            if self.current_side == "LONG":
                breakeven_floor = entry + fee_buffer
                trail_sl = max(round(self.best_price - trail_offset, 2), breakeven_floor)
                if not self._is_radar_active() or trail_sl > self.current_sl:
                    self.current_sl = max(self.current_sl or entry, trail_sl)
            else:
                breakeven_floor = entry - fee_buffer
                trail_sl = min(round(self.best_price + trail_offset, 2), breakeven_floor)
                if not self._is_radar_active() or trail_sl < self.current_sl:
                    self.current_sl = min(self.current_sl or entry, trail_sl)
            logger.info(
                f"📡 重启雷达恢复: 进度 {progress:.0%} | best={self.best_price:.2f} | "
                f"SL={self.current_sl:.2f}"
            )
        elif self.current_sl == 0.0:
            self.current_sl = entry

    def _nuclear_realign_tp(self, live_qty, entry, dynamic_sl=None, rounds=3):
        sl_preserve = dynamic_sl is not None
        last_audit = self._audit_tp_levels(live_qty)
        for r in range(rounds):
            logger.warning(
                f"☢️ 核武级止盈清场重挂 {r + 1}/{rounds} | 持仓 {live_qty}张 | "
                f"当前 {last_audit['matched_full']}/{last_audit['expected']} | "
                f"{self._format_audit_summary(last_audit)}"
            )
            if sl_preserve:
                self._cancel_all_tp_limit_orders()
            else:
                deepcoin_client.cancel_all_open_orders(self.symbol)
            time.sleep(1.0)
            tp_sl = None if sl_preserve else dynamic_sl
            placed = self._rebuild_defenses(live_qty, entry, dynamic_sl=tp_sl)
            logger.info(f"☢️ 核武轮 {r + 1} 新挂 {placed} 笔限价止盈")
            if sl_preserve:
                time.sleep(0.6)
                self._ensure_radar_sl(live_qty, dynamic_sl)
            time.sleep(1.0)
            last_audit = self._audit_tp_levels(live_qty)
            if self._defenses_fully_ok(live_qty, dynamic_sl):
                logger.info(f"☢️ 核武重挂成功: {self._format_audit_summary(last_audit)}")
                return last_audit
            logger.warning(
                f"☢️ 核武轮 {r + 1} 仍未对齐: {self._format_audit_summary(last_audit)}"
            )
            time.sleep(1.5)
        return last_audit

    def _full_rebuild_tp_loop(self, live_qty, entry, dynamic_sl=None):
        audit = self._nuclear_realign_tp(live_qty, entry, dynamic_sl=dynamic_sl, rounds=3)
        return audit["matched_full"], audit["pending_prices"], audit["expected"]

    def _ensure_defenses_on_recover(self, live_qty, entry, dynamic_sl=None):
        audit = self._audit_tp_levels(live_qty)
        expected = audit["expected"]
        matched = audit["matched_full"]
        pending_prices = audit["pending_prices"]
        logger.info(
            f"📊 防线审计: 持仓 {live_qty}张 | TP {matched}/{expected} | "
            f"{self._format_audit_summary(audit)}"
        )

        if self._audit_requires_nuclear(audit) or self._has_duplicate_tp_orders():
            logger.warning(
                f"☢️ 审计触发核武级重挂: {len(self._collect_tp_limit_orders())} 张止盈 | "
                f"{self._format_audit_summary(audit)}"
            )
            audit = self._nuclear_realign_tp(live_qty, entry, dynamic_sl=dynamic_sl, rounds=3)
            return audit["matched_full"], audit["pending_prices"], audit["expected"], True

        if self._defenses_fully_ok(live_qty, dynamic_sl):
            logger.info(
                f"✅ TP123 比例齐全 ({matched}/{expected}) @ {pending_prices}，跳过补挂"
            )
            if dynamic_sl and not self._has_trigger_sl_near(dynamic_sl):
                self._place_radar_sl(live_qty, dynamic_sl)
            return matched, pending_prices, expected, False

        self._cancel_orphan_tp_orders(live_qty)
        logger.info(f"📋 止盈未齐 ({matched}/{expected})，增量补挂缺失档（保留已有正确单）")
        self._patch_missing_tp_levels(live_qty)
        time.sleep(0.8)
        audit = self._audit_tp_levels(live_qty)
        matched = audit["matched_full"]

        if self._defenses_fully_ok(live_qty, dynamic_sl):
            logger.info(f"✅ 增量补挂成功 ({matched}/{expected}) @ {audit['pending_prices']}")
            if dynamic_sl and not self._has_trigger_sl_near(dynamic_sl):
                self._place_radar_sl(live_qty, dynamic_sl)
            return matched, audit["pending_prices"], expected, True

        logger.warning(
            f"⚠️ 增量补挂仍不足 ({matched}/{expected}) {audit['issues']}，升级核武级重挂"
        )
        audit = self._nuclear_realign_tp(live_qty, entry, dynamic_sl=dynamic_sl, rounds=3)
        return audit["matched_full"], audit["pending_prices"], expected, True

    def _smart_realign_defenses(self, live_qty, entry, dynamic_sl=None, reason=""):
        if reason:
            logger.info(f"🧠 智能防线对齐: {reason}")
        initial = self._audit_tp_levels(live_qty)
        if self._defenses_fully_ok(live_qty, dynamic_sl):
            logger.info(f"✅ 防线已齐，跳过: {self._format_audit_summary(initial)}")
            return {
                "matched": initial["matched_full"],
                "expected": initial["expected"],
                "pending_prices": initial["pending_prices"],
                "rebuilt": False,
                "audit": initial,
                "nuclear": False,
            }

        self._cancel_orphan_tp_orders(live_qty)
        matched, pending_prices, expected, rebuilt = self._ensure_defenses_on_recover(
            live_qty, entry, dynamic_sl=dynamic_sl,
        )
        audit = self._audit_tp_levels(live_qty)
        nuclear = False

        if expected > 0 and audit["matched_full"] < expected:
            logger.warning(
                f"⚠️ 常规对齐未达标 ({audit['matched_full']}/{expected})，"
                f"升级核武级清场重挂"
            )
            audit = self._nuclear_realign_tp(live_qty, entry, dynamic_sl=dynamic_sl, rounds=3)
            matched = audit["matched_full"]
            pending_prices = audit["pending_prices"]
            rebuilt = nuclear = True

        return {
            "matched": matched,
            "expected": expected,
            "pending_prices": pending_prices,
            "rebuilt": rebuilt,
            "audit": audit,
            "nuclear": nuclear,
        }

    def _place_radar_sl(self, live_qty, sl_price):
        close_side = "sell" if self.current_side == "LONG" else "buy"
        pos_side = "long" if self.current_side == "LONG" else "short"
        sl_qty = self._resolve_live_qty(live_qty)
        deepcoin_client.place_trigger_order(
            self.symbol, close_side, pos_side, sl_qty, sl_price,
            order_type="market", td_mode="cross", mrg_position="merge",
        )

    def _has_tp_limit_at_price(self, price, tolerance=1.0):
        if price <= 0:
            return False
        for o in self._collect_tp_limit_orders():
            if abs(o["price"] - price) <= tolerance:
                return True
        return False

    def _detect_tp_fills(self, old_qty, new_qty):
        """根据张数变化 + 盘口 TP 单消失推断成交档位"""
        if new_qty >= old_qty:
            return []
        ratios = self.regime_settings[self.regime]["ratios"]
        o1, o2, o3 = self._calculate_tp_quantities(old_qty, ratios)
        fills = []
        budget = old_qty - new_qty
        for level, tp_px, slice_qty in (
            (1, self.tv_tps[0], o1),
            (2, self.tv_tps[1], o2),
            (3, self.tv_tps[2], o3),
        ):
            if tp_px <= 0 or slice_qty <= 0 or budget <= 0:
                continue
            order_gone = not self._has_tp_limit_at_price(tp_px)
            if order_gone and budget >= max(1, slice_qty - 1):
                fill_qty = min(budget, slice_qty)
                fills.append({"level": level, "price": tp_px, "qty": fill_qty})
                budget -= fill_qty
        return fills

    def _report_qty_change_dingtalk(self, old_qty, new_qty, realign_result):
        """TP 成交 / 减仓：REST 重试核查后必达钉钉"""
        verified_pos = self._wait_verify(
            lambda: self._verify_position(self.current_side),
            retries=8,
            delay=0.5,
        )
        verified = (
            verified_pos is not None
            and self._safe_qty(verified_pos.get("size", 0)) == new_qty
        )
        entry_px = (
            float(verified_pos.get("entry_price", self.watched_entry))
            if verified_pos else self.watched_entry
        )
        verify_note = (
            f"核实 {new_qty}张 @ {entry_px:.2f} | "
            f"止盈 {realign_result['matched']}/{realign_result['expected']} 档 | "
            f"{self._format_audit_summary(realign_result['audit'])}"
        )
        if not verified:
            verify_note += f" | {dingtalk.VERIFY_DELAY_MARK}"

        fills = self._detect_tp_fills(old_qty, new_qty)
        if fills:
            for fill in fills:
                self._call_dingtalk(
                    dingtalk.report_tp_fill,
                    tp_level=fill["level"],
                    tp_price=fill["price"],
                    filled_qty=fill["qty"],
                    remain_qty=new_qty,
                    entry_px=entry_px,
                    side=self.current_side or "?",
                    regime=self.regime,
                    verify_note=verify_note,
                    verified=verified,
                )
                logger.info(
                    f"📣 TP{fill['level']} 成交钉钉已推送 @ {fill['price']:.2f} "
                    f"({fill['qty']}张)"
                )
        else:
            action_msg = (
                "手动加仓" if new_qty > old_qty else "部分止盈吃单 / 手动减仓"
            )
            self._call_dingtalk(
                dingtalk.report_manual_position_change,
                action_type=action_msg,
                old_qty=old_qty,
                new_qty=new_qty,
                new_entry_price=entry_px,
                verify_note=verify_note,
                tp_audit=realign_result["audit"],
                verified=verified,
            )

        if realign_result["expected"] > 0 and realign_result["matched"] < realign_result["expected"]:
            dingtalk.report_system_alert(
                "人工异动后止盈未对齐",
                f"{self._format_audit_summary(realign_result['audit'])}",
            )

    def _report_radar_intervention(self, real_amt, new_sl, action_msg, sl_placed=True):
        """每轮雷达推止损：REST 重试核查，挂单成功则必达钉钉"""
        verified = self._wait_verify(
            lambda: self._has_trigger_sl_near(new_sl),
            retries=8,
            delay=0.5,
        )
        base_note = (
            f"条件止损 @ {new_sl:.2f} | 持仓 {real_amt}张 | 轮询 {SENTINEL_POLL_RADAR}s"
        )
        if not sl_placed and not verified:
            logger.warning(f"雷达钉钉：止损 @ {new_sl:.2f} 提交失败且盘口未核查到")
            return
        if verified:
            verify_note = base_note
        else:
            verify_note = f"{base_note} | {dingtalk.VERIFY_DELAY_MARK}"
            logger.info(f"雷达钉钉：止损已挂 REST 延迟，仍推送 @{new_sl:.2f}")
        self._call_dingtalk(
            dingtalk.report_intervention,
            qty=real_amt,
            entry_px=self.watched_entry,
            new_sl=new_sl,
            action_msg=action_msg,
            verify_note=verify_note,
            verified=verified,
        )

    def _realign_radar_defenses(self, live_qty, entry, new_sl):
        self._cancel_stop_orders()
        time.sleep(0.35)
        sl_placed = False
        if not self._defenses_fully_ok(live_qty, dynamic_sl=None):
            if self._audit_requires_nuclear(self._audit_tp_levels(live_qty)):
                self._nuclear_realign_tp(live_qty, entry, dynamic_sl=new_sl, rounds=2)
                sl_placed = self._has_trigger_sl_near(new_sl) or self._ensure_radar_sl(live_qty, new_sl)
            else:
                self._cancel_orphan_tp_orders(live_qty)
                self._patch_missing_tp_levels(live_qty)
                time.sleep(0.6)
                sl_placed = self._ensure_radar_sl(live_qty, new_sl)
        else:
            self._place_radar_sl(live_qty, new_sl)
            time.sleep(0.35)
            sl_placed = self._has_trigger_sl_near(new_sl)
        time.sleep(0.4)
        return sl_placed

    def _wait_tp_hung(self, tp_pxs, live_qty=None, retries=5, delay=0.8):
        expected = self._expected_tp_count(tp_pxs)
        matched, pending = 0, []
        for _ in range(retries):
            if live_qty is not None and live_qty > 0:
                audit = self._audit_tp_levels(live_qty)
                matched = audit["matched_full"]
                pending = audit["pending_prices"]
            else:
                matched, pending = self._count_matched_tp_orders(tp_pxs)
            if expected == 0 or matched >= expected:
                return matched, pending
            time.sleep(delay)
        return matched, pending

    def _has_trigger_sl_near(self, sl_price, tolerance=2.0):
        for t in deepcoin_client.get_trigger_orders_pending(self.symbol):
            for key in ("triggerPx", "slTriggerPrice", "triggerPrice"):
                val = t.get(key)
                if val is not None and str(val).strip() not in ("", "0"):
                    try:
                        if abs(float(val) - sl_price) <= tolerance:
                            return True
                    except (TypeError, ValueError):
                        pass
        return False

    def _wait_verify(self, checks_fn, retries=3, delay=0.6):
        for i in range(retries):
            result = checks_fn()
            if result:
                return result
            time.sleep(delay)
        return checks_fn()

    def _calculate_tp_quantities(self, total_qty: int, ratios: list) -> tuple:
        """深币最小 1 张限制 + 余数吸收：qty1+qty2+qty3 恒等于 total_qty"""
        if total_qty <= 0:
            return 0, 0, 0

        qty1 = max(1, round(total_qty * ratios[0]))
        remaining = total_qty - qty1
        if remaining <= 0:
            return qty1, 0, 0

        ratio_sum_23 = ratios[1] + ratios[2]
        if ratio_sum_23 <= 0:
            return qty1, 0, remaining

        qty2 = max(0, round(remaining * (ratios[1] / ratio_sum_23)))
        qty3 = remaining - qty2
        if qty3 < 0:
            qty3, qty2 = 0, remaining

        if qty2 == 0 and remaining >= 2:
            qty2, qty3 = 1, remaining - 1
        if qty3 == 0 and remaining >= 2 and qty2 > 1:
            qty3, qty2 = 1, remaining - 1

        assert qty1 + qty2 + qty3 == total_qty, f"TP 分档不守恒: {qty1}+{qty2}+{qty3}!={total_qty}"
        return qty1, qty2, qty3

    def _resolve_live_qty(self, fallback_qty: int) -> int:
        """挂 reduceOnly 前重新读取交易所落账张数，避免冻结/部分成交导致数量漂移"""
        pos = self._get_active_position()
        if pos and self._safe_qty(pos.get("size")) > 0:
            live = self._safe_qty(pos["size"])
            if live != fallback_qty:
                logger.info(f"📐 实盘张数校正: 账本 {fallback_qty} → 交易所 {live}")
            return live
        return fallback_qty

    def handle_signal(self, payload):
        """兼容旧调用路径"""
        self.enqueue_signal(payload)

    def _safe_float(self, val, default=0.0):
        try:
            if val is None or val == "":
                return default
            return float(val)
        except (TypeError, ValueError):
            return default

    def _safe_int(self, val, default=3):
        try:
            if val is None or val == "":
                return default
            return int(float(val))
        except (TypeError, ValueError):
            return default

    def _process_signal(self, payload):
        raw_action = str(payload.get("action", "")).strip().upper()
        self.regime = self._safe_int(payload.get("regime"), 3)
        if self.regime not in self.regime_settings:
            self.regime = 3

        self.current_atr = self._safe_float(payload.get("atr"), 30.0)
        self.tv_price = self._safe_float(payload.get("price"), 0.0)
        self.tv_tps = self._sanitize_tp_prices([
            self._safe_float(payload.get("tv_tp1"), 0),
            self._safe_float(payload.get("tv_tp2"), 0),
            self._safe_float(payload.get("tv_tp3"), 0),
        ])
        close_reason = str(payload.get("reason") or "策略指标反转/波动率安全退出").strip()
        close_side = str(payload.get("side") or "").strip().upper()
        pnl_pct = payload.get("pnl_pct")

        if not raw_action:
            logger.warning("TV 信号缺少 action，已忽略")
            return
        if raw_action in ("LONG", "SHORT", "CLOSE", "CLOSE_PROTECT", "CLOSE_TP3") or \
                raw_action.startswith("CLOSE"):
            self._record_tv_signal(payload, raw_action)

        if not self._lock.acquire(timeout=120.0):
            logger.error(f"⏱️ 锁等待 120s 超时，信号 {raw_action} 重新入队")
            self._signal_queue.put(payload)
            return

        try:
            self.monitoring = False
            if raw_action == "CLOSE_PROTECT" or raw_action.startswith("CLOSE_PROTECT"):
                extra = ""
                if close_side:
                    extra += f" | TV方向 {close_side}"
                if pnl_pct is not None and pnl_pct != "":
                    extra += f" | 近似盈亏 {pnl_pct}%"
                pos = self._get_active_position()
                if not pos or self._safe_qty(pos.get("size", 0)) <= 0:
                    logger.info(f"🛡️ 保护性全平到达但盘口已空仓 → 撤单复位 | {close_reason}{extra}")
                    self._handle_manual_flat_detected(
                        f"🛡️ TV保护性全平（盘口已空）: {close_reason}{extra}"
                    )
                else:
                    self._close_all(f"🛡️ 保护性全平：{close_reason}{extra}")
            elif raw_action == "CLOSE_TP3":
                self._close_all("🎯 完美胜利：大趋势吃满，TP3 终极收网")
            elif raw_action == "CLOSE":
                self._close_all(f"🧹 换防清场：{close_reason}")
            elif raw_action in ["LONG", "SHORT"]:
                self.last_tv_side = raw_action
                self._save_state()
                self._handle_smart_entry(raw_action)
            else:
                logger.warning(f"未识别的 TV action: {raw_action}")
        finally:
            self._lock.release()

    def _handle_manual_flat_detected(self, reason):
        """人工全平 / 止盈吃满：智能复位账本"""
        logger.info(f"📭 感知空仓: {reason}")
        self.monitoring = False
        self.watched_qty = 0
        self.initial_qty = 0
        self.current_side = None
        deepcoin_client.cancel_all_open_orders(self.symbol)
        self._save_state()
        self._report_flat_close(reason or "仓位归零 (人工全平 / 止盈吃满)")

    def _handle_smart_entry(self, action):
        """三重把关之一：新 TV 方向到达 → 先平后开（撤单→平仓→再开仓）"""
        logger.info(f"⚡ 收到建仓信号 [{action}]，启动绝对先平后开机制")
        deepcoin_client.cancel_all_open_orders(self.symbol)
        time.sleep(0.5)

        pos = self._get_active_position()
        if pos and pos.get('size', 0) > 0:
            current_side = "LONG" if pos["posSide"] == "long" else "SHORT"
            if current_side == action:
                self._close_all("同方向新指令到达，触发【先平后开】洗清旧阵地")
            else:
                self._close_all("反方向指令到达，触发【先平后开】原子对冲换防")
            time.sleep(1.2)
            deepcoin_client.cancel_all_open_orders(self.symbol)
            time.sleep(0.5)

        curr_px = deepcoin_client.get_current_price(self.symbol)
        if curr_px > 0:
            self._open_position(action, curr_px)

    def _open_position(self, action, curr_px):
        balance = deepcoin_client.get_available_balance()
        margin_pct = self.regime_settings[self.regime]["margin"]

        deepcoin_client.set_leverage(self.symbol, leverage=self.leverage)
        qty = max(int((balance * margin_pct * self.leverage) / (curr_px * self.face_value)), 1)
        open_side = "buy" if action == "LONG" else "sell"
        pos_side = "long" if action == "LONG" else "short"

        logger.info(f"🚀 [唯一主仓] 极速开仓: {open_side} {qty} 张 | 档位 {self.regime}")
        deepcoin_client.place_market_order(self.symbol, open_side, pos_side, qty)
        time.sleep(2.0)

        pos = self._get_active_position()
        if pos and pos.get('size', 0) > 0:
            self.current_side = action
            real_qty = self._safe_qty(pos['size'])
            self.initial_qty = real_qty
            self._protect_and_monitor(real_qty, pos['entry_price'])

    def _protect_and_monitor(self, qty, entry_price):
        tp_pxs = self.tv_tps
        self.current_sl = entry_price
        self.best_price = entry_price
        self.watched_qty, self.watched_entry, self.monitoring = qty, entry_price, True
        self._save_state()

        self._ensure_price_ws()
        self._rebuild_defenses(qty, entry_price, dynamic_sl=None)

        verified = self._wait_verify(lambda: self._verify_position(self.current_side))
        if verified:
            result = self._smart_realign_defenses(
                self._safe_qty(verified["size"]), verified["entry_price"],
                reason="开仓后二次核查",
            )
            matched, expected = result["matched"], result["expected"]
            audit = result["audit"]
            verify_note = (
                f"持仓 {verified['size']}张 @ {verified['entry_price']:.2f} | "
                f"限价止盈 {matched}/{expected} 档 | {self._format_audit_summary(audit)}"
            )
            self._record_open_log(
                self.current_side, self._safe_qty(verified["size"]), verified["entry_price"], source="open",
            )
            dingtalk.report_supervisor_open(
                self.current_side, verified['entry_price'], self.tv_price,
                verified['size'], tp_pxs, self.current_atr, self.regime, self.tv_tps,
                verify_note=verify_note,
                tp_audit=audit,
            )
            if expected > 0 and matched < expected:
                dingtalk.report_system_alert(
                    "开仓后限价止盈未全部挂上",
                    f"{self.current_side} {verified['size']}张 | 仅 {matched}/{expected} 档 | "
                    f"{self._format_audit_summary(audit)}",
                )
        else:
            logger.warning("开仓钉钉跳过：实盘持仓核查未通过")
        threading.Thread(target=self._sentinel_loop, daemon=True).start()

    def _ensure_price_ws(self):
        deepcoin_client.start_public_price_ws(self.symbol)

    def _tp1_distance(self):
        if self.tv_tps[0] > 0 and self.watched_entry:
            return abs(self.tv_tps[0] - self.watched_entry)
        return self.current_atr * 1.5

    def _radar_activation_price(self):
        activation_ratio = self.regime_settings[self.regime]["activation"]
        tp1_dist = self._tp1_distance()
        if self.current_side == "LONG":
            return self.watched_entry + tp1_dist * activation_ratio
        return self.watched_entry - tp1_dist * activation_ratio

    def _should_radar_trail(self, curr_px):
        """已激活后不再要求现价回到激活线以上，否则回撤时会停止推止损"""
        if self._is_radar_active():
            return True
        if curr_px <= 0 or not self.watched_entry:
            return False
        if self.current_side == "LONG":
            return curr_px >= self._radar_activation_price()
        return curr_px <= self._radar_activation_price()

    def _compute_radar_sl(self):
        if not self.watched_entry or self.best_price <= 0:
            return None
        trail_offset = self.current_atr * self.regime_settings[self.regime]["trail_offset"]
        fee_buffer = self.watched_entry * 0.0015
        if self.current_side == "LONG":
            return max(round(self.best_price - trail_offset, 2), self.watched_entry + fee_buffer)
        if self.current_side == "SHORT":
            return min(round(self.best_price + trail_offset, 2), self.watched_entry - fee_buffer)
        return None

    def _sync_radar_sl_from_best(self, curr_px):
        """TP 重对齐前刷新内存止损位，避免把旧止损重新挂回交易所"""
        if not self._should_radar_trail(curr_px):
            return self.current_sl
        new_sl = self._compute_radar_sl()
        if new_sl is None:
            return self.current_sl
        if self.current_side == "LONG" and new_sl > self.current_sl:
            logger.info(
                f"📈 雷达止损预算刷新: {self.current_sl:.2f} → {new_sl:.2f} "
                f"(best={self.best_price:.2f})"
            )
            self.current_sl = new_sl
            self._save_state()
        elif self.current_side == "SHORT" and (
                self.current_sl >= self.watched_entry or new_sl < self.current_sl
        ):
            logger.info(
                f"📉 雷达止损预算刷新: {self.current_sl:.2f} → {new_sl:.2f} "
                f"(best={self.best_price:.2f})"
            )
            self.current_sl = new_sl
            self._save_state()
        return self.current_sl

    def _bump_best_on_tp_fill(self, old_qty, new_qty, curr_px):
        """部分止盈后把 best_price 抬到已触及的 TP 价，避免漏记冲高"""
        if new_qty >= old_qty or curr_px <= 0:
            return
        if self.current_side == "LONG":
            candidates = [self.best_price, curr_px]
            for tp in self.tv_tps:
                if tp > 0 and curr_px >= tp - 2.0:
                    candidates.append(tp)
            new_best = max(candidates)
            if new_best > self.best_price + 0.01:
                logger.info(
                    f"📊 止盈吃单刷新 best_price: {self.best_price:.2f} → {new_best:.2f} "
                    f"(qty {old_qty}→{new_qty})"
                )
                self.best_price = new_best
        else:
            candidates = [self.best_price, curr_px]
            for tp in self.tv_tps:
                if tp > 0 and curr_px <= tp + 2.0:
                    candidates.append(tp)
            new_best = min(candidates)
            if new_best < self.best_price - 0.01:
                logger.info(
                    f"📊 止盈吃单刷新 best_price: {self.best_price:.2f} → {new_best:.2f} "
                    f"(qty {old_qty}→{new_qty})"
                )
                self.best_price = new_best

    def _radar_activation_progress(self, curr_px):
        if curr_px <= 0 or not self.watched_entry:
            return 0.0
        tp1_dist = self._tp1_distance()
        activation_ratio = self.regime_settings[self.regime]["activation"]
        if self.current_side == "LONG":
            required = self.watched_entry + tp1_dist * activation_ratio
            span = required - self.watched_entry
            if span <= 0:
                return 0.0
            return max(0.0, min(1.0, (curr_px - self.watched_entry) / span))
        required = self.watched_entry - tp1_dist * activation_ratio
        span = self.watched_entry - required
        if span <= 0:
            return 0.0
        return max(0.0, min(1.0, (self.watched_entry - curr_px) / span))

    def _sentinel_poll_sec(self, curr_px=0.0):
        if self._is_radar_active():
            return SENTINEL_POLL_RADAR
        if curr_px > 0 and self._radar_activation_progress(curr_px) >= 0.5:
            return SENTINEL_POLL_ARMING
        return SENTINEL_POLL_NORMAL

    def _process_radar_trailing(self, real_amt, curr_px):
        if not self._should_radar_trail(curr_px):
            return False
        new_sl = self._compute_radar_sl()
        if new_sl is None:
            return False

        if self.current_side == "LONG":
            if new_sl > self.current_sl + 1.0:
                self.current_sl = new_sl
                self._save_state()
                sl_placed = self._realign_radar_defenses(real_amt, self.watched_entry, new_sl)
                self._report_radar_intervention(
                    real_amt, new_sl,
                    f"🚀 档位{self.regime} 雷达实时跟踪：保本盾推升至 {new_sl:.2f}",
                    sl_placed=sl_placed,
                )
                return True
        else:
            if self.current_sl >= self.watched_entry or new_sl < self.current_sl - 1.0:
                self.current_sl = new_sl
                self._save_state()
                sl_placed = self._realign_radar_defenses(real_amt, self.watched_entry, new_sl)
                self._report_radar_intervention(
                    real_amt, new_sl,
                    f"🚀 档位{self.regime} 雷达实时跟踪：保本顶线下压至 {new_sl:.2f}",
                    sl_placed=sl_placed,
                )
                return True
        return False

    def _sentinel_loop(self):
        """哨兵：持仓/TP 防线 + 雷达移动保本（自适应轮询 2~6 秒）"""
        last_px = 0.0
        while self.monitoring:
            try:
                if not self._lock.acquire(timeout=2.0):
                    continue
                try:
                    pos = self._get_active_position()
                    real_amt = self._safe_qty(pos.get("size")) if pos else 0
                    actual_side = "LONG" if pos and pos.get('posSide') == "long" else "SHORT"

                    if real_amt == 0:
                        if self.watched_qty > 0:
                            self._handle_manual_flat_detected(
                                "仓位归零 (止盈吃单 / 人工全平 / TV 强制平仓)"
                            )
                        break

                    if self.watched_qty > 0 and self._should_finalize_tp_victory(real_amt):
                        self._sweep_dust_and_finalize(
                            "仓位归零 (止盈吃单 / 人工全平 / TV 强制平仓)"
                        )
                        break

                    if actual_side != self.last_tv_side:
                        reason = f"致命方向背离：实盘({actual_side}) vs TV({self.last_tv_side})"
                        self._close_all(reason, force_align=(actual_side, self.last_tv_side))
                        break

                    curr_px = deepcoin_client.get_current_price(self.symbol)
                    if curr_px <= 0:
                        curr_px = last_px
                    elif curr_px > 0:
                        last_px = curr_px
                    if curr_px > 0:
                        if self.current_side == "LONG":
                            self.best_price = max(self.best_price, curr_px)
                        else:
                            self.best_price = min(self.best_price, curr_px)

                    qty_changed = real_amt != self.watched_qty
                    if qty_changed:
                        old_qty = self.watched_qty
                        self.watched_qty = real_amt
                        self.watched_entry = pos['entry_price']
                        pct = abs(real_amt - old_qty) / old_qty if old_qty > 0 else 1.0
                        action_msg = (
                            "手动加仓" if real_amt > old_qty
                            else "部分止盈吃单 / 手动减仓"
                        )
                        logger.info(
                            f"🔄 [智慧大脑] 仓位变化 {old_qty} ➔ {real_amt} ({pct:.1%})，智能重对齐"
                        )
                        self._bump_best_on_tp_fill(old_qty, real_amt, curr_px)
                        self._sync_radar_sl_from_best(curr_px)
                        sl_to_pass = self._radar_sl_to_pass()
                        result = self._smart_realign_defenses(
                            real_amt, self.watched_entry, dynamic_sl=sl_to_pass,
                            reason=f"人工异动: {action_msg}",
                        )
                        self._save_state()
                        self._report_qty_change_dingtalk(old_qty, real_amt, result)

                    self._scan_ticks += 1
                    if not qty_changed and self._scan_ticks % 10 == 0:
                        audit = self._audit_tp_levels(real_amt)
                        if audit["issues"]:
                            logger.info(
                                f"🔍 定期扫描发现异常: {audit['issues']}，触发智能补挂"
                            )
                            sl_to_pass = self._radar_sl_to_pass()
                            self._smart_realign_defenses(
                                real_amt, self.watched_entry, dynamic_sl=sl_to_pass,
                                reason="定期防线扫描",
                            )

                    if curr_px <= 0:
                        continue

                    progress = self._radar_activation_progress(curr_px)
                    if self._should_radar_trail(curr_px):
                        self._process_radar_trailing(real_amt, curr_px)
                    elif progress >= 0.5 and self._scan_ticks % 5 == 0:
                        logger.info(
                            f"📡 雷达预热: 进度 {progress:.0%} | 现价 {curr_px:.2f} | "
                            f"轮询 {SENTINEL_POLL_ARMING}s"
                        )
                finally:
                    self._lock.release()
            except Exception as e:
                logger.error(f"哨兵异常: {e}")
            if self.monitoring:
                time.sleep(self._sentinel_poll_sec(last_px))

    def _rebuild_defenses(self, qty, entry, dynamic_sl=None):
        close_side = "sell" if self.current_side == "LONG" else "buy"
        pos_side = "long" if self.current_side == "LONG" else "short"
        ratios = self.regime_settings[self.regime]["ratios"]

        live_qty = self._resolve_live_qty(qty)
        if live_qty <= 0:
            logger.warning(f"重建防线跳过：交易所无可用持仓 (传入 {qty} 张)")
            return 0
        if live_qty != qty:
            self.watched_qty = live_qty
            self._save_state()

        qty1, qty2, qty3 = self._calculate_tp_quantities(live_qty, ratios)
        tp_pxs = self.tv_tps
        placed = 0

        logger.info(
            f"🕸️ 补挂 TP123: 总 {live_qty}张 → TP1={qty1} TP2={qty2} TP3={qty3} "
            f"(合计 {qty1 + qty2 + qty3})"
        )

        for q, px in ((qty1, tp_pxs[0]), (qty2, tp_pxs[1]), (qty3, tp_pxs[2])):
            if q > 0 and px > 0:
                res = deepcoin_client.place_limit_order(
                    self.symbol, close_side, pos_side, px, q, reduce_only=True,
                )
                if res and deepcoin_client._is_success(res):
                    placed += 1
                time.sleep(0.35)

        if dynamic_sl:
            sl_qty = self._resolve_live_qty(live_qty)
            deepcoin_client.place_trigger_order(
                self.symbol, close_side, pos_side, sl_qty, dynamic_sl,
                order_type="market", td_mode="cross", mrg_position="merge",
            )
        return placed

    def _close_all(self, reason="", force_align=None):
        """三重把关之二：TV 全平/保护性全平 → 先撤单释放冻结仓位，6 轮阶梯强平至归零"""
        deepcoin_client.cancel_all_open_orders(self.symbol)
        time.sleep(0.5)
        closed_successfully = False

        for round_i in range(6):
            pos = self._get_active_position()
            if not pos or self._safe_qty(pos.get("size")) == 0:
                closed_successfully = True
                break

            close_side = "sell" if pos["posSide"] == "long" else "buy"
            live_sz = self._safe_qty(pos["size"])
            logger.info(f"🔪 强平第 {round_i + 1}/6 轮: {close_side} {live_sz}张 reduceOnly")
            deepcoin_client.place_market_order(
                self.symbol, close_side, pos["posSide"], live_sz, reduce_only=True,
            )
            time.sleep(1.5)

        if not closed_successfully:
            residual = self._get_active_position()
            residual_sz = self._safe_qty(residual["size"]) if residual else 0
            if residual_sz > 0 and self._is_dust_qty(residual_sz):
                close_side = "sell" if residual["posSide"] == "long" else "buy"
                logger.warning(f"🐜 强平后残 {residual_sz}张，触发蚂蚁仓扫尾")
                deepcoin_client.place_market_order(
                    self.symbol, close_side, residual["posSide"], residual_sz, reduce_only=True,
                )
                time.sleep(1.0)
                closed_successfully = self._verify_flat()
            if not closed_successfully:
                residual = self._get_active_position()
                residual_sz = self._safe_qty(residual["size"]) if residual else 0
                logger.error(f"❌ 6 轮强平后仍有残单: {residual_sz}张")
                dingtalk.report_system_alert(
                    "强平未完全归零",
                    f"6 轮市价平仓后仍剩 {residual_sz} 张，请人工核查 Deepcoin 盘口",
                )

        self.monitoring = False
        self.watched_qty = 0
        self.initial_qty = 0
        self.current_side = None
        self._save_state()
        deepcoin_client.cancel_all_open_orders(self.symbol)

        if reason and closed_successfully:
            if force_align:
                real_side, expected_side = force_align
                flat = self._wait_verify(self._verify_flat, retries=6, delay=0.5)
                verify_note = "盘口无持仓 | 挂单已清空 | 智慧大脑复位待命"
                if not flat:
                    verify_note += " | REST 同步略延迟"
                dingtalk.report_force_align(real_side, expected_side, verify_note=verify_note)
            else:
                self._report_flat_close(reason)

    def recover_state_on_startup(self):
        """重启闪电接管：对账 TV/开仓日志 → 核实实盘 → 智能补挂 TP123 → 恢复雷达"""
        try:
            saved_monitoring = False
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r') as f:
                    s = json.load(f)
                    saved_monitoring = bool(s.get("monitoring"))
                    self.last_tv_side = s.get("last_tv_side")
                    self.current_side = s.get("current_side")
                    self.current_sl = s.get("current_sl", 0.0)
                    self.regime = s.get("regime", 3)
                    self.current_atr = s.get("current_atr", 30.0)
                    self.tv_tps = self._sanitize_tp_prices(s.get("tv_tps", [0.0, 0.0, 0.0]))
                    self.tv_price = float(s.get("tv_price", 0.0) or 0.0)
                    self.best_price = s.get("best_price", 0.0)
                    self.watched_qty = s.get("watched_qty", 0)
                    self.watched_entry = s.get("watched_entry", 0.0)
                    self.initial_qty = s.get("initial_qty", 0)
                    self.last_tv_signal = s.get("last_tv_signal")

            if self._scan_and_sweep_dust_on_startup(was_monitoring=saved_monitoring):
                return

            if self._recover_missed_flat_on_startup(was_monitoring=saved_monitoring):
                return

            pos = self._get_active_position()
            if pos and self._safe_qty(pos.get("size", 0)) != 0:
                reconcile = self._reconcile_context_on_recover(pos)
                reconcile_notes = reconcile["notes"]
                real_amt = self._safe_qty(pos["size"])
                side = "LONG" if pos.get("posSide") == "long" else "SHORT"
                self.current_side = side

                align_notes = self._apply_recover_live_alignment(side, reconcile)
                reconcile_notes.extend(align_notes)

                saved_initial = self._safe_qty(self.initial_qty)
                if saved_initial <= 0:
                    saved_initial = real_amt
                self.watched_qty = real_amt
                self.initial_qty = saved_initial
                self.watched_entry = float(pos["entry_price"])
                qty_change = reconcile.get("qty_manual_change")

                curr_px = deepcoin_client.get_current_price(self.symbol)
                self._refresh_radar_state_on_recover(curr_px, self.watched_entry)

                radar_active = self._is_radar_active()
                sl_to_pass = self.current_sl if radar_active else None

                logger.info(
                    f"🔄 [系统重启点火] 检测到实盘持仓 {self.current_side} {real_amt}张 @ "
                    f"{self.watched_entry:.2f} | 雷达={'已激活' if radar_active else '待命'} | "
                    f"TV对齐 {self.last_tv_side} | 对账 {len(reconcile_notes)} 项"
                )

                result = self._smart_realign_defenses(
                    real_amt, self.watched_entry, dynamic_sl=sl_to_pass,
                    reason="重启闪电接管" + (
                        f" | {qty_change[2]}" if qty_change else ""
                    ),
                )
                matched = result["matched"]
                expected = result["expected"]
                _rebuilt = result["rebuilt"]
                audit = result["audit"]

                self.monitoring = True
                self._save_state()
                self._ensure_price_ws()
                self._record_open_log(
                    self.current_side, real_amt, self.watched_entry, source="recover",
                )

                sl_ok = True
                if radar_active:
                    sl_ok = self._ensure_radar_sl(real_amt, self.current_sl)
                    logger.info(
                        f"📡 [重启] 雷达哨兵已点火 | SL={self.current_sl:.2f} | "
                        f"止损={'已挂/已确认' if sl_ok else '待哨兵补挂'}"
                    )

                threading.Thread(target=self._sentinel_loop, daemon=True).start()

                verified = self._wait_verify(
                    lambda: self._verify_position_qty(real_amt, self.current_side),
                    retries=8,
                    delay=0.5,
                )
                entry_px = float(
                    (verified or pos).get("entry_price", self.watched_entry)
                )
                tv_note = ""
                if self.last_tv_signal:
                    tv_note = (
                        f" | 最新TV: {self.last_tv_signal.get('action')} "
                        f"@{self.last_tv_signal.get('ts', '')}"
                    )
                reconcile_txt = (" | " + " ; ".join(reconcile_notes)) if reconcile_notes else ""
                skip_note = " | 盘口已齐全，未重复补挂" if not _rebuilt else ""
                verify_note = (
                    f"接管 {real_amt}张 @ {entry_px:.2f} | "
                    f"TV方向 {self.last_tv_side} | "
                    f"止盈 {matched}/{expected} 档 | "
                    f"{self._format_audit_summary(audit)}{skip_note}{tv_note}{reconcile_txt}"
                )
                if not verified:
                    verify_note += f" | {dingtalk.VERIFY_DELAY_MARK}"
                self._call_dingtalk(
                    dingtalk.report_recover_takeover,
                    side=self.current_side,
                    qty=real_amt,
                    entry=entry_px,
                    tv_tps=self.tv_tps,
                    regime=self.regime,
                    radar_active=radar_active,
                    sl_price=self.current_sl,
                    verify_note=verify_note,
                    tp_matched=matched,
                    tp_expected=expected,
                    tp_audit=audit,
                    last_tv_signal=self.last_tv_signal,
                    radar_sl_ok=sl_ok,
                )
                if qty_change:
                    old_q, new_q, action_msg = qty_change
                    self._call_dingtalk(
                        dingtalk.report_manual_position_change,
                        action_type=action_msg,
                        old_qty=old_q,
                        new_qty=new_q,
                        new_entry_price=entry_px,
                        verify_note=f"重启接管检测 | {verify_note}",
                        tp_audit=audit,
                        verified=bool(verified),
                    )
                if expected > 0 and matched < expected:
                    dingtalk.report_system_alert(
                        "重启接管后限价止盈未对齐",
                        f"{self.current_side} {real_amt}张 @ {entry_px:.2f} | "
                        f"仅 {matched}/{expected} 档 | {self._format_audit_summary(audit)} | "
                        f"请查 logs/deepcoin_brain.log",
                    )
                logger.info("  -> 🎉 实盘阵地接管完毕，TP123 及雷达系统已复位。")
            else:
                deepcoin_client.cancel_all_open_orders(self.symbol)
                logger.info("🔄 [系统重启点火] 盘口干净无持仓，账本复位为空仓待命。")
                self.monitoring = False
                self.watched_qty = 0
                self.initial_qty = 0
                self.current_side = None
                self._save_state()
                flat_ok = self._wait_verify(self._verify_flat, retries=6, delay=0.5)
                standby_note = (
                    f"重启完成 | 盘口无持仓 | 挂单已清空 | "
                    f"{DEEPCOIN_SUPERVISOR_VERSION}"
                )
                if not flat_ok:
                    standby_note += f" | {dingtalk.VERIFY_DELAY_MARK}"
                dingtalk.report_recover_standby(
                    verify_note=standby_note,
                    version=DEEPCOIN_SUPERVISOR_VERSION,
                )
        except Exception as e:
            logger.error(f"❌ 闪电接管异常: {e}")
            dingtalk.report_system_alert("重启接管失败", str(e))


position_supervisor = PositionSupervisor()

# 仅在被 app / gunicorn 导入时执行一次闪电接管（避免 deploy 重复启动双进程）
if __name__ != "__main__":
    position_supervisor.recover_state_on_startup()
