#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TV Webhook 时序：bar_index + seq + 缓存窗口
- 缓存窗口：同 symbol 首包后 **固定 1.0s** settle，到期统一处理（不无限等待）
- 排序：先 bar_index 升序；同 bar 内 **动作优先**（CLOSE → UPDATE → OPEN），再 seq
- 铁律：同窗有平仓时 **一律先平后开**（平仓一次 + 最新开仓）；禁止先开后秒平
- 折叠：平仓消息幂等只执行一次；开仓只执行窗口内最新一条
- 幂等：symbol_bar_index_seq_action（Redis 优先，否则本地文件 TTL）
- CLOSE 后释放开仓幂等键，允许同 bar 再开（刷新仓位）
- 乱序：前置 seq 缺失时暂存等待，超时报警后按已有顺序执行
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

SEQ_IDEMPOTENCY_TTL_SEC = int(os.getenv("TV_SEQ_IDEMPOTENCY_TTL", "86400"))  # 24h
SEQ_PENDING_WAIT_SEC = float(os.getenv("TV_SEQ_PENDING_WAIT", "3.0"))  # 2~5s 窗口
# 缓存窗口固定 1.0s（需求锁定；忽略环境变量漂移）
SAME_BAR_SETTLE_SEC = 1.0
LEGACY_SETTLE_SEC = 1.0
# 仅开仓先到：最多再等一会，给迟到平仓拼进同窗（先平后开）
OPEN_ALONE_MAX_WAIT_SEC = 2.5
MAX_PAIR_WAIT_SEC = 3.0
SEQ_STORE_FILE = os.getenv("TV_SEQ_STORE_FILE", "logs/tv_seq_idempotency.json")
REDIS_URL = os.getenv("REDIS_URL", "").strip()

# P0 反转保护平仓（市价全平）；P1 = LONG/SHORT
FLATTEN_PRIORITY_ACTIONS = frozenset({"CLOSE_QUICK_EXIT", "CLOSE_RSI_EXIT"})


def action_exec_rank(action: Any) -> int:
    """
    同 bar 执行优先级：平仓永远最先，开仓永远最后。
    0=CLOSE*  1=UPDATE*  2=LONG/SHORT开仓  3=其它
    """
    a = str(action or "").strip().upper()
    if a.startswith("CLOSE"):
        return 0
    if a.startswith("UPDATE"):
        return 1
    if a in ("LONG", "SHORT"):
        return 2
    return 3


def is_close_action(action: Any) -> bool:
    return str(action or "").strip().upper().startswith("CLOSE")


def is_open_action(action: Any) -> bool:
    return str(action or "").strip().upper() in ("LONG", "SHORT")


def is_flatten_action(action: Any) -> bool:
    """P0：反转保护快平（CLOSE_QUICK_EXIT / CLOSE_RSI_EXIT）。"""
    return str(action or "").strip().upper() in FLATTEN_PRIORITY_ACTIONS


def sort_same_bar_items(items: List[Tuple[int, dict]]) -> List[Tuple[int, dict]]:
    """同 bar：(动作优先级, seq, 到达序) — 保证开平并存时永远先平后开。"""
    decorated = []
    for i, (sq, pl) in enumerate(items or []):
        act = (pl or {}).get("action", "")
        decorated.append((action_exec_rank(act), int(sq or 0), i, sq, pl))
    decorated.sort(key=lambda t: (t[0], t[1], t[2]))
    return [(sq, pl) for _, _, _, sq, pl in decorated]


def bar_has_close_and_open(items: List[Tuple[int, dict]]) -> bool:
    acts = [str((pl or {}).get("action", "") or "").strip().upper() for _, pl in (items or [])]
    return any(a.startswith("CLOSE") for a in acts) and any(a in ("LONG", "SHORT") for a in acts)


def make_seq_key(symbol: str, bar_index: int, seq: int, action: str = "") -> str:
    """幂等键含 action：同 bar 不同动作不互杀；先平后开靠 CLOSE 后 release。"""
    sym = str(symbol or "UNKNOWN").upper().replace(".P", "")
    act = str(action or "NA").strip().upper() or "NA"
    return f"{sym}_{int(bar_index)}_{int(seq)}_{act}"


def extract_seq_meta(payload: dict) -> Tuple[Optional[int], Optional[int]]:
    """从已归一化 payload 取 bar_index / seq；缺一则返回 (None, None)。"""
    if not isinstance(payload, dict):
        return None, None
    try:
        bi = payload.get("bar_index")
        sq = payload.get("seq")
        if bi is None or sq is None:
            return None, None
        bi_i = int(bi)
        sq_i = int(sq)
        if bi_i < 0 or sq_i < 1:
            return None, None
        return bi_i, sq_i
    except (TypeError, ValueError):
        return None, None


class SeqIdempotencyStore:
    """跨进程尽量去重：Redis SETEX，否则文件+内存。"""

    def __init__(self, ttl_sec: int = SEQ_IDEMPOTENCY_TTL_SEC):
        self.ttl = int(ttl_sec)
        self._lock = threading.Lock()
        self._mem: Dict[str, float] = {}  # key -> expire_ts
        self._redis = None
        if REDIS_URL:
            try:
                import redis  # optional dependency

                self._redis = redis.from_url(REDIS_URL, decode_responses=True)
                self._redis.ping()
                logger.info(f"📦 TV时序幂等：Redis 已连接 ({REDIS_URL.split('@')[-1]})")
            except Exception as e:
                self._redis = None
                logger.warning(f"📦 TV时序幂等：Redis 不可用 → 文件回退 | {e}")

    def _purge_mem(self, now: float):
        dead = [k for k, exp in self._mem.items() if exp <= now]
        for k in dead:
            self._mem.pop(k, None)

    def _load_file(self) -> Dict[str, float]:
        if not os.path.exists(SEQ_STORE_FILE):
            return {}
        try:
            with open(SEQ_STORE_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
            now = time.time()
            return {
                str(k): float(v)
                for k, v in (raw or {}).items()
                if float(v) > now
            }
        except Exception:
            return {}

    def _save_file(self, data: Dict[str, float]):
        try:
            os.makedirs(os.path.dirname(SEQ_STORE_FILE) or ".", exist_ok=True)
            tmp = SEQ_STORE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f)
            os.replace(tmp, SEQ_STORE_FILE)
        except Exception as e:
            logger.warning(f"TV时序幂等文件写入失败: {e}")

    def is_duplicate_and_mark(self, key: str) -> bool:
        """
        True = 已见过（丢弃）；False = 首次，已标记。
        """
        key = str(key or "").strip()
        if not key:
            return False
        now = time.time()
        if self._redis is not None:
            try:
                # SET NX EX：仅首次成功
                ok = self._redis.set(name=f"tvseq:{key}", value="1", nx=True, ex=self.ttl)
                return not bool(ok)
            except Exception as e:
                logger.warning(f"Redis 幂等失败，回退本地: {e}")

        with self._lock:
            self._purge_mem(now)
            file_map = self._load_file()
            # merge
            for k, exp in file_map.items():
                self._mem[k] = max(self._mem.get(k, 0), exp)
            if key in self._mem and self._mem[key] > now:
                return True
            self._mem[key] = now + self.ttl
            self._save_file(dict(self._mem))
            return False

    def release_keys(self, keys: List[str]) -> int:
        """释放幂等键（CLOSE 后允许同 bar 再开）。"""
        freed = 0
        now = time.time()
        for key in keys or []:
            key = str(key or "").strip()
            if not key:
                continue
            if self._redis is not None:
                try:
                    self._redis.delete(f"tvseq:{key}")
                except Exception as e:
                    logger.warning(f"Redis 释放幂等失败 {key}: {e}")
            with self._lock:
                self._purge_mem(now)
                file_map = self._load_file()
                for k, exp in file_map.items():
                    self._mem[k] = max(self._mem.get(k, 0), exp)
                if key in self._mem:
                    self._mem.pop(key, None)
                    freed += 1
                self._save_file(dict(self._mem))
        return freed

    def release_bar_open_keys(self, symbol: str, bar_index: int) -> int:
        """
        同 K 线先平后开：平仓后释放 LONG/SHORT/OPEN 幂等，允许更大 seq 再开。
        不释放 CLOSE* 键。
        """
        sym = str(symbol or "UNKNOWN").upper().replace(".P", "")
        prefix = f"{sym}_{int(bar_index)}_"
        open_acts = ("LONG", "SHORT", "OPEN")
        candidates: List[str] = []
        now = time.time()
        with self._lock:
            self._purge_mem(now)
            file_map = self._load_file()
            for k, exp in file_map.items():
                self._mem[k] = max(self._mem.get(k, 0), exp)
            for k in list(self._mem.keys()):
                if not str(k).startswith(prefix):
                    continue
                upper = str(k).upper()
                if any(upper.endswith(f"_{a}") for a in open_acts):
                    candidates.append(k)
        # 兼容旧键无 action 后缀：{sym}_{bar}_{seq}
        for seq_guess in range(1, 32):
            legacy = f"{sym}_{int(bar_index)}_{seq_guess}"
            candidates.append(legacy)
        # 去重
        uniq = list(dict.fromkeys(candidates))
        n = self.release_keys(uniq)
        if n:
            logger.info(
                f"📬 TV时序先平后开：释放 bar={bar_index} 开仓幂等 {n} 键 "
                f"(允许同 bar 再开)"
            )
        return n


_global_idempotency = None
_idem_lock = threading.Lock()


def get_idempotency_store() -> SeqIdempotencyStore:
    global _global_idempotency
    with _idem_lock:
        if _global_idempotency is None:
            _global_idempotency = SeqIdempotencyStore()
        return _global_idempotency


class TVSeqBuffer:
    """
    按品种缓冲：收到消息后按 (bar_index, seq) 排序弹出。
    若 seq>1 且前置缺失 → 暂存至 pending_wait 超时，再按已有顺序冲刷并报警。
    无 bar_index/seq 的旧信号：同样走 LEGACY_SETTLE_SEC 缓存窗口再冲刷。
    同 bar 先平后开：CLOSE 后 call release_bar_for_reentry() 再收 OPEN。
    """

    def __init__(
        self,
        symbol: str,
        pending_wait_sec: float = SEQ_PENDING_WAIT_SEC,
        on_gap_alert=None,
    ):
        self.symbol = str(symbol or "").upper()
        self.pending_wait = float(pending_wait_sec)
        self.on_gap_alert = on_gap_alert  # callable(str)
        self._lock = threading.RLock()
        # bar -> list of (seq, payload) 按到达追加；同 seq 不同波次可并存（刷新仓）
        self._bars: Dict[int, List[Tuple[int, dict]]] = defaultdict(list)
        self._bar_first_ts: Dict[int, float] = {}
        self._legacy: List[dict] = []
        self._legacy_first_ts: Optional[float] = None
        self._cv = threading.Condition(self._lock)

    def _depth_unlocked(self) -> int:
        n = len(self._legacy)
        for items in self._bars.values():
            n += len(items)
        return n

    def depth(self) -> int:
        with self._lock:
            return self._depth_unlocked()

    def release_bar_for_reentry(self, bar_index: int) -> int:
        """CLOSE 后调用：释放开仓幂等，清空本 bar 缓冲中已消费的开仓槽位占用。"""
        store = get_idempotency_store()
        n = store.release_bar_open_keys(self.symbol, int(bar_index))
        return n

    def add(self, payload: dict) -> str:
        """
        返回: "queued" | "duplicate" | "legacy"
        """
        bi, sq = extract_seq_meta(payload)
        if bi is None or sq is None:
            with self._cv:
                # 窗口内相同 action+price 合并为一条
                act = str((payload or {}).get("action", "") or "").strip().upper()
                try:
                    px = round(float((payload or {}).get("price") or 0), 2)
                except (TypeError, ValueError):
                    px = 0.0
                for existing in self._legacy:
                    ex_act = str((existing or {}).get("action", "") or "").strip().upper()
                    try:
                        ex_px = round(float((existing or {}).get("price") or 0), 2)
                    except (TypeError, ValueError):
                        ex_px = 0.0
                    if ex_act == act and ex_px == px:
                        logger.warning(
                            f"📬 [{self.symbol}] legacy 窗口内相同消息合并 "
                            f"action={act} px={px}"
                        )
                        return "duplicate"
                if not self._legacy:
                    self._legacy_first_ts = time.time()
                self._legacy.append(dict(payload))
                logger.info(
                    f"📬 [{self.symbol}] legacy 入缓存窗口 action={act} "
                    f"| 深度 {self._depth_unlocked()} "
                    f"| settle={LEGACY_SETTLE_SEC:.1f}s"
                )
                self._cv.notify_all()
            return "legacy"

        sym = str(
            payload.get("symbol")
            or payload.get("ticker")
            or self.symbol
            or "UNKNOWN"
        )
        action = str(payload.get("action", "") or "").strip().upper()
        key = make_seq_key(sym, bi, sq, action)
        store = get_idempotency_store()
        if store.is_duplicate_and_mark(key):
            logger.warning(
                f"📬 [{self.symbol}] TV时序去重丢弃 key={key} "
                f"action={action}"
            )
            return "duplicate"

        with self._cv:
            # 同 bar/seq/action 仍在缓冲未弹出 → 真重复
            for existing_sq, existing_pl in self._bars[bi]:
                if existing_sq != sq:
                    continue
                ex_act = str(existing_pl.get("action", "") or "").strip().upper()
                if ex_act == action:
                    logger.warning(
                        f"📬 [{self.symbol}] 同 bar/seq/action 缓冲已有 → 丢弃 {key}"
                    )
                    return "duplicate"
            self._bars[bi].append((sq, dict(payload)))
            self._bar_first_ts.setdefault(bi, time.time())
            logger.info(
                f"📬 [{self.symbol}] 时序入缓冲 bar={bi} seq={sq} "
                f"action={action} | 缓冲深度 {self._depth_unlocked()}"
            )
            self._cv.notify_all()
        return "queued"

    def _missing_prefix(self, bar: int, seqs: List[int]) -> List[int]:
        if not seqs:
            return []
        mx = max(seqs)
        have = set(seqs)
        return [i for i in range(1, mx + 1) if i not in have]

    def _flush_legacy_locked(self, now: float) -> List[dict]:
        """无时序消息：首包后 settle；仅开仓时延长等待，拼迟到平仓。"""
        if not self._legacy:
            return []
        age = now - float(self._legacy_first_ts or now)
        acts = [
            str((m or {}).get("action", "") or "").strip().upper()
            for m in self._legacy
        ]
        has_close = any(a in FLATTEN_PRIORITY_ACTIONS or a.startswith("CLOSE") for a in acts)
        has_open = any(a in ("LONG", "SHORT") for a in acts)
        coalesced = has_close and has_open
        only_open = has_open and not has_close
        if coalesced or age >= MAX_PAIR_WAIT_SEC:
            settle_done = True
        elif only_open and age < OPEN_ALONE_MAX_WAIT_SEC:
            settle_done = False
        elif age >= LEGACY_SETTLE_SEC:
            settle_done = True
        else:
            settle_done = False
        if not settle_done:
            return []
        out = list(self._legacy)
        self._legacy = []
        self._legacy_first_ts = None
        logger.info(
            f"📬 [{self.symbol}] legacy 缓存到期冲刷 n={len(out)} "
            f"age={age:.2f}s coalesced={coalesced} only_open={only_open}"
        )
        return out

    def _flush_ready_locked(self, now: float) -> List[dict]:
        out: List[dict] = []
        out.extend(self._flush_legacy_locked(now))

        for bar in sorted(self._bars.keys()):
            items = self._bars[bar]
            if not items:
                continue
            seqs = sorted({sq for sq, _ in items})
            missing = self._missing_prefix(bar, seqs)
            age = now - float(self._bar_first_ts.get(bar, now))
            wait_done = age >= self.pending_wait
            coalesced = bar_has_close_and_open(items)
            only_open = (
                any(is_open_action((pl or {}).get("action")) for _, pl in items)
                and not any(is_close_action((pl or {}).get("action")) for _, pl in items)
            )
            if coalesced or age >= MAX_PAIR_WAIT_SEC:
                settle_done = True
            elif only_open and age < OPEN_ALONE_MAX_WAIT_SEC:
                # 开仓先到：再等一会儿拼迟到平仓，避免先开后秒平
                settle_done = False
            elif age >= SAME_BAR_SETTLE_SEC:
                settle_done = True
            else:
                settle_done = False

            if missing and not wait_done:
                break
            if not settle_done:
                break

            if missing and wait_done:
                msg = (
                    f"[{self.symbol}] bar_index={bar} 前置 seq 缺失 {missing} "
                    f"已等待 {age:.1f}s → 按先平后开规则冲刷"
                )
                logger.error(f"⚠️ {msg}")
                if self.on_gap_alert:
                    try:
                        self.on_gap_alert(msg)
                    except Exception:
                        pass

            # 铁律：同 bar 开平并存 → CLOSE* 永远先于 LONG/SHORT（无视 TV seq 颠倒）
            items_sorted = sort_same_bar_items(items)
            if coalesced:
                chain = " → ".join(
                    f"seq{sq}:{str((pl or {}).get('action', '')).upper()}"
                    for sq, pl in items_sorted
                )
                raw_by_seq = " | ".join(
                    f"seq{sq}:{str((pl or {}).get('action', '')).upper()}"
                    for sq, pl in sorted(items, key=lambda t: (t[0],))
                )
                logger.info(
                    f"📬 [{self.symbol}] 同bar强制先平后开 bar={bar} | "
                    f"TV原始(按seq) {raw_by_seq} → 执行序 {chain}"
                )

            for sq, pl in items_sorted:
                out.append(pl)
            self._bars.pop(bar, None)
            self._bar_first_ts.pop(bar, None)

        return out

    def pop_ready(self, timeout: float = 1.0) -> List[dict]:
        deadline = time.time() + max(0.05, float(timeout))
        with self._cv:
            while True:
                now = time.time()
                ready = self._flush_ready_locked(now)
                if ready:
                    return ready
                remain = deadline - time.time()
                if remain <= 0:
                    return self._flush_ready_locked(time.time())
                self._cv.wait(timeout=min(remain, 0.25))


def sort_webhooks_by_seq(messages: List[dict]) -> List[dict]:
    """
    批处理工具：先 bar_index；同 bar 内动作优先（CLOSE→OPEN）再 seq。
    无时序的保持相对顺序垫后。
    """
    timed = []
    legacy = []
    for i, m in enumerate(messages or []):
        bi, sq = extract_seq_meta(m)
        if bi is None:
            legacy.append((i, m))
        else:
            rank = action_exec_rank((m or {}).get("action"))
            timed.append((bi, rank, sq, i, m))
    timed.sort(key=lambda x: (x[0], x[1], x[2], x[3]))
    return [m for _, _, _, _, m in timed] + [m for _, m in legacy]


def reorder_batch_close_then_open(messages: List[dict]) -> List[dict]:
    """消费侧二次保险：同 bar 开平并存时重排为永远先平后开。"""
    return sort_webhooks_by_seq(list(messages or []))


def collapse_batch_for_execution(messages: List[dict]) -> List[dict]:
    """
    缓存窗口到期后折叠执行集（无论到达顺序）：
    1. P0 平仓（CLOSE_QUICK_EXIT / CLOSE_RSI_EXIT）→ 只保留一条（幂等）
    2. 其它非开仓消息（RECONCILE 等）按原序保留
    3. P1 开仓（LONG/SHORT）→ 只保留最新一条（msgs[-1]）
    最终顺序：平仓 → 其它 → 开仓（至多一条）
    """
    msgs = list(messages or [])
    if not msgs:
        return []

    exit_msgs: List[dict] = []
    entry_msgs: List[dict] = []
    other_msgs: List[dict] = []
    seen_exit_fp = set()
    seen_entry_fp = set()
    seen_other_fp = set()

    def _fp(m: dict) -> Tuple[str, str, float]:
        a = str((m or {}).get("action", "") or "").strip().upper()
        sym = str(
            (m or {}).get("symbol") or (m or {}).get("ticker") or ""
        ).strip().upper()
        try:
            px = round(float((m or {}).get("price") or 0), 2)
        except (TypeError, ValueError):
            px = 0.0
        return (a, sym, px)

    for m in msgs:
        act = str((m or {}).get("action", "") or "").strip().upper()
        fp = _fp(m)
        if is_flatten_action(act):
            if fp in seen_exit_fp:
                continue  # 窗口内相同平仓合并
            seen_exit_fp.add(fp)
            exit_msgs.append(m)
        elif is_open_action(act):
            if fp in seen_entry_fp:
                # 同键重复开仓：丢掉旧的，保留最新到达
                entry_msgs = [x for x in entry_msgs if _fp(x) != fp]
            seen_entry_fp.add(fp)
            entry_msgs.append(m)
        else:
            if fp in seen_other_fp:
                continue
            seen_other_fp.add(fp)
            other_msgs.append(m)

    out: List[dict] = []
    if exit_msgs:
        # 只执行一次平仓：优先 QUICK，否则取首条
        preferred = next(
            (
                m
                for m in exit_msgs
                if str((m or {}).get("action", "")).upper() == "CLOSE_QUICK_EXIT"
            ),
            exit_msgs[0],
        )
        out.append(preferred)
        if len(exit_msgs) > 1:
            acts = ",".join(
                str((m or {}).get("action", "")).upper() for m in exit_msgs
            )
            logger.info(
                f"📬 缓存折叠：{len(exit_msgs)} 条平仓 → 只执行 "
                f"{str((preferred or {}).get('action', '')).upper()}（丢弃重复: {acts}）"
            )

    out.extend(other_msgs)

    if entry_msgs:
        entry = entry_msgs[-1]
        out.append(entry)
        if len(entry_msgs) > 1:
            acts = " → ".join(
                str((m or {}).get("action", "")).upper() for m in entry_msgs
            )
            logger.info(
                f"📬 缓存折叠：{len(entry_msgs)} 条开仓 → 只执行最新 "
                f"{str((entry or {}).get('action', '')).upper()}（序列: {acts}）"
            )

    return out
