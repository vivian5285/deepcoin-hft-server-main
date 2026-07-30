#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""深币 Telegram Bot 通知 — 与钉钉并行播报，统一格式"""
import os
import time
import logging
import threading
import requests
from datetime import datetime
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))
logger = logging.getLogger(__name__)

# TG 配置（从 .env 读取，与钉钉配置并行）
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "").strip()
TG_API_URL = f"https://api.telegram.org/bot{TG_BOT_TOKEN}" if TG_BOT_TOKEN else ""

EXCHANGE_LABEL = "深币 Deepcoin"
UNIT_LABEL = "张"

# 去重（与钉钉一致：防雷同告警刷屏）
TG_TITLE_DEDUP_SEC = float(os.getenv("TG_DEDUP_SEC", "120"))
TG_ALERT_DEDUP_SEC = float(os.getenv("TG_ALERT_DEDUP_SEC", "600"))
_title_dedup_lock = threading.Lock()
_title_dedup_ts = {}


def _is_enabled():
    return bool(TG_BOT_TOKEN and TG_CHAT_ID)


def _title_dedup_window(title):
    t = str(title or "")
    if "异常" in t or "告警" in t or "危险" in t or "拒绝" in t:
        return TG_ALERT_DEDUP_SEC
    return TG_TITLE_DEDUP_SEC


def _send_tg(payload, method="sendMessage"):
    """通过 Telegram Bot API 发送请求。"""
    if not _is_enabled():
        return
    url = f"{TG_API_URL}/{method}"
    try:
        resp = requests.post(url, json=payload, timeout=8)
        data = resp.json()
        if not data.get("ok"):
            logger.warning(f"TG API error: {data.get('description', 'unknown')}")
    except Exception as e:
        logger.error(f"TG 发送失败: {e}")


def send_text(text, parse_mode="Markdown"):
    """发送纯文本消息。"""
    if not _is_enabled():
        return
    raw = str(text or "")
    dedup_key = raw[:96]
    now = time.time()
    window = _title_dedup_window(raw)
    with _title_dedup_lock:
        dead = [k for k, ts in _title_dedup_ts.items() if now - float(ts) > max(window, TG_TITLE_DEDUP_SEC) * 4]
        for k in dead:
            _title_dedup_ts.pop(k, None)
        last = float(_title_dedup_ts.get(dedup_key) or 0)
        if last > 0 and now - last < window:
            return
        _title_dedup_ts[dedup_key] = now

    payload = {
        "chat_id": TG_CHAT_ID,
        "text": raw,
        "parse_mode": parse_mode if parse_mode else None,
    }
    _send_tg(payload)


def send_html(html_text, method="sendMessage"):
    """发送 HTML 格式消息。"""
    if not _is_enabled():
        return
    raw = str(html_text or "")
    dedup_key = raw[:96]
    now = time.time()
    window = _title_dedup_window(raw)
    with _title_dedup_lock:
        dead = [k for k, ts in _title_dedup_ts.items() if now - float(ts) > max(window, TG_TITLE_DEDUP_SEC) * 4]
        for k in dead:
            _title_dedup_ts.pop(k, None)
        last = float(_title_dedup_ts.get(dedup_key) or 0)
        if last > 0 and now - last < window:
            return
        _title_dedup_ts[dedup_key] = now

    payload = {
        "chat_id": TG_CHAT_ID,
        "text": raw,
        "parse_mode": "HTML",
    }
    _send_tg(payload)


def _fmt_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _esc_md(text):
    """Escape special MarkdownV2 characters."""
    special = ["_", "*", "[", "]", "(", ")", "~", "`", ">", "#", "+", "-", "=", "|", "{", "}", ".", "!"]
    for ch in special:
        text = text.replace(ch, f"\\{ch}")
    return text


def _fmt_side(side):
    emoji = "🟢 LONG" if side == "LONG" else "🔴 SHORT"
    return emoji


def _fmt_tp(tps):
    lines = []
    for i, px in enumerate(tps or [], 1):
        if px > 0:
            lines.append(f"  TP{i}: `{px:.2f}`")
    return "\n".join(lines) if lines else "TP1/2/3: —"


def report_position_opened(side, qty, entry, regime, atr, tps, tv_sl, leverage, tier_label=""):
    """开仓播报。"""
    if not _is_enabled():
        return
    tier_str = f" | {_esc_md(tier_label)}" if tier_label else ""
    text = (
        f"📈 *{_esc_md(EXCHANGE_LABEL)} 开仓通知*\n"
        f"{_fmt_time()}\n"
        f"\n"
        f"{_fmt_side(side)} | {qty} {UNIT_LABEL}\n"
        f"入场价: `{entry:.2f}`\n"
        f"档位: R{regime}{tier_str}\n"
        f"ATR: `{atr:.2f}`\n"
        f"\n"
        f"*止盈:*\n{_fmt_tp(tps)}\n"
        f"*止损:* `{tv_sl:.2f}`\n"
        f"杠杆: {leverage}x\n"
    )
    send_text(text)


def report_position_closed(side, qty, entry, close_px, close_type, regime, pnl, tier_label=""):
    """平仓播报。"""
    if not _is_enabled():
        return
    tier_str = f" | {_esc_md(tier_label)}" if tier_label else ""
    emoji = "✅" if pnl is not None and pnl >= 0 else "❌"
    pnl_str = f"{pnl:+.2f}%" if pnl is not None else "—"
    text = (
        f"{emoji} *{_esc_md(EXCHANGE_LABEL)} 平仓通知*\n"
        f"{_fmt_time()}\n"
        f"\n"
        f"{_fmt_side(side)} | {qty} {UNIT_LABEL}\n"
        f"入场: `{entry:.2f}` → 平仓: `{close_px:.2f}`\n"
        f"档位: R{regime}{tier_str}\n"
        f"平仓类型: `{_esc_md(close_type)}`\n"
        f"收益率: {pnl_str}\n"
    )
    send_text(text)


def report_system_alert(title, detail, level="紧急", suggestion=""):
    """系统告警。"""
    if not _is_enabled():
        return
    emoji = "🚨"
    if level == "危险":
        emoji = "🔴"
    elif level == "警告":
        emoji = "⚠️"
    elif level == "信息":
        emoji = "ℹ️"

    text = (
        f"{emoji} *系统告警*\n"
        f"{_fmt_time()}\n"
        f"\n"
        f"*{_esc_md(title)}*\n"
        f"\n"
        f"{_esc_md(detail)}\n"
    )
    if suggestion:
        text += f"\n💡 {_esc_md(suggestion)}\n"
    send_text(text)


def report_radar_activation(side, qty, entry, curr_px, new_sl, regime):
    """雷达激活播报。"""
    if not _is_enabled():
        return
    text = (
        f"📡 *雷达激活*\n"
        f"{_fmt_time()}\n"
        f"\n"
        f"{_fmt_side(side)} | {qty} {UNIT_LABEL}\n"
        f"入场: `{entry:.2f}` → 现价: `{curr_px:.2f}`\n"
        f"雷达止损: `{new_sl:.2f}`\n"
        f"档位: R{regime}\n"
    )
    send_text(text)


def report_reentry_opened(side, qty, entry, attempt, regime):
    """重入开仓播报。"""
    if not _is_enabled():
        return
    text = (
        f"🔄 *重入开仓*\n"
        f"{_fmt_time()}\n"
        f"\n"
        f"{_fmt_side(side)} | {qty} {UNIT_LABEL}\n"
        f"重入价: `{entry:.2f}`\n"
        f"第 {attempt} 次重入 | R{regime}\n"
    )
    send_text(text)
