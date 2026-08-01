#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""深币 Telegram Bot 通知 — 统一播报格式"""
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

# TG 配置
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "").strip()
TG_API_URL = f"https://api.telegram.org/bot{TG_BOT_TOKEN}" if TG_BOT_TOKEN else ""

EXCHANGE_LABEL = "深币 Deepcoin"
LEVERAGE_LABEL = "5x"
UNIT_LABEL = "张"

# 去重
TG_TITLE_DEDUP_SEC = float(os.getenv("TG_DEDUP_SEC", "120"))
TG_ALERT_DEDUP_SEC = float(os.getenv("TG_ALERT_DEDUP_SEC", "600"))
VERIFY_TAG = "实盘核查通过"
VERIFY_DELAY_MARK = "REST 同步略延迟"
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


def _fmt_tp_levels(tp_pxs):
    """格式化TP等级"""
    lines = []
    for i, px in enumerate(tp_pxs or [], 1):
        if px > 0:
            lines.append(f"TP{i}: `{px:.2f}`")
    return " / ".join(lines) if lines else "—"


# ========== 通知函数 ==========

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


def report_radar_activation(side, qty, entry, curr_px, new_sl, regime, profit_pct=0):
    """雷达激活播报（文档2.6：通知数值必须匹配实际状态）"""
    if not _is_enabled():
        return
    profit_str = f"{profit_pct:+.2f}%" if profit_pct != 0 else "—"
    text = (
        f"📡 *雷达激活 · 追踪起步*\n"
        f"{_fmt_time()}\n"
        f"\n"
        f"{_fmt_side(side)} | {qty} {UNIT_LABEL}\n"
        f"入场: `{entry:.2f}` → 现价: `{curr_px:.2f}`\n"
        f"雷达止损: `{new_sl:.2f}`\n"
        f"档位: R{regime} | 浮盈: {profit_str}\n"
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


def report_manual_position_change(action_type, old_qty, new_qty, new_entry_price,
                                 verify_note="", tp_audit=None, verified=True):
    """手动仓位变动"""
    if not _is_enabled():
        return
    action_txt = "手动增仓" if "加仓" in str(action_type) else "手动部分减仓"
    text = (
        f"🔄 *仓位变动*\n"
        f"{_fmt_time()}\n"
        f"\n"
        f"动作: *{action_txt}*\n"
        f"数量: `{old_qty}` → `{new_qty}` {UNIT_LABEL}\n"
        f"均价: `{new_entry_price:.2f}`\n"
    )
    if tp_audit:
        text += f"TP审计: `{tp_audit}`\n"
    if verify_note:
        text += f"核实: {verify_note}\n"
    send_text(text)


def report_recover_takeover(side, qty, entry, tv_tps, regime, radar_active, sl_price,
                           verify_note="", tp_matched=0, tp_expected=0, tp_audit=None,
                           last_tv_signal=None, radar_sl_ok=True, pnl_label="",
                           defense_plan="", shield_status="", radar_progress=0.0,
                           tv_aligned=True, qty_aligned=True, initial_qty=0,
                           tp_consumed_levels=None):
    """VPS 重启接管报告"""
    if not _is_enabled():
        return
    if radar_active:
        radar_txt = f"保本起步 (进度 {radar_progress:.0%})"
    else:
        radar_txt = "待命"
    tp_txt = _fmt_tp_levels(tv_tps)
    text = (
        f"🔄 *VPS 重启接管*\n"
        f"{_fmt_time()}\n"
        f"\n"
        f"{_fmt_side(side)} | {qty} {UNIT_LABEL}\n"
        f"入场价: `{entry:.2f}` | 档位: R{regime}\n"
        f"止盈: {tp_txt}\n"
        f"雷达: {radar_txt}\n"
        f"止损: `{sl_price:.2f}`\n"
    )
    if verify_note:
        text += f"核实: {verify_note}\n"
    send_text(text)


def report_tv_signal_received(action, entry_type="", price=0, regime=3, atr=0,
                             tv_sl=0, risk_pct=0, leverage=None, qty_ratio=1.0,
                             reason="", vps_sizing_meta=None):
    """TV信号接收"""
    if not _is_enabled():
        return
    text = (
        f"📡 *TV信号接收*\n"
        f"{_fmt_time()}\n"
        f"\n"
        f"动作: *{action}*\n"
        f"价格: `{price:.2f}` | ATR: `{atr:.2f}`\n"
        f"止损: `{tv_sl:.2f}` | 档位: R{regime}\n"
    )
    if reason:
        text += f"原因: {reason}\n"
    send_text(text)


def report_principal_snapshot(reason, principal, regime=None, margin_pct=None,
                             target_qty=None, leverage=None, verify_note="",
                             vps_sizing_meta=None):
    """本金快照"""
    if not _is_enabled():
        return
    text = (
        f"📸 *本金快照*\n"
        f"{_fmt_time()}\n"
        f"\n"
        f"时机: {reason}\n"
        f"本金: *{principal:.2f}* USDT\n"
    )
    if target_qty:
        text += f"目标仓位: {target_qty} {UNIT_LABEL}\n"
    send_text(text)


def report_supervisor_close(reason, verify_note="", verified=True, swept_dust=False,
                            tv_pnl_pct=None, tv_side="", tv_price=None,
                            close_action="", tv_regime=None, tv_atr=None,
                            tv_field_sources=None,
                            close_type="", tv_reason="", entry_px=None,
                            closed_qty=None, live_exit_px=None):
    """平仓报告"""
    if not _is_enabled():
        return
    emoji = "✅" if (tv_pnl_pct is not None and tv_pnl_pct >= 0) else "❌"
    pnl_str = f"{tv_pnl_pct:+.2f}%" if tv_pnl_pct is not None else "—"
    text = (
        f"{emoji} *平仓通知*\n"
        f"{_fmt_time()}\n"
        f"\n"
        f"原因: *{reason}*\n"
        f"方向: {tv_side}\n"
    )
    if entry_px:
        text += f"入场价: `{entry_px:.2f}`\n"
    if closed_qty:
        text += f"平仓数量: {closed_qty} {UNIT_LABEL}\n"
    if tv_pnl_pct is not None:
        text += f"收益率: {pnl_str}\n"
    if verify_note:
        text += f"核实: {verify_note}\n"
    if tv_field_sources:
        from webhook_parser import format_tv_field_sources
        text += f"TV字段: {format_tv_field_sources(tv_field_sources)}\n"
    send_text(text)


def report_tv_sl_updated(side, live_qty, entry, tv_sl, exchange_stop=None,
                         radar_active=False, radar_sl=None, regime=3,
                         verify_note="", verified=True, curr_px=None, profit_pct=0):
    """TV 硬止损更新（文档2.6：通知数值必须匹配实际状态）"""
    if not _is_enabled():
        return
    profit_str = f"{profit_pct:+.2f}%" if profit_pct != 0 else "—"
    text = (
        f"🔒 *止损更新*\n"
        f"{_fmt_time()}\n"
        f"\n"
        f"{_fmt_side(side)} | {live_qty} {UNIT_LABEL}\n"
        f"入场: `{entry:.2f}`\n"
        f"TV 硬止损: `{tv_sl:.2f}`\n"
    )
    if exchange_stop:
        text += f"交易所止损: `{exchange_stop:.2f}`\n"
    if curr_px and curr_px > 0:
        text += f"现价: `{curr_px:.2f}` | 浮盈: {profit_str}\n"
    if radar_active:
        text += f"雷达止损: `{radar_sl:.2f}`\n"
    else:
        text += "雷达: 待命\n"
    if verify_note:
        text += f"核实: {verify_note}\n"
    send_text(text)


def report_tv_tp_updated(side, live_qty, entry, old_tps=None, new_tps=None,
                         placed=0, regime=3, verify_note="", verified=True, curr_px=0):
    """止盈更新"""
    if not _is_enabled():
        return
    old_txt = _fmt_tp_levels(old_tps)
    new_txt = _fmt_tp_levels(new_tps)
    text = (
        f"🚀 *止盈更新*\n"
        f"{_fmt_time()}\n"
        f"\n"
        f"{_fmt_side(side)} | {live_qty} {UNIT_LABEL}\n"
        f"入场: `{entry:.2f}`\n"
        f"原TP: {old_txt}\n"
        f"新TP: {new_txt}\n"
    )
    if curr_px:
        text += f"现价: `{curr_px:.2f}`\n"
    if verify_note:
        text += f"核实: {verify_note}\n"
    send_text(text)


def report_shield_disarmed(side, live_qty, entry, cancelled_count, reason="",
                           radar_progress=0.0, verify_note=""):
    """止损撤防"""
    if not _is_enabled():
        return
    text = (
        f"🛡️ *止损撤防*\n"
        f"{_fmt_time()}\n"
        f"\n"
        f"{_fmt_side(side)} | {live_qty} {UNIT_LABEL}\n"
        f"入场: `{entry:.2f}`\n"
        f"撤销笔数: {cancelled_count}\n"
        f"雷达进度: {radar_progress:.0%}\n"
    )
    if reason:
        text += f"原因: {reason}\n"
    send_text(text)


def report_adverse_shield_armed(side, entry, live_qty, adverse_pct, tier_prices,
                               tier_pcts, verify_note=""):
    """止损武装"""
    if not _is_enabled():
        return
    stop_px = tier_prices[0] if tier_prices else entry
    text = (
        f"🛡️ *止损武装*\n"
        f"{_fmt_time()}\n"
        f"\n"
        f"{_fmt_side(side)} | {live_qty} {UNIT_LABEL}\n"
        f"入场: `{entry:.2f}`\n"
        f"止损价: `{stop_px:.2f}`\n"
    )
    if verify_note:
        text += f"核实: {verify_note}\n"
    send_text(text)


def report_radar_activated(side, qty, entry, new_sl, radar_progress=1.0, regime=3,
                           shield_cleared=True, verify_note="", verified=True,
                           breathing_coefficient=None, trail_dist=None,
                           symbol=None, open_kind=None, activation_frac=None,
                           activation_price=None, trigger_gate="", tier=None,
                           entry_sl=None, curr_px=None, profit_pct=0):
    """雷达激活（文档2.6：通知数值必须匹配实际状态）"""
    if not _is_enabled():
        return
    kind = str(open_kind or "").strip() or "首次开仓"
    profit_str = f"{profit_pct:+.2f}%" if profit_pct != 0 else "—"
    text = (
        f"📡 *雷达激活 · 追踪起步 · {kind}*\n"
        f"{_fmt_time()}\n"
        f"\n"
        f"{_fmt_side(side)} | {qty} {UNIT_LABEL}\n"
        f"入场: `{entry:.2f}`\n"
    )
    if curr_px and curr_px > 0:
        text += f"现价: `{curr_px:.2f}` | 浮盈: {profit_str}\n"
    text += f"雷达止损: `{new_sl:.2f}`\n"
    text += f"档位: R{regime} | 进度: {radar_progress:.0%}\n"
    if trigger_gate:
        text += f"激活门: {trigger_gate}\n"
    if activation_price > 0:
        text += f"门限价: `{activation_price:.2f}`\n"
    if verify_note:
        text += f"核实: {verify_note}\n"
    send_text(text)


def report_radar_guardian_realigned(side, qty, tp_audit=None, verify_note=""):
    """雷达守护对齐"""
    if not _is_enabled():
        return
    text = (
        f"📡 *雷达守护对齐*\n"
        f"{_fmt_time()}\n"
        f"\n"
        f"{_fmt_side(side)} | {qty} {UNIT_LABEL}\n"
    )
    if tp_audit:
        text += f"TP审计: {tp_audit}\n"
    send_text(text)


def report_radar_regime_cap_trim(*args, **kwargs):
    """已废除的档位裁减通知"""
    return


def report_shield_tier_fill(side, tier_pct, tier_price, filled_qty, remain_qty,
                            entry_px, remaining_tiers=None, verify_note=""):
    """止损层成交"""
    if not _is_enabled():
        return
    text = (
        f"🛡️ *止损层成交*\n"
        f"{_fmt_time()}\n"
        f"\n"
        f"{_fmt_side(side)} | 触发 -{tier_pct:.0%}\n"
        f"止损价: `{tier_price:.2f}`\n"
        f"成交: {filled_qty} {UNIT_LABEL}\n"
        f"剩余: {remain_qty} {UNIT_LABEL}\n"
        f"入场: `{entry_px:.2f}`\n"
    )
    send_text(text)


def report_tp_fill(tp_level, tp_price, filled_qty, remain_qty, entry_px, side,
                   regime, verify_note="", verified=True):
    """TP成交"""
    if not _is_enabled():
        return
    text = (
        f"🎯 *TP{tp_level}成交*\n"
        f"{_fmt_time()}\n"
        f"\n"
        f"TP{tp_level}: `{tp_price:.2f}`\n"
        f"成交: {filled_qty} {UNIT_LABEL}\n"
        f"剩余: {remain_qty} {UNIT_LABEL}\n"
        f"均价: `{entry_px:.2f}` | 档位: R{regime}\n"
    )
    if verify_note:
        text += f"核实: {verify_note}\n"
    send_text(text)


def report_intervention(qty, entry_px, new_sl, action_msg, verify_note="", verified=True,
                       curr_px=0, profit_pct=0):
    """追踪雷达止损移动（文档2.6：通知数值必须匹配实际状态）"""
    if not _is_enabled():
        return
    profit_str = f"{profit_pct:+.2f}%" if profit_pct != 0 else "—"
    text = (
        f"📈 *追踪雷达*\n"
        f"{_fmt_time()}\n"
        f"\n"
        f"数量: {qty} {UNIT_LABEL}\n"
        f"入场: `{entry_px:.2f}`\n"
        f"新止损: `{new_sl:.2f}`\n"
    )
    if curr_px and curr_px > 0:
        text += f"现价: `{curr_px:.2f}` | 浮盈: {profit_str}\n"
    text += f"动作: {action_msg}\n"
    if verify_note:
        text += f"核实: {verify_note}\n"
    send_text(text)


def report_smart_same_dir_decision(side, decision, live_entry, tv_price, diff_pct,
                                   threshold_pct, open_regime, tv_regime, open_atr,
                                   tv_atr, qty, tp_audit=None, verify_note=""):
    """同向决策"""
    if not _is_enabled():
        return
    decision_map = {
        "skip_duplicate_flat": "短时重复同向 · 已忽略",
        "reentry_atr_changed": "ATR变化 · 先平后开",
        "reentry_regime_changed": "参数刷新 · 先平后开",
        "reentry_spread_ok": "理论价差满足 · 先平后开",
    }
    decision_txt = decision_map.get(decision, decision)
    text = (
        f"🧠 *智能筛选*\n"
        f"{_fmt_time()}\n"
        f"\n"
        f"决策: *{decision_txt}*\n"
        f"{_fmt_side(side)} | {qty} {UNIT_LABEL}\n"
        f"入场: `{live_entry:.2f}` | TV: `{tv_price:.2f}`\n"
        f"ATR: 开仓{open_atr:.2f} / TV{tv_atr:.2f}\n"
        f"价差: {diff_pct:.3f}% / 阈值 {threshold_pct}%\n"
    )
    if verify_note:
        text += f"核实: {verify_note}\n"
    send_text(text)


def report_tv_position_add(side, entry_type, add_qty, old_qty, new_qty, old_entry,
                          new_entry, tv_sl=0, risk_pct=0, leverage=None,
                          qty_ratio=1.0, verify_note="", verified=True,
                          base_qty=0, vps_sizing_meta=None, add_count=0,
                          max_add_times=2, regime=3, tp_audit="", radar_note="",
                          open_regime=None, tp_ratio_label=""):
    """加仓报告"""
    if not _is_enabled():
        return
    type_label = {
        "PYRAMID": "金字塔加仓",
        "PROFIT_ADD": "浮盈加仓",
    }.get(str(entry_type or "").upper(), str(entry_type or "加仓"))
    text = (
        f"➕ *{type_label}*\n"
        f"{_fmt_time()}\n"
        f"\n"
        f"{_fmt_side(side)} | +{add_qty} {UNIT_LABEL}\n"
        f"数量: `{old_qty}` → `{new_qty}`\n"
        f"均价: `{old_entry:.2f}` → `{new_entry:.2f}`\n"
        f"止损: `{tv_sl:.2f}` | 档位: R{regime}\n"
        f"加仓次数: {add_count}/{max_add_times}\n"
    )
    if radar_note:
        text += f"雷达: {radar_note}\n"
    send_text(text)


def report_force_align(real_side, expected_side, verify_note=""):
    """方向对齐"""
    if not _is_enabled():
        return
    text = (
        f"🚨 *方向对齐*\n"
        f"{_fmt_time()}\n"
        f"\n"
        f"实盘方向: *{real_side}*\n"
        f"预期方向: {expected_side}\n"
        f"已执行核武全平\n"
    )
    if verify_note:
        text += f"核实: {verify_note}\n"
    send_text(text)


def report_recover_tp_repair(side, initial_qty, live_qty, entry, consumed_levels,
                              tp_audit=None, verify_note="", verified=True):
    """TP修复报告"""
    if not _is_enabled():
        return
    consumed_txt = ", ".join(f"TP{lv}" for lv in (consumed_levels or [])) or "无"
    text = (
        f"🎯 *TP修复*\n"
        f"{_fmt_time()}\n"
        f"\n"
        f"{_fmt_side(side)}\n"
        f"开仓: {initial_qty} {UNIT_LABEL} @ `{entry:.2f}`\n"
        f"剩余: {live_qty} {UNIT_LABEL}\n"
        f"已成交: {consumed_txt}\n"
    )
    send_text(text)


def report_recover_standby(verify_note="", version=""):
    """空仓待命报告"""
    if not _is_enabled():
        return
    text = (
        f"🔄 *空仓待命*\n"
        f"{_fmt_time()}\n"
        f"\n"
        f"系统状态: 空仓待命 · 挂单已清空\n"
        f"版本: {version or 'deepcoin_webhook'}\n"
    )
    send_text(text)


def report_supervisor_open(side, entry_price, tv_price, qty, tp_pxs, atr,
                          regime, verify_note="", verified=True, principal_balance=0,
                          margin_pct=0, margin_usdt=0, leverage=5,
                          vps_sizing_meta=None, tv_field_sources=None,
                          symbol="ETH-USDT-SWAP", unit_label="张", tp_audit=None):
    """开仓审计报告"""
    if not _is_enabled():
        return
    tp_txt = _fmt_tp_levels(tp_pxs)
    emoji = "✅" if verified else "⚠️"
    text = (
        f"{emoji} *开仓审计*\n"
        f"{_fmt_time()}\n"
        f"\n"
        f"{_fmt_side(side)} | {qty} {unit_label}\n"
        f"入价: `{entry_price:.2f}` (TV `{tv_price:.2f}`)\n"
        f"ATR: `{atr:.2f}` | 档位: R{regime}\n"
        f"止盈: {tp_txt}\n"
        f"保证金: {margin_usdt:.0f} USDT ({margin_pct:.1%})\n"
        f"本金余额: `{principal_balance:.2f}`\n"
        f"杠杆: {leverage}x\n"
    )
    if verify_note:
        text += f"核实: {verify_note}\n"
    send_text(text)
