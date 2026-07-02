#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""深币专用钉钉 — 全紫色主题，与币安金色播报区分"""
import os
import time
import hmac
import hashlib
import base64
import urllib.parse
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))
logger = logging.getLogger(__name__)

DINGTALK_WEBHOOK = os.getenv("DINGTALK_WEBHOOK", "")
DINGTALK_SECRET = os.getenv("DINGTALK_SECRET", "")

EXCHANGE_LABEL = "深币 Deepcoin"
LEVERAGE_LABEL = "20x"
UNIT_LABEL = "张"

# 深币专属紫色色板（与币安 #F3BA2F 金色完全区分）
P_TITLE = "#4B0082"
P_MAIN = "#9B59B6"
P_DEEP = "#6C3483"
P_LIGHT = "#BB8FCE"
P_ACCENT = "#8E44AD"
P_MUTED = "#A569BD"

FOOTER = "*🖨️ Quant AI · 深币紫金趋势大波段引擎*"
VERIFY_TAG = "✅ 实盘核查通过"
VERIFY_DELAY_MARK = "REST 同步略延迟"


def _verify_line(verify_note, ok_text, delay_text):
    if verify_note and VERIFY_DELAY_MARK in verify_note:
        return _p(delay_text, P_ACCENT)
    return _p(ok_text, P_MAIN)


def _p(text, color=P_MAIN):
    return f'<font color="{color}">{text}</font>'


def _get_signed_url():
    if not DINGTALK_WEBHOOK:
        return ""
    if not DINGTALK_SECRET:
        return DINGTALK_WEBHOOK
    ts = str(round(time.time() * 1000))
    hmac_code = hmac.new(
        DINGTALK_SECRET.encode('utf-8'),
        f'{ts}\n{DINGTALK_SECRET}'.encode('utf-8'),
        hashlib.sha256,
    ).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
    return f"{DINGTALK_WEBHOOK}&timestamp={ts}&sign={sign}"


def send_alert(title, data_dict, header_color=P_TITLE):
    signed_url = _get_signed_url()
    if not signed_url:
        return

    text_lines = [f"- **{k}** : {v}" for k, v in data_dict.items()]
    body_text = "\n".join(text_lines)
    now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    markdown_text = f"""### <font color="{header_color}">{title}</font>
> **⏱ 军区时间**：`{now_time}`
> **📍 阵地标识**：[ {EXCHANGE_LABEL} · 150分钟核心主阵地 ]
> **🔮 主题色带**：`深币紫金`（与币安金色播报区分）

---
{body_text}

---
{FOOTER}
"""
    payload = {"msgtype": "markdown", "markdown": {"title": title, "text": markdown_text}}
    try:
        requests.post(signed_url, json=payload, timeout=6)
    except Exception as e:
        logger.error(f"钉钉发送失败: {e}")


def get_regime_name(regime_code):
    names = {
        1: "🧊 [1档] 极弱震荡 (保守防守)",
        2: "🚶 [2档] 弱势波段 (稳健推升)",
        3: "🏃 [3档] 中势推升 (标准波段)",
        4: "🚀 [4档] 强势单边 (趋势吃满)",
    }
    shade = [P_MUTED, P_LIGHT, P_MAIN, P_ACCENT]
    idx = regime_code if 1 <= regime_code <= 4 else 0
    return _p(names.get(regime_code, "未知状态"), shade[idx - 1] if idx else P_MUTED)


def _format_tp_compare(tp_pxs, tv_tps=None):
    tp_str = ""
    for i, px in enumerate(tp_pxs):
        if px <= 0:
            continue
        prefix = "" if tp_str == "" else "\n\n  ➔ "
        line = f"{prefix}TP{i + 1} 物理挂单 `{px:.2f}`"
        if tv_tps and i < len(tv_tps) and tv_tps[i] > 0:
            diff = px - tv_tps[i]
            line += f" | TV理论 `{tv_tps[i]:.2f}` (偏差 {diff:+.2f})"
        tp_str += line
    return tp_str or "暂无有效 TP 价格"


def _format_tp_audit(audit, tv_tps=None):
    if not audit or not audit.get("levels"):
        return _format_tp_compare(tv_tps or [], tv_tps)
    lines = []
    for lv in audit["levels"]:
        if lv.get("price", 0) <= 0:
            continue
        prefix = "" if not lines else "\n\n  ➔ "
        if lv.get("status") == "ok":
            lines.append(
                f"{prefix}TP{lv['level']} ✅ `{lv['actual_qty']}` 张 @ `{lv['price']:.2f}` "
                f"(比例期望 `{lv['qty']}` 张)"
            )
        else:
            lines.append(
                f"{prefix}TP{lv['level']} ❌ 期望 `{lv['qty']}` 张 @ `{lv['price']:.2f}` "
                f"→ 状态 `{lv['status']}`"
                + (f" 实盘 `{lv.get('actual_qty', 0)}` 张" if lv.get("actual_qty") else "")
            )
    return "".join(lines) or "暂无有效 TP 审计"


def report_supervisor_open(side, entry_price, tv_price, qty, tp_pxs, atr, regime, tv_tps=None,
                           verify_note="", tp_audit=None):
    side_str = _p("🟣 开多 (LONG)", P_LIGHT) if side == "LONG" else _p("🟪 开空 (SHORT)", P_DEEP)
    slip_txt = (
        f"{(entry_price - tv_price if side == 'LONG' else tv_price - entry_price):+.2f} 刀"
        if tv_price > 0 else "未知"
    )

    data = {
        "🎛️ 趋势方向": side_str,
        "📊 市场强度": get_regime_name(regime),
        "💰 进场成本": _p(f"**{entry_price:.2f}** USDT (滑点: **{slip_txt}**)", P_MAIN),
        "📦 唯一头寸": _p(f"**{qty}** {UNIT_LABEL} ({EXCHANGE_LABEL} {LEVERAGE_LABEL} 稳健火力)", P_ACCENT),
        "🕸️ 止盈布防比对": _p(
            _format_tp_audit(tp_audit, tv_tps) if tp_audit else _format_tp_compare(tp_pxs, tv_tps),
            P_LIGHT,
        ),
        "📏 波动参考": _p(f"ATR = {atr:.4f}", P_MUTED),
        "📡 哨兵状态": _p(f"🟢 {VERIFY_TAG} | 限价 TP123 已挂，雷达待命", P_MAIN),
    }
    if verify_note:
        data["🔍 核查明细"] = _p(verify_note, P_MUTED)
    send_alert("🖨️ 战神出击：深币大级别主阵地建立", data)


def report_intervention(qty, entry_px, new_sl, action_msg, verify_note="", verified=True):
    data = {
        "🛡️ 战术动作": _p(action_msg, P_ACCENT),
        "📦 利润头寸": _p(f"`{qty}` {UNIT_LABEL}", P_MAIN),
        "💰 原始成本": _p(f"`{entry_px:.2f}` USDT", P_MUTED),
        "🔒 最新硬防线": _p(f"**{new_sl:.2f}** USDT (条件保本单已挂)", P_LIGHT),
        "📡 实盘核查": _verify_line(
            verify_note if not verified else "",
            f"{VERIFY_TAG} | 移动保本机制已触发",
            f"⏳ 止损已提交，{VERIFY_DELAY_MARK} | 移动保本机制已触发",
        ),
    }
    if verify_note:
        data["🔍 核查明细"] = _p(verify_note, P_MUTED)
    send_alert("📈 捷报：追踪雷达锁死趋势利润", data, P_DEEP)


def report_tp_fill(tp_level, tp_price, filled_qty, remain_qty, entry_px, side, regime,
                   verify_note="", verified=True):
    data = {
        "🎯 成交档位": _p(f"**TP{tp_level}** @ **{tp_price:.2f}** USDT", P_LIGHT),
        "📦 本次止盈": _p(f"`{filled_qty}` {UNIT_LABEL}", P_ACCENT),
        "📊 剩余头寸": _p(f"`{remain_qty}` {UNIT_LABEL}", P_MAIN),
        "💰 持仓均价": _p(f"`{entry_px:.2f}` USDT", P_MUTED),
        "🧭 方向/档位": _p(f"{side} | Regime {regime}", P_MUTED),
        "📡 实盘核查": _verify_line(
            verify_note if not verified else "",
            f"{VERIFY_TAG} | TP{tp_level} 限价止盈已成交",
            f"⏳ 止盈已成交，{VERIFY_DELAY_MARK} | 哨兵持续对齐",
        ),
    }
    if verify_note:
        data["🔍 核查明细"] = _p(verify_note, P_MUTED)
    send_alert(f"🎯 捷报：深币 TP{tp_level} 止盈成交", data, P_DEEP)


def report_manual_position_change(action_type, old_qty, new_qty, new_entry_price,
                                  verify_note="", tp_audit=None, verified=True):
    action_txt = _p("手动增仓", P_LIGHT) if "加仓" in action_type else _p("手动部分减仓", P_ACCENT)
    data = {
        "触发机制": _p("🛡️ 智慧大脑态势感知同步", P_MAIN),
        "实盘动作": action_txt,
        "数量变化": _p(f"`{old_qty}` ➔ `{new_qty}` {UNIT_LABEL}", P_ACCENT),
        "最新均价": _p(f"**{new_entry_price:.2f}** USDT", P_MAIN),
        "后续动作": _verify_line(
            verify_note if not verified else "",
            f"{VERIFY_TAG} | 已按最新仓位比例智能重挂 TP123",
            f"⏳ 重挂已提交，{VERIFY_DELAY_MARK} | 哨兵持续对齐",
        ),
    }
    if tp_audit:
        data["🕸️ TP123 审计"] = _p(_format_tp_audit(tp_audit), P_ACCENT)
    if verify_note:
        data["🔍 核查明细"] = _p(verify_note, P_MUTED)
    send_alert("🔄 深币阵地异动重置", data, P_ACCENT)


def report_force_align(real_side, expected_side, verify_note=""):
    data = {
        "🚨 异常状况": _p("**实盘方向与 TV 战略指令发生严重背离！**", P_DEEP),
        "🕵️ 现场方向": _p(real_side, P_ACCENT),
        "🧠 策略指令": _p(expected_side, P_LIGHT),
        "⚡ 仲裁结果": _p(f"{VERIFY_TAG} | 已核武全平，账本归零", P_MAIN),
    }
    if verify_note:
        data["🔍 核查明细"] = _p(verify_note, P_MUTED)
    send_alert("🚨 严重警告：方向强行物理对齐", data, P_TITLE)


def report_supervisor_close(reason, verify_note="", verified=True, swept_dust=False):
    r = reason or ""
    note = verify_note or ""
    is_dust_ctx = swept_dust or "蚂蚁仓" in note or "蚂蚁仓" in r or "重启扫描" in r or "扫尾" in r

    if "TP3" in r or "完美胜利" in r or "止盈" in r or "重启对账" in note:
        title = "🏆 完美胜利：深币大趋势吃满收网"
        status = _p(
            "三档网格已全部吃掉，暴利安全落袋。"
            + ("（含蚂蚁仓扫尾）" if is_dust_ctx else "")
            + ("（重启对账补发）" if "重启对账" in note else ""),
            P_LIGHT,
        )
    elif "保护" in r:
        title = "🛡️ 战术防守：保护平仓机制触发"
        status = _p("趋势警报解除，多空网格全撤，打扫战场空仓待命。", P_ACCENT)
    elif is_dust_ctx:
        title = "🐜 扫尾收网：深币蚂蚁仓/残张已清零"
        status = _p("止盈残张或蚂蚁仓已 reduceOnly 扫平，账本复位待命。", P_LIGHT)
    else:
        title = "🧹 先平后开 / 常规清场"
        status = _p("旧阵地已原子级爆破，账本归零等待新指令。", P_MUTED)

    if verified:
        verify_line = _p(f"{VERIFY_TAG} | 盘口已无持仓", P_MAIN)
    elif note and "REST 同步略延迟" in note:
        verify_line = _p("⏳ 已提交，REST 同步略延迟 | 盘口稍后对齐", P_ACCENT)
    else:
        verify_line = _p("⚠️ 核查待确认", P_DEEP)

    data = {
        "📋 平仓原理解析": _p(f"**{reason}**", P_MAIN),
        "✅ 账本状态": status,
        "📡 实盘核查": verify_line,
    }
    if verify_note:
        data["🔍 核查明细"] = _p(verify_note, P_MUTED)
    send_alert(title, data)


def report_recover_takeover(side, qty, entry, tv_tps, regime, radar_active, sl_price,
                            verify_note="", tp_matched=0, tp_expected=0, tp_audit=None,
                            last_tv_signal=None, radar_sl_ok=True):
    radar_txt = (
        _p(
            f"已激活 (硬防线 `{sl_price:.2f}` | "
            f"{'止损已挂/已确认' if radar_sl_ok else '止损待哨兵补挂'})",
            P_LIGHT,
        )
        if radar_active else _p("待命 (未达 TP1 激活阈值)", P_MUTED)
    )
    expected = tp_expected or sum(1 for t in tv_tps if t > 0)
    if expected > 0 and tp_matched >= expected:
        action_txt = f"{VERIFY_TAG} | 头寸+TV对账 → 比例 TP123 已对齐 → 恢复哨兵"
        action_color = P_MAIN
    elif tp_matched > 0:
        action_txt = f"⚠️ 部分对齐 | 止盈 {tp_matched}/{expected} 档 (价量审计未全过) → 恢复哨兵"
        action_color = P_ACCENT
    elif expected > 0:
        action_txt = "❌ 止盈补挂失败 | 持仓已接管但限价 TP 未对齐，请人工核查"
        action_color = P_DEEP
    else:
        action_txt = f"{VERIFY_TAG} | 已接管 → 恢复哨兵（无 TP 价格记录，请等 TV 信号）"
        action_color = P_MAIN

    tv_ref = ""
    if last_tv_signal:
        tv_ref = (
            f"{last_tv_signal.get('action', '?')} "
            f"R{last_tv_signal.get('regime', '?')} "
            f"@{last_tv_signal.get('ts', '')}"
        )

    data = {
        "🎛️ 实盘方向": _p(side, P_LIGHT if side == "LONG" else P_DEEP),
        "📦 核实头寸": _p(f"**{qty}** {UNIT_LABEL} @ `{entry:.2f}`", P_MAIN),
        "📊 恢复档位": get_regime_name(regime),
        "📡 最新 TV 信号": _p(tv_ref or "无日志记录", P_MUTED),
        "🕸️ TP123 比例审计": _p(
            _format_tp_audit(tp_audit, tv_tps) if tp_audit else _format_tp_compare(tv_tps, tv_tps),
            P_ACCENT,
        ),
        "📡 雷达状态": radar_txt,
        "✅ 接管动作": _p(action_txt, action_color),
    }
    if verify_note:
        data["🔍 核查明细"] = _p(verify_note, P_MUTED)
    send_alert("🔄 深币 VPS 重启 · 闪电接管报告", data)


def report_system_alert(title, detail):
    send_alert(f"⚠️ 系统告警：{title}", {
        "⚠️ 告警级别": _p("最高级别 (CRITICAL)", P_DEEP),
        "📝 核心详情": _p(f"**{detail}**", P_ACCENT),
    }, P_TITLE)
