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
from webhook_parser import (
    format_tv_field_sources,
    classify_tv_close,
    close_type_display_label,
    format_vps_sizing_note,
    format_tv_sizing_note,
    format_regime_tp_ratios_label,
    VPS_RISK_PCT,
    VPS_REGIME_SCALE,
    VPS_MARGIN_LEVERAGE,
    EXCHANGE_LEVERAGE,
    normalize_entry_type,
    ENTRY_TYPE_OPEN,
    ENTRY_TYPE_PYRAMID,
    ENTRY_TYPE_PROFIT_ADD,
    CLOSE_TYPE_TP3,
    CLOSE_TYPE_PROTECT,
    CLOSE_TYPE_BREAKEVEN,
    CLOSE_TYPE_HARD_SL,
    CLOSE_TYPE_VPS_SHIELD,
    CLOSE_TYPE_GENERIC,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))
logger = logging.getLogger(__name__)

DINGTALK_WEBHOOK = os.getenv("DINGTALK_WEBHOOK", "")
DINGTALK_SECRET = os.getenv("DINGTALK_SECRET", "")

EXCHANGE_LABEL = "深币 Deepcoin"
LEVERAGE_LABEL = f"{int(EXCHANGE_LEVERAGE)}x"
DEFAULT_LEVERAGE = EXCHANGE_LEVERAGE
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


def _format_vps_sizing_basis(principal, meta=None, leverage=None):
    """VPS OPEN 仓位预算公式 — 管理员一眼看懂"""
    meta = meta or {}
    eff = float(meta.get("effective_risk_pct", VPS_RISK_PCT) or VPS_RISK_PCT)
    regime = int(meta.get("regime", 3) or 3)
    scale = float(meta.get("regime_scale", VPS_REGIME_SCALE.get(regime, 0.95)) or 0.95)
    margin = float(meta.get("margin", 0) or 0)
    order_amount = float(meta.get("order_amount", 0) or meta.get("position_value", 0) or 0)
    exch_lev = int(round(float(
        leverage or meta.get("leverage") or EXCHANGE_LEVERAGE
    )))
    lines = [
        f"本金快照 **{float(principal):.2f}** USDT × **{VPS_RISK_PCT:.0f}%** "
        f"× **{VPS_MARGIN_LEVERAGE}** × R{regime}系数 **{scale:.2f}** "
        f"= 保证金 **{margin:.2f}** USDT" if margin > 0 else
        f"本金快照 **{float(principal):.2f}** USDT × **{VPS_RISK_PCT:.0f}%** "
        f"× **{VPS_MARGIN_LEVERAGE}** × R{regime}系数 **{scale:.2f}**",
    ]
    if order_amount > 0:
        lines.append(
            f"→ 保证金 × **{exch_lev}x** 杠杆 = 头寸 **{order_amount:.2f}** USDT"
        )
    stop_dist = float(meta.get("stop_dist", 0) or 0)
    if stop_dist > 0:
        lines.append(f"（TV tv_sl 止损距离 **{stop_dist:.2f}**，仅挂止损用，不参与 sizing）")
    return "\n".join(lines)


def _format_sizing_basis(principal, margin_pct, leverage, margin_usdt=None):
    """兼容旧调用 — 实为 VPS 有效风险%"""
    if margin_usdt is None:
        margin_usdt = float(principal or 0) * float(margin_pct or 0)
    return (
        f"本金快照 **{float(principal):.2f}** USDT × VPS有效风险 **{float(margin_pct or 0):.1%}** "
        f"× **{leverage}x** 杠杆 = **{margin_usdt:.2f}** USDT 下单额"
    )


def report_principal_snapshot(reason, principal, regime=None, margin_pct=None, target_qty=None,
                              leverage=None, verify_note="", vps_sizing_meta=None):
    lev = leverage or LEVERAGE_LABEL.replace("x", "")
    meta = vps_sizing_meta or {}
    data = {
        "📸 快照时机": _p(reason or "本金重置", P_MAIN),
        "💰 合约本金": _p(f"**{float(principal):.2f}** USDT（cashBal，非可用保证金）", P_ACCENT),
        "📌 口径说明": _p(
            "VPS 自主风控：本金 × VPS_RISK_PCT% × REGIME_SCALE × GLOBAL_SCALE × 杠杆 ÷ |price-tv_sl|；"
            "完全忽略 TV risk_pct；禁止用 availBal / 剩余保证金",
            P_MUTED,
        ),
    }
    if regime and margin_pct is not None:
        data["🔢 TV 档位"] = get_regime_name(int(regime))
        if meta:
            data["📐 预算公式"] = _p(
                _format_vps_sizing_basis(principal, meta=meta, leverage=f"{lev}x"),
                P_LIGHT,
            )
        else:
            data["📐 预算公式"] = _p(
                _format_sizing_basis(principal, margin_pct, f"{lev}x"),
                P_LIGHT,
            )
    if target_qty is not None and float(target_qty) > 0:
        data["🎯 目标仓位"] = _p(f"**{target_qty}** {UNIT_LABEL}", P_MAIN)
    if verify_note:
        data["🔍 核实明细"] = _p(verify_note, P_MUTED)
    send_alert("📸 本金快照 · 档位预算基数已锁定", data, P_TITLE)


def report_supervisor_open(side, entry_price, tv_price, qty, tp_pxs, atr, regime, tv_tps=None,
                           verify_note="", tp_audit=None, verified=True,
                           principal_balance=None, margin_pct=None, margin_usdt=None, leverage=None,
                           tv_field_sources=None, vps_sizing_meta=None, symbol=None, unit_label=None):
    side_str = _p("🟣 开多 (LONG)", P_LIGHT) if side == "LONG" else _p("🟪 开空 (SHORT)", P_DEEP)
    slip_txt = (
        f"{(entry_price - tv_price if side == 'LONG' else tv_price - entry_price):+.2f} 刀"
        if tv_price > 0 else "未知"
    )
    lev = leverage or DEFAULT_LEVERAGE
    unit = unit_label or UNIT_LABEL
    sym = str(symbol or "").strip() or "ETH-USDT-SWAP"

    data = {
        "🎛️ 品种": _p(f"**{sym}**", P_ACCENT),
        "🎛️ 趋势方向": side_str,
        "📊 市场强度": get_regime_name(regime),
        "🕸️ TP123 比例": _p(
            f"开仓 R{regime} → **{format_regime_tp_ratios_label(regime)}%** (对齐 TV qty_percent)",
            P_LIGHT,
        ),
        "💰 进场成本": _p(f"**{entry_price:.2f}** USDT (滑点: **{slip_txt}**)", P_MAIN),
        "📦 唯一头寸": _p(f"**{qty}** {unit} ({EXCHANGE_LABEL} {LEVERAGE_LABEL} 稳健火力)", P_ACCENT),
        "🕸️ 止盈布防比对": _p(
            _format_tp_audit(tp_audit, tv_tps) if tp_audit else _format_tp_compare(tp_pxs, tv_tps),
            P_LIGHT,
        ),
        "📏 波动参考": _p(f"ATR = {atr:.4f}", P_MUTED),
        "📡 TV字段": _p(format_tv_field_sources(tv_field_sources or {}), P_MUTED),
        "📡 哨兵状态": _verify_line(
            verify_note if not verified else "",
            f"🟢 {VERIFY_TAG} | TP123已挂 · 雷达等TP1三角对账(价到+限价成交+量匹配)",
            "⏳ 开仓已提交，REST 同步略延迟 | 哨兵待确认",
        ),
    }
    if principal_balance and margin_pct is not None:
        if vps_sizing_meta:
            data["📐 仓位预算"] = _p(
                _format_vps_sizing_basis(principal_balance, meta=vps_sizing_meta, leverage=lev),
                P_LIGHT,
            )
            data["📐 VPS参数"] = _p(format_vps_sizing_note(vps_sizing_meta, qty=qty), P_MUTED)
        else:
            data["📐 仓位预算"] = _p(
                _format_sizing_basis(principal_balance, margin_pct, lev, margin_usdt),
                P_LIGHT,
            )
    if verify_note:
        data["🔍 核查明细"] = _p(verify_note, P_MUTED)
    send_alert(f"🖨️ 战神出击：深币 {sym} 阵地建立", data)


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
        "🧭 方向/档位": _p(f"{side} | TV {regime} 档", P_MUTED),
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
    is_manual_open = "人工开仓" in str(action_type or "")
    if is_manual_open:
        action_txt = _p("人工首仓 · 系统接管", P_LIGHT)
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
    if is_manual_open:
        data["📡 雷达/止损"] = _p(
            "TP1 成交前 **仅 tv_sl 宽止损** · 雷达 **待命**（禁止提前保本）",
            P_MUTED,
        )
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


def _classify_close(reason, verify_note="", swept_dust=False, close_type="", close_action="",
                    tv_reason=""):
    r = reason or ""
    note = verify_note or ""
    is_dust_ctx = swept_dust or "蚂蚁仓" in note or "蚂蚁仓" in r or "重启扫描" in r or "扫尾" in r
    ct = close_type or classify_tv_close(close_action, tv_reason or r)

    if ct == CLOSE_TYPE_TP3:
        return {
            "title": "🏆 TP3止盈 · 完美收网",
            "tag": _p("**TP3止盈**", P_LIGHT),
            "status": _p(
                "三档网格全部吃尽，暴利安全落袋。"
                + ("（含蚂蚁仓扫尾）" if is_dust_ctx else "")
                + ("（重启对账补发）" if "重启对账" in note else ""),
                P_LIGHT,
            ),
            "header": P_TITLE,
        }
    if ct == CLOSE_TYPE_PROTECT:
        return {
            "title": "🛡️ 风控拦截 · 保护性全平",
            "tag": _p("**风控拦截**", P_ACCENT),
            "status": _p("策略风控触发，多空网格全撤，空仓待命。", P_ACCENT),
            "header": P_ACCENT,
        }
    if ct == CLOSE_TYPE_BREAKEVEN:
        return {
            "title": "💚 防回吐保本 · 全平收网",
            "tag": _p("**防回吐保本**", P_LIGHT),
            "status": _p("追踪保本/微利护体触发，利润锁死离场。", P_MAIN),
            "header": P_LIGHT,
        }
    if ct in (CLOSE_TYPE_HARD_SL, CLOSE_TYPE_VPS_SHIELD):
        title = (
            "🛡️ TV硬止损 · 全平"
            if ct == CLOSE_TYPE_VPS_SHIELD
            else "🛑 硬止损 · 全平离场"
        )
        tag_txt = "TV硬止损" if ct == CLOSE_TYPE_VPS_SHIELD else "硬止损"
        return {
            "title": title,
            "tag": _p(f"**{tag_txt}**", P_DEEP),
            "status": _p("止损触发全平，多空网格全撤，账本复位待命。", P_DEEP),
            "header": P_DEEP,
        }
    if is_dust_ctx:
        return {
            "title": "🐜 扫尾收网：蚂蚁仓/残张已清零",
            "tag": _p("**扫尾收网**", P_MUTED),
            "status": _p("止盈残张或蚂蚁仓已 reduceOnly 扫平，账本复位待命。", P_LIGHT),
            "header": P_DEEP,
        }
    return {
        "title": "🧹 先平后开 / 常规清场",
        "tag": _p("**常规清场**", P_MUTED),
        "status": _p("旧阵地已原子级爆破，账本归零等待新指令。", P_MUTED),
        "header": P_MUTED,
    }


def report_supervisor_close(reason, verify_note="", verified=True, swept_dust=False,
                            tv_pnl_pct=None, tv_side="", tv_price=None, close_action="",
                            tv_regime=None, tv_atr=None, tv_field_sources=None,
                            close_type="", tv_reason="", entry_px=None, closed_qty=None,
                            live_exit_px=None):
    theme = _classify_close(
        reason, verify_note, swept_dust=swept_dust,
        close_type=close_type, close_action=close_action, tv_reason=tv_reason or reason,
    )
    ok_verify = f"{VERIFY_TAG} | 盘口已无持仓 | 挂单已清空"
    delay_verify = f"⏳ 全平已提交，{VERIFY_DELAY_MARK} | 盘口对齐中"
    if swept_dust or "蚂蚁仓" in (verify_note or ""):
        ok_verify = f"{VERIFY_TAG} | 蚂蚁仓已扫平，盘口已无持仓"
        delay_verify = f"⏳ 蚂蚁仓扫尾已提交，{VERIFY_DELAY_MARK} | 盘口对齐中"

    ct = close_type or classify_tv_close(close_action, tv_reason or reason, tv_pnl_pct)
    data = {
        "🏷️ 收网类型": theme.get("tag") or _p(close_type_display_label(ct, reason), P_MAIN),
        "📋 策略原由": _p(f"**{tv_reason or reason}**", P_MAIN),
        "✅ 账本状态": theme["status"],
        "📡 实盘核查": _verify_line(
            verify_note if not verified else "",
            ok_verify,
            delay_verify,
        ),
    }
    if close_action:
        data["📡 TV动作"] = _p(close_action, P_MUTED)
    if tv_side:
        data["🎛️ 方向"] = _p(tv_side, P_LIGHT if tv_side == "LONG" else P_DEEP)
    if entry_px is not None and float(entry_px or 0) > 0:
        data["💰 开仓成本"] = _p(f"`{float(entry_px):.2f}` USDT", P_MUTED)
    if closed_qty is not None and float(closed_qty or 0) > 0:
        data["📦 平仓数量"] = _p(f"**{float(closed_qty):.0f}** {UNIT_LABEL}", P_MAIN)
    if live_exit_px is not None and float(live_exit_px or 0) > 0:
        data["💹 平仓价格"] = _p(f"`{float(live_exit_px):.2f}` USDT", P_ACCENT)
    elif tv_price is not None and float(tv_price or 0) > 0:
        data["💹 TV价格"] = _p(f"`{float(tv_price):.2f}` USDT", P_MUTED)
    if tv_pnl_pct is not None and tv_pnl_pct != "":
        pnl = float(tv_pnl_pct)
        data["📈 盈亏"] = _p(f"**{pnl:+.2f}%**", P_ACCENT if pnl >= 0 else P_DEEP)
    if tv_regime is not None:
        data["📊 TV档位"] = get_regime_name(int(tv_regime))
    if tv_atr is not None and float(tv_atr or 0) > 0:
        data["📏 TV ATR"] = _p(f"`{float(tv_atr):.4f}`", P_MUTED)
    if tv_field_sources:
        data["📡 TV字段"] = _p(format_tv_field_sources(tv_field_sources), P_MUTED)
    if verify_note:
        data["🔍 核查明细"] = _p(verify_note, P_MUTED)
    send_alert(theme["title"], data, theme["header"])


def report_recover_tp_repair(side, initial_qty, live_qty, entry, consumed_levels,
                             tp_audit=None, verify_note="", verified=True):
    consumed_txt = ", ".join(f"TP{lv}" for lv in (consumed_levels or [])) or "无"
    data = {
        "🎛️ 实盘方向": _p(side, P_LIGHT if side == "LONG" else P_DEEP),
        "📦 开单头寸": _p(f"**{initial_qty}** {UNIT_LABEL} @ `{entry:.2f}`", P_MUTED),
        "📦 现仓剩余": _p(f"**{live_qty}** {UNIT_LABEL} (= 剩余TP)", P_MAIN),
        "✂️ 已成交档": _p(consumed_txt, P_ACCENT),
        "🕸️ 剩余止盈审计": _p(
            _format_tp_audit(tp_audit, []) if tp_audit else "核查中",
            P_MAIN,
        ),
        "✅ 修复动作": _p(
            "撤多余已成交档 → 按现仓重分剩余 TP → 雷达保本接力",
            P_MAIN,
        ),
        "📡 实盘核查": _verify_line(
            verify_note if not verified else "",
            f"{VERIFY_TAG} | 部分止盈修复完成",
            f"⏳ 修复已提交，{VERIFY_DELAY_MARK}",
        ),
    }
    if verify_note:
        data["🔍 核实明细"] = _p(verify_note, P_MUTED)
    send_alert("🎯 重启 · 部分止盈修复", data, P_TITLE)


def report_recover_takeover(side, qty, entry, tv_tps, regime, radar_active, sl_price,
                            verify_note="", tp_matched=0, tp_expected=0, tp_audit=None,
                            last_tv_signal=None, radar_sl_ok=True,
                            pnl_label="", defense_plan="", shield_status="",
                            radar_progress=0.0, tv_aligned=True, qty_aligned=True,
                            initial_qty=0, tp_consumed_levels=None):
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

    if radar_active:
        sl_state = "止损已挂/已确认" if radar_sl_ok else "止损待哨兵补挂"
        radar_txt = _p(
            f"已激活 · 进度 {radar_progress:.0%} | 保本 `{sl_price:.2f}` | {sl_state}",
            P_LIGHT,
        )
    else:
        radar_txt = _p(
            f"待命 (雷达进度 {radar_progress:.0%}，达 TP1 激活比后推升止损)",
            P_MUTED,
        )

    tv_ref = ""
    if last_tv_signal:
        tv_ref = (
            f"{last_tv_signal.get('action', '?')} "
            f"R{last_tv_signal.get('regime', '?')} "
            f"@{last_tv_signal.get('ts', '')}"
        )
    tv_align_txt = "一致" if tv_aligned else "⚠️ 与实盘方向有偏差(以实盘为准)"
    qty_align_txt = "一致" if qty_aligned else "⚠️ 账本数量有偏差(已同步实盘)"

    data = {
        "🎛️ 实盘方向": _p(side, P_LIGHT if side == "LONG" else P_DEEP),
        "📦 核实头寸": _p(f"**{qty}** {UNIT_LABEL} @ `{entry:.2f}`", P_MAIN),
        "📊 恢复档位": get_regime_name(regime),
    }
    if initial_qty and int(initial_qty) > int(qty):
        consumed_txt = ", ".join(f"TP{lv}" for lv in (tp_consumed_levels or [])) or "推断中"
        data["📦 开单原始"] = _p(f"**{initial_qty}** {UNIT_LABEL}", P_MUTED)
        data["✂️ 已成交档"] = _p(consumed_txt, P_ACCENT)
    data.update({
        "📡 最新 TV 信号": _p(f"{tv_ref or '无日志记录'} ({tv_align_txt})", P_MUTED),
        "⚖️ 仓位核对": _p(qty_align_txt, P_MAIN if qty_aligned else P_ACCENT),
        "📈 盈亏态势": _p(pnl_label or "核查中", P_ACCENT if "浮亏" in (pnl_label or "") else P_MAIN),
        "🛡️ TV硬止损": _p(shield_status or "核查中", P_MAIN),
        "🕸️ TP123 比例审计": _p(
            _format_tp_audit(tp_audit, tv_tps) if tp_audit else _format_tp_compare(tv_tps, tv_tps),
            P_ACCENT,
        ),
        "📡 雷达状态": radar_txt,
        "🧭 防线路由": _p(defense_plan or "哨兵接力维护", P_LIGHT),
        "✅ 接管动作": _p(action_txt, action_color),
    })
    if verify_note:
        data["🔍 核查明细"] = _p(verify_note, P_MUTED)
    send_alert("🔄 深币 VPS 重启 · 闪电接管报告", data)


def report_recover_standby(verify_note="", version=""):
    data = {
        "📡 实盘核查": _p(f"{VERIFY_TAG} | 盘口无持仓", P_MAIN),
        "✅ 系统状态": _p("空仓待命 · 挂单已清空 · 雷达/哨兵复位", P_LIGHT),
        "🔮 版本": _p(version or "deepcoin_webhook", P_MUTED),
    }
    if verify_note:
        data["🔍 核查明细"] = _p(verify_note, P_MUTED)
    send_alert("🔄 深币 VPS 重启 · 空仓待命", data, P_ACCENT)


def report_smart_same_dir_decision(side, decision, live_entry, tv_price, diff_pct, threshold_pct,
                                   open_regime, tv_regime, open_atr, tv_atr, qty,
                                   tp_audit=None, verify_note=""):
    atr_txt = f"持仓 `{open_atr:.2f}` · TV `{tv_atr:.2f}`"
    atr_changed = abs(float(open_atr or 0) - float(tv_atr or 0)) > 0 and (
        max(abs(open_atr), abs(tv_atr), 1) == 0 or
        abs(float(open_atr) - float(tv_atr)) / max(abs(open_atr), abs(tv_atr), 1) > 0.03
    )

    if decision == "skip_duplicate_flat":
        title = "🧠 智能筛选：短时重复同向 · 已忽略"
        status = _p(
            f"**5 分钟内** ATR 未变 ({atr_txt})，价差 **{diff_pct:.3f}%** < **{threshold_pct}%**，"
            f"档位 **R{tv_regime}** → **未重复下单**。",
            P_ACCENT,
        )
    elif decision.startswith("reentry_"):
        reason_map = {
            "reentry_atr_changed": f"**① ATR 变化** ({atr_txt}) → **先平后开** 刷新仓位",
            "reentry_regime_changed": f"**② 档位** R{open_regime}→R{tv_regime} → **先平后开** 刷新仓位",
            "reentry_spread_ok": (
                f"**③ 理论价差** **{diff_pct:.3f}%** ≥ **{threshold_pct}%** "
                f"(ATR 未变 {atr_txt}) → **先平后开**"
            ),
        }
        title = "🧠 智能筛选：同向持仓 · 刷新仓位"
        status = _p(reason_map.get(decision, "同向刷新仓位 → **先平后开**"), P_TITLE)
    else:
        title = "🧠 智能筛选：同向持仓 · 仅刷新止盈"
        status = _p(
            f"**① ATR 未变** ({atr_txt}) + **③ 价差** **{diff_pct:.3f}%** < **{threshold_pct}%** "
            f"(档位 R{open_regime}) → **未再开仓**，已核实持仓并按新 TV 价刷新 TP123。",
            P_LIGHT,
        )
    data = {
        "📊 智能决策": status,
        "🎯 TV方向": _p(side, P_MAIN),
        "💰 实盘成本": _p(f"`{live_entry:.2f}` USDT" if live_entry > 0 else "空仓", P_MUTED),
        "📡 TV理论价": _p(f"`{tv_price:.2f}` USDT", P_MUTED),
        "🌊 ATR (优先)": _p(
            f"{atr_txt}" + (" ⚡已变化" if atr_changed and decision == "reentry_atr_changed" else " ✓未变"),
            P_ACCENT if atr_changed else P_MUTED,
        ),
        "📏 理论价差": _p(f"{diff_pct:.3f}% / 阈值 {threshold_pct}%", P_ACCENT),
        "🔢 档位": _p(f"开仓 R{open_regime} · TV R{tv_regime}", P_MUTED),
        "📦 持有": _p(f"**{qty}** {UNIT_LABEL}" if qty > 0 else "无持仓", P_ACCENT),
    }
    if tp_audit:
        data["🕸️ TP123 审计"] = _p(_format_tp_audit(tp_audit), P_ACCENT)
    if verify_note:
        data["🔍 核实明细"] = _p(verify_note, P_MUTED)
    color = P_ACCENT if decision == "skip_duplicate_flat" else P_TITLE
    send_alert(title, data, color)


def report_system_alert(title, detail, level="紧急", suggestion=""):
    data = {
        "⚠️ 告警级别": _p(f"【{level}】需管理员关注", P_DEEP),
        "📝 发生了什么": _p(f"**{title}**", P_MAIN),
        "📋 详细说明": _p(detail, P_ACCENT),
    }
    if suggestion:
        data["💡 建议操作"] = _p(suggestion, P_LIGHT)
    send_alert(f"⚠️ 系统告警：{title}", data, P_TITLE)


def report_radar_guardian_realigned(side, qty, tp_audit=None, verify_note=""):
    data = {
        "🎛️ 实盘方向": _p(side, P_LIGHT if side == "LONG" else P_DEEP),
        "📦 核实头寸": _p(f"**{qty}** {UNIT_LABEL}", P_MAIN),
        "🕸️ TP123 比例审计": _p(
            _format_tp_audit(tp_audit, None) if tp_audit else "已对齐",
            P_MAIN,
        ),
        "✅ 纠偏结果": _p("雷达守护已完成止盈对齐（重启接管竞态后补报）", P_MAIN),
    }
    if verify_note:
        data["🔍 核实明细"] = _p(verify_note, P_MUTED)
    send_alert("📡 雷达守护 · 止盈已重新对齐", data, P_MAIN)


def report_radar_regime_cap_trim(side, old_qty, new_qty, target_qty, regime, margin_pct,
                                 tp_audit=None, verify_note="",
                                 principal_balance=None, margin_usdt=None, leverage=None,
                                 trim_qty=None):
    lev = leverage or DEFAULT_LEVERAGE
    excess = max(0.0, float(old_qty) - float(target_qty))
    data = {
        "🎛️ 实盘方向": _p(side, P_LIGHT if side == "LONG" else P_DEEP),
        "📊 TV 档位上限": _p(
            f"**R{regime}** 档 · VPS有效风险 **{margin_pct:.1%}** · 允许持仓 **{target_qty}** {UNIT_LABEL}",
            P_ACCENT,
        ),
        "📐 核算公式": _p(
            _format_sizing_basis(
                principal_balance or 0, margin_pct, lev, margin_usdt,
            ) if principal_balance else "本金快照 × 档位% × 杠杆（详见核实明细）",
            P_LIGHT,
        ),
        "⚖️ 超标情况": _p(
            f"实盘 **{old_qty}** {UNIT_LABEL} 超出目标 **{excess:.3f}** {UNIT_LABEL}"
            + (f" · 本次裁减 **{trim_qty}** {UNIT_LABEL}" if trim_qty else ""),
            P_ACCENT,
        ),
        "✂️ 裁减结果": _p(f"`{old_qty}` ➔ `{new_qty}` {UNIT_LABEL}", P_MAIN),
        "🕸️ TP123 重挂": _p(
            _format_tp_audit(tp_audit, None) if tp_audit else "已按新仓位重挂",
            P_MAIN,
        ),
        "✅ 纠偏结果": _p(
            "雷达最高权限：超标裁减至档位额度 → TP123 已对齐 · 移动止损逻辑不变",
            P_MAIN,
        ),
    }
    if verify_note:
        data["🔍 核实明细"] = _p(verify_note, P_MUTED)
    send_alert("📡 雷达守护 · 档位限额强制对齐", data, P_TITLE)


def report_tv_signal_received(action, entry_type="", price=0, regime=3, atr=0,
                              tv_sl=0, risk_pct=0, leverage=None, qty_ratio=1.0,
                              reason="", vps_sizing_meta=None):
    """TV Webhook 信号到达（接收确认，非成交核实）"""
    act = str(action or "").upper()
    et = normalize_entry_type(entry_type)
    type_map = {
        ENTRY_TYPE_OPEN: "首次开仓 OPEN",
        ENTRY_TYPE_PYRAMID: "金字塔加仓 PYRAMID",
        ENTRY_TYPE_PROFIT_ADD: "浮盈加仓 PROFIT_ADD",
    }
    type_txt = type_map.get(et, et or "—")
    close_actions = {
        "CLOSE_PROTECT": "保护性全平",
        "CLOSE_TP3": "TP3 收网",
        "CLOSE_STOPLOSS": "止损/保本平仓",
        "UPDATE_SL": "动态止损 UPDATE_SL",
        "UPDATE_TP": "动能止盈升级 UPDATE_TP",
        "CLOSE": "换防清场",
    }
    if act in close_actions:
        type_txt = close_actions[act]
    data = {
        "📡 信号类型": _p(f"**{act}** · {type_txt}", P_ACCENT),
        "💹 TV价格": _p(f"`{float(price or 0):.2f}` USDT", P_MUTED),
        "📊 档位": get_regime_name(regime),
        "📡 ATR": _p(f"`{float(atr or 0):.2f}`", P_MUTED),
    }
    if tv_sl and float(tv_sl) > 0:
        data["📡 tv_sl"] = _p(f"`{float(tv_sl):.2f}`", P_LIGHT)
    if et == ENTRY_TYPE_OPEN and vps_sizing_meta:
        data["📐 VPS预算"] = _p(
            format_vps_sizing_note(vps_sizing_meta, entry_type=ENTRY_TYPE_OPEN),
            P_MUTED,
        )
    elif risk_pct and float(risk_pct) > 0:
        data["📐 比例参数"] = _p(
            format_tv_sizing_note(
                risk_pct, EXCHANGE_LEVERAGE, qty_ratio, regime=regime,
            ),
            P_MUTED,
        )
    data["⚙️ 实盘杠杆"] = _p(
        f"VPS **{EXCHANGE_LEVERAGE}x**（忽略 TV payload leverage 字段）",
        P_MUTED,
    )
    if reason:
        data["📝 原因"] = _p(str(reason)[:120], P_MUTED)
    data["✅ 状态"] = _p("信号已入队 · 等待实盘核实后二次播报", P_MAIN)
    send_alert(f"📡 TV信号接收 · {act}", data, P_MUTED)


def report_tv_sl_updated(side, live_qty, entry, tv_sl, exchange_stop=None,
                         radar_active=False, radar_sl=None, regime=3,
                         verify_note="", verified=True):
    """TV UPDATE_SL 核实成功后播报（TV底线 + 交易所止损，不动状态机）"""
    tv_sl = float(tv_sl or 0)
    exchange_stop = float(exchange_stop or tv_sl or 0)
    merged = (
        radar_active
        and radar_sl
        and abs(float(exchange_stop) - tv_sl) > 0.01
    )
    if merged:
        action_txt = (
            f"TV UPDATE_SL → 交易所止损 @ `{exchange_stop:.2f}` "
            f"(TV底线 `{tv_sl:.2f}` · 雷达 `{float(radar_sl):.2f}` 分层)"
        )
    elif radar_active:
        action_txt = (
            f"TV UPDATE_SL → TV底线 @ `{tv_sl:.2f}` · "
            f"雷达 @ `{float(radar_sl or exchange_stop):.2f}` 独立运行"
        )
    else:
        action_txt = f"TV UPDATE_SL → 硬止损触发 @ `{tv_sl:.2f}`"

    data = {
        "🎛️ 实盘方向": _p(side, P_LIGHT if side == "LONG" else P_DEEP),
        "📦 保护头寸": _p(f"**{live_qty}** {UNIT_LABEL}", P_MAIN),
        "💰 开仓成本": _p(f"`{entry:.2f}` USDT", P_MUTED),
        "📊 档位": get_regime_name(regime),
        "📡 TV底线 tv_sl": _p(f"**{tv_sl:.2f}** USDT", P_ACCENT),
        "🔒 交易所止损": _p(f"**{exchange_stop:.2f}** USDT", P_LIGHT),
        "📡 雷达状态": _p(
            f"已激活 @ `{float(radar_sl):.2f}`" if radar_active and radar_sl
            else ("已激活" if radar_active else "待命监控中"),
            P_MAIN,
        ),
        "✅ 风控动作": _p(
            action_txt + " · 雷达与 TV 底线分层运行",
            P_MAIN,
        ),
        "📡 实盘核查": _verify_line(
            verify_note if not verified else "",
            f"{VERIFY_TAG} | UPDATE_SL 止损已在盘口对齐",
            f"⏳ 止损已提交，{VERIFY_DELAY_MARK} | 哨兵将继续核实",
        ),
    }
    if verify_note:
        data["🔍 核实明细"] = _p(verify_note, P_MUTED)
    send_alert("📡 TV硬止损 · UPDATE_SL 已同步", data, P_TITLE)


def report_tv_tp_updated(side, live_qty, entry, old_tps=None, new_tps=None,
                         placed=0, regime=3, verify_note="", verified=True, curr_px=0):
    """TV UPDATE_TP 动能止盈升级：只换限价 TP，不动硬止损/雷达"""
    old_tps = old_tps or []
    new_tps = new_tps or []

    def _fmt(tps):
        parts = []
        for i, t in enumerate(tps[:3]):
            try:
                v = float(t or 0)
            except (TypeError, ValueError):
                v = 0.0
            parts.append(f"TP{i + 1}=`{v:.2f}`" if v > 0 else f"TP{i + 1}=—")
        return " / ".join(parts) if parts else "—"

    data = {
        "🎛️ 实盘方向": _p(side, P_LIGHT if side == "LONG" else P_DEEP),
        "📦 保护头寸": _p(f"**{live_qty}** {UNIT_LABEL}", P_MAIN),
        "💰 开仓成本": _p(f"`{float(entry or 0):.2f}` USDT", P_MUTED),
        "📊 档位": get_regime_name(regime),
        "📉 原 TP123": _p(_fmt(old_tps), P_MUTED),
        "🚀 新 TP123": _p(_fmt(new_tps), P_ACCENT),
        "📌 新挂档数": _p(f"**{int(placed or 0)}** 笔限价止盈", P_LIGHT),
        "💹 参考市价": _p(
            f"`{float(curr_px or 0):.2f}` USDT" if float(curr_px or 0) > 0 else "—",
            P_MUTED,
        ),
        "✅ 风控动作": _p(
            "动能 UPDATE_TP → 仅替换限价 TP123 · 硬止损与雷达未触碰",
            P_MAIN,
        ),
        "📡 实盘核查": _verify_line(
            verify_note if not verified else "",
            f"{VERIFY_TAG} | UPDATE_TP 止盈已在盘口对齐",
            f"⏳ 止盈已提交，{VERIFY_DELAY_MARK} | 哨兵将继续核实",
        ),
    }
    if verify_note:
        data["🔍 核实明细"] = _p(verify_note, P_MUTED)
    send_alert("🚀 动能止盈 · UPDATE_TP 已同步", data, P_TITLE)


def report_tv_position_add(side, entry_type, add_qty, old_qty, new_qty, old_entry, new_entry,
                           tv_sl=0, risk_pct=0, leverage=None, qty_ratio=1.0,
                           verify_note="", verified=True, base_qty=0, vps_sizing_meta=None,
                           add_count=0, max_add_times=2, regime=3, tp_audit="", radar_note="",
                           open_regime=None, tp_ratio_label=""):
    """PYRAMID / PROFIT_ADD 加仓核实 — 首仓×TV比例 + 新总头寸重挂 TP123/雷达"""
    type_label = {
        ENTRY_TYPE_PYRAMID: "金字塔加仓 PYRAMID",
        ENTRY_TYPE_PROFIT_ADD: "浮盈加仓 PROFIT_ADD",
    }.get(str(entry_type or "").upper(), str(entry_type or "ADD"))
    lev = leverage or DEFAULT_LEVERAGE
    data = {
        "🎛️ 实盘方向": _p(side, P_LIGHT if side == "LONG" else P_DEEP),
        "📡 加仓类型": _p(type_label, P_ACCENT),
        "📊 档位": get_regime_name(regime),
        "🕸️ TP123 比例": _p(
            f"开仓 R{int(open_regime or regime)} → **{tp_ratio_label or format_regime_tp_ratios_label(open_regime or regime)}%**",
            P_LIGHT,
        ),
        "➕ 追加数量": _p(f"**+{add_qty}** {UNIT_LABEL}", P_MAIN),
        "📦 持仓变化": _p(
            f"`{old_qty}` → **`{new_qty}`** {UNIT_LABEL}",
            P_LIGHT,
        ),
        "💰 均价变化": _p(
            f"`{old_entry:.2f}` → **`{new_entry:.2f}`** USDT",
            P_MUTED,
        ),
        "📡 TV底线 tv_sl": _p(f"**{float(tv_sl or 0):.2f}** USDT", P_ACCENT),
        "📐 加仓公式": _p(
            format_vps_sizing_note(
                vps_sizing_meta or {
                    "base_qty": base_qty,
                    "qty_ratio": qty_ratio,
                    "regime": regime,
                    "max_add_times": max_add_times,
                    "sizing_mode": "VPS_ADD",
                },
                qty=add_qty,
                entry_type=entry_type,
            ),
            P_MUTED,
        ),
        "📡 TV加仓比例": _p(f"**{float(qty_ratio):.2f}** × 首仓", P_LIGHT),
        "🔢 加仓次数": _p(f"**{add_count}/{max_add_times}**", P_LIGHT),
        "🕸️ TP123 重挂": _p(tp_audit or "已按新总头寸重算", P_MAIN),
        "📡 雷达状态": _p(radar_note or "待命(TP1前)", P_LIGHT),
        "✅ 风控动作": _p(
            "加仓成交 → TV TP123 按新头寸重挂 + tv_sl/雷达同步",
            P_MAIN,
        ),
        "📡 实盘核查": _verify_line(
            verify_note if not verified else "",
            f"{VERIFY_TAG} | 加仓 + TP123 + 止损/雷达已同步",
            f"⏳ 加仓已提交，{VERIFY_DELAY_MARK} | 哨兵继续核实",
        ),
    }
    if verify_note:
        data["🔍 核实明细"] = _p(verify_note, P_MUTED)
    send_alert(f"➕ TV加仓 · {type_label}", data, P_TITLE)


def report_adverse_shield_armed(side, entry, live_qty, adverse_pct, tier_prices, tier_pcts,
                                verify_note=""):
    stop_px = tier_prices[0] if tier_prices else entry
    pct = tier_pcts[0] if tier_pcts else adverse_pct
    data = {
        "🎛️ 实盘方向": _p(side, P_LIGHT if side == "LONG" else P_DEEP),
        "💰 开仓成本": _p(f"`{entry:.2f}` USDT", P_MUTED),
        "📦 保护头寸": _p(f"**{live_qty}** {UNIT_LABEL} 全平", P_MAIN),
        "🛡️ TV硬止损": _p(f"`{stop_px:.2f}` USDT", P_ACCENT),
        "✅ 风控动作": _p(
            "开单即挂：TV 透传 tv_sl 条件止损全平 · "
            "价格达 TP1 激活比例后撤 TV 硬止损 → 切换雷达移动保本防回吐",
            P_MAIN,
        ),
    }
    if verify_note:
        data["🔍 核实明细"] = _p(verify_note, P_MUTED)
    send_alert("🛡️ TV硬止损 · 已武装", data, P_TITLE)


def report_shield_tier_fill(side, tier_pct, tier_price, filled_qty, remain_qty, entry_px,
                            remaining_tiers=None, verify_note=""):
    data = {
        "🎛️ 实盘方向": _p(side, P_LIGHT if side == "LONG" else P_DEEP),
        "🛡️ 触发止损": _p(f"**-{tier_pct:.0%}** 硬止损 @ `{tier_price:.2f}` USDT", P_ACCENT),
        "✂️ 本次平仓": _p(f"`{filled_qty}` {UNIT_LABEL}", P_MAIN),
        "📊 剩余头寸": _p(f"`{remain_qty}` {UNIT_LABEL}", P_MAIN),
        "✅ 风控动作": _p("TV硬止损成交 → TP123 已重算", P_MAIN),
    }
    if verify_note:
        data["🔍 核实明细"] = _p(verify_note, P_MUTED)
    send_alert("🛡️ TV硬止损 · 成交", data, P_TITLE)


def report_shield_disarmed(side, live_qty, entry, cancelled_count, reason="",
                           radar_progress=0.0, verify_note=""):
    data = {
        "🎛️ 实盘方向": _p(side, P_LIGHT if side == "LONG" else P_DEEP),
        "💰 开仓成本": _p(f"`{entry:.2f}` USDT", P_MUTED),
        "📦 剩余头寸": _p(f"**{live_qty}** {UNIT_LABEL}", P_MAIN),
        "📈 价格方向": _p("达 **TP1 激活比例** → 转雷达", P_LIGHT),
        "🗑️ 撤销止损": _p(f"**{cancelled_count}** 笔 TV硬止损", P_ACCENT),
        "📡 雷达状态": _p(
            "已激活移动保本" if radar_progress >= 1.0
            else f"进度 {radar_progress:.0%}，专注雷达推升止损",
            P_MAIN,
        ),
        "✅ 风控动作": _p(
            reason or "雷达接管 → 撤 TV 硬止损 → 移动保本防利润回吐",
            P_MAIN,
        ),
    }
    if verify_note:
        data["🔍 核实明细"] = _p(verify_note, P_MUTED)
    send_alert("🛡️ TV硬止损 · 已撤销（转雷达）", data, P_TITLE)


def report_radar_activated(side, qty, entry, new_sl, radar_progress=1.0, regime=3,
                           shield_cleared=True, verify_note="", verified=True):
    data = {
        "🎛️ 实盘方向": _p(side, P_LIGHT if side == "LONG" else P_DEEP),
        "📦 利润头寸": _p(f"**{qty}** {UNIT_LABEL} @ `{entry:.2f}`", P_MAIN),
        "📊 恢复档位": get_regime_name(regime),
        "📡 雷达进度": _p(f"**{radar_progress:.0%}** (达 TP1 激活比)", P_ACCENT),
        "🗑️ 硬止损": _p("已撤销" if shield_cleared else "清理中", P_MAIN),
        "🔒 保本止损": _p(f"**{new_sl:.2f}** USDT (触发止损)", P_LIGHT),
        "✅ 风控动作": _p(
            "先撤 TV 硬止损 → 挂雷达移动保本 → 专注推升止损防利润回吐",
            P_MAIN,
        ),
        "📡 实盘核查": _verify_line(
            verify_note if not verified else "",
            f"{VERIFY_TAG} | 雷达移动保本已启动",
            f"⏳ 止损已提交，{VERIFY_DELAY_MARK} | 雷达已启动",
        ),
    }
    if verify_note:
        data["🔍 核实明细"] = _p(verify_note, P_MUTED)
    send_alert("📡 雷达 · 移动保本已激活", data, P_DEEP)
