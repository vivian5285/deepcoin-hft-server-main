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
LEVERAGE_LABEL = "15x"
DEFAULT_LEVERAGE = 15
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


def _format_sizing_basis(principal, margin_pct, leverage, margin_usdt=None):
    if margin_usdt is None:
        margin_usdt = float(principal or 0) * float(margin_pct or 0)
    return (
        f"本金快照 **{float(principal):.2f}** USDT × 档位保证金 **{margin_pct:.0%}** "
        f"= **{margin_usdt:.2f}** USDT × **{leverage}x** 杠杆"
    )


def report_principal_snapshot(reason, principal, regime=None, margin_pct=None, target_qty=None,
                              leverage=None, verify_note=""):
    lev = leverage or LEVERAGE_LABEL.replace("x", "")
    data = {
        "📸 快照时机": _p(reason or "本金重置", P_MAIN),
        "💰 合约本金": _p(f"**{float(principal):.2f}** USDT（cashBal，非可用保证金）", P_ACCENT),
        "📌 口径说明": _p(
            "仅用 USDT 合约本金余额 × TV 档位% × 杠杆计算仓位；"
            "禁止用 availBal / 剩余保证金",
            P_MUTED,
        ),
    }
    if regime and margin_pct is not None:
        data["🔢 TV 档位"] = get_regime_name(int(regime))
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
                           principal_balance=None, margin_pct=None, margin_usdt=None, leverage=None):
    side_str = _p("🟣 开多 (LONG)", P_LIGHT) if side == "LONG" else _p("🟪 开空 (SHORT)", P_DEEP)
    slip_txt = (
        f"{(entry_price - tv_price if side == 'LONG' else tv_price - entry_price):+.2f} 刀"
        if tv_price > 0 else "未知"
    )
    lev = leverage or DEFAULT_LEVERAGE

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
        "📡 哨兵状态": _verify_line(
            verify_note if not verified else "",
            f"🟢 {VERIFY_TAG} | 限价 TP123 已挂，雷达待命",
            "⏳ 开仓已提交，REST 同步略延迟 | 哨兵待确认",
        ),
    }
    if principal_balance and margin_pct is not None:
        data["📐 仓位预算"] = _p(
            _format_sizing_basis(principal_balance, margin_pct, lev, margin_usdt),
            P_LIGHT,
        )
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


def report_supervisor_close(reason, verify_note="", verified=True, swept_dust=False,
                            tv_pnl_pct=None, tv_side="", tv_price=None, close_action=""):
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
    elif "被动止损" in r or "STOPLOSS" in r or "保本线" in r or "硬止损" in r:
        title = "🛑 被动止损：硬止损或追踪保本触发"
        status = _p("策略被动离场，多空网格全撤，账本复位待命。", P_ACCENT)
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
    if close_action:
        data["📡 TV动作"] = _p(close_action, P_MUTED)
    if tv_side:
        data["🎛️ TV方向"] = _p(tv_side, P_LIGHT if tv_side == "LONG" else P_DEEP)
    if tv_price is not None and float(tv_price or 0) > 0:
        data["💹 TV价格"] = _p(f"`{float(tv_price):.2f}`", P_MUTED)
    if tv_pnl_pct is not None and tv_pnl_pct != "":
        pnl = float(tv_pnl_pct)
        data["📈 TV盈亏"] = _p(f"**{pnl:+.2f}%**", P_ACCENT if pnl >= 0 else P_DEEP)
    if verify_note:
        data["🔍 核查明细"] = _p(verify_note, P_MUTED)
    send_alert(title, data)


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
        "🛡️ 10%硬止损": _p(shield_status or "核查中", P_MAIN),
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
            f"**R{regime}** 档 · 保证金比例 **{margin_pct:.0%}** · 允许持仓 **{target_qty}** {UNIT_LABEL}",
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


def report_adverse_shield_armed(side, entry, live_qty, adverse_pct, tier_prices, tier_pcts,
                                verify_note=""):
    stop_px = tier_prices[0] if tier_prices else entry
    pct = tier_pcts[0] if tier_pcts else adverse_pct
    data = {
        "🎛️ 实盘方向": _p(side, P_LIGHT if side == "LONG" else P_DEEP),
        "💰 开仓成本": _p(f"`{entry:.2f}` USDT", P_MUTED),
        "📦 保护头寸": _p(f"**{live_qty}** {UNIT_LABEL} 全平", P_MAIN),
        "🛡️ 硬止损线": _p(f"**-{pct:.0%}** → `{stop_px:.2f}` USDT", P_ACCENT),
        "✅ 风控动作": _p(
            "开单即挂：以开仓价为基准 ±10% 条件止损全平 · "
            "价格达 TP1 激活比例后撤硬止损 → 切换雷达移动保本防回吐",
            P_MAIN,
        ),
    }
    if verify_note:
        data["🔍 核实明细"] = _p(verify_note, P_MUTED)
    send_alert("🛡️ 10%硬止损 · 已武装", data, P_TITLE)


def report_shield_tier_fill(side, tier_pct, tier_price, filled_qty, remain_qty, entry_px,
                            remaining_tiers=None, verify_note=""):
    data = {
        "🎛️ 实盘方向": _p(side, P_LIGHT if side == "LONG" else P_DEEP),
        "🛡️ 触发止损": _p(f"**-{tier_pct:.0%}** 硬止损 @ `{tier_price:.2f}` USDT", P_ACCENT),
        "✂️ 本次平仓": _p(f"`{filled_qty}` {UNIT_LABEL}", P_MAIN),
        "📊 剩余头寸": _p(f"`{remain_qty}` {UNIT_LABEL}", P_MAIN),
        "✅ 风控动作": _p("10% 硬止损成交 → TP123 已重算", P_MAIN),
    }
    if verify_note:
        data["🔍 核实明细"] = _p(verify_note, P_MUTED)
    send_alert("🛡️ 10%硬止损 · 成交", data, P_TITLE)


def report_shield_disarmed(side, live_qty, entry, cancelled_count, reason="",
                           radar_progress=0.0, verify_note=""):
    data = {
        "🎛️ 实盘方向": _p(side, P_LIGHT if side == "LONG" else P_DEEP),
        "💰 开仓成本": _p(f"`{entry:.2f}` USDT", P_MUTED),
        "📦 剩余头寸": _p(f"**{live_qty}** {UNIT_LABEL}", P_MAIN),
        "📈 价格方向": _p("达 **TP1 激活比例** → 转雷达", P_LIGHT),
        "🗑️ 撤销止损": _p(f"**{cancelled_count}** 笔 10% 硬止损", P_ACCENT),
        "📡 雷达状态": _p(
            "已激活移动保本" if radar_progress >= 1.0
            else f"进度 {radar_progress:.0%}，专注雷达推升止损",
            P_MAIN,
        ),
        "✅ 风控动作": _p(
            reason or "雷达接管 → 撤 10% 硬止损 → 移动保本防利润回吐",
            P_MAIN,
        ),
    }
    if verify_note:
        data["🔍 核实明细"] = _p(verify_note, P_MUTED)
    send_alert("🛡️ 10%硬止损 · 已撤销（转雷达）", data, P_TITLE)


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
            "先撤 10% 硬止损 → 挂雷达移动保本 → 专注推升止损防利润回吐",
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
