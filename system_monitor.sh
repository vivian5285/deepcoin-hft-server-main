#!/bin/bash
# system_monitor.sh (Deepcoin 引擎专属巡检 — 读取 .env 端口，支持多实例)
#
# 增强功能（v16.18）：
#   - TV Webhook 端点连通性检测
#   - 本地服务 + webhook 双重确认

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PORT=5004
if [ -f "$DIR/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "$DIR/.env"
    set +a
fi
PORT="${FLASK_PORT:-5004}"
WEBHOOK_URL="${DINGTALK_WEBHOOK:-}"
INSTANCE_LABEL="${INSTANCE_LABEL:-deepcoin}"
# TV webhook 目标地址
TV_WEBHOOK_URL="${TV_WEBHOOK_URL:-http://187.77.130.144/deepcoin/webhook}"

port_listen() {
    if command -v ss >/dev/null 2>&1 && ss -lnt "sport = :${PORT}" 2>/dev/null | grep -q LISTEN; then
        return 0
    fi
    if command -v netstat >/dev/null 2>&1 && netstat -tuln 2>/dev/null | grep -q ":${PORT} "; then
        return 0
    fi
    return 1
}

# 检测 TV webhook 连通性（ICMP + TCP）
check_tv_webhook() {
    TV_HOST=$(echo "$TV_WEBHOOK_URL" | sed -E 's|^https?://||' | cut -d'/' -f1)
    TV_PORT=$(echo "$TV_WEBHOOK_URL" | sed -E 's|^https?://||' | cut -d':' -f2 | cut -d'/' -f1)
    [ -z "$TV_PORT" ] && TV_PORT=80
    echo "$TV_WEBHOOK_URL" | grep -q "https" && TV_PORT=443

    # TCP 检测
    if command -v nc >/dev/null 2>&1; then
        if nc -zw 5 "$TV_HOST" "$TV_PORT" 2>/dev/null; then
            return 0
        fi
    else
        HTTP_CODE=$(curl -sf --connect-timeout 5 \
            -o /dev/null -w "%{http_code}" "$TV_WEBHOOK_URL" 2>/dev/null || echo "000")
        if [ "$HTTP_CODE" != "000" ]; then
            return 0
        fi
    fi
    return 1
}

if ! port_listen; then
    echo "$(date +'%Y-%m-%d %H:%M:%S') - 🚨 警告: [${INSTANCE_LABEL}] 深币引擎(端口${PORT})已离线，正在执行紧急抢救..."

    cd "$DIR" || exit 1
    bash deploy_deepcoin.sh
    sleep 3

    if port_listen; then
        STATUS_TEXT="✅ **抢救成功**：守护脚本已自动执行启动程序，${INSTANCE_LABEL} 现已恢复监听 ${PORT} 端口！"
    else
        STATUS_TEXT="❌ **抢救失败**：重启尝试无效，请立即使用 SSH 登入服务器排查日志！"
    fi

    if [ -n "$WEBHOOK_URL" ]; then
        MSG=$(cat <<EOF
{
    "msgtype": "markdown",
    "markdown": {
        "title": "🚨 深币引擎掉线警报",
        "text": "### 🚨 深币(Deepcoin) 极速引擎意外宕机！\n\n> **实例**: ${INSTANCE_LABEL}\n> **发生时间**: $(date +'%Y-%m-%d %H:%M:%S')\n> **进程状态**: 端口 ${PORT} 丢失\n> **目录**: ${DIR}\n\n**自动应对措施**:\n$STATUS_TEXT\n\n*🛡️ 深币系统底层巡检哨兵*"
    },
    "at": {"isAtAll": true}
}
EOF
)
        curl -s -H "Content-Type: application/json" -d "$MSG" "$WEBHOOK_URL" > /dev/null
    fi
else
    # 端口在线，额外检查 TV webhook 连通性
    if check_tv_webhook; then
        echo "$(date +'%Y-%m-%d %H:%M:%S') - ✅ 巡检正常: [${INSTANCE_LABEL}] Deepcoin 引擎 (Port ${PORT}) + TV Webhook 均正常。"
    else
        echo "$(date +'%Y-%m-%d %H:%M:%S') - ⚠️ 巡检告警: [${INSTANCE_LABEL}] Deepcoin 引擎在线 (Port ${PORT})，但 TV Webhook ($TV_WEBHOOK_URL) 连通异常，请检查 VPS 防火墙/Nginx！"
    fi
fi
