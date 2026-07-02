#!/bin/bash
# system_monitor.sh (Deepcoin 引擎专属巡检 — 读取 .env 端口，支持多实例)

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

port_listen() {
    if command -v ss >/dev/null 2>&1 && ss -lnt "sport = :${PORT}" 2>/dev/null | grep -q LISTEN; then
        return 0
    fi
    if command -v netstat >/dev/null 2>&1 && netstat -tuln 2>/dev/null | grep -q ":${PORT} "; then
        return 0
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
    echo "$(date +'%Y-%m-%d %H:%M:%S') - ✅ 巡检正常: [${INSTANCE_LABEL}] Deepcoin 引擎 (Port ${PORT}) 运行健康中。"
fi
