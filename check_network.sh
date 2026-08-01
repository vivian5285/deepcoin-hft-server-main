#!/usr/bin/env bash
# ==========================================
# Deepcoin HFT Server - 网络连通性检测脚本
#
# 检测内容:
#   - 内部网络（网关 / DNS / 内网段）
#   - 外部网络（8.8.8.8 / baidu / github）
#   - TradingView Webhook 端点 (http://187.77.130.144/deepcoin/webhook)
#   - VPS 本地 Webhook 服务 (127.0.0.1:port/health)
#   - VPS 公网 Webhook (http://<公网IP>/deepcoin/webhook)
#   - Deepcoin API
#   - Telegram API
#   - GitHub
#   - VPS 本地服务进程
#
# 用法:
#   bash check_network.sh
#   bash check_network.sh --quick    # 仅检测关键项
# ==========================================

set -uo pipefail

# ── 颜色 ─────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[1;36m'
NC='\033[0m'

# ── 配置 ─────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "$SCRIPT_DIR/.env"
    set +a
fi

FLASK_PORT="${FLASK_PORT:-5004}"
# TradingView webhook 目标地址
TV_WEBHOOK_URL="${TV_WEBHOOK_URL:-http://187.77.130.144/deepcoin/webhook}"

QUICK=false
[[ "${1:-}" == "--quick" ]] && QUICK=true

# ── 检测函数 ─────────────────────────────────────────────
check_pass()  { echo -e "  ${GREEN}✅ $1${NC}"; }
check_fail()  { echo -e "  ${RED}❌ $1${NC}"; }
check_warn()  { echo -e "  ${YELLOW}⚠️  $1${NC}"; }
check_info()  { echo -e "  ${CYAN}➤  $1${NC}"; }

# ── 1. 内部网络检测 ─────────────────────────────────────
check_internal_network() {
    echo -e "\n${CYAN}═══════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  1. 内部网络检测${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"

    # 默认网关
    GATEWAY=$(ip route 2>/dev/null | grep default | awk '{print $3}' | head -1)
    [ -z "$GATEWAY" ] && GATEWAY=$(route -n 2>/dev/null | grep UG | awk '{print $2}' | head -1)

    if [ -n "$GATEWAY" ]; then
        check_pass "默认网关: $GATEWAY"
        if ping -c 1 -W 2 "$GATEWAY" >/dev/null 2>&1; then
            check_pass "网关 ping 正常"
        else
            check_warn "网关 ping 失败（网络可能有问题）"
        fi
    else
        check_warn "未检测到默认网关"
    fi

    # DNS
    if [ -f /etc/resolv.conf ]; then
        DNS=$(grep nameserver /etc/resolv.conf 2>/dev/null | head -1 | awk '{print $2}')
        if [ -n "$DNS" ]; then
            check_pass "DNS 服务器: $DNS"
            if nslookup google.com "$DNS" >/dev/null 2>&1 || nslookup baidu.com "$DNS" >/dev/null 2>&1; then
                check_pass "DNS 解析正常"
            else
                check_warn "DNS 解析失败"
            fi
        fi
    fi

    # 内网段连通性（VPS 常见内网）
    INTERNAL_HOSTS=("10.0.0.1" "192.168.0.1" "172.16.0.1")
    for h in "${INTERNAL_HOSTS[@]}"; do
        if ping -c 1 -W 2 "$h" >/dev/null 2>&1; then
            check_info "内网主机可达: $h"
            break
        fi
    done

    # 外网连通性（多手段）
    check_info "测试外网连通性..."
    if ping -c 2 -W 3 8.8.8.8 >/dev/null 2>&1; then
        check_pass "外网连通正常 (ping 8.8.8.8)"
    elif curl -sf --connect-timeout 5 https://www.baidu.com >/dev/null 2>&1; then
        check_pass "外网连通正常 (curl baidu.com)"
    elif curl -sf --connect-timeout 5 https://github.com >/dev/null 2>&1; then
        check_pass "外网连通正常 (curl github.com)"
    else
        check_fail "外网连通全部失败"
    fi
}

# ── 2. Deepcoin API 检测 ─────────────────────────────────────
check_deepcoin_api() {
    echo -e "\n${CYAN}═══════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  2. Deepcoin API 检测${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"

    check_info "检测 Deepcoin 域名..."

    ENDPOINTS=(
        "https://www.deepcoin.com"
        "https://api.deepcoin.com"
        "https://open-api.deepcoin.com"
    )

    ANY_OK=false
    for endpoint in "${ENDPOINTS[@]}"; do
        HTTP_CODE=$(curl -sf --connect-timeout 5 \
            -o /dev/null -w "%{http_code}" "$endpoint" 2>/dev/null || echo "000")
        if [ "$HTTP_CODE" = "000" ]; then
            check_fail "不可达: $endpoint"
        else
            check_pass "Deepcoin API: $endpoint (HTTP $HTTP_CODE)"
            ANY_OK=true
        fi
    done

    if [ "$ANY_OK" = false ]; then
        check_fail "Deepcoin API 全部不可达，请检查防火墙或网络策略"
    fi
}

# ── 3. TradingView Webhook 端点检测 ─────────────────────────────────────
check_tv_webhook() {
    echo -e "\n${CYAN}═══════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  3. TradingView Webhook 端点检测${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"

    check_info "TV webhook 目标: $TV_WEBHOOK_URL"

    # 提取 host / port
    TV_HOST=$(echo "$TV_WEBHOOK_URL" | sed -E 's|^https?://||' | cut -d'/' -f1)
    TV_PORT=$(echo "$TV_WEBHOOK_URL" | sed -E 's|^https?://||' | cut -d':' -f2 | cut -d'/' -f1)
    [ -z "$TV_PORT" ] && TV_PORT=80
    echo "$TV_WEBHOOK_URL" | grep -q "https" && TV_PORT=443

    check_info "目标主机: $TV_HOST"
    check_info "目标端口: $TV_PORT"

    # --- ICMP ping ---
    check_info "ICMP ping $TV_HOST..."
    if ping -c 2 -W 4 "$TV_HOST" >/dev/null 2>&1; then
        check_pass "ICMP ping 成功: $TV_HOST"
    else
        check_warn "ICMP ping 失败（可能被防火墙屏蔽 ICMP，但仍可 TCP 连接）"
    fi

    # --- TCP 连通性 ---
    check_info "TCP 连通性测试..."
    if command -v nc >/dev/null 2>&1; then
        if nc -zw 5 "$TV_HOST" "$TV_PORT" 2>/dev/null; then
            check_pass "TCP 连通: $TV_HOST:$TV_PORT"
        else
            check_fail "TCP 连通失败: $TV_HOST:$TV_PORT"
        fi
    elif command -v timeout >/dev/null 2>&1; then
        if timeout 5 bash -c "echo >/dev/tcp/$TV_HOST/$TV_PORT" 2>/dev/null; then
            check_pass "TCP 连通: $TV_HOST:$TV_PORT"
        else
            check_fail "TCP 连通失败: $TV_HOST:$TV_PORT"
        fi
    else
        HTTP_CODE=$(curl -sf --connect-timeout 5 \
            -o /dev/null -w "%{http_code}" "$TV_WEBHOOK_URL" 2>/dev/null || echo "000")
        if [ "$HTTP_CODE" = "000" ]; then
            check_fail "TCP/Http 连通失败: $TV_HOST:$TV_PORT"
        else
            check_pass "HTTP 连通成功: HTTP $HTTP_CODE"
        fi
    fi

    # --- HTTP 检测 ---
    check_info "HTTP 请求测试..."
    HTTP_CODE=$(curl -sf --connect-timeout 5 \
        -o /dev/null -w "%{http_code}" "$TV_WEBHOOK_URL" 2>/dev/null || echo "000")
    case "$HTTP_CODE" in
        200)  check_pass "HTTP 响应正常: 200 OK" ;;
        405)  check_pass "HTTP 正常 (POST only): 405 Method Not Allowed" ;;
        502)  check_warn "HTTP 502 Bad Gateway（Nginx/服务问题）" ;;
        503)  check_warn "HTTP 503 Service Unavailable（服务未启动）" ;;
        000)  check_fail "HTTP 连接超时/拒绝" ;;
        *)    check_warn "HTTP 响应异常: $HTTP_CODE" ;;
    esac

    # --- 本地回路（VPS 内部服务）---
    check_info "测试本地 Webhook 服务: http://127.0.0.1:$FLASK_PORT/health"
    LOCAL_HEALTH=$(curl -sf --connect-timeout 3 \
        "http://127.0.0.1:$FLASK_PORT/health" 2>/dev/null || echo "")
    if echo "$LOCAL_HEALTH" | grep -q "deepcoin"; then
        VER=$(echo "$LOCAL_HEALTH" | grep -oE '"version":"[^"]+"' | head -1 | cut -d'"' -f4)
        check_pass "本地回路正常: 版本 $VER"
    else
        check_warn "本地回路失败（服务可能未启动）"
    fi

    # --- 公网 webhook（通过公网 IP）---
    PUBLIC_IP=$(curl -sf --connect-timeout 5 https://api.ipify.org 2>/dev/null || echo "")
    if [ -n "$PUBLIC_IP" ]; then
        check_info "公网 IP: $PUBLIC_IP"
        PUB_URL="http://$PUBLIC_IP/deepcoin/webhook"
        check_info "测试公网 Webhook: $PUB_URL"
        PUB_CODE=$(curl -sf --connect-timeout 5 \
            -o /dev/null -w "%{http_code}" "$PUB_URL" 2>/dev/null || echo "000")
        case "$PUB_CODE" in
            200)  check_pass "公网 Webhook 正常: 200" ;;
            405)  check_pass "公网 Webhook 正常 (POST only): 405" ;;
            000)  check_fail "公网 Webhook 连接失败（防火墙未开 ${FLASK_PORT}）" ;;
            *)    check_warn "公网 Webhook HTTP: $PUB_CODE" ;;
        esac
    else
        check_info "公网 IP 获取失败，跳过公网 Webhook 检测"
    fi
}

# ── 4. Telegram API 检测 ─────────────────────────────────────
check_telegram_api() {
    [ "$QUICK" = true ] && return
    echo -e "\n${CYAN}═══════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  4. Telegram API 检测${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"

    if [ -z "${TG_BOT_TOKEN:-}" ]; then
        check_warn "TG_BOT_TOKEN 未配置"
        return
    fi

    check_info "TG Bot Token: ${TG_BOT_TOKEN:0:10}..."

    RESP=$(curl -sf --connect-timeout 5 \
        "https://api.telegram.org/bot${TG_BOT_TOKEN}/getMe" 2>/dev/null || echo "")

    if echo "$RESP" | grep -q '"ok":true'; then
        BOT_NAME=$(echo "$RESP" | grep -oE '"username":"[^"]+"' | head -1 | cut -d'"' -f4)
        check_pass "Telegram API 正常"
        check_info "Bot 名称: @$BOT_NAME"
    else
        check_fail "Telegram API 异常"
        check_info "错误: $(echo "$RESP" | grep -oE '"description":"[^"]+"' | head -1 | cut -d'"' -f4)"
    fi
}

# ── 5. GitHub 检测 ─────────────────────────────────────
check_github() {
    [ "$QUICK" = true ] && return
    echo -e "\n${CYAN}═══════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  5. GitHub 检测${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"

    check_info "测试 GitHub 连通性..."
    if curl -sf --connect-timeout 5 \
        -o /dev/null -w "%{http_code}" "https://github.com" 2>/dev/null | grep -qE "200|301"; then
        check_pass "GitHub 连通正常"
    else
        check_fail "GitHub 连通失败"
    fi

    if curl -sf --connect-timeout 5 \
        -o /dev/null -w "%{http_code}" "https://api.github.com" 2>/dev/null | grep -qE "200"; then
        check_pass "GitHub API 连通正常"
    else
        check_warn "GitHub API 连通失败"
    fi

    if [ -d ".git" ]; then
        REMOTE=$(git remote get-url origin 2>/dev/null || echo "")
        if echo "$REMOTE" | grep -q "github.com"; then
            check_pass "Git 远程仓库: $REMOTE"
        fi
    fi
}

# ── 6. 系统信息 ─────────────────────────────────────
show_system_info() {
    [ "$QUICK" = true ] && return
    echo -e "\n${CYAN}═══════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  6. 系统信息${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"

    check_info "主机名: $(hostname 2>/dev/null || echo '未知')"
    check_info "操作系统: $(uname -s) $(uname -r)"
    check_info "Python: $(python3 --version 2>&1 || echo '未安装')"
    check_info "负载: $(uptime 2>/dev/null | awk -F'load average:' '{print $2}' || echo '未知')"

    check_info "网络接口:"
    ip -br addr show 2>/dev/null | grep -v "lo" | head -5 | while read -r line; do
        echo -e "    ${CYAN}  $line${NC}"
    done

    check_info "监听端口:"
    if command -v ss >/dev/null 2>&1; then
        ss -tlnp 2>/dev/null | grep LISTEN | head -5 | while read -r line; do
            echo -e "    ${CYAN}  $line${NC}"
        done
    fi
}

# ── 7. VPS 本地服务状态 ─────────────────────────────────────
check_local_service() {
    echo -e "\n${CYAN}═══════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  7. VPS 本地服务状态${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"

    PORT="$FLASK_PORT"
    check_info "Flask 端口: $PORT"

    # 端口监听
    if command -v lsof >/dev/null 2>&1; then
        if lsof -Pi :"${PORT}" -sTCP:LISTEN -t >/dev/null 2>&1; then
            check_pass "端口 $PORT 已被监听"
            lsof -Pi :"${PORT}" -sTCP:LISTEN 2>/dev/null | grep -v COMMAND | while read -r line; do
                echo -e "    ${CYAN}  $line${NC}"
            done
        else
            check_warn "端口 $PORT 未被监听（服务可能未启动）"
        fi
    fi

    # 健康检查
    check_info "健康检查..."
    HEALTH=$(curl -sf --connect-timeout 3 \
        "http://127.0.0.1:$PORT/health" 2>/dev/null || echo "")
    if echo "$HEALTH" | grep -q "deepcoin"; then
        VER=$(echo "$HEALTH" | grep -oE '"version":"[^"]+"' | head -1 | cut -d'"' -f4)
        check_pass "服务健康检查通过 (v$VER)"
        check_info "响应: ${HEALTH:0:100}"
    else
        check_warn "服务健康检查失败"
    fi

    # 进程检测
    check_info "Gunicorn 进程:"
    if pgrep -af "gunicorn.*:${PORT}" >/dev/null 2>&1; then
        pgrep -af "gunicorn.*:${PORT}" 2>/dev/null | while read -r line; do
            echo -e "    ${GREEN}  $line${NC}"
        done
    else
        check_warn "未发现 Gunicorn 进程"
    fi

    # 内存占用
    if command -v ps >/dev/null 2>&1; then
        MEM=$(ps aux 2>/dev/null | grep "gunicorn.*:${PORT}" | grep -v grep | awk '{sum+=$6} END {printf "%.1f MB", sum/1024}')
        if [ -n "$MEM" ] && [ "$MEM" != "0.0 MB" ]; then
            check_info "Gunicorn 内存占用: $MEM"
        fi
    fi
}

# ── 主流程 ─────────────────────────────────────────────────────
main() {
    echo -e "\n${CYAN}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  Deepcoin HFT Server - 网络连通性检测${NC}"
    echo -e "${CYAN}  $(date '+%Y-%m-%d %H:%M:%S')${NC}"
    echo -e "${CYAN}  模式: $([ "$QUICK" = true ] && echo '快速' || echo '完整')${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════════════${NC}"

    check_internal_network
    check_deepcoin_api
    check_tv_webhook
    check_telegram_api
    check_github
    show_system_info
    check_local_service

    echo -e "\n${CYAN}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  检测完成${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════════════${NC}"
    echo ""
}

main "$@"
