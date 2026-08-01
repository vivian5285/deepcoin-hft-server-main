#!/usr/bin/env bash
# ==========================================
# Deepcoin HFT Server - 网络连通性检测脚本
# 
# 检测内容:
#   - 内部网络
#   - Deepcoin API
#   - TradingView webhook 端点
#   - Telegram API
#   - VPS 本地服务
#
# 用法:
#   bash check_network.sh
# ==========================================

set -uo pipefail

# ── 颜色 ─────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[1;36m'
NC='\033[0m'

# ── 配置 ─────────────────────────────────────────────
# 从 .env 加载配置
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "$SCRIPT_DIR/.env"
    set +a
fi

# VPS webhook URL (TradingView 推送地址)
TV_WEBHOOK_URL="${TV_WEBHOOK_URL:-http://187.77.130.144/deepcoin/webhook}"

# ── 检测函数 ─────────────────────────────────────────────

check_pass() { echo -e "  ${GREEN}✅ $1${NC}"; }
check_fail() { echo -e "  ${RED}❌ $1${NC}"; }
check_warn() { echo -e "  ${YELLOW}⚠️  $1${NC}"; }
check_info() { echo -e "  ${CYAN}➤  $1${NC}"; }

# ── 1. 内部网络检测 ─────────────────────────────────────
check_internal_network() {
    echo -e "\n${CYAN}═══════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  1. 内部网络检测${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"
    
    # 默认网关
    GATEWAY=$(ip route | grep default | awk '{print $3}' | head -1 2>/dev/null || echo "")
    if [ -z "$GATEWAY" ]; then
        GATEWAY=$(route -n | grep UG | awk '{print $2}' | head -1 2>/dev/null || echo "")
    fi
    
    if [ -n "$GATEWAY" ]; then
        check_pass "默认网关: $GATEWAY"
    else
        check_warn "未检测到默认网关"
    fi
    
    # DNS
    if [ -f /etc/resolv.conf ]; then
        DNS=$(grep nameserver /etc/resolv.conf | head -1 | awk '{print $2}' 2>/dev/null || echo "")
        if [ -n "$DNS" ]; then
            check_pass "DNS服务器: $DNS"
        fi
    fi
    
    # 外网连通性
    check_info "测试外网连通性..."
    if ping -c 1 -W 3 8.8.8.8 >/dev/null 2>&1; then
        check_pass "外网连通正常 (ping 8.8.8.8)"
    elif curl -sf --connect-timeout 3 https://www.baidu.com >/dev/null 2>&1; then
        check_pass "外网连通正常 (curl baidu.com)"
    elif curl -sf --connect-timeout 3 https://github.com >/dev/null 2>&1; then
        check_pass "外网连通正常 (curl github.com)"
    else
        check_fail "外网连通失败"
    fi
}

# ── 2. Deepcoin API 检测 ─────────────────────────────────────
check_deepcoin_api() {
    echo -e "\n${CYAN}═══════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  2. Deepcoin API 检测${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"
    
    check_info "检测 Deepcoin 域名..."
    
    # 检测多个可能的 Deepcoin API 端点
    ENDPOINTS=(
        "https://www.deepcoin.com"
        "https://api.deepcoin.com"
        "https://open-api.deepcoin.com"
    )
    
    for endpoint in "${ENDPOINTS[@]}"; do
        if curl -sf --connect-timeout 5 -o /dev/null -w "%{http_code}" "$endpoint" 2>/dev/null | grep -qE "200|301|302|400|401|403"; then
            check_pass "Deepcoin API: $endpoint"
        fi
    done
    
    # 如果都失败，尝试通用检测
    if ! curl -sf --connect-timeout 5 "https://www.deepcoin.com" >/dev/null 2>&1; then
        check_fail "Deepcoin API 连通失败"
        check_info "提示: 检查防火墙/网络策略"
    fi
}

# ── 3. TradingView Webhook 端点检测 ─────────────────────────────────────
check_tv_webhook() {
    echo -e "\n${CYAN}═══════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  3. TradingView Webhook 端点检测${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"
    
    # 提取域名/IP
    TV_HOST=$(echo "$TV_WEBHOOK_URL" | sed -E 's|^https?://||' | cut -d'/' -f1)
    TV_PORT=$(echo "$TV_WEBHOOK_URL" | sed -E 's|^https?://||' | cut -d':' -f2 | cut -d'/' -f1)
    
    if [ -z "$TV_PORT" ]; then
        TV_PORT=80
        if echo "$TV_WEBHOOK_URL" | grep -q "https"; then
            TV_PORT=443
        fi
    fi
    
    check_info "目标端点: $TV_WEBHOOK_URL"
    check_info "主机: $TV_HOST"
    
    # TCP 连通性
    check_info "测试 TCP 连通性..."
    if command -v nc >/dev/null 2>&1; then
        if nc -zw 3 "$TV_HOST" "$TV_PORT" 2>/dev/null; then
            check_pass "TCP 连通: $TV_HOST:$TV_PORT"
        else
            check_fail "TCP 连通失败: $TV_HOST:$TV_PORT"
        fi
    elif command -v timeout >/dev/null 2>&1; then
        if timeout 3 bash -c "echo >/dev/tcp/$TV_HOST/$TV_PORT" 2>/dev/null; then
            check_pass "TCP 连通: $TV_HOST:$TV_PORT"
        else
            check_fail "TCP 连通失败: $TV_HOST:$TV_PORT"
        fi
    else
        check_warn "无法测试 TCP 连通性 (nc/timeout 未安装)"
    fi
    
    # HTTP 检测
    check_info "测试 HTTP 响应..."
    HTTP_CODE=$(curl -sf --connect-timeout 5 -o /dev/null -w "%{http_code}" "$TV_WEBHOOK_URL" 2>/dev/null || echo "000")
    
    if [ "$HTTP_CODE" = "200" ]; then
        check_pass "HTTP 响应正常: $HTTP_CODE"
    elif [ "$HTTP_CODE" = "405" ]; then
        check_pass "HTTP 响应正常 (POST 方法): $HTTP_CODE"
    elif [ "$HTTP_CODE" = "000" ]; then
        check_fail "HTTP 请求失败 (连接超时/拒绝)"
    else
        check_warn "HTTP 响应异常: $HTTP_CODE"
    fi
    
    # 本地 VPS 自检
    check_info "测试本地回路..."
    LOCAL_PORT=$(echo "$TV_WEBHOOK_URL" | sed -E 's|^https?://||' | grep -oE ':[0-9]+' | tr -d ':' | head -1)
    if [ -n "$LOCAL_PORT" ]; then
        if curl -sf --connect-timeout 3 "http://127.0.0.1:$LOCAL_PORT/health" >/dev/null 2>&1; then
            check_pass "本地回路正常 (port=$LOCAL_PORT)"
        else
            check_warn "本地回路失败 (port=$LOCAL_PORT) - 服务可能未启动"
        fi
    fi
}

# ── 4. Telegram API 检测 ─────────────────────────────────────
check_telegram_api() {
    echo -e "\n${CYAN}═══════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  4. Telegram API 检测${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"
    
    if [ -z "${TG_BOT_TOKEN:-}" ]; then
        check_warn "TG_BOT_TOKEN 未配置"
        return
    fi
    
    check_info "TG Bot Token: ${TG_BOT_TOKEN:0:10}..."
    
    RESP=$(curl -sf --connect-timeout 5 "https://api.telegram.org/bot${TG_BOT_TOKEN}/getMe" 2>/dev/null || echo "")
    
    if echo "$RESP" | grep -q '"ok":true'; then
        BOT_NAME=$(echo "$RESP" | grep -oE '"username":"[^"]+"' | head -1 | cut -d'"' -f4)
        check_pass "Telegram API 正常"
        check_info "Bot名称: @$BOT_NAME"
    else
        check_fail "Telegram API 异常"
        check_info "错误: $(echo "$RESP" | grep -oE '"description":"[^"]+"' | head -1 | cut -d'"' -f4)"
    fi
}

# ── 5. GitHub 检测 ─────────────────────────────────────
check_github() {
    echo -e "\n${CYAN}═══════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  5. GitHub 检测${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"
    
    check_info "测试 GitHub 连通性..."
    
    if curl -sf --connect-timeout 5 -o /dev/null -w "%{http_code}" "https://github.com" 2>/dev/null | grep -qE "200|301"; then
        check_pass "GitHub 连通正常"
    else
        check_fail "GitHub 连通失败"
    fi
    
    # 测试 GitHub API
    if curl -sf --connect-timeout 5 -o /dev/null -w "%{http_code}" "https://api.github.com" 2>/dev/null | grep -qE "200"; then
        check_pass "GitHub API 连通正常"
    else
        check_warn "GitHub API 连通失败"
    fi
    
    # 检查仓库
    if [ -d ".git" ]; then
        REMOTE=$(git remote get-url origin 2>/dev/null || echo "")
        if echo "$REMOTE" | grep -q "github.com"; then
            check_pass "Git 远程仓库: $REMOTE"
        fi
    fi
}

# ── 6. 系统信息 ─────────────────────────────────────
show_system_info() {
    echo -e "\n${CYAN}═══════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  6. 系统信息${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"
    
    check_info "主机名: $(hostname 2>/dev/null || echo '未知')"
    check_info "操作系统: $(uname -s) $(uname -r)"
    check_info "内核: $(uname -v 2>/dev/null || echo '未知')"
    check_info "Python: $(python3 --version 2>&1 || echo '未安装')"
    
    # 网络接口
    check_info "网络接口:"
    ip -br addr show 2>/dev/null | grep -v "lo" | while read -r line; do
        echo -e "    ${CYAN}  $line${NC}"
    done
    
    # 监听端口
    check_info "监听端口:"
    if command -v ss >/dev/null 2>&1; then
        ss -tlnp 2>/dev/null | grep LISTEN | grep -v "127.0.0.1" | head -5 | while read -r line; do
            echo -e "    ${CYAN}  $line${NC}"
        done
    fi
}

# ── 7. VPS 本地服务状态 ─────────────────────────────────────
check_local_service() {
    echo -e "\n${CYAN}═══════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  7. VPS 本地服务状态${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"
    
    PORT="${FLASK_PORT:-5004}"
    
    check_info "Flask 端口: $PORT"
    
    # 端口检测
    if command -v lsof >/dev/null 2>&1; then
        if lsof -Pi :"$PORT" -sTCP:LISTEN -t >/dev/null 2>&1; then
            check_pass "端口 $PORT 已被监听"
            lsof -Pi :"$PORT" -sTCP:LISTEN 2>/dev/null | grep -v COMMAND | while read -r line; do
                echo -e "    ${CYAN}  $line${NC}"
            done
        else
            check_warn "端口 $PORT 未被监听 (服务可能未启动)"
        fi
    fi
    
    # 健康检查
    check_info "健康检查..."
    HEALTH=$(curl -sf --connect-timeout 3 "http://127.0.0.1:$PORT/health" 2>/dev/null || echo "")
    if echo "$HEALTH" | grep -q "deepcoin"; then
        check_pass "服务健康检查通过"
        check_info "响应: ${HEALTH:0:80}"
    else
        check_warn "服务健康检查失败"
    fi
    
    # 进程检测
    check_info "Gunicorn 进程:"
    if pgrep -af "gunicorn.*:$PORT" >/dev/null 2>&1; then
        pgrep -af "gunicorn.*:$PORT" 2>/dev/null | while read -r line; do
            echo -e "    ${GREEN}  $line${NC}"
        done
    else
        check_warn "未发现 Gunicorn 进程"
    fi
}

# ── 主流程 ─────────────────────────────────────────────────────
main() {
    echo -e "\n${CYAN}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  Deepcoin HFT Server - 网络连通性检测${NC}"
    echo -e "${CYAN}  $(date '+%Y-%m-%d %H:%M:%S')${NC}"
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
