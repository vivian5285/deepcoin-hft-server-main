#!/usr/bin/env bash
# ==========================================
# Deepcoin HFT Server - 部署脚本
# ==========================================
# 功能：
#   1. 自检网络连通性（内部 + TV webhook + Deepcoin API）
#   2. Git pull 最新代码
#   3. 安装/更新依赖
#   4. 重启 Gunicorn 服务
#   5. 健康检查确认
#
# 用法:
#   bash deploy_deepcoin.sh          # 完整部署
#   bash deploy_deepcoin.sh --check  # 仅自检，不重启
# ==========================================

set -uo pipefail

# ── 颜色 ─────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[1;36m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHECK_ONLY=false

# ── 参数解析 ─────────────────────────────────────────────
if [[ "${1:-}" == "--check" ]]; then
    CHECK_ONLY=true
fi

# ── 加载 .env ─────────────────────────────────────────────
if [ -f "$SCRIPT_DIR/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "$SCRIPT_DIR/.env"
    set +a
fi

DEEPCOIN_API_KEY="${DEEPCOIN_API_KEY:-}"
DEEPCOIN_API_SECRET="${DEEPCOIN_API_SECRET:-}"
DEEPCOIN_PASSPHRASE="${DEEPCOIN_PASSPHRASE:-}"
FLASK_PORT="${FLASK_PORT:-5004}"
INSTANCE_LABEL="${INSTANCE_LABEL:-deepcoin}"
WEBHOOK_SECRET="${WEBHOOK_SECRET:-}"

# TV webhook 目标地址（TradingView 推送地址）
TV_WEBHOOK_URL="${TV_WEBHOOK_URL:-http://187.77.130.144/deepcoin/webhook}"
# Deepcoin WebSocket endpoint
DEEPCOIN_WS_URL="${DEEPCOIN_WS_URL:-wss://www.deepcoin.com}"
# Deepcoin REST endpoint
DEEPCOIN_REST_URL="${DEEPCOIN_REST_URL:-https://api.deepcoin.com}"

# ── 辅助函数 ─────────────────────────────────────────────
pass()   { echo -e "  ${GREEN}✅ $1${NC}"; }
fail()   { echo -e "  ${RED}❌ $1${NC}"; }
warn()   { echo -e "  ${YELLOW}⚠️  $1${NC}"; }
info()   { echo -e "  ${CYAN}➤  $1${NC}"; }
section(){ echo -e "\n${CYAN}══ $1 ══${NC}"; }

# ── 1. 预检 ─────────────────────────────────────────────
preflight_check() {
    section "1. 预检"
    [ -n "$DEEPCOIN_API_KEY" ] && pass "DEEPCOIN_API_KEY 已配置"
    [ -n "$DEEPCOIN_API_SECRET" ] && pass "DEEPCOIN_API_SECRET 已配置"
    [ -n "$DEEPCOIN_PASSPHRASE" ] && pass "DEEPCOIN_PASSPHRASE 已配置"
    [ -n "$WEBHOOK_SECRET" ] && pass "WEBHOOK_SECRET 已配置"

    if [ -z "$DEEPCOIN_API_KEY" ] || [ -z "$DEEPCOIN_API_SECRET" ]; then
        fail "Deepcoin API 密钥未配置，请编辑 .env"
        return 1
    fi
    return 0
}

# ── 2. 网络连通性自检 ─────────────────────────────────────────────
network_check() {
    section "2. 网络连通性自检"

    ALL_OK=true

    # --- 内部网络 ---
    info "测试外网连通性 (ping 8.8.8.8)..."
    if ping -c 2 -W 3 8.8.8.8 >/dev/null 2>&1; then
        pass "外网连通正常"
    else
        warn "ping 8.8.8.8 失败，尝试 curl..."
        if curl -sf --connect-timeout 5 https://www.baidu.com >/dev/null 2>&1; then
            pass "外网连通正常 (curl)"
        else
            fail "外网连通失败"
            ALL_OK=false
        fi
    fi

    # --- Deepcoin API ---
    info "测试 Deepcoin API..."
    if curl -sf --connect-timeout 5 -o /dev/null "https://www.deepcoin.com" 2>/dev/null; then
        pass "Deepcoin 主站连通"
    elif curl -sf --connect-timeout 5 -o /dev/null "https://api.deepcoin.com" 2>/dev/null; then
        pass "Deepcoin API 连通"
    else
        warn "Deepcoin API 连通性检测失败（可能需要代理）"
    fi

    # --- TV Webhook 目标 ---
    info "测试 TV Webhook 目标连通性: $TV_WEBHOOK_URL"
    TV_HOST=$(echo "$TV_WEBHOOK_URL" | sed -E 's|^https?://||' | cut -d'/' -f1)
    TV_PORT=$(echo "$TV_WEBHOOK_URL" | sed -E 's|^https?://||' | cut -d':' -f2 | cut -d'/' -f1)
    [ -z "$TV_PORT" ] && TV_PORT=80
    if echo "$TV_WEBHOOK_URL" | grep -q "https"; then TV_PORT=443; fi

    # TCP ping
    if command -v nc >/dev/null 2>&1; then
        if nc -zw 5 "$TV_HOST" "$TV_PORT" 2>/dev/null; then
            pass "TV Webhook TCP 连通: $TV_HOST:$TV_PORT"
        else
            fail "TV Webhook TCP 连通失败: $TV_HOST:$TV_PORT"
            ALL_OK=false
        fi
    else
        # fallback: curl
        HTTP_CODE=$(curl -sf --connect-timeout 5 -o /dev/null -w "%{http_code}" \
            "$TV_WEBHOOK_URL" 2>/dev/null || echo "000")
        if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "405" ]; then
            pass "TV Webhook HTTP 响应: $HTTP_CODE"
        elif [ "$HTTP_CODE" = "000" ]; then
            fail "TV Webhook 连接超时/拒绝: $TV_HOST:$TV_PORT"
            ALL_OK=false
        else
            warn "TV Webhook HTTP 异常: $HTTP_CODE"
        fi
    fi

    # --- VPS 内部 webhook 服务（127.0.0.1）---
    info "测试本地 Webhook 服务: http://127.0.0.1:$FLASK_PORT/health"
    HEALTH=$(curl -sf --connect-timeout 5 "http://127.0.0.1:$FLASK_PORT/health" 2>/dev/null || echo "")
    if echo "$HEALTH" | grep -q "deepcoin"; then
        VER=$(echo "$HEALTH" | grep -oE '"version":"[^"]+"' | head -1 | cut -d'"' -f4)
        pass "本地服务健康: 版本 $VER"
    else
        warn "本地服务未响应或版本不匹配（服务可能未启动）"
    fi

    # --- VPS 外部暴露的 webhook（通过公网 IP）---
    PUBLIC_IP=$(curl -sf --connect-timeout 5 https://api.ipify.org 2>/dev/null || echo "")
    if [ -n "$PUBLIC_IP" ]; then
        info "测试公网 Webhook: http://$PUBLIC_IP/deepcoin/webhook"
        PUB_CODE=$(curl -sf --connect-timeout 5 \
            -o /dev/null -w "%{http_code}" \
            "http://$PUBLIC_IP/deepcoin/webhook" 2>/dev/null || echo "000")
        if [ "$PUB_CODE" = "200" ] || [ "$PUB_CODE" = "405" ]; then
            pass "公网 Webhook 正常: $PUB_CODE"
        elif [ "$PUB_CODE" = "000" ]; then
            warn "公网 Webhook 连接失败（可能防火墙未开 $FLASK_PORT）"
        else
            warn "公网 Webhook HTTP: $PUB_CODE"
        fi
    else
        info "公网 IP 获取失败，跳过公网 Webhook 检测"
    fi

    if [ "$ALL_OK" = false ]; then
        warn "部分网络检测失败，部署可能受影响"
    fi
}

# ── 3. Git 更新 ─────────────────────────────────────────────
git_update() {
    section "3. Git 更新"

    if [ ! -d ".git" ]; then
        warn "非 Git 仓库，跳过 Git 更新"
        return 0
    fi

    info "当前分支: $(git branch --show-current 2>/dev/null || echo 'unknown')"
    info "远程仓库: $(git remote get-url origin 2>/dev/null || echo 'unknown')"

    # 暂存本地修改
    if git status --porcelain | grep -q .; then
        warn "存在本地修改，暂存..."
        git stash push -m "deploy_$(date +%Y%m%d_%H%M%S)" 2>/dev/null || true
    fi

    info "拉取最新代码..."
    if git fetch origin 2>/dev/null; then
        LOCAL=$(git rev-parse HEAD 2>/dev/null)
        REMOTE=$(git rev-parse origin/$(git branch --show-current 2>/dev/null || echo 'main') 2>/dev/null)
        if [ "$LOCAL" != "$REMOTE" ]; then
            git reset --hard origin/$(git branch --show-current 2>/dev/null || echo 'main') 2>/dev/null
            pass "代码已更新到最新"
        else
            pass "代码已是最新，无需更新"
        fi
    else
        warn "git fetch 失败，使用现有代码"
    fi
}

# ── 4. 依赖安装 ─────────────────────────────────────────────
install_deps() {
    section "4. 依赖安装"

    if [ ! -f "requirements.txt" ]; then
        warn "requirements.txt 不存在，跳过"
        return 0
    fi

    # 虚拟环境
    if [ ! -d "venv" ]; then
        info "创建虚拟环境..."
        python3 -m venv venv
        pass "虚拟环境已创建"
    fi

    info "安装/更新依赖..."
    # shellcheck disable=SC1091
    if ! source venv/bin/activate 2>/dev/null; then
        warn "激活虚拟环境失败，尝试系统 pip"
        pip install -q -r requirements.txt 2>/dev/null || \
        pip3 install -q -r requirements.txt 2>/dev/null || true
    else
        pip install -q --upgrade pip
        pip install -q -r requirements.txt
    fi
    pass "依赖安装完成"
}

# ── 5. 服务重启 ─────────────────────────────────────────────
restart_service() {
    section "5. 服务重启"

    PORT="$FLASK_PORT"

    # 查找并停止旧进程
    info "停止旧进程 (port=$PORT)..."
    for pid in $(lsof -ti:"$PORT" 2>/dev/null || ss -tlnp | grep ":$PORT " | grep -oE 'pid=[0-9]+' | cut -d= -f2 2>/dev/null || true); do
        if [ -n "$pid" ] && [ "$pid" != "$$" ]; then
            kill -TERM "$pid" 2>/dev/null && info "已发送 TERM → $pid" || true
        fi
    done
    sleep 2

    # 强制 kill
    for pid in $(lsof -ti:"$PORT" 2>/dev/null || true); do
        if [ -n "$pid" ]; then
            kill -9 "$pid" 2>/dev/null && info "强制 kill → $pid" || true
        fi
    done
    sleep 1

    # 启动新进程
    info "启动服务 (port=$PORT)..."
    mkdir -p logs

    # shellcheck disable=SC1091
    if [ -d "venv" ] && source venv/bin/activate 2>/dev/null; then
        nohup venv/bin/python -m gunicorn \
            -w 2 \
            -b "0.0.0.0:$PORT" \
            --timeout 60 \
            --access-logfile logs/gunicorn_access.log \
            --error-logfile logs/gunicorn_error.log \
            --capture-output \
            app:app \
            > /dev/null 2>&1 &
    else
        nohup python3 -m gunicorn \
            -w 2 \
            -b "0.0.0.0:$PORT" \
            --timeout 60 \
            --access-logfile logs/gunicorn_access.log \
            --error-logfile logs/gunicorn_error.log \
            --capture-output \
            app:app \
            > /dev/null 2>&1 &
    fi

    NEW_PID=$!
    sleep 3

    if kill -0 "$NEW_PID" 2>/dev/null; then
        pass "进程已启动 (PID=$NEW_PID)"
    else
        fail "进程启动失败，查看 logs/gunicorn_error.log"
    fi
}

# ── 6. 健康检查 ─────────────────────────────────────────────
health_check() {
    section "6. 健康检查"

    PORT="$FLASK_PORT"
    for i in 1 2 3 4 5; do
        HEALTH=$(curl -sf --connect-timeout 3 "http://127.0.0.1:$PORT/health" 2>/dev/null || echo "")
        if echo "$HEALTH" | grep -q "deepcoin"; then
            VER=$(echo "$HEALTH" | grep -oE '"version":"[^"]+"' | head -1 | cut -d'"' -f4)
            pass "服务健康检查通过 (v$VER)"
            echo -e "\n${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
            echo -e "${GREEN}  🎉 部署成功！${NC}"
            echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
            info "Webhook: http://127.0.0.1:$PORT/webhook"
            info "TV 推送: $TV_WEBHOOK_URL"
            info "健康检查: curl http://127.0.0.1:$PORT/health"
            info "日志: tail -f logs/deepcoin_brain.log"
            return 0
        fi
        sleep 2
    done

    fail "健康检查失败，请查看日志:"
    info "tail -100 logs/gunicorn_error.log"
    info "tail -100 logs/deepcoin_brain.log"
    return 1
}

# ── 主流程 ─────────────────────────────────────────────
main() {
    echo -e "\n${CYAN}════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  Deepcoin HFT 部署脚本${NC}"
    echo -e "${CYAN}  $(date '+%Y-%m-%d %H:%M:%S')${NC}"
    echo -e "${CYAN}  实例: ${INSTANCE_LABEL} | 端口: ${FLASK_PORT}${NC}"
    echo -e "${CYAN}════════════════════════════════════════════════════════${NC}"

    cd "$SCRIPT_DIR" || exit 1

    # 自检模式
    if [ "$CHECK_ONLY" = true ]; then
        preflight_check
        network_check
        echo -e "\n${CYAN}════════════════════════════════════════════════════════${NC}"
        echo -e "${CYAN}  自检完成（未部署）${NC}"
        echo -e "${CYAN}════════════════════════════════════════════════════════${NC}"
        exit 0
    fi

    # 完整部署
    preflight_check || exit 1
    network_check
    git_update
    install_deps
    restart_service
    health_check
}

main "$@"
