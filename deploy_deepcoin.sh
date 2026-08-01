#!/usr/bin/env bash
# ==========================================
# 深币 Deepcoin — 工业级自动部署脚本 (v17.1)
#
# 功能: GitHub拉取 → 核武清场 → 依赖安装 → 启动 → 多重健康审计 + 网络检测
#
# 用法:
#   bash deploy_deepcoin.sh          # 标准方式
#   ./deploy_deepcoin.sh            # 需先 chmod +x
# ==========================================

set -uo pipefail

SCRIPT_VERSION="v17.1-deploy-robust"
GITHUB_REMOTE_URL="https://github.com/vivian5285/deepcoin-hft-server-main.git"
GITHUB_BRANCH="main"

WORKERS=1
THREADS=10
BIND_HOST="0.0.0.0"
MAX_CLEANUP_ROUNDS=5
HEALTH_WAIT_SEC=5
HEALTH_RETRIES=6

# ── 工作目录自动检测 ──────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ "$(basename "$SCRIPT_DIR")" = "deepcoin-hft-server" ]; then
    DIR="$SCRIPT_DIR"
else
    if [ -f "$SCRIPT_DIR/position_supervisor_deepcoin.py" ]; then
        DIR="$SCRIPT_DIR"
    elif [ -f "$(dirname "$SCRIPT_DIR")/position_supervisor_deepcoin.py" ]; then
        DIR="$(dirname "$SCRIPT_DIR")"
    else
        DIR="$(pwd)"
        while [ "$DIR" != "/" ]; do
            if [ -f "$DIR/position_supervisor_deepcoin.py" ]; then
                break
            fi
            DIR="$(dirname "$DIR")"
        done
    fi
fi
cd "$DIR"

# ── 端口配置（优先读 .env）────────────────────────────────
PORT=5004
if [ -f "$DIR/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "$DIR/.env"
    set +a
fi
PORT="${FLASK_PORT:-5004}"

LOG_DIR="$DIR/logs"
LOG_FILE="$LOG_DIR/supervisor_deepcoin.log"
BRAIN_LOG="$LOG_DIR/deepcoin_brain.log"
PID_FILE="$LOG_DIR/gunicorn_deepcoin.pid"

# ── 日志颜色 ─────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[1;36m'
NC='\033[0m'

DEPLOY_OK=1
DEPLOY_STEP=0

log_step() { DEPLOY_STEP=$((DEPLOY_STEP + 1)); echo -e "\n${YELLOW}  [${DEPLOY_STEP}] $1${NC}"; }
log_ok()   { echo -e "    ${GREEN}✅ $1${NC}"; }
log_warn() { echo -e "    ${YELLOW}⚠️  $1${NC}"; }
log_fail() { echo -e "    ${RED}❌ $1${NC}"; DEPLOY_OK=0; }

# ═══════════════════════════════════════════════════════════
# 步骤 0：网络连通性检测
# ═══════════════════════════════════════════════════════════
check_network() {
    log_step "网络连通性检测..."
    
    local net_ok=1
    
    # 内部连通性：检测本机网关
    echo -e "    ${CYAN}  检测内部网络...${NC}"
    if ip route get 8.8.8..8 >/dev/null 2>&1 || ping -c 1 -W 2 8.8.8.8 >/dev/null 2>&1; then
        log_ok "内部网络正常 (能访问外网)"
    else
        log_warn "内部网络可能受限"
    fi
    
    # Deepcoin API 连通性
    echo -e "    ${CYAN}  检测 Deepcoin API...${NC}"
    if curl -sf --connect-timeout 5 "https://www.deepcoin.com" >/dev/null 2>&1; then
        log_ok "Deepcoin API 连通正常"
    elif curl -sf --connect-timeout 5 "https://api.deepcoin.com" >/dev/null 2>&1; then
        log_ok "Deepcoin API 连通正常"
    else
        log_warn "Deepcoin API 连通性检测失败 (继续部署)"
    fi
    
    # TradingView webhook 端点连通性 (如果配置了)
    if [ -n "${TV_WEBHOOK_URL:-}" ]; then
        echo -e "    ${CYAN}  检测 TV Webhook URL...${NC}"
        if curl -sf --connect-timeout 5 -o /dev/null -w "%{http_code}" "$TV_WEBHOOK_URL" >/dev/null 2>&1; then
            log_ok "TV Webhook URL 连通正常"
        else
            log_warn "TV Webhook URL 连通性检测失败"
        fi
    fi
    
    # Telegram API 连通性
    if [ -n "${TG_BOT_TOKEN:-}" ]; then
        echo -e "    ${CYAN}  检测 Telegram API...${NC}"
        if curl -sf --connect-timeout 5 "https://api.telegram.org/bot${TG_BOT_TOKEN}/getMe" >/dev/null 2>&1; then
            log_ok "Telegram API 连通正常"
        else
            log_warn "Telegram API 连通性检测失败 (TG通知可能不可用)"
        fi
    fi
}

# ═══════════════════════════════════════════════════════════
# 步骤 1：GitHub 拉取最新代码
# ═══════════════════════════════════════════════════════════
git_update() {
    log_step "从 GitHub 拉取最新代码..."

    if ! command -v git >/dev/null 2>&1; then
        log_warn "git 未安装，跳过代码更新（直接使用当前代码）"
        return 0
    fi

    if [ ! -d "$DIR/.git" ]; then
        log_warn "非 git 仓库，无法自动拉取（请手动克隆或检查目录）"
        return 0
    fi

    CURRENT_REMOTE=$(git remote get-url origin 2>/dev/null || echo "")
    if [ -n "$CURRENT_REMOTE" ]; then
        log_ok "远程仓库: $CURRENT_REMOTE"
    fi

    if [ "$CURRENT_REMOTE" != "$GITHUB_REMOTE_URL" ]; then
        git remote add upstream "$GITHUB_REMOTE_URL" 2>/dev/null || \
        git remote set-url upstream "$GITHUB_REMOTE_URL" 2>/dev/null || true
    fi

    CURRENT_BRANCH=$(git branch --show-current 2>/dev/null || git symbolic-ref --short HEAD 2>/dev/null || echo "")
    if [ "$CURRENT_BRANCH" != "$GITHUB_BRANCH" ]; then
        log_warn "当前分支: $CURRENT_BRANCH，切换到 $GITHUB_BRANCH..."
        git fetch origin "$GITHUB_BRANCH" 2>/dev/null || true
        if git show-ref --verify --quiet "refs/heads/$GITHUB_BRANCH" 2>/dev/null; then
            git checkout "$GITHUB_BRANCH" 2>/dev/null || true
        fi
    fi

    echo -e "    ${CYAN}    git fetch origin...${NC}"
    if git fetch origin "$GITHUB_BRANCH" 2>&1 | tee /dev/stderr | grep -q "Already up to date"; then
        log_ok "代码已是最新"
    else
        GIT_OUTPUT=$(git pull origin "$GITHUB_BRANCH" 2>&1)
        if echo "$GIT_OUTPUT" | grep -qE "(Already up to date|拉取到最新|up.to.date)"; then
            log_ok "代码已是最新"
        elif echo "$GIT_OUTPUT" | grep -qE "(Updating|Fast-forward|Merge)"; then
            log_ok "代码已更新: $(echo "$GIT_OUTPUT" | tail -1)"
        else
            log_warn "git pull 输出: $(echo "$GIT_OUTPUT" | tail -1)"
        fi
    fi

    # 显示当前版本
    SUP_VER=$(grep 'DEEPCOIN_SUPERVISOR_VERSION' "$DIR/position_supervisor_deepcoin.py" 2>/dev/null \
        | head -1 | sed 's/.*= *//' | tr -d '"' | tr -d "'" | tr -d ' ' || echo "未知")
    CLI_VER=$(grep 'CLIENT_VERSION' "$DIR/deepcoin_client.py" 2>/dev/null \
        | head -1 | sed 's/.*= *//' | tr -d '"' | tr -d "'" | tr -d ' ' || echo "未知")
    echo -e "    ${CYAN}    supervisor: $SUP_VER | client: $CLI_VER${NC}"
    
    # Git commit hash
    if [ -d "$DIR/.git" ]; then
        COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "未知")
        echo -e "    ${CYAN}    commit: $COMMIT${NC}"
    fi
}

# ═══════════════════════════════════════════════════════════
# 步骤 2：核武清场
# ═══════════════════════════════════════════════════════════
pids_listening_on_port() {
    local port=$1
    local pids=""
    if command -v lsof >/dev/null 2>&1; then
        pids="$(lsof -t -iTCP:"${port}" -sTCP:LISTEN 2>/dev/null || true)"
    fi
    if [ -z "$pids" ] && command -v ss >/dev/null 2>&1; then
        pids="$(ss -lptn "sport = :${port}" 2>/dev/null \
            | sed -n 's/.*pid=\([0-9][0-9]*\).*/\1/p' | sort -u || true)"
    fi
    echo "$pids"
}

pid_belongs_to_instance() {
    local pid=$1
    local cmd=""
    cmd="$(ps -p "$pid" -o args= 2>/dev/null || true)"
    [ -n "$cmd" ] && echo "$cmd" | grep -qF "${DIR}"
}

kill_port_if_ours() {
    local killed=0
    for pid in $(pids_listening_on_port "$PORT"); do
        if pid_belongs_to_instance "$pid"; then
            kill -9 "$pid" 2>/dev/null || true
            echo -e "    ${GREEN}    已结束本实例进程 PID=$pid (port=$PORT)${NC}"
            killed=$((killed + 1))
        else
            echo -e "    ${YELLOW}    跳过非本目录进程 PID=$pid${NC}"
        fi
    done
    [ "$killed" -gt 0 ] && return 0 || return 0
}

kill_residual_processes() {
    if [ -f "$PID_FILE" ]; then
        OLD_PID="$(cat "$PID_FILE" 2>/dev/null || true)"
        if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" 2>/dev/null; then
            if pid_belongs_to_instance "$OLD_PID"; then
                kill -9 "$OLD_PID" 2>/dev/null || true
                echo -e "    ${GREEN}    已结束 PID 文件进程 ${OLD_PID}${NC}"
            else
                echo -e "    ${YELLOW}    PID 文件 ${OLD_PID} 非本目录，跳过${NC}"
            fi
        fi
        rm -f "$PID_FILE"
    fi

    if command -v pgrep >/dev/null 2>&1; then
        pgrep -af "gunicorn" 2>/dev/null \
            | grep ":${PORT}" | grep -F "${DIR}" \
            | awk '{print $1}' \
            | while read -r pid; do
                [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null || true
            done
    fi

    rm -f "${DIR}/logs/.recover_singleton.lock" 2>/dev/null || true
}

port_in_use() {
    if command -v lsof >/dev/null 2>&1 && lsof -Pi :"${PORT}" -sTCP:LISTEN -t >/dev/null 2>&1; then
        return 0
    fi
    if command -v ss >/dev/null 2>&1 && ss -lnt "sport = :${PORT}" 2>/dev/null | grep -q LISTEN; then
        return 0
    fi
    if command -v netstat >/dev/null 2>&1 && netstat -tuln 2>/dev/null | grep -q ":${PORT} "; then
        return 0
    fi
    return 1
}

show_port_holders() {
    echo -e "    ${YELLOW}    端口 ${PORT} 当前监听进程:${NC}"
    if command -v lsof >/dev/null 2>&1; then
        lsof -Pi :"${PORT}" -sTCP:LISTEN 2>/dev/null || true
    elif command -v ss >/dev/null 2>&1; then
        ss -lptn "sport = :${PORT}" 2>/dev/null || true
    elif command -v netstat >/dev/null 2>&1; then
        netstat -tulnp 2>/dev/null | grep ":${PORT} " || true
    fi
}

force_cleanup() {
    log_step "核武清场 — 清理旧进程与端口..."
    echo -e "    ${CYAN}    目标端口: $PORT | 目录: $DIR${NC}"
    local round=1
    while [ "$round" -le "$MAX_CLEANUP_ROUNDS" ]; do
        echo -e "    ${CYAN}    第 ${round}/${MAX_CLEANUP_ROUNDS} 轮清场...${NC}"
        kill_residual_processes
        kill_port_if_ours
        sleep 1.2
        if ! port_in_use; then
            log_ok "端口 ${PORT} 已完全释放，清场成功"
            return 0
        fi
        round=$((round + 1))
    done
    show_port_holders
    log_fail "经过 ${MAX_CLEANUP_ROUNDS} 轮清场，端口 ${PORT} 仍被占用，部署中止"
    return 1
}

# ═══════════════════════════════════════════════════════════
# 步骤 3：依赖安装
# ═══════════════════════════════════════════════════════════
install_deps() {
    log_step "检查 Python 环境与依赖..."

    if [ -d "$DIR/venv" ]; then
        # shellcheck disable=SC1091
        source "$DIR/venv/bin/activate"
        log_ok "已激活 venv"
    elif [ -d "$HOME/deepcoin-hft-server/venv" ]; then
        # shellcheck disable=SC1091
        source "$HOME/deepcoin-hft-server/venv/bin/activate"
        log_ok "已激活上级目录 venv"
    else
        log_warn "未找到 venv，使用系统 Python"
    fi

    if ! command -v python3 >/dev/null 2>&1; then
        log_fail "未找到 python3"
        return 1
    fi
    log_ok "python3 已就绪: $(python3 --version 2>&1)"

    PIP_CMD="pip"
    command -v pip3 >/dev/null 2>&1 && PIP_CMD="pip3"
    if [ -f "$DIR/requirements.txt" ]; then
        $PIP_CMD install -q -r "$DIR/requirements.txt" 2>&1 | tail -3
        log_ok "requirements.txt 依赖已安装"
    else
        log_warn "requirements.txt 不存在，跳过"
    fi

    find "$DIR" -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
    find "$DIR" -name "*.pyc" -delete 2>/dev/null || true

    CORE_FILES="$DIR/app.py $DIR/deepcoin_client.py $DIR/telegram_notify.py $DIR/position_supervisor_deepcoin.py"
    PYCRC=$(python3 -m py_compile $CORE_FILES 2>&1 || true)
    if [ -z "$PYCRC" ]; then
        log_ok "核心 Python 文件语法检查通过"
    else
        log_warn "语法预检有警告（非致命）: ${PYCRC:0:120}"
    fi

    SUP_VER=$(grep 'DEEPCOIN_SUPERVISOR_VERSION' "$DIR/position_supervisor_deepcoin.py" 2>/dev/null \
        | head -1 | sed 's/.*= *//' | tr -d '"' | tr -d "'" | tr -d ' ' || echo "未知")
    CLI_VER=$(grep 'CLIENT_VERSION' "$DIR/deepcoin_client.py" 2>/dev/null \
        | head -1 | sed 's/.*= *//' | tr -d '"' | tr -d "'" | tr -d ' ' || echo "未知")
    log_ok "supervisor: $SUP_VER"
    log_ok "client: $CLI_VER"
}

# ═══════════════════════════════════════════════════════════
# 步骤 4：启动服务
# ═══════════════════════════════════════════════════════════
start_service() {
    log_step "启动 Gunicorn 网关 (workers=${WORKERS}, threads=${THREADS})..."
    mkdir -p "$LOG_DIR"
    touch "$BRAIN_LOG" 2>/dev/null || true
    chmod 664 "$BRAIN_LOG" 2>/dev/null || true
    chmod 775 "$LOG_DIR" 2>/dev/null || true
    : > "$LOG_FILE"

    nohup gunicorn \
        --workers "$WORKERS" \
        --threads "$THREADS" \
        --timeout 120 \
        --graceful-timeout 30 \
        --bind "${BIND_HOST}:${PORT}" \
        --pid "$PID_FILE" \
        --access-logfile "$LOG_DIR/gunicorn_access.log" \
        --error-logfile "$LOG_DIR/gunicorn_error.log" \
        --capture-output \
        app:app >> "$LOG_FILE" 2>&1 &

    sleep 2
    GUNICORN_PID="$(cat "$PID_FILE" 2>/dev/null || echo "")"
    if [ -z "$GUNICORN_PID" ] || ! kill -0 "$GUNICORN_PID" 2>/dev/null; then
        log_fail "Gunicorn 启动失败，请检查日志"
        tail -n 20 "$LOG_FILE" 2>/dev/null || true
        return 1
    fi
    log_ok "Gunicorn 已启动 PID=${GUNICORN_PID}"
}

# ═══════════════════════════════════════════════════════════
# 步骤 5：等待端口监听
# ═══════════════════════════════════════════════════════════
wait_for_listen() {
    log_step "等待端口 ${PORT} 进入 LISTEN 状态..."
    local i=1
    while [ "$i" -le "$HEALTH_RETRIES" ]; do
        if port_in_use; then
            log_ok "端口 ${PORT} 已开始监听 (第 ${i} 次检测)"
            return 0
        fi
        sleep 1
        i=$((i + 1))
    done
    log_fail "Gunicorn 进程存在但端口 ${PORT} 未监听"
    tail -n 20 "$LOG_FILE" 2>/dev/null || true
    return 1
}

# ═══════════════════════════════════════════════════════════
# 步骤 6：多重健康审计
# ═══════════════════════════════════════════════════════════
health_check() {
    log_step "多重健康审计..."
    sleep "$HEALTH_WAIT_SEC"

    # 6a. 进程存活
    GUNICORN_PID="$(cat "$PID_FILE" 2>/dev/null || echo "")"
    if [ -n "$GUNICORN_PID" ] && kill -0 "$GUNICORN_PID" 2>/dev/null; then
        log_ok "Gunicorn 主进程存活 PID=${GUNICORN_PID}"
    else
        log_fail "Gunicorn 主进程已退出"
    fi

    # 6b. GET /health
    HEALTH_BODY="$(curl -sf "http://127.0.0.1:${PORT}/health" 2>/dev/null || echo "")"
    if echo "$HEALTH_BODY" | grep -q "deepcoin_webhook"; then
        log_ok "GET /health 正常 → ${HEALTH_BODY:0:120}"
    else
        log_fail "GET /health 异常 → ${HEALTH_BODY:-无响应}"
    fi

    # 6c. POST /webhook 回路
    HTTP_STATUS="$(curl -s -o /dev/null -w "%{http_code}" \
        -X POST "http://127.0.0.1:${PORT}/webhook" \
        -H "Content-Type: application/json" \
        -d "{\"secret\":\"${WEBHOOK_SECRET:-528586}\",\"action\":\"PING\"}" 2>/dev/null || echo "000")"
    if [ "$HTTP_STATUS" = "200" ]; then
        log_ok "POST /webhook 回路 200 OK（secret 校验通过）"
    else
        log_fail "POST /webhook 异常 HTTP=${HTTP_STATUS}"
    fi

    # 6d. 大脑加载日志
    sleep 2
    if grep -qE 'v(13\.(4\.[6-9]|[5-9]|[1-9][0-9])|16\.|17\.)' "$BRAIN_LOG" 2>/dev/null; then
        log_ok "VPS 大脑已成功加载"
    elif grep -q "深币 VPS" "$BRAIN_LOG" 2>/dev/null; then
        log_ok "VPS 大脑已加载"
    elif grep -q "深币 VPS" "$LOG_DIR/gunicorn_error.log" 2>/dev/null; then
        log_ok "VPS 大脑模块已加载 (gunicorn_error.log)"
    else
        log_warn "日志中暂未看到大脑加载字样（请 tail -f 确认）"
    fi

    # 6e. 外部访问测试
    echo -e "    ${CYAN}    外部访问测试...${NC}"
    if curl -sf --connect-timeout 5 "http://127.0.0.1:${PORT}/health" >/dev/null 2>&1; then
        log_ok "本地回路测试通过"
    else
        log_warn "本地回路测试失败"
    fi

    # 6f. 进程清单
    echo -e "    ${CYAN}    当前实例进程 (port=$PORT):${NC}"
    ps -ef 2>/dev/null | grep -E "gunicorn.*:${PORT}" | grep -v grep \
        | awk '{print "     PID="$2" CMD="$8" "$9" "$10" "$11}' || true
}

# ═══════════════════════════════════════════════════════════
# 步骤 7：TG 部署通知
# ═══════════════════════════════════════════════════════════
send_tg_deploy_notification() {
    if [ -z "${TG_BOT_TOKEN:-}" ] || [ -z "${TG_CHAT_ID:-}" ]; then
        echo -e "    ${YELLOW}    TG 配置未设置，跳过部署通知${NC}"
        return
    fi
    
    local status="$1"
    local message=""
    if [ "$status" = "success" ]; then
        message="✅ *Deepcoin 部署成功*
部署时间: $(date '+%Y-%m-%d %H:%M:%S')
版本: $SCRIPT_VERSION
端口: $PORT
Webook: http://$(hostname -I 2>/dev/null | awk '{print $1}'):${PORT}/webhook
健康检查: http://127.0.0.1:${PORT}/health"
    else
        message="❌ *Deepcoin 部署失败*
部署时间: $(date '+%Y-%m-%d %H:%M:%S')
版本: $SCRIPT_VERSION
请检查日志排查问题"
    fi
    
    curl -sf -X POST "https://api.telegram.org/bot${TG_BOT_TOKEN}/sendMessage" \
        -d "chat_id=${TG_CHAT_ID}" \
        -d "text=${message}" \
        -d "parse_mode=Markdown" >/dev/null 2>&1 || true
}

# ═══════════════════════════════════════════════════════════
# 步骤 8：汇总
# ═══════════════════════════════════════════════════════════
print_summary() {
    log_step "部署结果汇总"
    echo ""
    if [ "$DEPLOY_OK" -eq 1 ]; then
        echo -e "${GREEN}===  深币(Deepcoin) 部署成功  ===${NC}"
        echo -e "  网关地址: http://0.0.0.0:${PORT}/webhook"
        echo -e "  健康检查: http://127.0.0.1:${PORT}/health"
        echo -e "  大脑日志: tail -f ${BRAIN_LOG}"
        echo -e "  访问日志: tail -f ${LOG_DIR}/gunicorn_access.log"
        echo -e "  错误日志: tail -f ${LOG_DIR}/gunicorn_error.log"
        echo ""
        send_tg_deploy_notification "success"
    else
        echo -e "${RED}===  深币部署未完全通过，请排查上述失败项  ===${NC}"
        echo -e "  最近日志:"
        tail -n 15 "$LOG_FILE" 2>/dev/null || true
        send_tg_deploy_notification "failure"
        exit 1
    fi
}

# ────────────────────────────────────────────────────────
# 主流程
# ────────────────────────────────────────────────────────
echo -e "\n${CYAN}═══════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  深币 Deepcoin 自动部署 [${SCRIPT_VERSION}]${NC}"
echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"
echo -e "  工作目录: ${DIR}"
echo -e "  目标端口: ${PORT}"
echo -e "  脚本版本: ${SCRIPT_VERSION}"
echo ""

check_network
git_update
force_cleanup || exit 1
install_deps || exit 1
start_service || exit 1
wait_for_listen || exit 1
health_check
print_summary
