#!/usr/bin/env bash
# ==========================================
# 深币 Deepcoin — 工业级干净重部署脚本
# 流程: 强制核武清场 → 确认端口空闲 → 依赖 → 启动 → 多重健康审计
# ==========================================

set -uo pipefail

WORKERS=1
THREADS=10
BIND_HOST="0.0.0.0"
MAX_CLEANUP_ROUNDS=5
HEALTH_WAIT_SEC=5
HEALTH_RETRIES=6

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

# 端口优先读 .env 的 FLASK_PORT（多实例各自独立目录 + 独立 .env）
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

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[1;36m'
NC='\033[0m'

DEPLOY_OK=1

log_step() { echo -e "${YELLOW}$1${NC}"; }
log_ok()   { echo -e "  ${GREEN}✅ $1${NC}"; }
log_warn() { echo -e "  ${YELLOW}⚠️  $1${NC}"; }
log_fail() { echo -e "  ${RED}❌ $1${NC}"; DEPLOY_OK=0; }

load_env() {
    if [ -f "$DIR/.env" ]; then
        set -a
        # shellcheck disable=SC1091
        source "$DIR/.env"
        set +a
        log_ok "已加载 .env 配置"
    else
        log_warn "未找到 .env，将使用默认/环境变量"
    fi
    WEBHOOK_SECRET="${WEBHOOK_SECRET:-528586}"
    PORT="${FLASK_PORT:-5004}"
}

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
    # 只杀【本目录】启动的 gunicorn，绝不 fuser 整端口（防误杀币安/其他深币实例）
    local pid
    for pid in $(pids_listening_on_port "$PORT"); do
        if pid_belongs_to_instance "$pid"; then
            kill -9 "$pid" 2>/dev/null || true
            log_ok "已结束本实例监听进程 PID=${pid} (port=${PORT})"
        else
            log_warn "端口 ${PORT} 上 PID=${pid} 非本目录 ${DIR}，跳过误杀"
        fi
    done
}

kill_residual_processes() {
    # 仅清理【本实例】PID 文件 + 本目录 + 本端口 gunicorn
    if [ -f "$PID_FILE" ]; then
        OLD_PID="$(cat "$PID_FILE" 2>/dev/null || true)"
        if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" 2>/dev/null; then
            if pid_belongs_to_instance "$OLD_PID"; then
                kill -9 "$OLD_PID" 2>/dev/null || true
                log_ok "已结束 PID 文件记录进程 ${OLD_PID}"
            else
                log_warn "PID 文件 ${OLD_PID} 非本目录进程，跳过"
            fi
        fi
        rm -f "$PID_FILE"
    fi
    if command -v pgrep >/dev/null 2>&1; then
        pgrep -af "gunicorn" 2>/dev/null | grep ":${PORT}" | grep -F "${DIR}" | awk '{print $1}' \
            | while read -r pid; do
                [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null || true
            done
    fi
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
    log_warn "端口 ${PORT} 仍被占用，当前监听进程:"
    if command -v lsof >/dev/null 2>&1; then
        lsof -Pi :"${PORT}" -sTCP:LISTEN 2>/dev/null || true
    elif command -v ss >/dev/null 2>&1; then
        ss -lptn "sport = :${PORT}" 2>/dev/null || true
    elif command -v netstat >/dev/null 2>&1; then
        netstat -tulnp 2>/dev/null | grep ":${PORT} " || true
    fi
}

force_cleanup() {
    log_step "[1/6] 本实例清场（仅 port=${PORT} + dir=${DIR}，不碰其他实例）..."
    local round=1
    while [ "$round" -le "$MAX_CLEANUP_ROUNDS" ]; do
        echo "  -> 清场第 ${round}/${MAX_CLEANUP_ROUNDS} 轮..."
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

install_deps() {
    log_step "[2/6] 检查 Python 环境与依赖..."
    if [ -d "$DIR/venv" ]; then
        # shellcheck disable=SC1091
        source "$DIR/venv/bin/activate"
        log_ok "已激活 venv"
    else
        log_warn "未找到 venv，使用系统 Python"
    fi
    if ! command -v python3 >/dev/null 2>&1; then
        log_fail "未找到 python3"
        return 1
    fi
    if ! command -v pip >/dev/null 2>&1 && ! command -v pip3 >/dev/null 2>&1; then
        log_fail "未找到 pip"
        return 1
    fi
    PIP_CMD="pip"
    command -v pip3 >/dev/null 2>&1 && PIP_CMD="pip3"
    $PIP_CMD install -q -r "$DIR/requirements.txt"
    log_ok "requirements.txt 依赖已就绪"

    # 清理旧字节码，避免 gunicorn 加载过期 .pyc
    find "$DIR" -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
    find "$DIR" -name "*.pyc" -delete 2>/dev/null || true

    if grep -qE "CLIENT_VERSION.*v13\.4\.(7|8)" "$DIR/deepcoin_client.py" 2>/dev/null; then
        log_ok "deepcoin_client.py 版本已就绪 ($(grep CLIENT_VERSION "$DIR/deepcoin_client.py" | head -1))"
    else
        log_fail "deepcoin_client.py 不是最新版！请先 push/pull v13.4.8+ 代码"
        return 1
    fi

    if grep -qE "DEEPCOIN_SUPERVISOR_VERSION.*v13\.4\.(7|8)" "$DIR/position_supervisor_deepcoin.py" 2>/dev/null; then
        log_ok "position_supervisor_deepcoin.py 版本已就绪 ($(grep DEEPCOIN_SUPERVISOR_VERSION "$DIR/position_supervisor_deepcoin.py" | head -1))"
    else
        log_fail "position_supervisor_deepcoin.py 不是最新版！请先 push/pull v13.4.8+ 代码"
        return 1
    fi

    python3 -m py_compile "$DIR/app.py" "$DIR/deepcoin_client.py" \
        "$DIR/dingtalk.py" "$DIR/position_supervisor_deepcoin.py" 2>/dev/null \
        && log_ok "核心 Python 文件语法检查通过" \
        || log_warn "语法预检跳过（非致命）"
}

start_service() {
    log_step "[3/6] 启动 Gunicorn 网关 (workers=${WORKERS}, threads=${THREADS})..."
    mkdir -p "$LOG_DIR"
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

    sleep 1
    GUNICORN_PID="$(cat "$PID_FILE" 2>/dev/null || echo "")"
    if [ -z "$GUNICORN_PID" ] || ! kill -0 "$GUNICORN_PID" 2>/dev/null; then
        log_fail "Gunicorn 启动失败，请检查日志"
        tail -n 20 "$LOG_FILE" 2>/dev/null || true
        return 1
    fi
    log_ok "Gunicorn 已启动 PID=${GUNICORN_PID}"
}

wait_for_listen() {
    log_step "[4/6] 等待端口 ${PORT} 进入 LISTEN 状态..."
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

health_check() {
    log_step "[5/6] 多重健康审计..."
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
        log_ok "GET /health 正常 → ${HEALTH_BODY}"
    else
        log_fail "GET /health 异常 → ${HEALTH_BODY:-无响应}"
    fi

    # 6c. POST /webhook 回路（使用 .env 中的 WEBHOOK_SECRET）
    HTTP_STATUS="$(curl -s -o /dev/null -w "%{http_code}" \
        -X POST "http://127.0.0.1:${PORT}/webhook" \
        -H "Content-Type: application/json" \
        -d "{\"secret\":\"${WEBHOOK_SECRET}\",\"action\":\"PING\"}" 2>/dev/null || echo "000")"
    if [ "$HTTP_STATUS" = "200" ]; then
        log_ok "POST /webhook 回路 200 OK（secret 校验通过）"
    else
        log_fail "POST /webhook 异常 HTTP=${HTTP_STATUS}"
    fi

    # 6d. 大脑加载日志（大脑写入 deepcoin_brain.log，非 gunicorn error log）
    sleep 2
    if grep -qE "v13\.4\.(7|8)" "$BRAIN_LOG" 2>/dev/null; then
        log_ok "VPS 大脑 v13.4.8+ 已成功加载"
    elif grep -q "深币 VPS" "$BRAIN_LOG" 2>/dev/null || grep -q "军师托管版" "$BRAIN_LOG" 2>/dev/null; then
        log_warn "大脑已加载但版本可能过旧（日志中无 v13.4.8+，请确认代码已更新）"
    elif grep -q "深币 VPS" "$LOG_DIR/gunicorn_error.log" 2>/dev/null; then
        log_ok "VPS 大脑模块已成功加载 (gunicorn_error.log)"
    else
        log_warn "日志中暂未看到大脑加载字样（请 tail -f logs/deepcoin_brain.log 确认）"
    fi

    # 6e. 进程清单（仅本实例端口）
    echo -e "  ${CYAN}→ 当前实例进程 (port=${PORT}):${NC}"
    ps -ef 2>/dev/null | grep -E "gunicorn.*:${PORT}" | grep -v grep \
        | awk '{print "     PID="$2" CMD="$8" "$9" "$10" "$11}' || true
}

print_summary() {
    log_step "[6/6] 部署结果汇总"
    echo ""
    if [ "$DEPLOY_OK" -eq 1 ]; then
        echo -e "${GREEN}=== 🚀 深币(Deepcoin) 干净重部署成功 ===${NC}"
        echo -e "  网关地址: http://${BIND_HOST}:${PORT}/webhook"
        echo -e "  健康检查: http://127.0.0.1:${PORT}/health"
        echo -e "  大脑日志: tail -f ${BRAIN_LOG}"
        echo -e "  访问日志: tail -f ${LOG_DIR}/gunicorn_access.log"
        echo -e "  错误日志: tail -f ${LOG_DIR}/gunicorn_error.log"
    else
        echo -e "${RED}=== ❌ 深币部署未完全通过，请排查上述失败项 ===${NC}"
        echo -e "  最近日志:"
        tail -n 15 "$LOG_FILE" 2>/dev/null || true
        exit 1
    fi
    echo ""
}

# ── 主流程 ──
echo -e "\n${CYAN}=== 深币系统 · 干净重部署开始 ===${NC}"
echo -e "  工作目录: ${DIR}"
echo -e "  目标端口: ${PORT}"
echo ""

load_env
force_cleanup || exit 1
install_deps || exit 1
start_service || exit 1
wait_for_listen || exit 1
health_check
print_summary
