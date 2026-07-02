#!/usr/bin/env bash
# ==========================================
# 深币 · 新用户实例一键初始化（root 下新建独立项目）
# 代码共用同一 GitHub 仓库，仅 .env + 端口 + 反向代理不同
#
# 用法:
#   bash setup_new_instance.sh <目录名> <端口> [git仓库URL]
#
# 示例:
#   bash setup_new_instance.sh /root/deepcoin-user2 5005
#   bash setup_new_instance.sh /root/deepcoin-user2 5005 git@github.com:you/deepcoin-hft-server.git
# ==========================================

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[1;36m'
NC='\033[0m'

TARGET_DIR="${1:-}"
TARGET_PORT="${2:-}"
GIT_REPO="${3:-}"

DEFAULT_REPO="git@github.com:vivian5285/deepcoin-hft-server-main.git"

if [ -z "$TARGET_DIR" ] || [ -z "$TARGET_PORT" ]; then
    echo -e "${RED}用法: bash setup_new_instance.sh <目录绝对路径> <端口> [git仓库URL]${NC}"
    echo -e "示例: bash setup_new_instance.sh /root/deepcoin-user2 5005"
    exit 1
fi

if ! [[ "$TARGET_PORT" =~ ^[0-9]+$ ]] || [ "$TARGET_PORT" -lt 1024 ] || [ "$TARGET_PORT" -gt 65535 ]; then
    echo -e "${RED}端口无效: ${TARGET_PORT}${NC}"
    exit 1
fi

if [ -z "$GIT_REPO" ]; then
    GIT_REPO="$DEFAULT_REPO"
    echo -e "${YELLOW}未指定仓库 URL，将使用脚本内 DEFAULT_REPO，请先编辑 setup_new_instance.sh${NC}"
fi

if [ -d "$TARGET_DIR" ] && [ "$(ls -A "$TARGET_DIR" 2>/dev/null | wc -l)" -gt 0 ]; then
    echo -e "${RED}目录已存在且非空: ${TARGET_DIR}${NC}"
    exit 1
fi

# 端口占用检查
port_busy() {
    if command -v ss >/dev/null 2>&1 && ss -lnt "sport = :${TARGET_PORT}" 2>/dev/null | grep -q LISTEN; then
        return 0
    fi
    if command -v lsof >/dev/null 2>&1 && lsof -Pi :"${TARGET_PORT}" -sTCP:LISTEN -t >/dev/null 2>&1; then
        return 0
    fi
    return 1
}

if port_busy; then
    echo -e "${RED}端口 ${TARGET_PORT} 已被占用，请换一个端口${NC}"
    exit 1
fi

echo -e "\n${CYAN}=== 深币新实例初始化 ===${NC}"
echo -e "  目录: ${TARGET_DIR}"
echo -e "  端口: ${TARGET_PORT}"
echo -e "  仓库: ${GIT_REPO}"
echo ""

mkdir -p "$(dirname "$TARGET_DIR")"
git clone "$GIT_REPO" "$TARGET_DIR"
cd "$TARGET_DIR"

echo -e "${YELLOW}[1/5] 创建 Python 虚拟环境...${NC}"
python3 -m venv venv
# shellcheck disable=SC1091
source venv/bin/activate
pip install -q --upgrade pip
pip install -q -r requirements.txt
echo -e "  ${GREEN}✅ venv 就绪${NC}"

echo -e "${YELLOW}[2/5] 生成 .env ...${NC}"
if [ ! -f .env.example ]; then
    echo -e "${RED}缺少 .env.example${NC}"
    exit 1
fi
cp .env.example .env
sed -i "s/^FLASK_PORT=.*/FLASK_PORT=${TARGET_PORT}/" .env
INSTANCE_NAME="$(basename "$TARGET_DIR")"
if grep -q "^INSTANCE_LABEL=" .env; then
    sed -i "s/^INSTANCE_LABEL=.*/INSTANCE_LABEL=${INSTANCE_NAME}/" .env
fi
# 随机 webhook secret
RAND_SECRET="$(python3 -c 'import secrets; print(secrets.token_hex(4))')"
sed -i "s/^WEBHOOK_SECRET=.*/WEBHOOK_SECRET=${RAND_SECRET}/" .env
echo -e "  ${GREEN}✅ .env 已生成 (FLASK_PORT=${TARGET_PORT}, WEBHOOK_SECRET=${RAND_SECRET})${NC}"
echo -e "  ${YELLOW}⚠️  请编辑 ${TARGET_DIR}/.env 填入 Deepcoin API 与钉钉${NC}"

echo -e "${YELLOW}[3/5] 生成实例巡检脚本...${NC}"
sed "s/__PORT__/${TARGET_PORT}/g; s|__DIR__|${TARGET_DIR}|g" \
    "$TARGET_DIR/deploy/system_monitor.instance.sh.template" \
    > "$TARGET_DIR/system_monitor.sh"
chmod +x "$TARGET_DIR/system_monitor.sh" "$TARGET_DIR/deploy_deepcoin.sh"
echo -e "  ${GREEN}✅ system_monitor.sh 已生成${NC}"

echo -e "${YELLOW}[4/5] 生成 Nginx 反向代理片段...${NC}"
NGINX_SNIP="$TARGET_DIR/deploy/nginx-${INSTANCE_NAME}.conf.snippet"
sed "s/__PORT__/${TARGET_PORT}/g; s/__INSTANCE__/${INSTANCE_NAME}/g" \
    "$TARGET_DIR/deploy/nginx.instance.conf.template" > "$NGINX_SNIP"
echo -e "  ${GREEN}✅ ${NGINX_SNIP}${NC}"

echo -e "${YELLOW}[5/5] 部署服务...${NC}"
if grep -q '^DEEPCOIN_API_KEY=$' .env 2>/dev/null || grep -q '^DEEPCOIN_API_KEY=\s*$' .env 2>/dev/null; then
    echo -e "  ${YELLOW}⚠️  API Key 未填写，跳过 deploy。请编辑 .env 后执行: bash deploy_deepcoin.sh${NC}"
else
    bash deploy_deepcoin.sh
fi

echo ""
echo -e "${GREEN}=== 🎉 新实例初始化完成 ===${NC}"
echo -e "  工作目录: ${TARGET_DIR}"
echo -e "  本地 Webhook: http://127.0.0.1:${TARGET_PORT}/webhook"
echo -e "  健康检查:     curl -s http://127.0.0.1:${TARGET_PORT}/health"
echo -e "  WEBHOOK_SECRET: ${RAND_SECRET}  (已写入 .env)"
echo ""
echo -e "${CYAN}下一步:${NC}"
echo -e "  1. vim ${TARGET_DIR}/.env   # 填入 DEEPCOIN_API_* 和 DINGTALK_*"
echo -e "  2. bash ${TARGET_DIR}/deploy_deepcoin.sh   # 改完 env 后再部署"
echo -e "  3. 配置 Nginx 反向代理 → 参考 ${NGINX_SNIP}"
echo -e "  4. TradingView 告警 URL → https://你的域名/deepcoin/${INSTANCE_NAME}/webhook"
echo -e "  5. crontab 巡检（可选）:"
echo -e "     */5 * * * * ${TARGET_DIR}/system_monitor.sh >> ${TARGET_DIR}/logs/monitor.log 2>&1"
echo ""
