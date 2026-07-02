#!/usr/bin/env bash
# ==========================================
# VPS 部署前审计 — 列出已有端口 / 目录 / Nginx 路径，避免新实例冲突
# 用法: bash deploy/audit_vps_before_new_instance.sh
# ==========================================

set -uo pipefail

CYAN='\033[1;36m'
YELLOW='\033[0;33m'
GREEN='\033[0;32m'
NC='\033[0m'

echo -e "\n${CYAN}=== VPS 现有服务审计（新深币实例前请先跑本脚本）===${NC}\n"

echo -e "${YELLOW}[1] 当前 LISTEN 端口 (5000-5010 及 gunicorn/python)${NC}"
if command -v ss >/dev/null 2>&1; then
    ss -lnt 2>/dev/null | grep -E ':(500[0-9]|5010) ' || echo "  (5000-5010 无监听或 ss 无输出)"
    echo ""
    ss -lntp 2>/dev/null | grep -E 'gunicorn|python.*app' || true
else
    netstat -tuln 2>/dev/null | grep -E ':(500[0-9]|5010) ' || true
fi

echo -e "\n${YELLOW}[2] 已知项目目录（若存在）${NC}"
for d in \
    /root/binance-engine \
    /root/deepcoin-hft-server \
    /root/deepcoin-hft-server-main \
    /root/gemini-engine \
    /root/gemini \
    /home/*/binance-engine \
    /home/*/deepcoin-hft-server \
    /home/*/gemini*
do
    # shellcheck disable=SC2086
    for path in $d; do
        if [ -d "$path" ]; then
            port_hint=""
            if [ -f "$path/.env" ]; then
                port_hint=$(grep -E '^FLASK_PORT=' "$path/.env" 2>/dev/null | head -1 || true)
            fi
            echo -e "  ${GREEN}存在${NC}: $path  ${port_hint}"
        fi
    done
done

echo -e "\n${YELLOW}[3] deploy/reserved_ports.conf（脚本禁止使用的端口）${NC}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESERVED="$SCRIPT_DIR/reserved_ports.conf"
if [ -f "$RESERVED" ]; then
    grep -v '^\s*#' "$RESERVED" | grep -v '^\s*$' | while read -r p; do
        echo "  保留: $p"
    done
else
    echo "  (未找到 reserved_ports.conf)"
fi

echo -e "\n${YELLOW}[4] Nginx 中 webhook / deepcoin / binance / gemini 相关 location${NC}"
for f in /etc/nginx/sites-enabled/* /etc/nginx/conf.d/*.conf; do
    [ -f "$f" ] || continue
    hits=$(grep -nE 'location.*/(webhook|deepcoin|binance|gemini)|proxy_pass.*127\.0\.0\.1:500' "$f" 2>/dev/null || true)
    if [ -n "$hits" ]; then
        echo "  --- $f ---"
        echo "$hits" | sed 's/^/    /'
    fi
done

echo -e "\n${YELLOW}[5] 建议${NC}"
echo "  · 新深币用户实例请用 reserved_ports.conf 中未出现的端口（例如 5007、5008）"
echo "  · 新目录勿覆盖 /root/binance-engine、/root/deepcoin-hft-server、gemini 目录"
echo "  · Nginx 使用独立路径，如 /deepcoin/用户名/ ，勿改现有 location 块"
echo "  · 确认后: bash setup_new_instance.sh /root/deepcoin-新用户 <新端口>"
echo ""
