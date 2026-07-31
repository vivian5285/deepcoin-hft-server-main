#!/usr/bin/env bash
# ==========================================
# 深币 Deepcoin — 一键启动入口
# 自动激活 venv 并执行 deploy_deepcoin.sh
# ==========================================

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 激活虚拟环境
if [ -f "$SCRIPT_DIR/venv/bin/activate" ]; then
    source "$SCRIPT_DIR/venv/bin/activate"
    echo "[run_deepcoin] venv 已激活"
elif [ -f "$SCRIPT_DIR/.venv/bin/activate" ]; then
    source "$SCRIPT_DIR/.venv/bin/activate"
    echo "[run_deepcoin] .venv 已激活"
else
    echo "[run_deepcoin] 警告: 未找到 venv，继续使用系统环境"
fi

# 执行部署脚本
exec bash "$SCRIPT_DIR/deploy_deepcoin.sh"
