# 深币 Deepcoin · ETH 永续 Webhook 交易系统

**当前版本：`v13.5.6-recover-race`**

TradingView Webhook → 深币 **ETH-USDT-SWAP 永续** 自动化引擎。与币安 VPS **实盘逻辑完全对齐**（仅单位/交易所 API 不同），按 **张** 计量（**0.1 ETH/张**），**10 倍全仓杠杆**，钉钉 **紫金主题**。

---

## VPS 部署信息

| 项目 | 值 |
|------|-----|
| 目录 | `~/deepcoin-hft-server` |
| 端口 | **5004** |
| 杠杆 | **10x**（cross，开仓前自动 set-leverage） |
| 合约面值 | **0.1 ETH/张** |
| 健康检查 | `GET /health` |
| 主日志 | `logs/deepcoin_brain.log` |
| 状态文件 | `deepcoin_vps_state.json` |
| 部署脚本 | `bash deploy_deepcoin.sh` |

---

## 系统架构

```
TradingView Webhook
        ↓
    app.py（网关 · 即时 200 响应）
        │  enqueue_signal → 信号队列
        ↓
position_supervisor_deepcoin.py（智慧大脑 · 工作线程）
├── 同向智能筛选（ATR 优先 · v13.5.1）
├── TV/开仓日志持久化
├── 重启闪电接管 + TV 对账
├── TP123 比例审计 + 增量/核武补挂
├── 雷达移动保本（WS 推价 + 条件止损）
├── 空仓对账 + 蚂蚁仓扫尾
└── 哨兵循环（持仓/人工异动/定期扫描）
        ↓
deepcoin_client.py（REST 交易 + 公开 WS 行情）
dingtalk.py（紫金钉钉 · 含智能筛选播报）
```

**设计原则：** 网关不做实盘决策，只负责验签、入队、快速应答；所有实盘逻辑在 `position_supervisor_deepcoin.py` 信号工作线程中串行执行——与币安架构 **一一对应**。

---

## 实盘需求与执行逻辑（v13.5.1）

> 本节与币安 README **逻辑完全一致**，差异仅在于：持仓单位为 **张**、止损为 **条件单**、张数 API 返回字符串需 `_safe_qty` 解析。

### 1. 信号总线

| 阶段 | 行为 |
|------|------|
| TV 推送 | `POST /webhook`，JSON 含 `action/regime/atr/price/tv_tp1~3` |
| 网关 | 校验 `secret` → 写入信号队列 → **立即返回 200** |
| 大脑线程 | 逐条 `_process_signal`：更新 `regime/atr/tv_price/tv_tps` → 执行动作 |

### 2. 反向信号（一律先平后开）

持 **多** 收到 `SHORT`，或持 **空** 收到 `LONG`：

1. 撤销全部挂单  
2. 市价全平  
3. 再次撤单清场  
4. 按新 TV 信号市价开仓 → 挂 TP123 → 启动哨兵 + WS  
5. 记录 `open_regime`、`open_atr`

### 3. 同向智能筛选（核心 · ATR 第一优先级）

已有持仓且 TV 方向与实盘 **相同** 时：

```
┌─────────────────────────────────────────────────────────┐
│  ① ATR 是否变化？（open_atr vs TV atr，偏差 >3%）       │
│     是 → 先平后开 + 钉钉「刷新仓位 · ATR变化」           │
├─────────────────────────────────────────────────────────┤
│  ② 档位 regime 是否变化？                               │
│     是 → 先平后开（保证金/TP比例/雷达参数更新）          │
├─────────────────────────────────────────────────────────┤
│  ③ 理论开仓价差 ≥ 0.15%？（实盘 entry vs TV price）     │
│     是 → 先平后开 + 钉钉「刷新仓位 · 价差达标」          │
├─────────────────────────────────────────────────────────┤
│  ④ 均未触发 → 不重复开仓 · 核实持仓 · 刷新 TP123        │
│     → 钉钉「同向持仓 · 仅刷新止盈」                      │
└─────────────────────────────────────────────────────────┘
```

### 4. 空仓短时去重（5 分钟）

无持仓时，5 分钟内重复同向信号：**ATR 相似 → 档位相同 → 价差 <0.15%** 则忽略开仓。

### 5. 智能筛选参数

| 常量 | 值 | 含义 |
|------|-----|------|
| `SAME_DIR_MIN_SPREAD_PCT` | **0.15%** | 理论开仓价差阈值 |
| `SAME_DIR_DEDUP_SEC` | **300s** | 空仓去重窗口 |
| `ATR_SIMILAR_RATIO` | **3%** | ATR 相似判定 |

### 6. 持久化字段（`deepcoin_vps_state.json`）

与币安相同：`open_regime`、`open_atr`、`watched_entry`、`tv_tps`、`last_tv_side` 等。

### 7. 钉钉智能筛选播报

| 类型 | 说明 |
|------|------|
| 仅刷新止盈 | ATR 未变 + 价差不足，TP123 已更新 |
| 刷新仓位 | ATR/档位/价差触发先平后开 |
| 忽略重复 | 空仓 5 分钟内重复信号 |

> ⚠️ **v13.5+ 必须同时更新 `position_supervisor_deepcoin.py` 与 `dingtalk.py`**（`open_atr` / `tv_atr` 参数）。

---

## 其他核心能力

### 重启闪电接管
- 状态文件 + TV/开仓日志对账  
- TV 方向对齐；背离则核武清场  
- 人工加减仓 → 钉钉 + 按比例重挂 TP（张数余数吸收到 TP3）  
- 雷达恢复 + WS + 哨兵  

### 限价止盈 TP123
- Regime 比例与币安相同（例 R3：`18/32/50%`）  
- 重复单核武撤净重挂  

### 雷达移动保本
- WS：`market-latest`（ETHUSDT）  
- 条件止损单；推止损时 **保留 TP123**  

### 空仓对账与蚂蚁仓扫尾（张数适配）
- 最小 **1 张**；孤立 1 张无主仓账本 → 蚂蚁仓  
- TP 吃完残张 ≤ `max(1, int(初始×12%))` 且无 TP 单 → 扫尾  
- 宕机补发收网钉钉；空仓 30s 空闲巡检  

### 日志与审计

| 文件 | 说明 |
|------|------|
| `logs/deepcoin_tv_journal.jsonl` | TV 信号 |
| `logs/deepcoin_open_journal.jsonl` | 开仓/接管 |
| `logs/deepcoin_brain.log` | 大脑主日志 |
| `deepcoin_vps_state.json` | 运行时状态 |

---

## 四档 Regime 矩阵

与币安 **完全相同**：

| 档位 | 保证金占比 | TP 比例 (1/2/3) | 雷达激活 | 追踪倍数 |
|------|-----------|-----------------|----------|----------|
| R1 | 15% | 25% / 35% / 40% | 40% | 0.40×ATR |
| R2 | 25% | 20% / 35% / 45% | 50% | 0.60×ATR |
| R3 | 35% | 18% / 32% / 50% | 60% | 0.90×ATR |
| R4 | 50% | 5% / 20% / 75% | 70% | 1.30×ATR |

开仓量：`余额 × margin × 10x ÷ (价格 × 0.1)`，取 **整数张**，最低 **1 张**。

---

## 环境变量（`.env`）

```env
DEEPCOIN_API_KEY=
DEEPCOIN_API_SECRET=
DEEPCOIN_PASSPHRASE=
WEBHOOK_SECRET=528586
DINGTALK_WEBHOOK=
DINGTALK_SECRET=
FLASK_HOST=0.0.0.0
FLASK_PORT=5004
```

---

## TradingView Webhook

**URL：** `http://你的VPS:5004/webhook`

```json
{
  "action": "LONG",
  "secret": "528586",
  "regime": 3,
  "atr": 30.0,
  "price": 1560.0,
  "tv_tp1": 1580.0,
  "tv_tp2": 1600.0,
  "tv_tp3": 1620.0,
  "reason": "可选说明"
}
```

| action | 说明 |
|--------|------|
| `LONG` / `SHORT` | 经同向智能筛选或先平后开 |
| `CLOSE` | 换防清场 |
| `CLOSE_PROTECT` | 保护性全平 |
| `CLOSE_TP3` | TP3 吃满收网 |

---

## VPS 部署（标准流程）

```bash
cd ~/deepcoin-hft-server
git fetch origin && git reset --hard origin/main

grep v13.5.1-atr-priority position_supervisor_deepcoin.py app.py
grep -E 'report_smart_same_dir_decision|open_atr|tv_atr' dingtalk.py

bash deploy_deepcoin.sh

curl -s http://127.0.0.1:5004/health
tail -60 logs/deepcoin_brain.log
```

**健康检查响应：**

```json
{"status":"ok","service":"deepcoin_webhook","version":"v13.5.1-atr-priority"}
```

---

## 与币安系统对比

| 项目 | 深币 | 币安 |
|------|------|------|
| 单位 | 张 | ETH |
| 杠杆 | 10x | 10x |
| 端口 | 5004 | 5003 |
| 钉钉主题 | 紫金 | 黄金 |
| 止损 | 条件单 trigger | STOP_MARKET |
| WS | market-latest | markPrice@1s |
| 同向智能筛选 | ✅ v13.5.1 | ✅ v13.5.1 |
| 蚂蚁仓 | ≤1 张 | ≤0.004 ETH |

---

## 版本演进摘要

| 版本 | 要点 |
|------|------|
| v13.4.1-qtyfix | 张数字符串 `'1.000000'` 解析修复 |
| v13.4.6-flat-reconcile | 空仓对账、蚂蚁仓扫尾、宕机补发 |
| v13.5.0-smart-same-dir | 同向价差/档位筛选、信号队列 |
| **v13.5.1-atr-priority** | **ATR 优先决策链、open_atr、钉钉三态** |
| **v13.5.2-flat-gate** | **空仓闸门、先平后开验证、本金口径仓位预算、TP 挂前撤净、叠仓告警** |
| **v13.5.3-radar-guardian** | **雷达守护实时 TP 审计、核武撤单重挂、禁止增量叠单** |
| **v13.5.4-recover-tp** | **重启接管核武撤单、open_regime 止盈比例** |
| **v13.5.5-scorch-verify** | **重启撤单多轮验证至盘口清零** |
| **v13.5.6-recover-race** | **接管/雷达竞态修复、 settle 后再告警、雷达纠偏钉钉补报** |

仓位预算公式与币安相同（见币安 README Regime 矩阵）；张数取整最低 1 张。

---

## 注意事项

1. 部署务必 `git reset --hard origin/main`；**supervisor + dingtalk 成对更新**。  
2. `deploy_deepcoin.sh` 校验 v13.4.6+ / v13.5+ 及 dingtalk 智能筛选函数。  
3. 重启后钉钉「闪电接管报告」TP 应为 **3/3 比例审计全绿**；若短暂 CRITICAL，随后应收到 **「雷达守护 · 止盈已重新对齐」** 补报。  
4. 仅单方向持仓；反向先平后开，同向走智能筛选。  
5. 开仓按张取整，最低 1 张。  

---

## 多用户 / 多实例部署（同一 VPS）

> **⚠️ 若 VPS 已有币安(5003)、深币主实例(5004) 等，新用户必须先审计，严禁复用端口/目录。**

每个用户 = **独立目录 + 独立 `.env` + 独立端口 + 独立 Nginx location**。

### 部署前审计

```bash
bash deploy/audit_vps_before_new_instance.sh
```

### 已占用资源（勿冲突）

| 服务 | 典型目录 | 典型端口 |
|------|---------|---------|
| 币安 | `~/binance-engine` | **5003** |
| 深币（主账户） | `~/deepcoin-hft-server` | **5004** |
| 新用户 | `~/deepcoin-user2` | **5007+** |

### 一键初始化

```bash
bash deploy/audit_vps_before_new_instance.sh
bash setup_new_instance.sh /root/deepcoin-user2 5007
vim /root/deepcoin-user2/.env
cd /root/deepcoin-user2 && bash deploy_deepcoin.sh
```

### 隔离保证

- `deploy_deepcoin.sh` **只杀本实例端口** 的 gunicorn  
- 每实例独立 `venv/`、`logs/`、`deepcoin_vps_state.json`  
- 各实例 **v13.5.1 同向智能筛选逻辑相同**，仅 `.env` 与 API 账户不同  

---

*Quant AI · 深币紫金趋势大波段引擎*
