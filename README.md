# 深币 Deepcoin · ETH 永续 Webhook 交易系统

**当前版本：`v13.4.6-flat-reconcile`**

TradingView Webhook → 深币 ETH-USDT-SWAP 永续合约自动化引擎。与币安 VPS 逻辑对齐，单位按 **张** 计算，实盘 **20 倍杠杆**，钉钉为 **紫金主题**。

---

## VPS 部署信息

| 项目 | 值 |
|------|-----|
| 目录 | `~/deepcoin-hft-server` |
| 端口 | **5004** |
| 杠杆 | **20x**（全仓 cross，开仓前自动 set-leverage） |
| 合约面值 | 0.1 ETH/张 |
| 健康检查 | `GET /health` |
| 主日志 | `logs/deepcoin_brain.log` |
| 部署脚本 | `bash deploy_deepcoin.sh` |

---

## 系统架构

```
TradingView Webhook
        ↓
    app.py（网关，异步线程）
        ↓
position_supervisor_deepcoin.py（智慧大脑）
├── TV/开仓日志持久化
├── 重启闪电接管 + TV 对账
├── TP123 比例审计 + 增量/核武补挂
├── 雷达移动保本（WS 推价 + 条件止损）
├── 空仓对账 + 蚂蚁仓扫尾（flat-reconcile）
└── 哨兵循环（持仓/人工异动/定期扫描）
        ↓
deepcoin_client.py（REST 交易 + 公开 WS 行情）
dingtalk.py（紫金钉钉播报）
```

---

## 核心能力（v13.4.x）

### 1. 重启闪电接管
- 读取 `deepcoin_vps_state.json` + **TV 日志** + **开仓日志**
- **TV 方向强制对齐**：`last_tv_side` 始终同步 TV 日志最新 LONG/SHORT
- **方向背离 / TV 已 CLOSE** → 核武清场，不盲目接管
- **人工加减仓**：账本张数 ≠ 实盘 → 写开仓日志 + 钉钉 + 按比例重挂 TP
- TP123 **价位 + 张数** 严格审计（regime 比例，余数吸收到 TP3）
- **雷达恢复**：按现价刷新 `best_price` / 激活状态 → 补挂条件止损
- 已齐全 → **跳过补挂**；不齐 → 增量补挂 → 仍失败 → **核武清场重挂**
- 启动 **WS 推价** + **哨兵循环**（与运行中一致）

### 2. 限价止盈 TP123
- 比例随档位（regime 1~4）变化，例如 3 档：`18% / 32% / 50%`
- 不多挂、少挂、漏挂：审计不通过自动修复
- 重复单（如 TP1 叠 6 张）→ 核武级撤净重挂

### 3. 雷达移动保本
- 价格达 TP1 距离的 60%（3 档默认）→ 激活雷达
- 跟踪 `best_price`，ATR × 档位倍数推升/下压条件止损
- **WebSocket** 订阅 `market-latest`（ETHUSDT），REST 查价仅 ≥30s 兜底
- 推止损时只撤条件单，**TP123 保留**

### 4. 人工异动
- 手动加/减仓、部分止盈吃单 → 智能重对齐 TP 比例
- 人工全平 → 撤单、复位账本、钉钉通知
- 方向与 TV 背离 → 核武全平

### 5. 空仓对账与蚂蚁仓扫尾（v13.4.6）
- **张数适配**：深币最小 1 张；无主仓账本时的孤立 1 张视为蚂蚁仓
- **止盈残张**：TP 吃完且无限价单，残张 ≤ `max(1, int(初始张数×12%))` → 自动扫尾
- **重启首检**：避免把残张误接管为正常持仓
- **宕机补发**：重启期间已全平但账本仍有仓 → 补发收网钉钉
- **空闲巡检**：空仓待命每 30s 扫描孤立残张

### 6. 日志与审计
| 文件 | 说明 |
|------|------|
| `logs/deepcoin_tv_journal.jsonl` | 每条 TV 信号 |
| `logs/deepcoin_open_journal.jsonl` | 开仓 / 接管记录 |
| `logs/deepcoin_brain.log` | 大脑主日志 |
| `deepcoin_vps_state.json` | 运行时状态（自动生成） |

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
| `LONG` / `SHORT` | 先平后开 → 挂 TP123 → 启动哨兵 |
| `CLOSE` | 换防清场 |
| `CLOSE_PROTECT` | 保护性全平 |
| `CLOSE_TP3` | TP3 吃满收网 |

---

## 本地开发

```bash
pip install -r requirements.txt
python app.py
# 或
gunicorn --bind 0.0.0.0:5004 --workers 1 --threads 10 app:app
```

---

## VPS 部署（标准流程）

```bash
cd ~/deepcoin-hft-server
git fetch origin && git reset --hard origin/main

# 版本门控
grep v13.4.6-flat-reconcile deepcoin_client.py position_supervisor_deepcoin.py

bash deploy_deepcoin.sh

# 验收
tail -60 logs/deepcoin_brain.log
curl -s http://127.0.0.1:5004/health
```

**部署成功日志示例：**

```
🧠 深币 VPS [v13.4.6-flat-reconcile/...] 军师托管版已加载
📡 深币公开 WS 启动: ETHUSDT market-latest
🔄 [系统重启点火] 检测到实盘持仓 ...
✅TP1 1张@1537.85 | ✅TP2 ... | ✅TP3 ...
```

---

## 与币安系统区别

| 项目 | 深币 | 币安 |
|------|------|------|
| 单位 | 张 | ETH |
| 杠杆 | **20x** | **20x** |
| 端口 | 5004 | 5003 |
| 钉钉主题 | 紫金 | 黄金 |
| 止损类型 | 条件单 trigger | STOP_MARKET |
| WS 频道 | market-latest | markPrice@1s |
| 空仓对账 | v13.4.6 蚂蚁仓扫尾 | v13.4.6 蚂蚁仓扫尾 |

---

## 注意事项

1. 部署务必 `git reset --hard origin/main`，避免 VPS 残留旧代码。
2. 重启后看钉钉「闪电接管报告」，TP 应为 **3/3 比例审计全绿**。
3. 实盘前确认 `.env` 与 Deepcoin API 权限（合约读写）正确。
4. 仅同时持有一个方向仓位；新 TV 信号触发 **先平后开**。
5. 开仓量按 `余额 × regime.margin × 20x ÷ (价格 × 0.1)` 取整张数，最低 1 张。

---

## 多用户 / 多实例部署（同一 VPS）

> **⚠️ 若 VPS 已有币安(5003)、深币主实例(5004)、Gemini 等，新用户必须先审计，严禁复用端口/目录/Nginx 路径。**

代码共用 **同一 GitHub 仓库**，每个用户 = **独立目录 + 独立 `.env` + 独立端口 + 独立反向代理**。

### 部署前必做：审计现有服务

```bash
# 在 VPS 任意目录 clone 后，或在新实例目录内执行：
bash deploy/audit_vps_before_new_instance.sh
```

会列出：5000–5010 监听端口、已知项目目录、`reserved_ports.conf`、Nginx 已有 location。

### 已占用资源（勿冲突）

| 服务 | 典型目录 | 典型端口 | 说明 |
|------|---------|---------|------|
| 币安 | `~/binance-engine` | **5003** | 勿动 |
| 深币（主账户） | `~/deepcoin-hft-server` | **5004** | 勿动 |
| Gemini | （以 VPS 实际为准） | **查 audit** | 写入 `deploy/reserved_ports.conf` |
| **新用户 B** | `~/deepcoin-user2` | **5007+** | 建议从 5007 起，避开 5005/5006 若已被占 |

编辑 `deploy/reserved_ports.conf` 登记 Gemini 实际端口后再跑 `setup_new_instance.sh`。

| 实例 | 目录示例 | 端口 |
|------|---------|------|
| 主账户 | `/root/deepcoin-hft-server` | 5004（已占用） |
| 用户 B | `/root/deepcoin-user2` | **5007**（示例，以 audit 为准） |

### 一键初始化（root 下执行）

```bash
# 1. 先审计
bash deploy/audit_vps_before_new_instance.sh

# 2. 确认 Gemini 端口后写入 reserved_ports.conf，再新建（勿用 5003/5004）
bash setup_new_instance.sh /root/deepcoin-user2 5007

# 3. 填入该用户的 API / 钉钉
vim /root/deepcoin-user2/.env

# 4. 部署（仅影响本目录、本端口）
cd /root/deepcoin-user2 && bash deploy_deepcoin.sh
```

### Nginx 反向代理

**只追加新 `location` 块，不要修改币安/深币/Gemini 已有配置。**

初始化后在 `deploy/nginx-<实例名>.conf.snippet` 有现成片段：

```
https://你的域名/deepcoin/deepcoin-user2/webhook
  → 127.0.0.1:5007/webhook
```

```bash
cat /root/deepcoin-user2/deploy/nginx-deepcoin-user2.conf.snippet >> /etc/nginx/sites-available/default
nginx -t && systemctl reload nginx
```

### 隔离保证

- `deploy_deepcoin.sh` **只杀本实例 `--bind` 端口** 的 gunicorn，不会误杀 5003/5004/Gemini
- 每个实例独立 `venv/`、`logs/`、`deepcoin_vps_state.json`
- `setup_new_instance.sh` 会拒绝 `reserved_ports.conf` 中的端口及受保护目录

---

*Quant AI · 深币紫金趋势大波段引擎*
