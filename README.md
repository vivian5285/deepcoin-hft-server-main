# 深币 Deepcoin · ETH/XAU 永续 Webhook 交易系统

**当前版本：`v16.17-eth-only-no-dingtalk`**

> **持仓模式说明**：Deepcoin 没有独立的 `set-position-mode` API，但 `posSide=long/short` 参数已在所有订单（开仓/平仓/限价止盈/条件单）中正确使用，保证双向持仓语义。`reduceOnly=True` 在止盈/止损中已正确设置，防止意外开仓。

与 `VPS完整系统规格_币安单账户版 v1.0` 完全对齐。

| 项目 | 值 |
|------|-----|
| GitHub | `vivian5285/deepcoin-hft-server-main` |
| VPS 目录 | `/home/deepcoin/deepcoin-hft-server` |
| 端口 | **5004** |
| 单位 | **张**（0.1 ETH/张） |
| 杠杆 | **5x** cross（双向持仓模式） |
| 健康检查 | `GET /health` → `"version":"v16.17-eth-only-no-dingtalk"` |
| 主日志 | `logs/deepcoin_brain.log` |
| 部署 | `bash deploy_deepcoin.sh` |

---

## 核心规格（对齐文档 v1.0）

### 硬止损（§3.3）
```
tv_stop_distance  = |TV.price − TV.stop_loss|
actual_distance   = tv_stop_distance × 1.15（统一系数，不分档）
多单：硬止损 = 成交价 − actual_distance
空单：硬止损 = 成交价 + actual_distance
```
无 `stop_loss` 字段 → **禁止开仓**，记录错误日志。

### 仓位比例（§3.5）
TP1 = **10%**，TP2 = **20%**，TP3 = **70%**。比例固定，不随档位变化。

### TP 限价单（§3.5 / §6.1）
仅挂 **TP1 + TP2** 限价。**TP3 永不挂限价**，完全交给雷达管理，无价格天花板。

### 雷达激活（§5.1，绝对价格锚定）
- 首次开仓：`激活价 = (TP1 + TP2) / 2`（TP1/TP2 为 webhook 原始信号价）
- 重入开仓：`激活价 = TP2`（价格必须真正到达 TP2 才接管）
- 不再使用 `ADX比例 × TP1距离` 旧公式。

### 雷达激活臂（§5.1）
激活瞬间保本位：`entry ± tick + fee_cover`，不跳 ATR。

### 雷达跟踪参数（§5.2 / §5.3 / §6.1）
步进/呼吸/呼吸空间统一使用 **TV webhook 的 `atr` 字段**，VPS 不独立拉取 ATR。

ETHUSDT.P：

| 档位 | 跟踪步长 | 跟进幅度 | TP1-TP2呼吸 | TP2-TP3呼吸 | TP3后 |
|------|---------|---------|------------|------------|-------|
| 弱趋势 T0 | 0.40×ATR | 0.25×ATR | 0.80×ATR | 1.00×ATR | 1.2~1.5×ATR |
| 中趋势 T1 | 0.50×ATR | 0.35×ATR | 1.20×ATR | 1.60×ATR | 2.0~2.5×ATR |
| 强趋势 T2 | 0.60×ATR | 0.40×ATR | 1.50×ATR | 2.00×ATR | 2.5~3.5×ATR |

XAUUSDT.P：

| 档位 | 跟踪步长 | 跟进幅度 | TP1-TP2呼吸 | TP2-TP3呼吸 | TP3后 |
|------|---------|---------|------------|------------|-------|
| 弱趋势 T0 | 0.35×ATR | 0.20×ATR | 0.70×ATR | 0.90×ATR | 1.0~1.3×ATR |
| 中趋势 T1 | 0.40×ATR | 0.30×ATR | 1.00×ATR | 1.40×ATR | 1.8~2.2×ATR |
| 强趋势 T2 | 0.50×ATR | 0.35×ATR | 1.30×ATR | 1.80×ATR | 2.2~3.0×ATR |

### 提前保本检查点（§5.0）
价格到达 `entry ± TP1距离 × 0.5` 时，单次将止损移至保本位（`entry ± tick + fee`）。仅触发一次，不启动雷达，不影响雷达激活判断。

### 雷达与硬止损（§5.4）
两层保护同时存在，互不干扰。硬止损永不撤销，雷达激活后两层并存。

### 重入机制（§8，最多 1 次）
**允许条件（必须全部满足）**：
1. 平仓来源 = 雷达扫出（硬止损扫出绝对不触发）
2. 平仓价格在允许区间内（ETH ±0.5×ATR，XAU ±0.3×ATR）
3. TV 信号仍有效
4. 该方向尚未重入过
5. **TP1 限价单从未成交过**
6. **当前档位为强趋势（tier=2）**

重入窗口：ETH 2根K线（约3小时），XAU 3根K线（约2.25小时）。

重入价格（§8.3，双保险）：
```
多单：min(最近已收盘5m K线最低价 + 1tick, TV信号价 × 0.997)
空单：max(最近已收盘5m K线最高价 − 1tick, TV信号价 × 1.003)
备选：3分钟K线
```
重入成功后雷达参数放宽一档（但不超过强趋势档），激活门槛固定为 TP2。

### 档位判断（§3.7）
`tier` 从 `tv_stop_distance / atr` 推导：
- `tv_stop_distance > 1.3 × ATR` → 弱趋势（tier=0）
- `tv_stop_distance ≤ 1.3 × ATR` → 强趋势（tier=2）
- 中间值 → 中趋势（tier=1）

### 状态持久化与重启恢复（§7）
所有影响仓位安全的状态持久化到 `deepcoin_vps_state_{symbol}.json`。重启时：
1. 读取本地状态
2. 查询交易所真实持仓与挂单，交叉核对
3. 以交易所为准修正本地状态
4. 如有持仓但缺少止损保护，**最高优先级重挂硬止损**
5. 恢复完成后才接受新信号

### 交易所异常处理（§12）
- **下单被拒**：不重试，立即告警，暂停新开仓
- **API 临时不可用**：指数退避（1s→2s→4s→8s→16s，最多5次），静默期间仅监控不操作
- **订单被交易所单方面撤销**：立即告警，重新核查持仓并补挂止损
- 优先保证已有仓位的止损保护不丢失

### 重复挂单防护（§9）
- 本地订单状态表（幂等标签 SHA-256），是判断"要不要挂单"的唯一依据
- 任何查询失败/超时不视为"无持仓/无挂单"，必须重试
- 每品种未成交挂单总数不超过 5 笔
- 平仓后立即核查盘口清空（任何数量>0的挂单均视为残留）

### 部分成交与头寸核算（§6.2）
任何成交回报（含部分成交）立即触发头寸重新核算，止损单数量同步更新。

### 平仓完整性（§9.4 / §9.5）
平仓前必须用 REST 查询交易所真实持仓作为最终依据，**平仓数量 ≤ 真实持仓**，绝不允许超出导致反向开仓。

### 钉钉通知（§11）

| 事件 | 触发时机 |
|------|---------|
| 开仓通知 | 品种、方向、价格、数量、档位、硬止损、TP123 |
| 雷达激活通知 | 激活价格、档位、初始止损、首次/重入类型 |
| 止损移动通知 | 新止损价、浮盈、档位 |
| TP成交通知 | 档位（TP1/TP2）、价格、剩余仓位 |
| 平仓通知 | 来源（TP1/TP2/雷达/硬止损/反转保护）、价格、盈亏、档位 |
| 重入尝试 | 原因、重入价、档位 |
| 重入成交 | 成交价、档位、窗口期剩余 |
| 重入放弃 | 原因（窗口期过期/价格不优/方向失效） |
| 重启恢复 | 状态核对结果、修正内容 |
| 异常告警 | 见 §12 交易所异常处理 |

---

## 规格 §13：旧代码清理确认

以下旧逻辑已清理或确认不存在：

| 项目 | 状态 |
|------|------|
| 旧 radar `activated` 状态变量 | ✅ 使用 `radar_activated` |
| 旧阶梯推进（0.5/0.3 ATR步长） | ✅ 已替换为档位参数表 |
| 旧"最多三次重入" | ✅ 最多 1 次 |
| TP3 限价单 | ✅ 已删除 |
| 旧仓位比例 30/30/40 | ✅ 已替换为 10/20/70 |
| 硬编码 1.5×ATR 常量 | ✅ 已替换为 1.15 |
| 按档位区分呼吸垫（1.1/1.2/1.3） | ✅ 统一 1.15 |
| 旧雷达激活公式（`距离×系数+成交价`） | ✅ 已替换为绝对价格锚定 |
| VPS 独立拉取 ATR 代码 | ✅ 已删除 |
| `market_engine.py` 中 ATR 独立计算 | ✅ 仅被其他模块引用，本服务不调用 |

---

## 深币独有实现差异

| 项目 | 深币 | 币安 |
|------|------|------|
| 止损 | tv_sl 条件单 + 雷达 `place_trigger_order` | 单槽 `closePosition` 合并 |
| 数量 | 张（字符串 API） | ETH 三位小数 |
| WS | `market-latest` | `markPrice@1s` |
| 钉钉主题 | 紫金 | 黄金 |
| 蚂蚁仓 | ≤ 1 张 | ≤ 0.004 ETH |

---

## VPS 更新

```bash
cd ~/deepcoin-hft-server

# 方式一：自动部署（脚本内含 git pull，推荐）
bash deploy_deepcoin.sh

# 方式二：手动拉取部署
git fetch origin
git stash                        # 暂存本地修改（如有）
git checkout main
git reset --hard origin/main
chmod +x deploy_deepcoin.sh system_monitor.sh
bash deploy_deepcoin.sh

# 验证
curl -s http://127.0.0.1:5004/health
tail -f logs/deepcoin_brain.log
```

---

## 本地测试

```bash
cd ~/deepcoin-hft-server
python3 test_radar_gate_tp12.py -v
```

测试覆盖：
- ADX 档位边界
- 统一呼吸垫 1.15
- 首次开仓雷达激活中点（TP1+TP2）/2
- 重入开仓雷达激活 TP2 绝对价
- 重入拦截：TP1已成交
- 重入拦截：弱/中趋势档位
- 重入拦截：硬止损扫出
- 重入拦截：窗口期过期
- 重入拦截：最多1次
- 双保险限价计算

---

## 架构

```
TradingView → app.py → position_supervisor_deepcoin.py → deepcoin_client.py
                      ↘ dingtalk.py（紫金）
```

核心模块：

| 文件 | 职责 |
|------|------|
| `app.py` | Flask 网关，webhook 接收与鉴权 |
| `position_supervisor_deepcoin.py` | 交易大脑（约8700行），全生命周期管理 |
| `deepcoin_client.py` | Deepcoin REST/WebSocket API |
| `radar_reentry_mixin.py` | 雷达激活 + 智能重入 |
| `smart_reentry_engine.py` | 重入状态机辅助 |
| `reentry_profiles.py` | 档位参数、重入配置 |
| `breath_profiles.py` | 呼吸跟踪基线参数 |
| `defense_profiles.py` | 硬止损缓冲、TP分仓比例 |
| `atr_scenario.py` | 硬止损计算 |
| `dingtalk.py` | 钉钉通知（紫金主题） |
| `webhook_parser.py` | TV 信号解析 |
| `order_idempotency.py` | 幂等订单标签 |
| `api_throttle.py` | REST API 限流 |
| `deploy_deepcoin.sh` | 工业级部署脚本 |

---

## 环境变量

```env
DEEPCOIN_API_KEY=
DEEPCOIN_API_SECRET=
DEEPCOIN_PASSPHRASE=
WEBHOOK_SECRET=                # webhook 鉴权密码
DINGTALK_WEBHOOK=             # 钉钉机器人 Webhook
DINGTALK_SECRET=              # 钉钉机器人加签密钥
FLASK_PORT=5004
```

---

## 版本演进

| 版本 | 要点 |
|------|------|
| v16.16 | 修复杠杆 25x→5x；新增双向持仓模式（hedge mode）启动设置；先平后开流程确认限价TP+硬止损+雷达全链路验证 |
| v16.15 | acked-tag deadlock fix: include acked in final states for GC + open-tag check; prevents nuclear-loop infinite blocking |
| v16.10.2 | 修复 adx_tier 从 tv_stop_distance/ATR 推导；重入 TP 分仓按 10/20/70 |
| v16.10 | 雷达激活绝对价锚定（首次中点/重入TP2）；删除 TP3 限价单 |
| v13.81 | TP3 永不挂限价；ATR 只信 TV；统一呼吸垫 1.15 |
| v13.x | 早期版本 |

---

*深币紫金引擎 · v16.17 · 对齐规格 v1.0*
