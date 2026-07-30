# 深币 Deepcoin · ETH 永续 Webhook 交易系统

**当前版本：`v16.10.2-deepcoin-aligned`**

与币安 VPS **同一套规格逻辑**（`VPS完整系统规格_币安单账户版 v1.0`）。

| 项目 | 值 |
|------|-----|
| GitHub | `vivian5285/deepcoin-hft-server-main` |
| VPS 目录 | `/home/deepcoin/deepcoin-hft-server` |
| 端口 | **5004** |
| 单位 | **张**（0.1 ETH/张） |
| 杠杆 | **20x** cross |
| 健康检查 | `GET /health` → `"version":"v16.10.2-deepcoin-aligned"` |
| 主日志 | `logs/deepcoin_brain.log` |
| 部署 | `bash deploy_deepcoin.sh` |

---

## 核心逻辑（对齐规格 v1.0）

- **硬止损**：`(|TV.price−TV.stop_loss|)×1.15` 锚定成交价；永久共存，不因雷达激活撤销
- **雷达启动（规格 §5.1 绝对价锚定）**：首次 `(TP1+TP2)/2` · 重入 `TP2`（TP1/TP2 为 webhook 原始信号价格）
- **激活臂**：雷达激活时，保本止损 = entry ± tick + fee_cover
- **TP**：固定 10/20/70；仅挂 TP1(10%)+TP2(20%) 限价；TP3(70%) 永不挂限价，完全交雷达管理
- **呼吸垫系数**：统一 1.15（不分弱/中/强档位）
- **重入**：最多 1 次；仅强趋势档（tier=2）+ TP1 未成交等闸门
- **ATR**：统一使用 TV webhook 的 atr 字段，VPS 不独立拉取
- **规格 §5.0**：提前保本检查点（entry + TP1距离×0.5 时单次移动止损到保本位）
- **规格 §3.7**：adx_tier 从 `tv_stop_distance / atr` 推导（弱0 / 中1 / 强2）

> **v16.10.2**：修复 adx_tier 从 tv_stop_distance/ATR 推导；修复重入成交后 TP1/TP2 分仓按 10%/20%。
> **v16.10**：雷达激活价格使用 TP1/TP2 绝对价格锚定；删除 TP3 限价单。

### 仓位比例（规格 §3.5 固定）

TP1 = **10%**，TP2 = **20%**，TP3 = **70%**。比例固定，不随档位变化。

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
git fetch origin
git reset --hard origin/main
bash deploy_deepcoin.sh
curl -s http://127.0.0.1:5004/health
tail -f logs/deepcoin_brain.log
```

---

## 架构

```
TradingView → app.py → position_supervisor_deepcoin.py → deepcoin_client.py
                      ↘ dingtalk.py（紫金）
```

生产模块：`app.py`、`position_supervisor_deepcoin.py`、`deepcoin_client.py`、`dingtalk.py`、`deploy_deepcoin.sh`。

---

## 环境变量

```env
DEEPCOIN_API_KEY=
DEEPCOIN_API_SECRET=
DEEPCOIN_PASSPHRASE=
WEBHOOK_SECRET=
DINGTALK_WEBHOOK=
DINGTALK_SECRET=
FLASK_PORT=5004
```

---

## 版本演进

| 版本 | 要点 |
|------|------|
| v16.10.2 | 修复 adx_tier 推导；重入 TP 分仓按 10/20/70 |
| v16.10 | 雷达激活绝对价锚定；删除 TP3 限价单；API 节流优化 |
| v13.81 | TP3 永不挂限价；ATR 只信 TV |

---

*深币紫金引擎 · v16.10.2-deepcoin-aligned*
