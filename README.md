# 深币 Deepcoin · ETH 永续 Webhook 交易系统

**当前版本：`v13.27.0-tp-radar-takeover-fix`**

与币安 VPS **同一套军师大脑逻辑**（`position_supervisor_deepcoin.py` 镜像 `position_supervisor_binance.py`）。本文档侧重深币部署差异；**完整统一逻辑见币安仓库 README**（两仓库 README 同步维护）。

| 项目 | 值 |
|------|-----|
| GitHub | `vivian5285/deepcoin-hft-server-main` |
| VPS 目录 | `~/deepcoin-hft-server` |
| 端口 | **5004** |
| 单位 | **张**（0.1 ETH/张） |
| 杠杆 | **20x** cross |
| 健康检查 | `GET /health` → `"version":"v13.26.0-add-tp-radar-realign"` |
| 主日志 | `logs/deepcoin_brain.log` |
| 部署 | `bash deploy_deepcoin.sh` |

---

## 与币安统一的核心逻辑（v13.26）

以下两工厂 **完全一致**，详见 [`eth-webhook-server` README](https://github.com/vivian5285/eth-webhook-server)：

- **防线总线** `_ensure_full_defense_stack()`：TP123 + `tv_sl` + 雷达待命  
- **硬止损** exclusively TV `tv_sl`（无 ±10% fallback）  
- **TP1 验证门控** `_tp1_filled_verified()`：伪 TP1 → `_disarm_premature_radar()`  
- **安全雷达交棒** `_perform_radar_handoff()`：先挂保本 STOP → 核实 → 再撤 tv_sl → 钉钉  
- **mark gap** `RADAR_STOP_MIN_GAP`：防刚激活就 closePosition 全平  
- **同向智能筛选** ATR → Regime → 价差 0.15%  
- **空闲巡检 12s** orphan 同向接管 / 反向强平  
- **动态加仓 v6.9.93**：OPEN 由 VPS sizing；PYRAMID/PROFIT_ADD = `base_qty × TV qty_ratio`  
- **加仓后重挂** `_realign_after_position_add()`：撤旧 TP → 按 TV TP123 价 + 新总头寸重挂 + 雷达/tv_sl 同步  
- **Regime activation** 92%/95%（对齐 TV v6.9.86）  
- **trailTight** TP1 后 0.20 ATR / TP2 后 0.30 ATR  

### 动态加仓档位（与币安同号）

| 档位 | TV 加仓比例 | 最多次数 | TP123 减仓比例 |
|------|-------------|----------|----------------|
| R1 | 0%（跳过） | 1 | **25/35/40** |
| R2 | 30% | 2 | **20/35/45** |
| R3 | 50% | 2 | **18/32/50** |
| R4 | 70% | 3 | **5/20/75** |

加仓单位：**张**（`base_qty` 为首仓张数）。加仓后按**新总张数**重算 TP123 数量；比例锁定 **开仓档位 `open_regime`**，与 TV `qty_percent` 一致。

---

## 深币独有实现差异

| 项目 | 深币 | 币安 |
|------|------|------|
| 止损 | tv_sl 条件单 + 雷达 `place_trigger_order` | 单槽 `closePosition` 合并 |
| 数量 | 张（字符串 API → `_safe_qty`） | ETH 三位小数 |
| WS | `market-latest` | `markPrice@1s` |
| 钉钉主题 | 紫金 | 黄金 |
| 蚂蚁仓 | ≤ 1 张 | ≤ 0.004 ETH |

---

## VPS 更新（git pull 报错时）

VPS 若出现：

```
error: Your local changes to the following files would be overwritten by merge:
        deploy_deepcoin.sh
```

说明 **GitHub 已推送成功**，VPS 本地脚本有未提交修改。请：

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

## 版本演进（与币安同号）

| 版本 | 要点 |
|------|------|
| v13.17~18 | TV 反向强平 + 空闲 orphan 接管 |
| v13.19~23 | 全链防线 + TP1 门控 + 伪 TP 解除 |
| **v13.24** | 安全雷达交棒 + README 统一 |
| **v13.25** | 动态加仓：首仓 VPS sizing，加仓 base×TV qty_ratio |
| **v13.26** | 加仓后 TP123 按新总头寸重挂 + 雷达/tv_sl 同步 |

---

*GEMINI Quant · 深币紫金引擎 · v13.26.0-add-tp-radar-realign*
