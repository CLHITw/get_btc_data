# BTC Regime Switching Strategy

基于 K-means 聚类标签的 BTC 量化交易系统，包含数据采集、策略回测、实盘执行和可视化监控。

---

## 策略概述

### 核心逻辑

使用 6 个不同 K 值（k10~k15）的 K-means 模型对每日 BTC 市场状态进行分类，通过多数投票（≥4/6）决定当天的市场 Regime：

| Regime  | 判定条件                          | 动作         |
|---------|----------------------------------|-------------|
| BULL    | fwd30d > 2.5% 且 win7d > 53%    | 做多         |
| BEAR    | fwd30d < -2.0% 或 win7d < 45%   | 做空         |
| NEUTRAL | 其余                              | 轻仓/观望    |

信号二次确认：
- BULL：`macd_hist_z > -0.5`（动量未严重转负）
- NEUTRAL：`rsi_norm > -1.0`（RSI 未极端超卖）
- BEAR：`macd_hist_z < 0.5`（动量未转正）

溢出过滤：`boll_width_z > 3.5` 时多头仓位上限 50%（极端波动保护）

### 双腿组合策略

**多头腿（月度）**
- 每月第一个交易日根据当日 Regime 决策，持有至次月第一个交易日
- BULL 票数决定杠杆：4票→1x / 5票→1.5x / 6票→2x
- NEUTRAL：0.5x，无杠杆
- 月内止损：带杠杆净亏损 ≤ -15% 时平仓

**空头腿（动态）**
- 连续 3 天 BEAR + 信号确认 → 开空
- BEAR 6票 → 2x 杠杆，其余 → 1x
- 止损：带杠杆净亏损 ≤ -8%
- 平仓：连续 3 天 BULL 信号翻转

### 回测绩效（2024-09 ~ 2026-03，18个月）

| 指标 | 数值 |
|------|------|
| 累计收益 | 323.2% |
| 夏普比率 | 1.80 |
| 最大回撤 | -28.5% |
| 月度盈利因子 | 4.69 |

> 样本从 2024 年初 BTC ETF 上市后开始，规避了 ETF 前的结构性差异。

---

## 文件结构

```
.
├── get_data_module.py          # 从 Binance + FGI API 抓取每日数据
├── feature_calculator.py       # 计算技术指标特征（RSI/MACD/布林带等）
├── kmeans_predict_module.py    # 加载已训练 K-means 模型，预测 k10~k15
├── data_process.py             # 数据流水线主程序（每日 00:05 UTC 运行）
├── regime_strategy.py          # 策略核心：Regime 分析 + 回测引擎
├── binance_trader.py           # 实盘交易程序（Binance USDT-M 合约）
├── btc_chart_app.py            # Flask 可视化网页（价格图 + 决策日志）
├── test_order.py               # 测试网下单验证脚本
│
├── leverage_analysis.py        # 杠杆条件分析
├── leverage_combined_test.py   # 双腿杠杆组合回测
├── short_leverage_search.py    # 空头杠杆网格搜索（DD ≤ 30% 约束）
├── strategy_evaluation.py      # 策略综合评估（Sharpe/Sortino/Calmar）
├── test_monthly_improvements.py # 月度策略改进方案对比测试
│
├── btc.xlsx                    # BTC 历史数据（含特征列和 k 值）
└── kmeans_model_*.joblib       # 已训练的 K-means 模型文件
```

---

## 部署

### 依赖安装

```bash
pip install pandas numpy scikit-learn joblib requests schedule flask openpyxl
```

### 数据流水线（每日 00:05 UTC）

```bash
# systemd 服务或 cron
python data_process.py
```

### 可视化网页（持续运行，端口 5000）

```bash
python btc_chart_app.py
```

访问 `http://<服务器IP>:5000`

页面包含：
- BTC 价格折线图 + K-means 组合标注（可筛选）
- 左下：**交易决策日志**（每日投票结果、开平仓情况）
- 右下：**运维日志**（启动状态、数据更新、持仓同步）

### 实盘交易程序（每日 00:08 UTC）

**配置 `binance_trader.py`：**

```python
TESTNET       = False           # 正式实盘
TOTAL_CAPITAL = 1000.0          # 分配给策略的 USDT 本金
MAX_LEVERAGE  = 3               # Binance 合约杠杆设置
```

**API 密钥（推荐环境变量）：**

```bash
export BINANCE_API_KEY="your_key"
export BINANCE_API_SECRET="your_secret"
```

**Binance 账户要求：**
- 启用 USDT-M 合约
- 手动开启**双向持仓（Hedge Mode）**
- 将服务器 IP 加入 API Key 白名单
- 账户余额 ≥ 2 × TOTAL_CAPITAL

**启动（systemd）：**

```ini
[Unit]
Description=BTC Regime Trader
After=network.target

[Service]
ExecStart=/path/to/venv/bin/python3 /path/to/binance_trader.py daemon
Restart=always
User=root
WorkingDirectory=/path/to/project
EnvironmentFile=/root/.bashrc_env
```

```bash
systemctl enable btc-trader && systemctl start btc-trader
```

### 测试网验证

```bash
# 1. 修改 test_order.py 填入测试网 API Key
# 2. 运行
python test_order.py
# 验证：开仓 → 查询持仓 → 平仓 全流程
```

---

## 每日执行时序（UTC）

```
00:05  data_process.py    → 抓取昨日 BTC 数据 + FGI，写入 btc.xlsx
00:08  binance_trader.py  → 计算 Regime，执行多头/空头策略
00:10  btc_chart_app.py   → 自动刷新图表数据
```

---

## 注意事项

- K-means 模型文件（`kmeans_model_*.joblib`）需与数据在同一目录
- `trader_signal.log` 为交易决策日志，`trader.log` 为运维日志
- 状态文件 `trader_state.json` 记录持仓和投票窗口，勿手动删除
- 测试网 `positionSideDual` API 不可用，需在 Binance 测试网 UI 手动设置 Hedge Mode
