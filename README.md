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

---

### 双腿组合策略

#### 多头腿（月度）

| 条件 | 动作 |
|------|------|
| 每月第一个交易日 BULL 信号 + 确认 | 按票数建多头仓位 |
| BULL 4票 | 1x 杠杆（TOTAL_CAPITAL 名义价值） |
| BULL 5票 | 1.5x 杠杆 |
| BULL 6票 | 2x 杠杆 |
| NEUTRAL 信号 | 0.5x，无杠杆 |
| boll_width_z > 3.5 | 仓位上限强制压至 0.5x |

**月中止损（双重保护）：**
1. **交易所止损单**：开仓时同步挂 `STOP_MARKET`（`MARK_PRICE` 触发），带杠杆净亏损达 -15% 时自动平仓
2. **程序软止损**：每日检查，亏损 ≤ -15% 时平仓（兜底）
3. **方案B — 月中提前平仓**：月中连续 **4 天** BEAR 信号 → 不等月末，立即平多头

持有至次月第一个交易日（未触发止损的情况下）

#### 空头腿（动态）

| 条件 | 动作 |
|------|------|
| 连续 3 天 BEAR 信号 + 信号确认 | 触发开空资格审查 |
| `macd_hist_z < -1.0` 且 `volume_log_z > 0` | ✅ 通过过滤，执行开空 |
| 不满足上述特征过滤 | ❌ 放弃本次开空信号 |
| BEAR 6票 | 2x 杠杆 |
| BEAR 4~5票 | 1x 杠杆 |
| 连续 3 天 BULL 信号 | 平空 |
| 带杠杆净亏损 ≤ -8% | 止损平空（交易所止损单 + 程序兜底） |

**特征过滤含义：**
- `macd_hist_z < -1.0`：MACD 柱状图 z-score 低于 -1，说明动量明显弱于历史均值（非假突破）
- `volume_log_z > 0`：成交量对数 z-score 大于 0，说明放量下跌（真实抛压，非缩量）

---

### 回测绩效（2024-09 ~ 2026-03，18个月）

| 指标 | 基础策略 | 含方案B + 空头过滤 |
|------|----------|-------------------|
| 多头累计收益 | 276% | **399%** |
| 多头夏普比率 | 1.70 | **1.92** |
| 空头有效过滤 | — | 4/4 盈利，0 亏损 |
| 最大回撤（组合） | -28.5% | -28.5% |

> 样本从 2024 年初 BTC ETF 上市后开始，规避了 ETF 前的结构性差异。

---

## 文件结构

```
.
├── get_data_module.py           # 从 Binance + FGI API 抓取每日数据
├── feature_calculator.py        # 计算技术指标特征（RSI/MACD/布林带等）
├── kmeans_predict_module.py     # 加载已训练 K-means 模型，预测 k10~k15
├── data_process.py              # 数据流水线主程序（每日 00:05 UTC 运行）
├── regime_strategy.py           # 策略核心：Regime 分析 + 回测引擎
├── binance_trader.py            # 实盘交易程序（Binance USDT-M 合约）
├── btc_chart_app.py             # Flask 可视化网页（价格图 + 决策日志）
├── test_order.py                # 测试网下单验证脚本
│
├── leverage_combined_test.py    # 双腿杠杆组合回测
├── short_leverage_search.py     # 空头杠杆网格搜索（DD ≤ 30% 约束）
├── strategy_evaluation.py       # 策略综合评估（Sharpe/Sortino/Calmar）
├── test_monthly_improvements.py # 月度策略改进方案对比测试
├── final_combined_test.py       # 方案B + 空头过滤综合测试
├── short_filter_test.py         # 空头特征过滤组合测试
├── long_stop_improve_test.py    # 多头月中止损方案对比（方案A/B）
│
├── btc.xlsx                     # BTC 历史数据（含特征列和 k 值）
└── kmeans_model_*.joblib        # 已训练的 K-means 模型文件
```

---

## 部署

### 依赖安装

```bash
pip install pandas numpy scikit-learn joblib requests schedule flask openpyxl plotly
```

### 数据流水线（每日 00:05 UTC）

```bash
python data_process.py
```

### 可视化网页（持续运行，端口 5000）

```bash
nohup python btc_chart_app.py > /root/Desktop/btc/chart_app.log 2>&1 &
```

访问 `http://<服务器IP>:5000`

页面包含：
- BTC 价格折线图 + K-means 组合标注（可筛选）
- **hover 背景色随投票变化**：BULL 绿色 / BEAR 红色 / NEUTRAL 灰色
- 左下：**交易决策日志**（每日投票结果、开平仓情况、月中BEAR窗口）
- 右下：**运维日志**（启动状态、数据更新、持仓同步）
- **累计收益标签页**：已实现收益曲线，盈利绿点 / 亏损红点

### 实盘交易程序（每日 00:08 UTC）

**配置 `binance_trader.py`：**

```python
TESTNET       = False           # 正式实盘
TOTAL_CAPITAL = 1000.0          # 分配给策略的 USDT 本金
MAX_LEVERAGE  = 3               # Binance 合约杠杆设置
```

**关键策略参数：**

```python
LONG_LEV_MAP         = {4: 1.0, 5: 1.5, 6: 2.0}   # 多头按票数杠杆
SHORT_LEV_MAP        = {4: 1.0, 5: 1.0, 6: 2.0}   # 空头按票数杠杆
LONG_STOP_LOSS       = -0.15    # 多头带杠杆净亏损止损线
SHORT_STOP_LOSS      = -0.08    # 空头带杠杆净亏损止损线
MIDMONTH_BEAR_CONSEC = 4        # 方案B：月中连续N天BEAR平多头
SHORT_MACD_HIST_MAX  = -1.0     # 空头入场：macd_hist_z 必须 < 此值
SHORT_VOL_MIN        = 0.0      # 空头入场：volume_log_z 必须 > 此值
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

**启动：**

```bash
nohup python binance_trader.py daemon > trader.log 2>&1 &
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
- 程序在开仓时**同步挂止损单**（`STOP_MARKET + closePosition=true`），网络中断时交易所侧仍可自动止损
- 止损单撤销使用 `DELETE /fapi/v1/order`（非 POST），如需手动排查请注意此区别
- 测试网 `positionSideDual` API 不可用，需在 Binance 测试网 UI 手动设置 Hedge Mode
