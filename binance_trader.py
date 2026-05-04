"""
combined_live_trader.py — 多空双向实盘（单账户全仓）
====================================================
策略：Baseline CP — 多头（纯投票+GMMA，L1日4x / 非L1日1x）+ 空头（A/dual_hit/L2s/CB形态）

多头腿（分信号强度）：
  - GMMA G2 方向过滤（mean(短组EWM) > mean(长组EWM) 才允许做多，无展宽约束）
  - S2 入场：近3天 ≥ 2天 bull_score ≥ 4（无需 L1/L2 形态确认，纯投票）
  - L1 信号日（k10=7,k11=1,k13=2,k14=11）→ 4x 杠杆，标准追踪（激活2%/回撤5%），最多 20 天
  - 非L1 信号日 → 1x 杠杆，紧缩追踪（激活1%/回撤2%）见好就收，最多 20 天
  - 止损: BTC 跌幅 ≥ 15%  → 开仓同时在交易所挂 STOP_MARKET 止损单
  - Chandelier Exit: 22日最高价 - 2×ATR22 = CE线；close 跌破 CE线后平多（ce_exit）
  - 附加平仓：连续3天 bull_score ≤ 2 / 达到最大持仓天数
  - override_day（k14=12 OR k15=7）：bull_score视为0，禁止新开多头；
    存量多头若亏损→立即平仓(override_stop)，若盈利→切换追踪止盈模式（禁用熊市投票）

空头腿（BN形态信号）：
  - GMMA G2 过滤（mean(短组EWM) < mean(长组EWM) AND 展宽 ≥ 各信号阈值）
    信号A=0.015 / dual_hit=0.000 / L2s=0.005 / CB=0.025
  - 信号A: k10=5,k11=4,k12=8,k13=4,k14=0  k15=0→3x  否则→2x  最多 30 天
  - 信号dual_hit: k14=12 AND k15=7 → 1x  最多 12 天（原信号C OR条件改为AND）
  - 信号L2s: k10=3,k11=6,k12=0,k13=10 → 3x  最多 25 天（原L2多头K型态转空头）
  - Combo Bear (CB): 从 combo.json 读取滚动窗口 bear patterns → 2.5x  最多 20 天
  - 止损: BTC 涨幅 ≥ 8%   → 开仓同时在交易所挂 STOP_MARKET 止损单
  - 追踪止盈: 激活 2% / 利润回撤 5%  → 每日检测 + 交易所利润锁定止损单（双保险）
  - 附加平仓：连续3天 bull_score ≥ 3（S_VOTE_T）/ 达到最大持仓天数
  - override_day 时 bull_score 视为0，空头 bull_vote_count 不累加

止损单机制：
  - 开仓时挂初始止损单（entry×0.85/1.08），永不撤销，仓位平仓后交易所自动取消
  - 利润锁定止损单：peak > 0 时每日更新（只升不降），锁住利润的90%，防止大幅回撤
  - 非止损平仓（信号/投票/到期）→ 先撤初始止损单 + 利润锁定单，再市价平仓
  - 若 Binance 显示无仓位但状态为持仓中 → 判定止损已触发，清理状态

开仓资金：每次使用账户全部可用余额（全仓复利）
多空可同时持仓（GMMA G2多头方向过滤 + GMMA G2空头展宽过滤，两者条件独立）

依赖：pip install ccxt schedule requests pandas openpyxl

用法：
  python combined_live_trader.py           # 立即执行一次
  python combined_live_trader.py daemon    # 每天 00:05 UTC 自动执行

部署（Linux）：
  nohup python combined_live_trader.py daemon > /root/btc/combined.log 2>&1 &
"""

import os
import sys
import json
import time
import hmac
import hashlib
import logging
import math
import schedule
import requests
import numpy as np
import pandas as pd
import io

try:
    from update_map import maybe_update_map as _maybe_update_map
except ImportError:
    _maybe_update_map = None   # 文件不存在时退化为不更新

try:
    from filelock import FileLock
except ImportError:
    os.system("pip install filelock -q")
    from filelock import FileLock

from urllib.parse import urlencode
from datetime import datetime, date
from zoneinfo import ZoneInfo

try:
    import ccxt
except ImportError:
    os.system("pip install ccxt -q")
    import ccxt

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ══════════════════════════════════════════════════════════════════════
# ① 配置区（上线前务必修改）
# ══════════════════════════════════════════════════════════════════════

# ┌─────────────────────────────────────────────────────────────────┐
# │  TESTNET 开关：True = 测试网  False = 正式实盘                   │
# └─────────────────────────────────────────────────────────────────┘
TESTNET = False   # ← 上实盘前改为 False

if TESTNET:
    API_KEY    = "YOUR_TESTNET_API_KEY"
    API_SECRET = "YOUR_TESTNET_API_SECRET"
else:
    API_KEY    = os.environ.get("BINANCE_API_KEY",    "YOUR_API_KEY_HERE")
    API_SECRET = os.environ.get("BINANCE_API_SECRET", "YOUR_API_SECRET_HERE")

# --- 交易标的 ---
SYMBOL      = "BTCUSDT"
BASE_ASSET  = "BTC"
QUOTE_ASSET = "USDT"

# --- 杠杆设置 ---
# 策略最大杠杆：多头L1日 4x，空头信号A k15=0 → 3x；设 5 留安全边际
MAX_LEVERAGE = 5

# --- 路径配置 ---
# 所有文件（btc.xlsx、data_process.py 等）与本脚本在同一目录
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR   = SCRIPT_DIR
BTC_FILE   = os.path.join(SCRIPT_DIR, "btc.xlsx")

STATE_FILE      = os.path.join(SCRIPT_DIR,
                  "combined_state_testnet.json" if TESTNET else "combined_state.json")
STATE_LOCK_FILE = os.path.join(SCRIPT_DIR, "combined_state.lock")
_state_lock     = FileLock(STATE_LOCK_FILE, timeout=30)
LOG_FILE        = os.path.join(SCRIPT_DIR,
                  "combined_testnet.log"        if TESTNET else "combined_trader.log")
SIGNAL_LOG_FILE = os.path.join(SCRIPT_DIR,
                  "combined_signal_testnet.log" if TESTNET else "combined_signal.log")
TRADE_LOG_FILE  = os.path.join(SCRIPT_DIR,
                  "combined_trades_testnet.json" if TESTNET else "combined_trades.json")

# --- Binance USDT-M 合约 REST API ---
FUTURES_URL = ("https://testnet.binancefuture.com"
               if TESTNET else "https://fapi.binance.com")

# --- 执行时间（UTC）---
DAILY_RUN_TIME = "00:05"

# ══════════════════════════════════════════════════════════════════════
# ② 策略参数（与 combined_backtest.py 完全一致，勿改）
# ══════════════════════════════════════════════════════════════════════

# ── 多头 ──────────────────────────────────────────────────────────────
# 硬止损（交易所止损单）和软止损（收盘价）按信号类型分开
L_STOP      = 0.15    # 全局默认（日志/兼容用途），实际开仓使用下方分组值
L_SOFT_STOP = 0.07    # 全局默认软止损（日志/兼容用途）

L_STOP_L1      = 0.12   # L1 硬止损
L_SOFT_STOP_L1 = 0.07   # L1 软止损

L_STOP_NL4     = 0.15   # NL bs=4 硬止损
L_SOFT_STOP_NL4 = 0.07  # NL bs=4 软止损

L_STOP_NL56    = 0.18   # NL bs=5/6 硬止损
L_SOFT_STOP_NL56 = 0.05 # NL bs=5/6 软止损

L_VOTE_W    = 3       # 熊市平仓：连续 N 天触发
L_VOTE_T    = 2       # 熊市判断：bull_score ≤ T 视为熊市票
L_MAX       = 20      # 多头最大持仓天数（L1 / 非L1 共用）

# L1 信号日（k10=7,k11=1,k13=2,k14=11）→ 强信号，3.5x 标准追踪
L_LEV_L1     = 3.5   # L1日杠杆（回测最优：3.5× 为转折点，4× 开始退化）
L_TRAIL_A_L1 = 0.020 # L1日追踪激活（涨≥2%）
L_TRAIL_PCT_L1 = 0.10  # L1日追踪回撤容忍（10%，宽松锁利）

# 非L1信号日（按开仓当天 bull_score 分 bs=4 / bs=5/6）→ 5x 统一杠杆
L_LEV_NL     = 5.0   # 非L1日杠杆（bs=4/5/6 统一 5×）
L_TRAIL_A_NL = 0.010 # 非L1日追踪激活（涨≥1%，与回测 NL_TI=1 一致）
L_TRAIL_PCT_NL = 0.10  # 非L1日追踪回撤容忍（10%）

# 向后兼容：旧代码引用 L_LEV / L_TRAIL_A / L_TRAIL_PCT 处的默认值（新代码应使用分组参数）
L_LEV       = L_LEV_L1   # 仅用于日志显示等无关入场逻辑的地方
L_TRAIL_A   = L_TRAIL_A_L1
L_TRAIL_D   = 0.01        # 保留字段

# ── 空头 ──────────────────────────────────────────────────────────────
S_STOP    = 0.08    # 全局默认（兼容/日志用途），实际使用下方分信号值
S_STOP_BY_SIG = {   # 各空头信号独立硬止损（回测最优）
    'A':        0.12,
    'dual_hit': 0.06,
    'L2s':      0.10,
    'CB':       0.09,
}
S_TRAIL_A = 0.02    # 追踪止盈激活线
S_TRAIL_D = 0.02    # 追踪止盈回撤容忍（保留字段）
S_VOTE_W  = 3       # 牛市平仓：连续 N 天触发
S_VOTE_T  = 3       # 牛市判断：bull_score ≥ T 视为牛市票
S_MAX_A   = 30;   S_MAX_C = 12     # 最大持仓天数（A/dual_hit信号）
L2S_LEV   = 3.0     # 空头 L2s 杠杆
L2S_MAX   = 25      # 空头 L2s 最大持仓天数
S_COMBO_LEV = 2.5   # Combo Bear 杠杆（网格搜索最优）
S_COMBO_MAX = 20    # Combo Bear 最大持仓天数

# ── 追踪止盈（利润回撤比例）────────────────────────────────────────
L_TRAIL_PCT = L_TRAIL_PCT_L1  # 向后兼容（新代码用 L_TRAIL_PCT_L1 / L_TRAIL_PCT_NL）
S_TRAIL_PCT = 0.05   # 空头：利润回撤5%触发

# ── 追踪止盈 Exchange Stop 开关 ──────────────────────────────────────
# False（推荐）：追踪止盈仅在每日 00:05 UTC 检查，不向交易所挂追踪止损单
#   优点：与回测逻辑完全一致（回测只检查日线开盘价），不会被日内噪音触发
#   缺点：日内突发性崩盘（跌穿追踪线但不触发硬止损）须等次日才平仓
#   硬止损单（stop_order_id）仍 24/7 在交易所保护极端下跌，不受此开关影响
# True：向交易所挂追踪利润锁定止损单（旧行为），Binance 24/7 监控
#   风险：日内"激活→小回撤→继续上涨"场景会被错误平仓，与回测逻辑不一致
PROFIT_LOCK_STOP_ENABLED = False

# ── Chandelier Exit（多头出场修饰） ─────────────────────────────────
CE_N    = 22         # 滚动最高价周期（日）
CE_MULT = 2.0        # ATR 乘数（回测最优值）
# CE 计算：CE_long = rolling_max(high, CE_N, shift=1) - CE_MULT × ATR22
# 当 close 跌破 CE_long（且前一日在 CE_long 上方）→ 触发 ce_exit 平多

# ── GMMA G2 宽松过滤（入场环境过滤器，替代 MA120/MA100） ────────────
GMMA_SHORT  = [3, 5, 8, 10, 12, 15]   # 短组 EWM spans
GMMA_LONG   = [30, 35, 40, 45, 50, 60] # 长组 EWM spans
GMMA_SP_LONG = 0.000  # 多头展宽阈值（≥0 = 无展宽约束，仅依赖方向）
GMMA_SP_A    = 0.015  # 信号A 展宽阈值
GMMA_SP_DUAL = 0.000  # dual_hit 展宽阈值（无展宽约束，仅依赖方向）
GMMA_SP_L2S  = 0.005  # L2s 展宽阈值
GMMA_SP_CB   = 0.025  # Combo Bear 展宽阈值
# g2_bull      = mean(短组) > mean(长组) AND 展宽 ≥ GMMA_SP_LONG → 允许做多
# g2_bear_A    = mean(短组) < mean(长组) AND 展宽 ≥ GMMA_SP_A    → 信号A 空头过滤
# g2_bear_dual = mean(短组) < mean(长组) AND 展宽 ≥ GMMA_SP_DUAL → dual_hit 空头过滤
# g2_bear_L2s  = mean(短组) < mean(长组) AND 展宽 ≥ GMMA_SP_L2S  → L2s 空头过滤
# g2_bear_CB   = mean(短组) < mean(长组) AND 展宽 ≥ GMMA_SP_CB   → CB 空头过滤

# ── S2 投票门槛 ─────────────────────────────────────────────────────
S2_THRESH   = 4      # bull_score ≥ 4 计入S2投票（原为3）

# ── 风险控制 ────────────────────────────────────────────────────────
MIN_OPEN_BALANCE = 15.0   # 开仓最低可用余额（USDT）
MARGIN_WARN_PCT  = 0.50   # 保证金率警告阈值：≥50% 发出警告
MARGIN_CRIT_PCT  = 0.75   # 保证金率危险阈值：≥75% 禁止开新仓

# ── K-means 牛市聚类定义（固定基准，OOS前或地图缺失时的回退） ──────────
BULL_CLUSTERS = {
    'k10': {0, 3, 4, 7}, 'k11': {1, 2, 6, 8}, 'k12': {0, 2, 4, 5, 11},
    'k13': {1, 2, 7, 8, 9, 10}, 'k14': {5, 6, 7, 9, 11}, 'k15': {1, 2, 3, 4, 5, 13},
}
K_COLS = ['k10', 'k11', 'k12', 'k13', 'k14', 'k15']

# ── 滚动投票地图路径（Roll_orig = 900_30_30，与脚本放同一目录）─────────
# 将 prediction_window_regime_maps_900_30_30.json 上传到服务器同目录即可
MAP_PATH = os.path.join(SCRIPT_DIR, "prediction_window_regime_maps_900_30_30.json")

# ── Combo Bear 信号路径（每月更新，与脚本放同一目录）─────────────────
# 将最新的 combo.json 上传到服务器同目录即可（月度更新）
COMBO_PATH = os.path.join(SCRIPT_DIR, "combo.json")

# ══════════════════════════════════════════════════════════════════════
# ③ 滚动投票地图（懒加载，首次调用时读取）
# ══════════════════════════════════════════════════════════════════════

_rolling_maps = None   # 缓存，避免每次 load_df 都重新读文件


def _load_rolling_maps():
    """加载滚动投票地图 JSON，缺失时返回 None（自动退回固定 BULL_CLUSTERS）。"""
    global _rolling_maps
    if _rolling_maps is not None:
        return _rolling_maps
    if os.path.exists(MAP_PATH):
        with open(MAP_PATH, encoding='utf-8') as f:
            _rolling_maps = json.load(f)
        # log 在 logging 配置之后才能用，这里只能用 print
        print(f"[INFO] 滚动投票地图已加载: {len(_rolling_maps)} 个预测窗口  ({MAP_PATH})")
    else:
        print(f"[WARN] 滚动投票地图文件不存在，退回固定 BULL_CLUSTERS: {MAP_PATH}")
    return _rolling_maps


# ══════════════════════════════════════════════════════════════════════
# ④ 日志配置
# ══════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# 决策信号专用 logger（供监控页面展示）
slog = logging.getLogger("signal")
slog.setLevel(logging.INFO)
slog.propagate = False
_sh = logging.FileHandler(SIGNAL_LOG_FILE, encoding="utf-8")
_sh.setFormatter(logging.Formatter("%(asctime)s  %(message)s"))
slog.addHandler(_sh)


# ══════════════════════════════════════════════════════════════════════
# ④ 状态持久化
# ══════════════════════════════════════════════════════════════════════

def default_state() -> dict:
    return {
        "long_leg": {
            "active":          False,
            "signal":          None,    # "L1_4x" / "NL_1x"
            "leverage":        0.0,
            "max_hold":        0,
            "entry_price":     None,
            "entry_date":      None,
            "hold_days":       0,
            "peak_gain":       0.0,     # 历史最高 BTC 收益（追踪止盈用）
            "bear_vote_count": 0,       # 连续熊市投票天数
            "is_l1":           False,   # True=L1信号日4x; False=非L1日1x
            "override_trail_mode": False,   # True=override_day触发后切换为追踪止盈模式
            "quantity":              0.0,
            "pos_usdt":              0.0,
            "stop_order_id":         None,    # 初始止损单 ID（永不撤销）
            "profit_lock_stop_id":   None,    # 利润锁定止损单 ID（每日更新）
            "profit_lock_stop_price": 0.0,    # 当前利润锁定止损价
        },
        "short_leg": {
            "active":          False,
            "signal":          None,    # "A" / "C"
            "leverage":        0.0,
            "max_hold":        0,
            "entry_price":     None,
            "entry_date":      None,
            "hold_days":       0,
            "peak_gain":       0.0,
            "bull_vote_count": 0,       # 连续牛市投票天数
            "quantity":              0.0,
            "pos_usdt":              0.0,
            "stop_order_id":         None,    # 初始止损单 ID（永不撤销）
            "profit_lock_stop_id":   None,    # 利润锁定止损单 ID（每日更新）
            "profit_lock_stop_price": 0.0,    # 当前利润锁定止损价
        },
        "last_run_date": None,
    }


def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)
            ds = default_state()
            for leg in ("long_leg", "short_leg"):
                for k, v in ds[leg].items():
                    state[leg].setdefault(k, v)
            return state
        except Exception as e:
            log.warning(f"读取状态文件失败，使用默认状态: {e}")
    return default_state()


def save_state(state: dict):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2, default=str)


_REASON_CN = {
    "trail_stop":    "追踪止盈触发",
    "soft_stop":     "软止损（收盘跌破软止损线）",
    "ce_exit":       "Chandelier Exit 翻空",
    "bear_vote":     "连续熊市投票",
    "bull_vote":     "连续牛市投票",
    "max_hold":      "达到最大持仓天数",
    "stop_loss":     "止损触发",
    "override_stop": "Override止损（k14=12|k15=7日开仓价亏损）",
}


def log_trade(action: str, side: str, price: float,
              qty: float, leverage: float, signal: str = None,
              pnl_pct: float = None, reason: str = None):
    # ── 主日志打印醒目的交易记录 ──────────────────────────────────────
    reason_cn = _REASON_CN.get(reason, reason or "")
    if action == "OPEN":
        log.info(
            f"{'▶'*3} 开仓  {side}  {signal}  {leverage}x"
            f"  价格=${price:,.0f}  数量={qty} BTC"
        )
        slog.info(
            f"{'▶'*3} 开仓  {side}  {signal}  {leverage}x"
            f"  价格=${price:,.0f}  数量={qty} BTC"
        )
    else:
        pnl_s = f"  盈亏={pnl_pct:+.2%}" if pnl_pct is not None else ""
        log.info(
            f"{'◀'*3} 平仓  {side}  {signal}  {leverage}x"
            f"  价格=${price:,.0f}  数量={qty} BTC"
            f"{pnl_s}  原因={reason_cn}"
        )
        slog.info(
            f"{'◀'*3} 平仓  {side}  {signal}  {leverage}x"
            f"  价格=${price:,.0f}  数量={qty} BTC"
            f"{pnl_s}  原因={reason_cn}"
        )

    # ── 写入交易记录 JSON ─────────────────────────────────────────────
    try:
        trades = []
        if os.path.exists(TRADE_LOG_FILE):
            with open(TRADE_LOG_FILE, 'r', encoding='utf-8') as f:
                trades = json.load(f)
        entry = {
            "datetime": str(datetime.now(ZoneInfo("UTC"))),
            "action":   action,      # OPEN / CLOSE / STOP_TRIGGERED
            "side":     side,        # LONG / SHORT
            "signal":   signal,
            "price":    round(price, 2),
            "qty":      qty,
            "leverage": leverage,
        }
        if pnl_pct is not None:
            entry["pnl_pct"] = round(pnl_pct * 100, 2)
        if reason:
            entry["reason"]    = reason
            entry["reason_cn"] = reason_cn
        trades.append(entry)
        with open(TRADE_LOG_FILE, 'w', encoding='utf-8') as f:
            json.dump(trades, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.warning(f"  交易记录写入失败: {e}")


# ══════════════════════════════════════════════════════════════════════
# ⑤ Binance REST API 封装
# ══════════════════════════════════════════════════════════════════════

_TIME_OFFSET: int = 0


def sync_server_time():
    global _TIME_OFFSET
    try:
        resp = requests.get(FUTURES_URL + "/fapi/v1/time", timeout=5)
        server_ts = resp.json()["serverTime"]
        local_ts  = int(time.time() * 1000)
        _TIME_OFFSET = server_ts - local_ts
        log.info(f"  时钟同步: 服务器-本地偏差 {_TIME_OFFSET} ms")
    except Exception as e:
        log.warning(f"  时钟同步失败（忽略）: {e}")


def _timestamp() -> int:
    return int(time.time() * 1000) + _TIME_OFFSET


def _sign(query_string: str) -> str:
    return hmac.new(
        API_SECRET.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _headers() -> dict:
    return {"X-MBX-APIKEY": API_KEY}


def fapi_get(path: str, params: dict = None, signed: bool = False) -> dict:
    params = params or {}
    if signed:
        params["timestamp"] = _timestamp()
        qs = urlencode(params)
        params["signature"] = _sign(qs)
    resp = requests.get(
        FUTURES_URL + path, params=params, headers=_headers(), timeout=10)
    resp.raise_for_status()
    return resp.json()


def fapi_post(path: str, params: dict, retries: int = 3) -> dict:
    headers = _headers()
    headers["Content-Type"] = "application/x-www-form-urlencoded"
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            p = dict(params)
            p["timestamp"] = _timestamp()
            qs  = urlencode(p)
            sig = _sign(qs)
            body = qs + "&signature=" + sig
            resp = requests.post(
                FUTURES_URL + path, data=body, headers=headers, timeout=15)
            data = resp.json() if resp.text.strip() else {}
            if resp.status_code != 200:
                raise RuntimeError(f"Binance API 错误 {resp.status_code}: {data}")
            return data
        except RuntimeError:
            raise
        except Exception as e:
            last_err = e
            if attempt < retries:
                log.warning(f"  POST {path} 第{attempt}次失败({e})，2秒后重试...")
                time.sleep(2)
    raise RuntimeError(f"POST {path} 重试{retries}次均失败: {last_err}")


def get_futures_balance() -> float:
    data = fapi_get("/fapi/v2/account", signed=True)
    for asset in data.get("assets", []):
        if asset["asset"] == "USDT":
            return float(asset["availableBalance"])
    return 0.0


def check_margin_safety() -> dict:
    """
    查询账户保证金安全状态。
    保证金率 = 维持保证金 / 账户净值（含浮盈）
    返回 dict:
        safe        : bool   — False 表示危险（≥ MARGIN_CRIT_PCT），应禁止开新仓
        margin_ratio: float
        margin_balance : float  账户净值
        maint_margin   : float  维持保证金
        available      : float  可用余额
        unrealized_pnl : float
        level          : str    'ok' / 'warn' / 'critical'
    """
    try:
        data = fapi_get("/fapi/v2/account", signed=True)
        mb   = float(data.get("totalMarginBalance",    0))
        mm   = float(data.get("totalMaintMargin",      0))
        av   = float(data.get("availableBalance",      0))
        upnl = float(data.get("totalUnrealizedProfit", 0))
        ratio = mm / mb if mb > 1e-6 else 0.0

        if ratio >= MARGIN_CRIT_PCT:
            level = 'critical'
            safe  = False
            slog.critical(
                f"🚨 保证金率危险 {ratio:.1%}  "
                f"(维持保证金 ${mm:,.0f} / 账户净值 ${mb:,.0f})  "
                f"距离强平较近，禁止开新仓！"
            )
        elif ratio >= MARGIN_WARN_PCT:
            level = 'warn'
            safe  = True
            slog.warning(
                f"⚠️ 保证金率偏高 {ratio:.1%}  "
                f"(维持保证金 ${mm:,.0f} / 账户净值 ${mb:,.0f})"
            )
        else:
            level = 'ok'
            safe  = True

        return dict(safe=safe, margin_ratio=ratio, margin_balance=mb,
                    maint_margin=mm, available=av, unrealized_pnl=upnl, level=level)
    except Exception as e:
        log.warning(f"  保证金率查询失败（忽略）: {e}")
        return dict(safe=True, margin_ratio=0.0, margin_balance=0.0,
                    maint_margin=0.0, available=0.0, unrealized_pnl=0.0, level='unknown')


def get_position_info(symbol: str = SYMBOL) -> dict:
    data = fapi_get("/fapi/v2/positionRisk", {"symbol": symbol}, signed=True)
    result = {}
    for p in data:
        side = p.get("positionSide", "BOTH")
        result[side] = {
            "positionAmt":      float(p["positionAmt"]),
            "entryPrice":       float(p["entryPrice"]),
            "unrealizedProfit": float(p["unRealizedProfit"]),
        }
    return result


def get_current_price(symbol: str = SYMBOL) -> float:
    data = fapi_get("/fapi/v1/ticker/price", {"symbol": symbol})
    return float(data["price"])


def init_futures_settings():
    """初始化：同步时钟 + 设置杠杆（双向持仓模式需在币安账户手动开启）"""
    log.info("初始化合约设置...")
    sync_server_time()
    try:
        res = fapi_post("/fapi/v1/leverage",
                        {"symbol": SYMBOL, "leverage": MAX_LEVERAGE})
        log.info(f"  ✅ 杠杆已设为 {MAX_LEVERAGE}x "
                 f"(maxNotionalValue={res.get('maxNotionalValue')})")
    except Exception as e:
        log.warning(f"  ⚠️ 杠杆设置失败: {e}")


_QTY_PRECISION = None


def get_quantity_precision(symbol: str = SYMBOL) -> int:
    try:
        info = fapi_get("/fapi/v1/exchangeInfo")
        for s in info.get("symbols", []):
            if s["symbol"] == symbol:
                for f in s.get("filters", []):
                    if f["filterType"] == "LOT_SIZE":
                        step = float(f["stepSize"])
                        return max(0, -int(math.log10(step)))
    except Exception:
        pass
    return 3


def calc_quantity(notional_usdt: float, price: float) -> float:
    global _QTY_PRECISION
    if _QTY_PRECISION is None:
        _QTY_PRECISION = get_quantity_precision()
    qty = notional_usdt / price
    factor = 10 ** _QTY_PRECISION
    return math.floor(qty * factor) / factor


def _set_leverage(lev: int):
    """开仓前设置交易所杠杆，确保 Binance 显示与策略一致。"""
    try:
        fapi_post("/fapi/v1/leverage", {"symbol": SYMBOL, "leverage": lev})
        log.info(f"  ✅ 交易所杠杆已设为 {lev}x")
    except Exception as e:
        log.warning(f"  ⚠️ 设置杠杆失败（继续开仓）: {e}")


def open_long(pos_usdt: float, leverage: float, price: float) -> float:
    _set_leverage(int(math.ceil(leverage)))   # ceil 允许小数杠杆（如 3.5× 需设为 4×）
    notional = pos_usdt * leverage
    qty = calc_quantity(notional, price)
    if qty <= 0:
        msg = f"开多失败：计算数量为0 (pos={pos_usdt:.2f} USDT, lev={leverage}x, price={price:.0f})"
        log.error(f"  ❌ {msg}"); slog.error(f"[多头] ❌ {msg}")
        return 0.0
    log.info(f"  📈 开多: qty={qty} BTC  {leverage}x  notional≈{notional:.0f} USDT @ {price:.0f}")
    try:
        res = fapi_post("/fapi/v1/order", {
            "symbol": SYMBOL, "side": "BUY", "positionSide": "LONG",
            "type": "MARKET", "quantity": qty,
        })
        log.info(f"     ✅ 开多成功  订单ID={res.get('orderId')}  状态={res.get('status')}")
        return qty
    except Exception as e:
        msg = f"开多下单失败: qty={qty} BTC @ {price:.0f}  原因: {e}"
        log.error(f"  ❌ {msg}"); slog.error(f"[多头] ❌ {msg}")
        return 0.0


def close_long(quantity: float):
    if quantity <= 0:
        return
    log.info(f"  📉 平多: qty={quantity} BTC")
    try:
        res = fapi_post("/fapi/v1/order", {
            "symbol": SYMBOL, "side": "SELL", "positionSide": "LONG",
            "type": "MARKET", "quantity": quantity,
        })
        log.info(f"     ✅ 平多成功  订单ID={res.get('orderId')}  状态={res.get('status')}")
    except Exception as e:
        msg = f"平多下单失败: qty={quantity} BTC  原因: {e}"
        log.error(f"  ❌ {msg}"); slog.error(f"[多头] ❌ {msg}")
        raise  # 平仓失败需上层感知，重新抛出


def open_short(pos_usdt: float, leverage: float, price: float) -> float:
    _set_leverage(int(leverage))
    notional = pos_usdt * leverage
    qty = calc_quantity(notional, price)
    if qty <= 0:
        msg = f"开空失败：计算数量为0 (pos={pos_usdt:.2f} USDT, lev={leverage}x, price={price:.0f})"
        log.error(f"  ❌ {msg}"); slog.error(f"[空头] ❌ {msg}")
        return 0.0
    log.info(f"  📉 开空: qty={qty} BTC  {leverage}x  notional≈{notional:.0f} USDT @ {price:.0f}")
    try:
        res = fapi_post("/fapi/v1/order", {
            "symbol": SYMBOL, "side": "SELL", "positionSide": "SHORT",
            "type": "MARKET", "quantity": qty,
        })
        log.info(f"     ✅ 开空成功  订单ID={res.get('orderId')}  状态={res.get('status')}")
        return qty
    except Exception as e:
        msg = f"开空下单失败: qty={qty} BTC @ {price:.0f}  原因: {e}"
        log.error(f"  ❌ {msg}"); slog.error(f"[空头] ❌ {msg}")
        return 0.0


def close_short(quantity: float):
    if quantity <= 0:
        return
    log.info(f"  📈 平空: qty={quantity} BTC")
    try:
        res = fapi_post("/fapi/v1/order", {
            "symbol": SYMBOL, "side": "BUY", "positionSide": "SHORT",
            "type": "MARKET", "quantity": quantity,
        })
        log.info(f"     ✅ 平空成功  订单ID={res.get('orderId')}  状态={res.get('status')}")
    except Exception as e:
        msg = f"平空下单失败: qty={quantity} BTC  原因: {e}"
        log.error(f"  ❌ {msg}"); slog.error(f"[空头] ❌ {msg}")
        raise  # 平仓失败需上层感知，重新抛出


# ══════════════════════════════════════════════════════════════════════
# ⑥ 交易所止损单（Binance Algo Order — 已测试通过）
# ══════════════════════════════════════════════════════════════════════

_exchange = None


def _get_exchange():
    """懒加载 ccxt Binance 实例"""
    global _exchange
    if _exchange is None:
        _exchange = ccxt.binance({
            'apiKey':  API_KEY,
            'secret':  API_SECRET,
            'options': {'defaultType': 'future'},
        })
    return _exchange


def place_stop_order(qty: float, stop_price: float, is_long: bool) -> str:
    """
    在 Binance 挂 STOP_MARKET 止损单（Algo Order / CONDITIONAL）。
    is_long=True  → SELL LONG  （多头止损，price 向下触发）
    is_long=False → BUY SHORT  （空头止损，price 向上触发）
    返回 algo_id 字符串，失败返回空字符串。
    """
    side      = 'SELL' if is_long else 'BUY'
    pos_side  = 'LONG' if is_long else 'SHORT'
    label     = '多头止损' if is_long else '空头止损'
    params = {
        'symbol':         SYMBOL,
        'algoType':       'CONDITIONAL',
        'type':           'STOP_MARKET',
        'side':           side,
        'positionSide':   pos_side,
        'quantity':       str(qty),
        'triggerPrice':   str(round(stop_price, 1)),
        'closeOnTrigger': 'true',
        'workingType':    'MARK_PRICE',
    }
    try:
        res = _get_exchange().fapiPrivatePostAlgoOrder(params)
        algo_id = str(res.get('algoId') or res.get('orderId') or '')
        log.info(f"  ✅ {label}单已挂: stopPrice={stop_price:.1f}  algoId={algo_id}")
        return algo_id
    except Exception as e:
        err = str(e)
        if '-2021' in err or 'immediately trigger' in err.lower():
            # 止损价已低于（多头）或高于（空头）当前价，挂单会立即触发
            # 正常情况：价格已回落到锁定线，由软件追踪止盈逻辑处理，不视为错误
            log.info(f"  [{label}] 止损价${stop_price:.1f}会立即触发，不激活利润锁定单（当前价已回落）")
            return ''
        log.error(f"  ❌ {label}单挂单失败: {e}")
        return ''


def cancel_stop_order(algo_id: str, is_long: bool = True):
    """
    撤销 Algo Order 止损单。
    止损已触发时撤销会失败（正常现象，忽略错误）。
    """
    if not algo_id:
        return
    label = '多头止损' if is_long else '空头止损'
    try:
        _get_exchange().fapiPrivateDeleteAlgoOrder({
            'symbol':  SYMBOL,
            'algoId':  str(algo_id),
        })
        log.info(f"  ✅ {label}单已撤销: algoId={algo_id}")
    except Exception as e:
        log.warning(f"  ⚠️ {label}单撤销失败（可能已触发）: {e}")


def update_profit_lock_stop(leg: dict, qty: float, entry: float,
                             peak: float, trail_pct: float, is_long: bool):
    """
    每日更新利润锁定止损单（追踪止盈双保险）。
    仅在有浮盈时触发，止损价只升不降：
      多头: lock_price = entry * (1 + peak * (1 - trail_pct))
      空头: lock_price = entry * (1 - peak * (1 - trail_pct))

    注意：PROFIT_LOCK_STOP_ENABLED=False 时不向交易所挂单，仅在每日 00:05 UTC
    做追踪止盈检查，与回测逻辑保持一致（避免日内噪音触发假平仓）。
    """
    label = "多头" if is_long else "空头"

    # ── 计算理论锁定价（用于日志显示，无论是否挂单都计算）──────────────
    if peak > 0:
        if is_long:
            lock_price = math.floor(entry * (1 + peak * (1 - trail_pct)) * 10) / 10
        else:
            lock_price = math.ceil(entry * (1 - peak * (1 - trail_pct)) * 10) / 10
    else:
        lock_price = 0.0

    if not PROFIT_LOCK_STOP_ENABLED:
        # 取消已有的利润锁定止损单（如果有残留旧单）
        old_id = leg.get("profit_lock_stop_id")
        if old_id:
            log.info(f"  [{label}] PROFIT_LOCK_STOP_ENABLED=False，撤销残留利润锁定止损单 algoId={old_id}")
            cancel_stop_order(old_id, is_long=is_long)
            leg["profit_lock_stop_id"]    = None
            leg["profit_lock_stop_price"] = 0.0
        # 仅记录理论止盈线（供日志参考），不向交易所挂单
        if peak > 0:
            log.info(
                f"  [{label}] 追踪止盈线（仅日检，不挂单）: ≈${lock_price:.1f}  "
                f"(峰值{peak:.2%} × 锁利{(1-trail_pct):.0%})"
            )
        return

    # ── 以下为 PROFIT_LOCK_STOP_ENABLED=True 时的旧逻辑 ─────────────────
    if peak <= 0:
        return

    old_price = float(leg.get("profit_lock_stop_price") or 0.0)

    # 只升不降（多头止损价只向上移，空头止损价只向下移）
    if is_long  and lock_price <= old_price:
        return
    if not is_long and old_price > 0 and lock_price >= old_price:
        return

    # 先挂新单——成功后再撤旧单，确保始终有保护
    new_id = place_stop_order(qty, lock_price, is_long=is_long)
    if not new_id:
        log.warning(
            f"  [{label}] 利润锁定止损挂单失败，保留旧单 "
            f"(旧价 ${old_price:.1f}，目标 ${lock_price:.1f})"
        )
        return  # 不更新状态，旧单继续有效

    # 新单挂成功，撤旧单并更新状态
    old_id = leg.get("profit_lock_stop_id")
    if old_id:
        cancel_stop_order(old_id, is_long=is_long)

    leg["profit_lock_stop_id"]    = new_id
    leg["profit_lock_stop_price"] = lock_price
    log.info(
        f"  [{label}] 利润锁定止损: ${old_price:.1f} → ${lock_price:.1f}  "
        f"(锁住利润{(1-trail_pct):.0%})  algoId={new_id}"
    )


# ══════════════════════════════════════════════════════════════════════
# ⑦ 数据加载与信号工具
# ══════════════════════════════════════════════════════════════════════

def update_data_pipeline() -> bool:
    try:
        sys.path.insert(0, BASE_DIR)
        from data_process import main as dp_main
        result = dp_main()
        if result is None:
            log.error("数据流水线返回 None")
            return False
        log.info(f"数据更新完成: {result.get('date', '?')}")
        return True
    except Exception as e:
        log.error(f"数据流水线异常: {e}", exc_info=True)
        return False


def load_df() -> pd.DataFrame:
    df = pd.read_excel(BTC_FILE)
    df['date']  = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)

    # ── bull_score：固定 BULL_CLUSTERS（与回测 ha_trail_sweep / ha_yearly 完全一致）──
    # 向量化计算，无滚动地图，保证实盘与回测行为相同
    _kv_raw = df[K_COLS].values
    _vm     = ~np.isnan(_kv_raw[:, 0])
    _kv     = np.where(np.isnan(_kv_raw), -1, _kv_raw).astype(int)
    _n      = len(df)
    _bs_arr = np.full(_n, np.nan)
    _sc     = np.zeros(_n, float)
    for _ci, _col in enumerate(K_COLS):
        _sc += np.isin(_kv[:, _ci], list(BULL_CLUSTERS[_col])).astype(float)
    _bs_arr[_vm] = _sc[_vm]
    df['bull_score'] = _bs_arr

    # ── GMMA G2 宽松过滤（入场环境）────────────────────────────────
    # 12 条 EWM 均线（adjust=False，与回测保持一致）
    for s in GMMA_SHORT + GMMA_LONG:
        df[f'_g{s}'] = df['close'].ewm(span=s, adjust=False).mean()
    _gS = df[[f'_g{s}' for s in GMMA_SHORT]].values   # (n, 6)
    _gL = df[[f'_g{s}' for s in GMMA_LONG]].values    # (n, 6)
    _gs_mean   = _gS.mean(axis=1)
    _gl_mean   = _gL.mean(axis=1)
    _gs_spread = (_gS.max(axis=1) - _gS.min(axis=1)) / df['close'].values
    _bull_dir = _gs_mean > _gl_mean
    _bear_dir = _gs_mean < _gl_mean
    df['g2_bull']      = _bull_dir & (_gs_spread >= GMMA_SP_LONG)  # 多头（实为纯方向）
    df['g2_bear_A']    = _bear_dir & (_gs_spread >= GMMA_SP_A)     # 信号A
    df['g2_bear_dual'] = _bear_dir & (_gs_spread >= GMMA_SP_DUAL)  # dual_hit
    df['g2_bear_L2s']  = _bear_dir & (_gs_spread >= GMMA_SP_L2S)   # L2s
    df['g2_bear_CB']   = _bear_dir & (_gs_spread >= GMMA_SP_CB)    # Combo Bear
    df['g2_bear']      = _bear_dir                                  # 通用方向标志（日志展示用）
    # 短组均值/展宽（供日志展示）
    df['_gs_mean']   = _gs_mean
    df['_gl_mean']   = _gl_mean
    df['_gs_spread'] = _gs_spread

    # ── Chandelier Exit 指标（多头出场）────────────────────────────
    # ATR22（EWM，adjust=False，与回测保持一致，无未来数据）
    _prev_c = df['close'].shift(1)
    _tr = np.maximum.reduce([
        (df['high'] - df['low']).values,
        (df['high'] - _prev_c).abs().values,
        (df['low']  - _prev_c).abs().values,
    ])
    _atr22 = pd.Series(_tr).ewm(span=CE_N, adjust=False).mean().values
    # CE_long = 过去 CE_N 日最高价（shift=1，不含当日）- CE_MULT × ATR22
    df['ce_long'] = df['high'].rolling(CE_N).max().shift(1) - CE_MULT * _atr22
    # ce_bull: close > CE_long = 多头结构；False = 已跌破 CE 线
    df['ce_bull'] = df['close'] > df['ce_long']

    return df


def get_latest_row_idx(df: pd.DataFrame):
    """返回 (row, idx)，idx 为 RangeIndex 中的整数位置"""
    valid = df.dropna(subset=K_COLS)
    if valid.empty:
        raise ValueError("数据中无有效 K 值行")
    idx = int(valid.index[-1])
    return df.iloc[idx], idx


def _k(row, col):
    v = row.get(col) if hasattr(row, 'get') else row[col]
    return None if pd.isna(v) else int(v)


def is_L1(row) -> bool:
    k10, k11, k13, k14 = _k(row,'k10'), _k(row,'k11'), _k(row,'k13'), _k(row,'k14')
    if None in (k10, k11, k13, k14): return False
    return k10==7 and k11==1 and k13==2 and k14==11


def is_L2(row) -> bool:
    k10, k11, k12, k13 = _k(row,'k10'), _k(row,'k11'), _k(row,'k12'), _k(row,'k13')
    if None in (k10, k11, k12, k13): return False
    return k10==3 and k11==6 and k12==0 and k13==10


def is_short_A_signal(row) -> bool:
    k10, k11, k12, k13, k14 = (_k(row, c) for c in ['k10','k11','k12','k13','k14'])
    if None in (k10, k11, k12, k13, k14): return False
    return k10==5 and k11==4 and k12==8 and k13==4 and k14==0


def get_short_signal(row):
    """返回 (signal_name, max_hold_days, leverage) 或 None
    优先级：A > dual_hit(原C，OR→AND) > L2s
    """
    ks = [_k(row, c) for c in K_COLS]
    if None in ks: return None
    k10, k11, k12, k13, k14, k15 = ks
    if k10==5 and k11==4 and k12==8 and k13==4 and k14==0:
        return ('A', S_MAX_A, 3.0 if k15==0 else 2.0)
    # dual_hit：k14=12 AND k15=7（原Signal C OR条件改为AND）
    if k14==12 and k15==7:
        return ('dual_hit', S_MAX_C, 1.0)
    # L2s：原多头 L2 K型态，在熊市趋势下转为空头信号
    if k10==3 and k11==6 and k12==0 and k13==10:
        return ('L2s', L2S_MAX, L2S_LEV)
    return None


def is_override_day(row) -> bool:
    """override_day：k14=12 OR k15=7 → bull_score视为0，禁止新开多头"""
    k14, k15 = _k(row, 'k14'), _k(row, 'k15')
    return (k14 is not None and k14 == 12) or (k15 is not None and k15 == 7)


# ── Combo Bear 信号（按日缓存，每日首次读取后当天复用）────────────────
_combo_cache: dict = {'date': None, 'patterns': []}


def _load_combo_bear_patterns(today: date) -> list:
    """
    加载 combo.json，读取 effective_start_date 校验有效性，
    再定位包含今日的预测窗口，返回 bear pattern 列表。
    结果按日缓存，同一天多次调用只读一次文件。

    有效性规则：
      effective_start_date 与运行当天同年同月 → COMBO有效
      否则 log.error 输出具体问题，仍尝试返回 patterns（降级运行）
    """
    global _combo_cache
    if _combo_cache['date'] == today:
        return _combo_cache['patterns']

    # 重置缓存
    _combo_cache['date'] = today
    _combo_cache['patterns'] = []

    if not os.path.exists(COMBO_PATH):
        log.error(f"  [COMBO] ⚠️ 文件不存在，CB信号将跳过: {COMBO_PATH}")
        return []

    try:
        with open(COMBO_PATH, 'r', encoding='utf-8') as f:
            raw = json.load(f)
    except Exception as e:
        log.error(f"  [COMBO] ⚠️ 读取失败: {e}")
        return []

    # ── 解析 JSON 结构（兼容 dict 和 list 两种格式）────────────────────
    # dict 格式：{"effective_start_date": "...", "windows": [...]}
    # list 格式：[{"effective_start_date": "...", ...}, ...]（第一个元素含该字段）
    if isinstance(raw, dict):
        eff_start_str = raw.get('effective_start_date')
        windows = raw.get('windows', raw.get('bear', []))
    else:
        windows = raw
        eff_start_str = None
        for item in windows:
            if isinstance(item, dict) and 'effective_start_date' in item:
                eff_start_str = item['effective_start_date']
                break

    # ── effective_start_date 有效性校验 ──────────────────────────────
    cur_ym = f"{today.year}-{today.month:02d}"
    if eff_start_str is None:
        log.error(
            f"  [COMBO] ⚠️ 未找到 effective_start_date 字段，"
            f"无法校验有效性，请检查 combo.json 格式！"
        )
    else:
        try:
            eff_ts = pd.Timestamp(eff_start_str)
            if eff_ts.year == today.year and eff_ts.month == today.month:
                log.info(
                    f"  [COMBO] ✅ COMBO有效  "
                    f"effective_start_date={eff_start_str}  当前月份={cur_ym}"
                )
            else:
                log.error(
                    f"  [COMBO] ⚠️ COMBO已过期！"
                    f"effective_start_date={eff_start_str}（{eff_ts.strftime('%Y-%m')}）"
                    f" ≠ 当前月份 {cur_ym}，请更新 combo.json！"
                )
        except Exception as e:
            log.error(
                f"  [COMBO] ⚠️ effective_start_date 解析失败（值={eff_start_str!r}）: {e}"
            )

    # ── 定位今日所在预测窗口 ──────────────────────────────────────────
    today_ts = pd.Timestamp(today)
    active_window = None
    for w in windows:
        if not isinstance(w, dict) or 'prediction_start_date' not in w:
            continue
        try:
            s = pd.Timestamp(w['prediction_start_date'])
            e = pd.Timestamp(w['prediction_end_date'])
        except Exception:
            continue
        if s <= today_ts <= e:
            active_window = w
            break

    if active_window is None:
        log.error(
            f"  [COMBO] ⚠️ 今日 {today} 无对应预测窗口，"
            f"CB信号将跳过，请更新 combo.json！"
        )
        return []

    patterns = active_window.get('bear', [])
    _combo_cache['patterns'] = patterns
    log.info(
        f"  [COMBO] 活跃窗口 {active_window['prediction_start_date']} ~ "
        f"{active_window['prediction_end_date']}  bear patterns={len(patterns)}"
    )
    return patterns


def check_combo_bear_signal(row, today: date):
    """
    检查当日 k 值是否命中 Combo Bear 模式。
    返回 ('CB', S_COMBO_MAX, S_COMBO_LEV) 或 None。
    优先级最低，仅在 A/dual_hit/L2s 均未命中时调用。
    """
    bear_pats = _load_combo_bear_patterns(today)
    if not bear_pats:
        return None
    ks = {c: _k(row, c) for c in K_COLS}
    if any(v is None for v in ks.values()):
        return None
    for pat in bear_pats:
        if all(ks.get(col) == int(val) for col, val in pat.items()):
            return ('CB', S_COMBO_MAX, S_COMBO_LEV)
    return None


def check_s2_vote(df: pd.DataFrame, latest_idx: int) -> bool:
    """近3行中至少2行 bull_score ≥ S2_THRESH（纯投票，不排除 L2v 日）。"""
    start = max(0, latest_idx - 2)
    count = 0
    for i in range(start, latest_idx + 1):
        row_i = df.iloc[i]
        bs = row_i.get('bull_score', float('nan'))
        if pd.isna(bs):
            continue
        if float(bs) >= S2_THRESH:
            count += 1
    return count >= 2


# ══════════════════════════════════════════════════════════════════════
# ⑧ 持仓与 Binance 状态同步
# ══════════════════════════════════════════════════════════════════════

def sync_state_with_binance(state: dict, current_price: float) -> dict | None:
    """
    从 Binance 读取真实持仓，修正状态文件。
    若状态显示持仓但 Binance 已无仓位 → 止损单已触发，记录并清理状态。
    返回持仓快照 dict，失败时返回 None。
    """
    try:
        pos       = get_position_info()
        long_amt  = abs(pos.get("LONG",  {}).get("positionAmt", 0.0))
        short_amt = abs(pos.get("SHORT", {}).get("positionAmt", 0.0))

        # ── 多头同步 ──────────────────────────────────────────────────
        ll = state["long_leg"]
        if ll["active"] and long_amt < 1e-6:
            # 有仓位记录但 Binance 已无仓位 → 止损单已触发
            entry    = float(ll["entry_price"] or 0)
            _ll_stop = ll.get("entry_stop", L_STOP)
            pnl      = -_ll_stop * float(ll["leverage"]) if entry else None
            slog.warning(
                f"[多头] ⚠ 止损单已触发  entry=${entry:,.0f}  "
                f"stopPrice≈${entry*(1-_ll_stop):,.0f}  "
                f"杠杆亏损≈{pnl*100:+.1f}%" if pnl else "[多头] ⚠ 止损单已触发"
            )
            log_trade("STOP_TRIGGERED", "LONG", current_price,
                      ll["quantity"], ll["leverage"],
                      signal=ll["signal"], pnl_pct=pnl, reason="stop_loss")
            ll.update({
                "active": False, "signal": None, "leverage": 0.0, "max_hold": 0,
                "entry_price": None, "entry_date": None,
                "hold_days": 0, "peak_gain": 0.0, "bear_vote_count": 0,
                "quantity": 0.0, "pos_usdt": 0.0, "stop_order_id": None,
                "profit_lock_stop_id": None, "profit_lock_stop_price": 0.0,
            })

        elif ll["active"] and abs(ll["quantity"] - long_amt) > 1e-6:
            log.info(f"  同步多头数量: {ll['quantity']} → {long_amt}")
            ll["quantity"] = long_amt

        if not ll["active"] and long_amt > 1e-6:
            # 状态文件丢失 / 首次运行 → 尝试从 Binance 自动恢复，防止重复开仓
            _long_pos    = pos.get("LONG", {})
            _entry_price = float(_long_pos.get("entryPrice", 0) or 0)
            _leverage    = float(_long_pos.get("leverage",   L_LEV_NL) or L_LEV_NL)
            if _entry_price > 0:
                ll.update({
                    "active": True,
                    "signal": f"RECOVERED_{_leverage:.0f}x",
                    "leverage": _leverage,
                    "max_hold": L_MAX,
                    "entry_price": _entry_price,
                    "entry_date": str(date.today()),
                    "hold_days": 1,
                    "peak_gain": 0.0,
                    "bear_vote_count": 0,
                    "is_l1": False,
                    "override_trail_mode": False,
                    "quantity": long_amt,
                    "pos_usdt": 0.0,
                    "stop_order_id": None,
                    "profit_lock_stop_id": None,
                    "profit_lock_stop_price": 0.0,
                    "entry_stop": L_STOP_NL4,
                    "entry_soft_stop": L_SOFT_STOP_NL4,
                })
                slog.warning(
                    f"[多头] ⚠ 发现未记录持仓，已自动恢复状态  "
                    f"entry=${_entry_price:,.0f}  qty={long_amt} BTC  lev={_leverage:.0f}x  "
                    f"(peak_gain/hold_days 将在下次正常运行后自动校正)"
                )
                log.warning(
                    f"  ⚠️ 多头持仓已自动恢复: entry={_entry_price} qty={long_amt} "
                    f"→ 请确认止损单是否存在！"
                )
            else:
                log.warning(
                    f"  ⚠️ 发现未记录多头持仓 {long_amt} BTC，"
                    f"无法从 Binance 获取开仓价，请手动处理"
                )

        # ── 空头同步 ──────────────────────────────────────────────────
        sl = state["short_leg"]
        if sl["active"] and short_amt < 1e-6:
            entry    = float(sl["entry_price"] or 0)
            _sl_stop = sl.get("entry_stop", S_STOP)
            pnl      = -_sl_stop * float(sl["leverage"]) if entry else None
            slog.warning(
                f"[空头] ⚠ 止损单已触发  entry=${entry:,.0f}  "
                f"stopPrice≈${entry*(1+_sl_stop):,.0f}  "
                f"杠杆亏损≈{pnl*100:+.1f}%" if pnl else "[空头] ⚠ 止损单已触发"
            )
            log_trade("STOP_TRIGGERED", "SHORT", current_price,
                      sl["quantity"], sl["leverage"],
                      signal=sl["signal"], pnl_pct=pnl, reason="stop_loss")
            sl.update({
                "active": False, "signal": None, "leverage": 0.0, "max_hold": 0,
                "entry_price": None, "entry_date": None,
                "hold_days": 0, "peak_gain": 0.0, "bull_vote_count": 0,
                "quantity": 0.0, "pos_usdt": 0.0, "stop_order_id": None,
                "profit_lock_stop_id": None, "profit_lock_stop_price": 0.0,
            })

        elif sl["active"] and abs(sl["quantity"] - short_amt) > 1e-6:
            log.info(f"  同步空头数量: {sl['quantity']} → {short_amt}")
            sl["quantity"] = short_amt

        if not sl["active"] and short_amt > 1e-6:
            _short_pos   = pos.get("SHORT", {})
            _s_entry     = float(_short_pos.get("entryPrice", 0) or 0)
            _s_leverage  = float(_short_pos.get("leverage",   L_LEV_NL) or L_LEV_NL)
            if _s_entry > 0:
                sl.update({
                    "active": True,
                    "signal": f"RECOVERED_{_s_leverage:.0f}x",
                    "leverage": _s_leverage,
                    "max_hold": S_MAX_HOLD,
                    "entry_price": _s_entry,
                    "entry_date": str(date.today()),
                    "hold_days": 1,
                    "peak_gain": 0.0,
                    "bull_vote_count": 0,
                    "quantity": short_amt,
                    "pos_usdt": 0.0,
                    "stop_order_id": None,
                    "profit_lock_stop_id": None,
                    "profit_lock_stop_price": 0.0,
                    "entry_stop": S_STOP,
                })
                slog.warning(
                    f"[空头] ⚠ 发现未记录持仓，已自动恢复状态  "
                    f"entry=${_s_entry:,.0f}  qty={short_amt} BTC  lev={_s_leverage:.0f}x"
                )
                log.warning(
                    f"  ⚠️ 空头持仓已自动恢复: entry={_s_entry} qty={short_amt} "
                    f"→ 请确认止损单是否存在！"
                )
            else:
                log.warning(
                    f"  ⚠️ 发现未记录空头持仓 {short_amt} BTC，"
                    f"无法从 Binance 获取开仓价，请手动处理"
                )

        return {
            'long_amt':         long_amt,
            'short_amt':        short_amt,
            'long_entry':       float(pos.get("LONG",  {}).get("entryPrice",       0) or 0),
            'short_entry':      float(pos.get("SHORT", {}).get("entryPrice",       0) or 0),
            'long_unrealized':  float(pos.get("LONG",  {}).get("unrealizedProfit", 0) or 0),
            'short_unrealized': float(pos.get("SHORT", {}).get("unrealizedProfit", 0) or 0),
        }
    except Exception as e:
        log.warning(f"  持仓同步失败（忽略）: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════
# ⑨ 多头腿（每日执行）
# ══════════════════════════════════════════════════════════════════════

def handle_long_leg(state: dict, df: pd.DataFrame, row: pd.Series,
                    latest_idx: int, current_price: float, today: date) -> dict:
    """
    持仓中 → 更新 peak_gain / bear_vote_count / hold_days，检查软性平仓条件
             （止损由交易所止损单负责，此处不再重复检查）
    无仓位 → 检查 GMMA G2 + S2投票，满足则开仓（PV风格，无需L1/L2形态）并同时挂止损单
    返回 report dict 供 daily_run 汇总报告使用
    """
    lg  = state["long_leg"]
    bs  = row.get('bull_score')
    bs  = float(bs) if not pd.isna(bs) else np.nan
    row_date = (row["date"].date()
                if hasattr(row["date"], "date")
                else pd.Timestamp(row["date"]).date())

    # 预计算入场条件（无论持仓与否，供报告使用）
    _close   = float(row['close'])
    g2_ok    = bool(row.get('g2_bull', False))   # GMMA G2 宽松多头过滤
    s2_ok    = check_s2_vote(df, latest_idx)

    # ── Chandelier Exit 翻空检测 ───────────────────────────────────
    # ce_flip_bear: 今日 ce_bull=False 且昨日 ce_bull=True → CE 线跌破信号
    def _safe_ce_bull(r) -> bool:
        v = r.get('ce_bull') if hasattr(r, 'get') else r['ce_bull']
        return bool(v) if (v is not None and not pd.isna(v)) else True  # 缺失时视为多头（不触发）
    _curr_ce  = _safe_ce_bull(row)
    _prev_ce  = _safe_ce_bull(df.iloc[latest_idx - 1]) if latest_idx >= 1 else True
    ce_flip_bear = (not _curr_ce) and _prev_ce   # 由多转空 = CE 出场信号

    # ── 持仓中：检查软性平仓条件 ────────────────────────────────────
    if lg["active"] and lg["entry_price"] is not None:
        entry = float(lg["entry_price"])

        # 用昨日最高价更新峰值（与回测一致：peak = max 日高涨幅）
        # 触发判断用 current_price（≈ 今日开盘价，与回测 curr_og 一致）
        yesterday_high = float(row['high'])
        high_gain = (yesterday_high - entry) / entry
        lg["peak_gain"] = max(float(lg["peak_gain"]), high_gain)
        peak = float(lg["peak_gain"])

        gain  = (current_price - entry) / entry   # 仅用于触发检查 / 显示

        # ── 0. Override Day 检查（最高优先级）────────────────────────
        # k14=12 OR k15=7：当日 bull_score 视为0，不允许新开多头；
        # 存量多头：亏损→立即平仓(override_stop)，盈利→切换追踪止盈模式（禁用熊市投票）
        _ovr_mode = bool(lg.get("override_trail_mode", False))
        if is_override_day(row) and not _ovr_mode:
            if current_price < entry:
                # 亏损 → 立即平仓
                pnl_pct_ovr = gain * float(lg["leverage"])
                slog.info(
                    f"[多头] Override止损  entry=${entry:,.0f}  now=${current_price:,.0f}  "
                    f"BTC {gain:+.2%}  杠杆盈亏 {pnl_pct_ovr:+.2%}  "
                    f"(k14={_k(row,'k14')} k15={_k(row,'k15')})"
                )
                cancel_stop_order(lg.get("stop_order_id"),       is_long=True)
                cancel_stop_order(lg.get("profit_lock_stop_id"), is_long=True)
                log_trade("CLOSE", "LONG", current_price,
                          lg["quantity"], lg["leverage"],
                          signal=lg["signal"], pnl_pct=pnl_pct_ovr, reason="override_stop")
                close_long(float(lg["quantity"]))
                _report = {
                    'was_active': True, 'action': 'close', 'close_reason': 'override_stop',
                    'gain': gain, 'pnl_pct': pnl_pct_ovr, 'peak': peak,
                    'trail_active': False, 'trail_line': None,
                    'bear_vote_count': int(lg["bear_vote_count"]),
                    'hold_days': int(lg["hold_days"]), 'max_hold': int(lg["max_hold"]),
                    'signal': lg["signal"], 'leverage': float(lg["leverage"]),
                    'entry_price': entry, 'g2_ok': g2_ok, 's2_ok': s2_ok,
                }
                lg.update({
                    "active": False, "signal": None, "leverage": 0.0, "max_hold": 0,
                    "entry_price": None, "entry_date": None,
                    "hold_days": 0, "peak_gain": 0.0, "bear_vote_count": 0,
                    "override_trail_mode": False,
                    "quantity": 0.0, "pos_usdt": 0.0, "stop_order_id": None,
                    "profit_lock_stop_id": None, "profit_lock_stop_price": 0.0,
                })
                return _report
            else:
                # 盈利 → 切换追踪止盈模式（后续禁用熊市投票出场）
                lg["override_trail_mode"] = True
                _ovr_mode = True
                log.info(
                    f"  [多头] Override模式启动 → 切换追踪止盈  "
                    f"(k14={_k(row,'k14')} k15={_k(row,'k15')})  "
                    f"gain={gain:+.2%}  peak={peak:+.2%}"
                )

        # 熊市投票：override_trail_mode 激活时禁用（不累加、不检查）
        if not _ovr_mode:
            if not np.isnan(bs) and bs <= L_VOTE_T:
                lg["bear_vote_count"] = int(lg["bear_vote_count"]) + 1
            else:
                lg["bear_vote_count"] = 0

        lg["hold_days"] = int(lg["hold_days"]) + 1

        # 根据入场类型选择追踪止盈参数（L1日标准 / 非L1日紧缩）
        _is_l1_pos   = bool(lg.get("is_l1", False))
        _cur_trail_a = L_TRAIL_A_L1   if _is_l1_pos else L_TRAIL_A_NL
        _cur_trail_p = L_TRAIL_PCT_L1 if _is_l1_pos else L_TRAIL_PCT_NL

        trail_active = peak >= _cur_trail_a - 5e-5   # 容差：避免浮点精度导致临界值误判
        trail_line   = entry * (1 + peak * (1 - _cur_trail_p)) if trail_active else None
        pnl_pct      = gain * float(lg["leverage"])

        reason = None

        # 1. 追踪止盈（利润从峰值回撤超过当前档位的容忍比例）
        if peak >= _cur_trail_a and gain <= peak * (1 - _cur_trail_p):
            reason = "trail_stop"

        # 2. 软止损（当日收盘价从开仓价累计跌 ≥ soft_stop，与回测逻辑一致）
        elif _close <= entry * (1 - lg.get("entry_soft_stop", L_SOFT_STOP)):
            _ess = lg.get("entry_soft_stop", L_SOFT_STOP)
            log.info(
                f"  [多头] 软止损触发 "
                f"(收盘={_close:.0f} ≤ 开仓价×{1-_ess:.0%}={entry*(1-_ess):.0f})"
            )
            reason = "soft_stop"

        # 3. Chandelier Exit 翻空（CE线跌破）
        elif ce_flip_bear:
            _ce_line = row.get('ce_long')
            _ce_s = f"CE线={float(_ce_line):.0f}" if (_ce_line is not None and not pd.isna(_ce_line)) else "CE线=N/A"
            log.info(f"  [多头] CE出场触发 ({_ce_s}  close={_close:.0f})")
            reason = "ce_exit"

        # 4. 连续熊市投票（override_trail_mode 激活时不检查）
        elif not _ovr_mode and int(lg["bear_vote_count"]) >= L_VOTE_W:
            reason = "bear_vote"

        # 5. 最大持仓天数
        elif int(lg["hold_days"]) >= int(lg["max_hold"]):
            reason = "max_hold"

        if reason:
            slog.info(
                f"[多头] 平仓 → {reason}  "
                f"entry=${entry:,.0f}  now=${current_price:,.0f}  "
                f"BTC {gain:+.2%}  杠杆盈亏 {pnl_pct:+.2%}  "
                f"持有 {lg['hold_days']} 天"
            )
            # 先撤两个止损单，再市价平仓
            cancel_stop_order(lg.get("stop_order_id"),        is_long=True)
            cancel_stop_order(lg.get("profit_lock_stop_id"),  is_long=True)
            log_trade("CLOSE", "LONG", current_price,
                      lg["quantity"], lg["leverage"],
                      signal=lg["signal"], pnl_pct=pnl_pct, reason=reason)
            close_long(float(lg["quantity"]))
            _report = {
                'was_active': True, 'action': 'close', 'close_reason': reason,
                'gain': gain, 'pnl_pct': pnl_pct, 'peak': peak,
                'trail_active': trail_active, 'trail_line': trail_line,
                'bear_vote_count': int(lg["bear_vote_count"]),
                'hold_days': int(lg["hold_days"]), 'max_hold': int(lg["max_hold"]),
                'signal': lg["signal"], 'leverage': float(lg["leverage"]),
                'entry_price': entry,
                'g2_ok': g2_ok, 's2_ok': s2_ok,
            }
            lg.update({
                "active": False, "signal": None, "leverage": 0.0, "max_hold": 0,
                "entry_price": None, "entry_date": None,
                "hold_days": 0, "peak_gain": 0.0, "bear_vote_count": 0,
                "override_trail_mode": False,
                "quantity": 0.0, "pos_usdt": 0.0, "stop_order_id": None,
                "profit_lock_stop_id": None, "profit_lock_stop_price": 0.0,
            })
            return _report

        # 每日更新利润锁定止损单（双保险追踪止盈，用当前档位的回撤容忍）
        update_profit_lock_stop(lg, float(lg["quantity"]), entry, peak,
                                _cur_trail_p, is_long=True)

        _ce_long_v = row.get('ce_long')
        _ce_long_s = f"{float(_ce_long_v):.0f}" if (_ce_long_v is not None and not pd.isna(_ce_long_v)) else "N/A"
        _gs_sp = row.get('_gs_spread', float('nan'))
        _gs_sp_s = f"{float(_gs_sp):.3f}" if not pd.isna(_gs_sp) else "N/A"
        log.info(
            f"  [多头] 持仓 {lg['signal']} {lg['leverage']}x  "
            f"entry={entry:.0f}  now={current_price:.0f}  "
            f"gain={gain:+.2%}  peak={peak:+.2%}  "
            f"hold={lg['hold_days']}d  bear_votes={lg['bear_vote_count']}  "
            f"G2={'✓' if g2_ok else '✗'}(展宽{_gs_sp_s})  "
            f"CE线={_ce_long_s}({'✓' if _curr_ce else '⚠跌破'})  "
            f"stopId={lg.get('stop_order_id') or 'N/A'}  "
            f"lockId={lg.get('profit_lock_stop_id') or 'N/A'}"
        )
        _ess = float(lg.get("entry_soft_stop", L_SOFT_STOP))
        return {
            'was_active': True, 'action': 'hold', 'close_reason': None,
            'gain': gain, 'pnl_pct': pnl_pct, 'peak': peak,
            'trail_active': trail_active, 'trail_line': trail_line,
            'trail_a': _cur_trail_a, 'trail_p': _cur_trail_p,
            'bear_vote_count': int(lg["bear_vote_count"]),
            'hold_days': int(lg["hold_days"]), 'max_hold': int(lg["max_hold"]),
            'signal': lg["signal"], 'leverage': float(lg["leverage"]),
            'entry_price': entry,
            'stop_price': entry * (1 - lg.get("entry_stop", L_STOP)),
            'soft_stop_line': entry * (1 - _ess),
            'yesterday_close': _close,
            'soft_stop_ok': _close > entry * (1 - _ess),
            'ce_flip_bear': ce_flip_bear,
            'is_l1': _is_l1_pos,
            'ovr_mode': _ovr_mode,
            'stop_order_id': lg.get("stop_order_id"),
            'profit_lock_stop_id': lg.get("profit_lock_stop_id"),
            'profit_lock_stop_price': float(lg.get("profit_lock_stop_price") or 0),
            'g2_ok': g2_ok, 's2_ok': s2_ok,
        }

    # ── 无仓位：检查入场 ─────────────────────────────────────────────
    else:
        # 互斥检查：空头持仓中禁止开多（保证金安全）
        if state["short_leg"]["active"]:
            log.info("  [多头] 互斥：空头腿持仓中，跳过多头入场")
            return {
                'was_active': False, 'action': 'none', 'skip_reason': 'mutex',
                'g2_ok': g2_ok, 's2_ok': s2_ok,
            }

        # override_day 拦截：k14=12 OR k15=7，当天绝对不允许新开多头
        if is_override_day(row):
            log.info(
                f"  [多头] Override拦截（k14={_k(row,'k14')} k15={_k(row,'k15')}），"
                f"当日禁止开多"
            )
            return {
                'was_active': False, 'action': 'none', 'skip_reason': 'override_day',
                'g2_ok': g2_ok, 's2_ok': s2_ok,
            }

        if not g2_ok:
            _gs_m  = row.get('_gs_mean', float('nan'))
            _gl_m  = row.get('_gl_mean', float('nan'))
            _gs_sp = row.get('_gs_spread', float('nan'))
            log.info(
                f"  [多头] G2方向未通过  "
                f"短组均={float(_gs_m):.0f}  长组均={float(_gl_m):.0f}  "
                f"展宽={float(_gs_sp):.3f}(多头仅需方向: 短组>长组)"
            )
            return {
                'was_active': False, 'action': 'none',
                'g2_ok': g2_ok, 's2_ok': s2_ok,
            }

        if not s2_ok:
            log.info(f"  [多头] S2投票未通过（近3天 bull_score≥{S2_THRESH} 不足2天）")
            return {
                'was_active': False, 'action': 'none',
                'g2_ok': g2_ok, 's2_ok': s2_ok,
            }

        # G2 + S2 均通过 → 开多（L1日3.5x；非L1日按 bull_score 分 bs=4/5/6 → 5x）
        _entry_is_l1 = is_L1(row)
        if _entry_is_l1:
            sig, lev, mh = 'L1_3.5x', L_LEV_L1, L_MAX
            _entry_stop      = L_STOP_L1
            _entry_soft_stop = L_SOFT_STOP_L1
        else:
            # 根据开仓当天 bull_score 确定 bs 组
            _bsi_raw = float(bs) if not np.isnan(bs) else 6.0
            _bsi     = max(4, min(6, int(round(_bsi_raw))))
            lev, mh  = L_LEV_NL, L_MAX
            if _bsi == 4:
                sig              = f'NL4_{L_LEV_NL:.1f}x'
                _entry_stop      = L_STOP_NL4
                _entry_soft_stop = L_SOFT_STOP_NL4
            else:
                sig              = f'NL{_bsi}_{L_LEV_NL:.1f}x'
                _entry_stop      = L_STOP_NL56
                _entry_soft_stop = L_SOFT_STOP_NL56

        # 保证金安全检查（在实际开仓前再确认一次）
        ms = check_margin_safety()
        if not ms['safe']:
            log.warning(f"  ⚠️ 保证金率危险 {ms['margin_ratio']:.1%}，跳过多头开仓")
            return {
                'was_active': False, 'action': 'none',
                'g2_ok': g2_ok, 's2_ok': s2_ok,
                'signal': sig, 'error': 'margin_unsafe',
            }
        balance = ms['available']
        if balance < MIN_OPEN_BALANCE:
            log.warning(f"  ⚠️ 可用余额不足 ({balance:.2f} USDT < {MIN_OPEN_BALANCE})，跳过多头开仓")
            return {
                'was_active': False, 'action': 'none',
                'g2_ok': g2_ok, 's2_ok': s2_ok,
                'signal': sig, 'error': 'balance_insufficient',
            }

        # 开仓
        qty = open_long(balance, lev, current_price)
        if qty <= 0:
            return {
                'was_active': False, 'action': 'none',
                'g2_ok': g2_ok, 's2_ok': s2_ok,
            }

        # 同时挂止损单（固定止损价，不随价格移动；使用各类型专属硬止损）
        stop_price = math.floor(current_price * (1 - _entry_stop) * 10) / 10
        stop_id    = place_stop_order(qty, stop_price, is_long=True)

        log_trade("OPEN", "LONG", current_price, qty, lev, signal=sig)
        slog.info(
            f"[多头] 开仓 {sig} {lev}x  qty={qty} BTC @ ${current_price:,.0f}  "
            f"余额={balance:.0f} USDT  max_hold={mh}d  "
            f"止损单=${stop_price:.0f}  stopId={stop_id or '挂单失败'}"
        )
        lg.update({
            "active": True, "signal": sig, "leverage": lev, "max_hold": mh,
            "entry_price": current_price, "entry_date": str(row_date),
            "hold_days": 0, "peak_gain": 0.0, "bear_vote_count": 0,
            "is_l1": _entry_is_l1, "override_trail_mode": False,
            "quantity": qty, "pos_usdt": balance, "stop_order_id": stop_id,
            "profit_lock_stop_id": None, "profit_lock_stop_price": 0.0,
            "entry_stop": _entry_stop, "entry_soft_stop": _entry_soft_stop,
        })
        return {
            'was_active': False, 'action': 'open',
            'signal': sig, 'leverage': lev, 'max_hold': mh,
            'entry_price': current_price, 'qty': qty, 'balance_used': balance,
            'stop_price': stop_price, 'stop_id': stop_id,
            'g2_ok': g2_ok, 's2_ok': s2_ok,
        }


# ══════════════════════════════════════════════════════════════════════
# ⑩ 空头腿（每日执行）
# ══════════════════════════════════════════════════════════════════════

def handle_short_leg(state: dict, df: pd.DataFrame, row: pd.Series,
                     latest_idx: int, current_price: float, today: date) -> dict:
    """
    持仓中 → 更新 peak_gain / bull_vote_count / hold_days，检查软性平仓条件
    无仓位 → 检查 GMMA G2空头环境 + A/C 信号，满足则开仓并同时挂止损单
    返回 report dict 供 daily_run 汇总报告使用
    """
    sg  = state["short_leg"]
    bs  = row.get('bull_score')
    bs  = float(bs) if not pd.isna(bs) else np.nan
    row_date = (row["date"].date()
                if hasattr(row["date"], "date")
                else pd.Timestamp(row["date"]).date())

    # 预计算入场条件（无论持仓与否，供报告使用）
    _close     = float(row['close'])
    g2_bear_ok = bool(row.get('g2_bear', False))   # GMMA G2 宽松空头过滤
    sig_info   = get_short_signal(row)
    if sig_info is None:
        sig_info = check_combo_bear_signal(row, today)
    _row_is_override = is_override_day(row)   # k14=12 OR k15=7

    # ── 持仓中：检查软性平仓条件 ────────────────────────────────────
    if sg["active"] and sg["entry_price"] is not None:
        entry = float(sg["entry_price"])

        # 空头用昨日最低价更新峰值（与回测一致：peak = max 日低跌幅）
        # 触发判断用 current_price（≈ 今日开盘价）
        yesterday_low = float(row['low'])
        low_gain = (entry - yesterday_low) / entry
        sg["peak_gain"] = max(float(sg["peak_gain"]), low_gain)
        peak = float(sg["peak_gain"])

        gain  = (entry - current_price) / entry   # 仅用于触发检查 / 显示

        # override_day 时 bull_score 视为0（不触发牛市投票，计数器重置）
        _eff_bs = 0.0 if _row_is_override else bs
        if not np.isnan(_eff_bs) and _eff_bs >= S_VOTE_T:
            sg["bull_vote_count"] = int(sg["bull_vote_count"]) + 1
        else:
            sg["bull_vote_count"] = 0

        sg["hold_days"] = int(sg["hold_days"]) + 1

        trail_active = peak >= S_TRAIL_A - 5e-5   # 容差：避免浮点精度导致临界值误判
        trail_line   = entry * (1 - peak * (1 - S_TRAIL_PCT)) if trail_active else None
        pnl_pct      = gain * float(sg["leverage"])

        reason = None

        # 1. 追踪止盈（利润从峰值回撤超过 S_TRAIL_PCT）
        if peak >= S_TRAIL_A and gain <= peak * (1 - S_TRAIL_PCT):
            reason = "trail_stop"

        # 2. 连续牛市投票
        elif int(sg["bull_vote_count"]) >= S_VOTE_W:
            reason = "vote_exit"

        # 3. 最大持仓天数
        elif int(sg["hold_days"]) >= int(sg["max_hold"]):
            reason = "max_hold"

        if reason:
            slog.info(
                f"[空头] 平仓 → {reason}  "
                f"entry=${entry:,.0f}  now=${current_price:,.0f}  "
                f"BTC {gain:+.2%}  杠杆盈亏 {pnl_pct:+.2%}  "
                f"持有 {sg['hold_days']} 天"
            )
            # 先撤两个止损单，再市价平仓
            cancel_stop_order(sg.get("stop_order_id"),        is_long=False)
            cancel_stop_order(sg.get("profit_lock_stop_id"),  is_long=False)
            log_trade("CLOSE", "SHORT", current_price,
                      sg["quantity"], sg["leverage"],
                      signal=sg["signal"], pnl_pct=pnl_pct, reason=reason)
            close_short(float(sg["quantity"]))
            _report = {
                'was_active': True, 'action': 'close', 'close_reason': reason,
                'gain': gain, 'pnl_pct': pnl_pct, 'peak': peak,
                'trail_active': trail_active, 'trail_line': trail_line,
                'bull_vote_count': int(sg["bull_vote_count"]),
                'hold_days': int(sg["hold_days"]), 'max_hold': int(sg["max_hold"]),
                'signal': sg["signal"], 'leverage': float(sg["leverage"]),
                'entry_price': entry,
                'g2_bear_ok': g2_bear_ok, 'sig_info': sig_info,
            }
            sg.update({
                "active": False, "signal": None, "leverage": 0.0, "max_hold": 0,
                "entry_price": None, "entry_date": None,
                "hold_days": 0, "peak_gain": 0.0, "bull_vote_count": 0,
                "quantity": 0.0, "pos_usdt": 0.0, "stop_order_id": None,
                "profit_lock_stop_id": None, "profit_lock_stop_price": 0.0,
            })
            return _report

        # 每日更新利润锁定止损单（双保险追踪止盈）
        update_profit_lock_stop(sg, float(sg["quantity"]), entry, peak,
                                S_TRAIL_PCT, is_long=False)

        _gs_sp = row.get('_gs_spread', float('nan'))
        _gs_sp_s = f"{float(_gs_sp):.3f}" if not pd.isna(_gs_sp) else "N/A"
        log.info(
            f"  [空头] 持仓 {sg['signal']} {sg['leverage']}x  "
            f"entry={entry:.0f}  now={current_price:.0f}  "
            f"gain={gain:+.2%}  peak={peak:+.2%}  "
            f"hold={sg['hold_days']}d  bull_votes={sg['bull_vote_count']}  "
            f"G2空头={'✓' if g2_bear_ok else '✗'}(展宽{_gs_sp_s})  "
            f"stopId={sg.get('stop_order_id') or 'N/A'}  "
            f"lockId={sg.get('profit_lock_stop_id') or 'N/A'}"
        )
        return {
            'was_active': True, 'action': 'hold', 'close_reason': None,
            'gain': gain, 'pnl_pct': pnl_pct, 'peak': peak,
            'trail_active': trail_active, 'trail_line': trail_line,
            'bull_vote_count': int(sg["bull_vote_count"]),
            'hold_days': int(sg["hold_days"]), 'max_hold': int(sg["max_hold"]),
            'signal': sg["signal"], 'leverage': float(sg["leverage"]),
            'entry_price': entry,
            'stop_price': entry * (1 + sg.get("entry_stop", S_STOP)),
            'stop_order_id': sg.get("stop_order_id"),
            'profit_lock_stop_id': sg.get("profit_lock_stop_id"),
            'profit_lock_stop_price': float(sg.get("profit_lock_stop_price") or 0),
            'g2_bear_ok': g2_bear_ok, 'sig_info': sig_info,
        }

    # ── 无仓位：检查入场 ─────────────────────────────────────────────
    else:
        # 互斥检查：多头持仓中禁止开空（保证金安全）
        if state["long_leg"]["active"]:
            log.info("  [空头] 互斥：多头腿持仓中，跳过空头入场")
            return {
                'was_active': False, 'action': 'none', 'skip_reason': 'mutex',
                'g2_bear_ok': g2_bear_ok, 'sig_info': sig_info,
            }

        if sig_info:
            # ── 按信号类型选择对应的 GMMA 展宽阈值 ───────────────────────
            _SIG_G2_COL = {
                'A':        'g2_bear_A',
                'dual_hit': 'g2_bear_dual',
                'L2s':      'g2_bear_L2s',
                'CB':       'g2_bear_CB',
            }
            _SIG_SP_REQ = {
                'A':        GMMA_SP_A,
                'dual_hit': GMMA_SP_DUAL,
                'L2s':      GMMA_SP_L2S,
                'CB':       GMMA_SP_CB,
            }
            sig_name_tmp = sig_info[0]
            _g2_col  = _SIG_G2_COL.get(sig_name_tmp, 'g2_bear_A')
            _sp_req  = _SIG_SP_REQ.get(sig_name_tmp, GMMA_SP_A)
            _g2_this = bool(row.get(_g2_col, False))

            if not _g2_this:
                _gs_m  = row.get('_gs_mean', float('nan'))
                _gl_m  = row.get('_gl_mean', float('nan'))
                _gs_sp = row.get('_gs_spread', float('nan'))
                log.info(
                    f"  [空头] {sig_name_tmp} G2过滤未通过  "
                    f"短组均={float(_gs_m):.0f}  长组均={float(_gl_m):.0f}  "
                    f"展宽={float(_gs_sp):.3f}(需≥{_sp_req})"
                )
                return {
                    'was_active': False, 'action': 'none',
                    'g2_bear_ok': g2_bear_ok, 'sig_info': sig_info,
                }

        if sig_info:
            sig_name, mh, lev = sig_info
            # 保证金安全检查（在实际开仓前再确认一次）
            ms = check_margin_safety()
            if not ms['safe']:
                log.warning(f"  ⚠️ 保证金率危险 {ms['margin_ratio']:.1%}，跳过空头开仓")
                return {
                    'was_active': False, 'action': 'none',
                    'g2_bear_ok': g2_bear_ok, 'sig_info': sig_info,
                    'error': 'margin_unsafe',
                }
            balance = ms['available']
            if balance < MIN_OPEN_BALANCE:
                log.warning(f"  ⚠️ 可用余额不足 ({balance:.2f} USDT < {MIN_OPEN_BALANCE})，跳过空头开仓")
                return {
                    'was_active': False, 'action': 'none',
                    'g2_bear_ok': g2_bear_ok, 'sig_info': sig_info,
                    'error': 'balance_insufficient',
                }

            # 开仓
            qty = open_short(balance, lev, current_price)
            if qty <= 0:
                return {
                    'was_active': False, 'action': 'none',
                    'g2_bear_ok': g2_bear_ok, 'sig_info': sig_info,
                }

            # 同时挂止损单（按信号类型选取止损幅度）
            _s_stop_this = S_STOP_BY_SIG.get(sig_name, S_STOP)
            stop_price   = math.ceil(current_price * (1 + _s_stop_this) * 10) / 10
            stop_id      = place_stop_order(qty, stop_price, is_long=False)

            log_trade("OPEN", "SHORT", current_price, qty, lev, signal=sig_name)
            slog.info(
                f"[空头] 开仓 {sig_name} {lev}x  qty={qty} BTC @ ${current_price:,.0f}  "
                f"余额={balance:.0f} USDT  max_hold={mh}d  "
                f"止损单=${stop_price:.0f}(止损{_s_stop_this:.0%})  stopId={stop_id or '挂单失败'}"
            )
            sg.update({
                "active": True, "signal": sig_name, "leverage": lev, "max_hold": mh,
                "entry_price": current_price, "entry_date": str(row_date),
                "hold_days": 0, "peak_gain": 0.0, "bull_vote_count": 0,
                "quantity": qty, "pos_usdt": balance, "stop_order_id": stop_id,
                "profit_lock_stop_id": None, "profit_lock_stop_price": 0.0,
                "entry_stop": _s_stop_this,
            })
            return {
                'was_active': False, 'action': 'open',
                'signal': sig_name, 'leverage': lev, 'max_hold': mh,
                'entry_price': current_price, 'qty': qty, 'balance_used': balance,
                'stop_price': stop_price, 'stop_id': stop_id,
                'g2_bear_ok': g2_bear_ok, 'sig_info': sig_info,
            }
        else:
            log.info("  [空头] 无 A/C 信号")
            return {
                'was_active': False, 'action': 'none',
                'g2_bear_ok': g2_bear_ok, 'sig_info': sig_info,
            }


# ══════════════════════════════════════════════════════════════════════
# ⑪ 每日主执行函数
# ══════════════════════════════════════════════════════════════════════

def daily_run():
    utc_now = datetime.now(ZoneInfo("UTC"))
    today   = datetime.now(ZoneInfo("Europe/Berlin")).date()

    # ── 1. 更新数据 ──────────────────────────────────────────────────
    log.info(f"{'='*68}")
    log.info(f"  每日执行  {utc_now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    log.info(f"{'='*68}")
    log.info("更新 BTC 数据...")
    ok = update_data_pipeline()
    if not ok:
        log.error("数据更新失败，本次跳过")
        return

    # ── 1b. 按需更新滚动投票地图 ─────────────────────────────────────
    if _maybe_update_map is not None:
        try:
            _updated, _n_added = _maybe_update_map(MAP_PATH, BTC_FILE, log)
            if _updated:
                # 使缓存失效，下一步 load_df() 会重新加载新地图
                global _rolling_maps
                _rolling_maps = None
                log.info(f"  滚动投票地图已更新（新增 {_n_added} 窗口），缓存已重置")
        except Exception as e:
            log.warning(f"  地图更新失败（不影响交易）: {e}")
    else:
        log.debug("  update_map 模块未找到，跳过地图自动更新")

    # ── 2. 加载数据与计算指标 ────────────────────────────────────────
    try:
        df  = load_df()
        row, latest_idx = get_latest_row_idx(df)
        row_date = (row["date"].date()
                    if hasattr(row["date"], "date")
                    else pd.Timestamp(row["date"]).date())
        close = float(row['close'])
        bs    = row.get('bull_score', float('nan'))
        bs_v  = int(bs) if not pd.isna(bs) else None
        start     = max(0, latest_idx - 2)
        recent_bs = [int(x) for x in df.iloc[start:latest_idx+1]['bull_score'].dropna().tolist()]
        # GMMA G2 状态
        g2_bull_v  = bool(row.get('g2_bull', False))
        g2_bear_v  = bool(row.get('g2_bear', False))
        gs_mean_v  = row.get('_gs_mean', float('nan'))
        gl_mean_v  = row.get('_gl_mean', float('nan'))
        gs_spread_v = row.get('_gs_spread', float('nan'))
        # CE 状态
        ce_long_v = row.get('ce_long')
        ce_bull_v = bool(row.get('ce_bull', True)) if not pd.isna(row.get('ce_bull', float('nan'))) else True
        _prev_ce_b = bool(df.iloc[latest_idx-1].get('ce_bull', True)) if latest_idx >= 1 else True
        ce_flip_today = (not ce_bull_v) and _prev_ce_b   # 今日刚翻空
    except Exception as e:
        log.error(f"数据加载失败: {e}", exc_info=True)
        return

    # ── 3-6. 加文件锁：读状态 → 执行 → 保存（防止与追踪止盈守护程序并发写）──
    with _state_lock:
        # ── 3. 查询 Binance 状态 ─────────────────────────────────────
        try:
            current_price = get_current_price()
            state = load_state()
            pos_snapshot  = sync_state_with_binance(state, current_price)
            margin_info = check_margin_safety()
            balance = margin_info['available']
        except Exception as e:
            log.error(f"Binance 查询失败: {e}", exc_info=True)
            return

        # ── 4. 执行多头腿 ──────────────────────────────────────────
        try:
            long_report = handle_long_leg(state, df, row, latest_idx, current_price, today)
        except Exception as e:
            log.error(f"多头腿执行异常: {e}", exc_info=True)
            long_report = {'was_active': False, 'action': 'error'}

        # ── 5. 执行空头腿 ──────────────────────────────────────────
        try:
            short_report = handle_short_leg(state, df, row, latest_idx, current_price, today)
        except Exception as e:
            log.error(f"空头腿执行异常: {e}", exc_info=True)
            short_report = {'was_active': False, 'action': 'error'}

        # ── 6. 保存状态 ────────────────────────────────────────────
        state["last_run_date"] = str(today)
        save_state(state)

    # ── 7. 每日结构化报告 ────────────────────────────────────────────
    ll = state["long_leg"]
    sl = state["short_leg"]

    # 辅助格式化
    def _pct(v): return f"{v:+.2%}"
    def _price(v): return f"${v:,.0f}"

    bs_s       = f"{bs_v}/6" if bs_v is not None else "N/A"

    sep = "  " + "─" * 64

    mr      = margin_info['margin_ratio']
    mr_icon = "🚨" if margin_info['level'] == 'critical' else ("⚠️" if margin_info['level'] == 'warn' else "✅")
    log.info(sep)
    log.info(f"  {'K线日期':<8}: {row_date}          {'收盘价':<6}: {_price(close)}")
    log.info(f"  {'当前价格':<8}: {_price(current_price)}       {'可用余额':<6}: {balance:,.0f} USDT")
    log.info(f"  {'保证金率':<8}: {mr_icon} {mr:.1%}  "
             f"(维持${margin_info['maint_margin']:,.0f} / 净值${margin_info['margin_balance']:,.0f}  "
             f"浮盈${margin_info['unrealized_pnl']:+,.0f})")
    _k_disp = ' '.join(
        f"{_c}={_k(row,_c)}({'✓' if _k(row,_c) is not None and _k(row,_c) in BULL_CLUSTERS[_c] else '✗'})"
        if _k(row, _c) is not None else f"{_c}=?"
        for _c in K_COLS
    )
    log.info(f"  {'bull_score':<8}: {bs_s}  近3日={recent_bs}  {_k_disp}")
    _gs_m_s  = f"{float(gs_mean_v):.0f}"   if not pd.isna(gs_mean_v)  else "N/A"
    _gl_m_s  = f"{float(gl_mean_v):.0f}"   if not pd.isna(gl_mean_v)  else "N/A"
    _gs_sp_s = f"{float(gs_spread_v):.3f}" if not pd.isna(gs_spread_v) else "N/A"
    _g2_bull_s = "✓ 多头" if g2_bull_v else "✗"
    _g2_bear_s = "✓ 空头" if g2_bear_v else "✗"
    log.info(f"  {'GMMA G2':<8}: 短组均={_gs_m_s}  长组均={_gl_m_s}  展宽={_gs_sp_s}  多头:{_g2_bull_s}  空头方向:{_g2_bear_s}")
    _ce_long_disp = f"{float(ce_long_v):.0f}" if (ce_long_v is not None and not pd.isna(ce_long_v)) else "N/A"
    _ce_status    = ("⚠ 今日翻空→触发ce_exit" if ce_flip_today
                     else ("✓ 多头结构" if ce_bull_v else "✗ 跌破CE线"))
    log.info(f"  {'CE线':<8}: {_ce_long_disp:>8}    close {'>' if ce_bull_v else '<'} CE线  {_ce_status}")
    log.info(sep)

    # 多头条件
    if not ll["active"]:
        s2_ok    = check_s2_vote(df, latest_idx)
        _mutex_long = state["short_leg"]["active"]
        _l1_today = is_L1(row)
        _lev_today = L_LEV_L1 if _l1_today else L_LEV_NL
        _ta_today  = L_TRAIL_A_L1 if _l1_today else L_TRAIL_A_NL
        _tp_today  = L_TRAIL_PCT_L1 if _l1_today else L_TRAIL_PCT_NL
        log.info(f"  [多头入场检查]  ({'L1信号日 ' + str(L_LEV_L1) + 'x' if _l1_today else '非L1日 ' + str(L_LEV_NL) + 'x'}，S2投票，追踪{_ta_today:.0%}/{_tp_today:.0%})")
        log.info(f"    互斥检查   : {'✗ 空头持仓中，不开多' if _mutex_long else '✓ 无持仓冲突'}")
        log.info(f"    保证金率   : {mr_icon} {mr:.1%}  {'→ 禁止开仓' if not margin_info['safe'] else '→ 安全'}")
        log.info(f"    GMMA G2   : {'✓' if g2_bull_v else '✗'}  "
                 f"短组均={_gs_m_s} {'>' if g2_bull_v else '<='} 长组均={_gl_m_s}  展宽={_gs_sp_s}(多头仅需方向)")
        log.info(f"    S2投票    : {'✓' if s2_ok else '✗'}  "
                 f"近3日≥{S2_THRESH}得分: {sum(x>=S2_THRESH for x in recent_bs)}/3天 (需≥2天)")
        log.info(f"    结论      : "
                 f"{'→ 开仓条件满足' if (not _mutex_long and margin_info['safe'] and g2_bull_v and s2_ok) else '→ 不开仓'}")
    else:
        ep   = float(ll["entry_price"])
        gain = (current_price - ep) / ep
        peak = float(ll["peak_gain"])
        stop = ep * (1 - ll.get("entry_stop", L_STOP))
        _is_l1_disp   = bool(ll.get("is_l1", False))
        _ta_disp  = L_TRAIL_A_L1   if _is_l1_disp else L_TRAIL_A_NL
        _tp_disp  = L_TRAIL_PCT_L1 if _is_l1_disp else L_TRAIL_PCT_NL
        _act_price   = ep * (1 + _ta_disp)           # 追踪激活对应的开仓后价格
        trail_active = peak >= _ta_disp - 5e-5        # 容差0.005%，覆盖显示舍入误差
        trail_line   = ep * (1 + peak * (1 - _tp_disp)) if trail_active else None
        log.info(f"  [多头持仓]  {ll['signal']} {ll['leverage']}x  "
                 f"qty={ll['quantity']} BTC")
        log.info(f"    开仓价    : {_price(ep)}   持有: {ll['hold_days']}/{ll['max_hold']}天")
        log.info(f"    浮动盈亏  : BTC {_pct(gain)}  →  杠杆 {_pct(gain * ll['leverage'])}")
        if trail_active:
            trail_l_str = f"✅ 已激活  触发线≈{_price(trail_line)}  (峰值高点≈{_price(ep*(1+peak))})"
        else:
            trail_l_str = f"未激活  激活价≈{_price(_act_price)}  当前峰值高点≈{_price(ep*(1+peak))}  差{_ta_disp-peak:.3%}"
        log.info(f"    追踪峰值  : {_pct(peak)}  {trail_l_str}  (回撤>{_tp_disp:.0%}平仓)")
        _lock_id = ll.get('profit_lock_stop_id') or 'N/A'
        _lock_p  = float(ll.get('profit_lock_stop_price') or 0)
        log.info(f"    止损价    : {_price(stop)}  (初始止损单 ID={ll.get('stop_order_id') or 'N/A'})")
        log.info(f"    利润锁定  : {_price(_lock_p) if _lock_p else '未激活'}  "
                 f"(利润锁定单 ID={_lock_id})")
        log.info(f"    熊市投票  : {ll['bear_vote_count']}/{L_VOTE_W}天  (bull_score≤{L_VOTE_T}计1票)")
        log.info(f"    CE线      : {_ce_long_disp}  {'✓ 多头结构' if ce_bull_v else ('⚠ 今日翻空→触发ce_exit' if ce_flip_today else '✗ 跌破CE线（持续）')}")
    log.info(sep)

    # 空头条件（无论持仓与否，都无条件检查 Combo 状态以输出日志）
    _sig_cb_always = check_combo_bear_signal(row, today)   # 触发 COMBO有效/过期 日志

    if not sl["active"]:
        _sig_orig = get_short_signal(row)
        # _sig_cb 已在上方无条件获取，直接复用
        _sig_cb   = _sig_cb_always
        _sig_disp = _sig_orig or _sig_cb
        sig_name  = f"{_sig_disp[0]}({_sig_disp[2]}x)" if _sig_disp else "无"
        _mutex_short   = state["long_leg"]["active"]
        _ovr_today_log = is_override_day(row)
        log.info(f"  [空头入场检查]")
        log.info(f"    互斥检查  : {'✗ 多头持仓中，不开空' if _mutex_short else '✓ 无持仓冲突'}")
        log.info(f"    保证金率  : {mr_icon} {mr:.1%}  {'→ 禁止开仓' if not margin_info['safe'] else '→ 安全'}")
        log.info(f"    GMMA G2  : {'✓' if g2_bear_v else '✗'}  "
                 f"短组均={_gs_m_s} {'<' if g2_bear_v else '>='} 长组均={_gl_m_s}  展宽={_gs_sp_s}"
                 f"(A≥{GMMA_SP_A}/dual≥{GMMA_SP_DUAL}/L2s≥{GMMA_SP_L2S}/CB≥{GMMA_SP_CB})")
        log.info(f"    Override : {'⚠ k14={} k15={} — 今日禁止开多'.format(_k(row,'k14'),_k(row,'k15')) if _ovr_today_log else '✓ 无override'}")
        log.info(f"    Combo Bear: {'✓ CB信号命中' if _sig_cb else '✗ 无CB信号'}")
        log.info(f"    信号A/dual/L2s/CB: {sig_name}  "
                 f"{'→ 开仓条件满足' if (not _mutex_short and margin_info['safe'] and g2_bear_v and _sig_disp) else '→ 不开空'}")
    else:
        ep   = float(sl["entry_price"])
        gain = (ep - current_price) / ep
        peak = float(sl["peak_gain"])
        stop = ep * (1 + sl.get("entry_stop", S_STOP))
        _s_act_price = ep * (1 - S_TRAIL_A)
        trail_active = peak >= S_TRAIL_A - 5e-5
        trail_line   = ep * (1 - peak * (1 - S_TRAIL_PCT)) if trail_active else None
        log.info(f"  [空头持仓]  {sl['signal']} {sl['leverage']}x  "
                 f"qty={sl['quantity']} BTC")
        log.info(f"    开仓价    : {_price(ep)}   持有: {sl['hold_days']}/{sl['max_hold']}天")
        log.info(f"    浮动盈亏  : BTC {_pct(gain)}  →  杠杆 {_pct(gain * sl['leverage'])}")
        if trail_active:
            trail_s_str = f"✅ 已激活  触发线≈{_price(trail_line)}  (峰值低点≈{_price(ep*(1-peak))})"
        else:
            trail_s_str = f"未激活  激活价≈{_price(_s_act_price)}  当前峰值低点≈{_price(ep*(1-peak))}  差{S_TRAIL_A-peak:.3%}"
        log.info(f"    追踪峰值  : {_pct(peak)}  {trail_s_str}  (回撤>{S_TRAIL_PCT:.0%}平仓)")
        _lock_id_s = sl.get('profit_lock_stop_id') or 'N/A'
        _lock_p_s  = float(sl.get('profit_lock_stop_price') or 0)
        log.info(f"    止损价    : {_price(stop)}  (初始止损单 ID={sl.get('stop_order_id') or 'N/A'})")
        log.info(f"    利润锁定  : {_price(_lock_p_s) if _lock_p_s else '未激活'}  "
                 f"(利润锁定单 ID={_lock_id_s})")
        log.info(f"    牛市投票  : {sl['bull_vote_count']}/{S_VOTE_W}天  (bull_score≥{S_VOTE_T}计1票)")
    log.info(sep)

    # ── 每日结构化决策报告（写入 SIGNAL_LOG_FILE，供监控页面展示）──
    _p   = lambda v: f"${v:,.0f}"
    _pc  = lambda v: f"{v:+.2%}"
    _SEP = "═" * 60

    slog.info(_SEP)
    slog.info(f"  每日决策报告  {utc_now.strftime('%Y-%m-%d %H:%M UTC')}  (K线: {row_date})")
    slog.info(_SEP)

    # ── 【市场状态】 ─────────────────────────────────────────────────
    slog.info("【市场状态】")
    slog.info(f"  收盘价  : {_p(close)}     当前价 : {_p(current_price)}     余额 : {balance:,.0f} USDT")
    slog.info(f"  保证金率: {mr_icon} {mr:.1%}  "
              f"(净值 ${margin_info['margin_balance']:,.0f}  "
              f"浮盈 ${margin_info['unrealized_pnl']:+,.0f}  "
              f"{'🚨危险-禁止开仓' if not margin_info['safe'] else ('⚠️偏高' if margin_info['level']=='warn' else '✅安全')})")
    _g2_long_flag = f"✓ 多头环境(短组均{_gs_m_s}>长组均{_gl_m_s} 展宽{_gs_sp_s})" if g2_bull_v else f"✗ 多头过滤未通过"
    _g2_short_flag = f"✓ 空头环境(短组均{_gs_m_s}<长组均{_gl_m_s} 展宽{_gs_sp_s})" if g2_bear_v else f"✗ 空头过滤未通过"
    _ce_flag = ("⚠ 今日翻空 → 触发ce_exit平多" if ce_flip_today
                else ("✓ 多头结构" if ce_bull_v else "✗ 已跌破（若持多则下次运行平仓）"))
    slog.info(f"  GMMA G2 多头: {_g2_long_flag}")
    slog.info(f"  GMMA G2 空头: {_g2_short_flag}")
    slog.info(f"  CE线    : {_ce_long_disp:>8}              {_ce_flag}")
    # ── 实盘持仓快照（直接来自 Binance，与 state 文件无关）──
    if pos_snapshot is not None:
        _ps = pos_snapshot
        _l_amt = _ps['long_amt']; _s_amt = _ps['short_amt']
        if _l_amt > 1e-6:
            _l_pnl_pct = _ps['long_unrealized'] / (_ps['long_entry'] * _l_amt) if _ps['long_entry'] > 0 else 0
            slog.info(
                f"  Binance多头: {_l_amt} BTC  开仓价 {_p(_ps['long_entry'])}  "
                f"浮盈 ${_ps['long_unrealized']:+,.1f}  ({_l_pnl_pct:+.2%})"
            )
        else:
            slog.info(f"  Binance多头: 无持仓")
        if _s_amt > 1e-6:
            _s_pnl_pct = _ps['short_unrealized'] / (_ps['short_entry'] * _s_amt) if _ps['short_entry'] > 0 else 0
            slog.info(
                f"  Binance空头: {_s_amt} BTC  开仓价 {_p(_ps['short_entry'])}  "
                f"浮盈 ${_ps['short_unrealized']:+,.1f}  ({_s_pnl_pct:+.2%})"
            )
        else:
            slog.info(f"  Binance空头: 无持仓")
    else:
        slog.info(f"  Binance持仓: 查询失败")

    # ── 【bull_score 评分】 ──────────────────────────────────────────
    slog.info("【bull_score 评分】")
    k_parts = []
    for _c in K_COLS:
        _kv = _k(row, _c)
        if _kv is not None:
            _bull = _kv in BULL_CLUSTERS[_c]
            k_parts.append(f"{_c}={_kv}({'✓' if _bull else '✗'})")
        else:
            k_parts.append(f"{_c}=?")
    slog.info(f"  {' '.join(k_parts)}")
    _s2_cnt  = sum(x >= S2_THRESH for x in recent_bs)
    _s2_mark = (f"✓ ({_s2_cnt}/3天≥{S2_THRESH}分，S2达标)"
                if _s2_cnt >= 2
                else f"✗ ({_s2_cnt}/3天≥{S2_THRESH}分，S2未达标，需≥2天)")
    slog.info(f"  今日得分 : {bs_s:<6}  近3日 : {recent_bs}  S2投票 {_s2_mark}")

    # ── 【多头策略】 ─────────────────────────────────────────────────
    lr = long_report or {}
    if lr.get('was_active') and lr.get('action') == 'hold':
        slog.info(f"【多头策略】 持仓中 — {lr['signal']} {lr['leverage']}x")
        slog.info(f"  开仓价  : {_p(lr['entry_price'])}    持有 : {lr['hold_days']}/{lr['max_hold']} 天  "
                  f"浮动盈亏: BTC {_pc(lr['gain'])}  →  杠杆 {_pc(lr['pnl_pct'])}")
        if lr.get('ovr_mode'):
            slog.info(f"  ⚠ Override模式 : 追踪止盈已接管，熊市投票已暂停")
        slog.info(f"【平仓规则检查】")
        # ① 硬止损
        _stop_p    = lr.get('stop_price', 0)
        _stop_dist = (current_price - _stop_p) / current_price if _stop_p else 0
        slog.info(f"  ① 硬止损   : {_p(_stop_p)}  距现价 +{_stop_dist:.1%}  ✓ 未触发  "
                  f"(止损单 ID={lr.get('stop_order_id') or 'N/A'})")
        # ② 软止损
        _ss_line = lr.get('soft_stop_line', 0)
        _yc      = lr.get('yesterday_close', 0)
        _ss_ok   = lr.get('soft_stop_ok', True)
        _ss_dist = (_yc - _ss_line) / _yc if (_yc and _ss_line) else 0
        _ss_icon = '✓ 未触发' if _ss_ok else '⚠ 已触发→平仓'
        slog.info(f"  ② 软止损   : 线={_p(_ss_line)}  昨收={_p(_yc)}  "
                  f"高于软止损线 {_ss_dist:.1%}  {_ss_icon}")
        # ③ 追踪止盈
        _slog_ta = lr.get('trail_a', L_TRAIL_A_NL)
        _slog_tp = lr.get('trail_p', L_TRAIL_PCT_NL)
        _to_act  = _slog_ta - lr['peak']
        if lr['trail_active'] or _to_act <= 0:   # _to_act<=0 兜底：浮点临界时也走激活分支
            _tl = lr.get('trail_line') or (lr['entry_price'] * (1 + lr['peak'] * (1 - _slog_tp)))
            _trail_dist = (current_price - _tl) / current_price if _tl else 0
            _t_icon = '⚠ 临近触发' if abs(_trail_dist) < 0.02 else '✓ 未触发'
            slog.info(f"  ③ 追踪止盈 : ✅ 已激活  峰值={_pc(lr['peak'])}  "
                      f"触发线≈{_p(_tl)}  距触发线{_trail_dist:+.1%}  {_t_icon}  (回撤>{_slog_tp:.0%}平仓)")
        else:
            _lk_p = lr.get('profit_lock_stop_price') or 0
            slog.info(f"  ③ 追踪止盈 : 未激活  峰值={_pc(lr['peak'])}  "
                      f"还需涨 {_to_act:.1%} 激活(阈值{_slog_ta:.0%})  "
                      f"利润锁定单: {_p(_lk_p) if _lk_p else '未挂单'}")
        # ④ CE翻空
        _ce_flip_l = lr.get('ce_flip_bear', False)
        if _ce_flip_l:
            _ce_l_icon = '⚠ 今日翻空 → 已触发平仓'
        elif ce_bull_v:
            _ce_l_icon = '✓ 多头结构'
        else:
            _ce_l_icon = '✗ 已跌破CE线（持续，等下次由多转空时触发）'
        slog.info(f"  ④ CE翻空   : {_ce_l_icon}  CE线={_ce_long_disp}")
        # ⑤ 熊市投票
        _bv = lr['bear_vote_count']
        if lr.get('ovr_mode'):
            slog.info(f"  ⑤ 熊市投票 : — Override模式下已暂停")
        else:
            _bv_icon = '⚠ 已触发→平仓' if _bv >= L_VOTE_W else f'✓ {_bv}/{L_VOTE_W}天'
            slog.info(f"  ⑤ 熊市投票 : {_bv_icon}  (bull_score≤{L_VOTE_T}计1票，连续{L_VOTE_W}天平仓)")
        # ⑥ 最大持仓
        _hd = lr['hold_days']; _mh = lr['max_hold']
        _mh_icon = '⚠ 已达上限→平仓' if _hd >= _mh else f'✓ {_hd}/{_mh}天'
        slog.info(f"  ⑥ 最大持仓 : {_mh_icon}")
        slog.info(f"  ▶ 今日操作 : 持仓不动")
    elif lr.get('was_active') and lr.get('action') == 'close':
        _reason_map = {
            'trail_stop':    '追踪止盈触发',
            'soft_stop':     f'软止损（收盘跌破软止损线）',
            'ce_exit':       f'Chandelier Exit翻空（CE线={_ce_long_disp}）',
            'bear_vote':     f'连续{L_VOTE_W}天熊市投票',
            'max_hold':      f'达到最大持仓天数({lr.get("max_hold")}天)',
            'override_stop': 'Override日亏损止损',
            'short_signal':  '空头信号出现（趋势逆转）',
        }
        _reason_s = _reason_map.get(lr['close_reason'], lr['close_reason'])
        slog.info(f"【多头策略】 ← 本日平仓 — {lr['signal']} {lr['leverage']}x")
        slog.info(f"  开仓价  : {_p(lr['entry_price'])}    持仓 : {lr['hold_days']} 天")
        slog.info(f"  平仓盈亏: BTC {_pc(lr['gain'])}  →  杠杆 {_pc(lr['pnl_pct'])}")
        _trail_s = (f"已激活  峰值 {_pc(lr['peak'])}  触发线已穿越"
                    if lr['trail_active']
                    else f"未激活  峰值 {_pc(lr['peak'])}")
        slog.info(f"  追踪止盈: {_trail_s}")
        slog.info(f"  ▶ 今日操作 : 平仓 — 原因：{_reason_s}")
    elif lr.get('action') == 'open':
        slog.info(f"【多头策略】 ← 本日开仓 — {lr['signal']} {lr['leverage']}x")
        slog.info(f"  开仓价  : {_p(lr['entry_price'])}    数量 : {lr['qty']} BTC    "
                  f"最大持仓 : {lr['max_hold']} 天")
        slog.info(f"  止损单  : {_p(lr['stop_price'])}  (ID={lr.get('stop_id') or '挂单失败'})")
        _open_is_l1 = (lr['signal'] == 'L1_4x')
        _open_ta    = L_TRAIL_A_L1   if _open_is_l1 else L_TRAIL_A_NL
        _open_tp    = L_TRAIL_PCT_L1 if _open_is_l1 else L_TRAIL_PCT_NL
        slog.info(f"  条件    : G2过滤 ✓  S2投票 ✓  {'L1信号 ✓' if _open_is_l1 else 'L1信号 ✗(非L1日1x)'}  CE线={_ce_long_disp}({'✓' if ce_bull_v else '✗'})")
        slog.info(f"  追踪参数: 激活{_open_ta:.0%} / 回撤容忍{_open_tp:.0%}")
        slog.info(f"  ▶ 今日操作 : 开仓")
    else:
        slog.info("【多头策略】 观望中")
        slog.info(f"【多头入场条件检查】")
        _ovr_day_l  = is_override_day(row)
        _l1_today_l = is_L1(row)
        _mutex_l    = state["short_leg"]["active"]
        # BS → 开仓分组
        _bsi_raw_l = float(bs_v) if bs_v is not None else float('nan')
        if not np.isnan(_bsi_raw_l):
            _bsi_grp_l = max(4, min(6, int(round(_bsi_raw_l))))
            _grp_str_l = f"NL{_bsi_grp_l}_{L_LEV_NL:.1f}x"
        else:
            _grp_str_l = "N/A (bs缺失)"
        _mutex_s = '✗ 空头持仓中，禁止开多' if _mutex_l else '✓ 无仓位冲突'
        _ovr_s   = (f"⚠ 是 (k14={_k(row,'k14')} k15={_k(row,'k15')}) 禁止开多"
                    if _ovr_day_l else '✓ 否')
        _g2_s    = (f"✓  短组均={_gs_m_s} > 长组均={_gl_m_s}  展宽={_gs_sp_s}(多头仅需方向)"
                    if g2_bull_v
                    else f"✗  短组均={_gs_m_s} ≤ 长组均={_gl_m_s}  展宽={_gs_sp_s}")
        _s2_s    = (f"✓  {_s2_cnt}/3天≥{S2_THRESH}分"
                    if _s2_cnt >= 2
                    else f"✗  {_s2_cnt}/3天≥{S2_THRESH}分 (需≥2天)")
        if _l1_today_l:
            _l1_s = f"✓ L1信号日 → {L_LEV_L1}x  追踪激活{L_TRAIL_A_L1:.0%}/回撤{L_TRAIL_PCT_L1:.0%}"
        else:
            _l1_s = f"✗ 非L1日 → {L_LEV_NL}x  追踪激活{L_TRAIL_A_NL:.0%}/回撤{L_TRAIL_PCT_NL:.0%}  BS分组: {_grp_str_l}"
        slog.info(f"  互斥检查   : {_mutex_s}")
        slog.info(f"  Override日 : {_ovr_s}")
        slog.info(f"  GMMA G2多头: {_g2_s}")
        slog.info(f"  S2投票     : {_s2_s}")
        slog.info(f"  L1信号日   : {_l1_s}")
        _can_open_l = (not _mutex_l) and (not _ovr_day_l) and g2_bull_v and (_s2_cnt >= 2) and margin_info['safe']
        slog.info(f"  ▶ 今日操作 : {'→ 满足开仓条件' if _can_open_l else '→ 不开仓'}")

    # ── 【空头策略】 ─────────────────────────────────────────────────
    sr = short_report or {}
    if sr.get('was_active') and sr.get('action') == 'hold':
        slog.info(f"【空头策略】 持仓中 — {sr['signal']} {sr['leverage']}x")
        slog.info(f"  开仓价  : {_p(sr['entry_price'])}    持有 : {sr['hold_days']}/{sr['max_hold']} 天  "
                  f"浮动盈亏: BTC {_pc(sr['gain'])}  →  杠杆 {_pc(sr['pnl_pct'])}")
        slog.info(f"【平仓规则检查】")
        # ① 硬止损
        _s_stop_p    = sr.get('stop_price', 0)
        _s_stop_dist = (_s_stop_p - current_price) / current_price if _s_stop_p else 0
        slog.info(f"  ① 硬止损   : {_p(_s_stop_p)}  距现价 +{_s_stop_dist:.1%}  ✓ 未触发  "
                  f"(止损单 ID={sr.get('stop_order_id') or 'N/A'})")
        # ② 追踪止盈
        _s_to_act = S_TRAIL_A - sr['peak']
        if sr['trail_active'] or _s_to_act <= 0:
            _stl = sr.get('trail_line') or (sr['entry_price'] * (1 - sr['peak'] * (1 - S_TRAIL_PCT)))
            _s_trail_dist = (_stl - current_price) / current_price if _stl else 0
            _st_icon = '⚠ 临近触发' if abs(_s_trail_dist) < 0.02 else '✓ 未触发'
            slog.info(f"  ② 追踪止盈 : ✅ 已激活  峰值={_pc(sr['peak'])}  "
                      f"触发线≈{_p(_stl)}  距触发线{_s_trail_dist:+.1%}  {_st_icon}  (回撤>{S_TRAIL_PCT:.0%}平仓)")
        else:
            slog.info(f"  ② 追踪止盈 : 未激活  峰值={_pc(sr['peak'])}  "
                      f"还需跌 {_s_to_act:.1%} 激活(阈值{S_TRAIL_A:.0%})")
        # ③ 牛市投票
        _bvc = sr['bull_vote_count']
        _bvc_icon = '⚠ 已触发→平仓' if _bvc >= S_VOTE_W else f'✓ {_bvc}/{S_VOTE_W}天'
        slog.info(f"  ③ 牛市投票 : {_bvc_icon}  (bull_score≥{S_VOTE_T}计1票，连续{S_VOTE_W}天平仓)")
        # ④ 最大持仓
        _s_hd = sr['hold_days']; _s_mh = sr['max_hold']
        _s_mh_icon = '⚠ 已达上限→平仓' if _s_hd >= _s_mh else f'✓ {_s_hd}/{_s_mh}天'
        slog.info(f"  ④ 最大持仓 : {_s_mh_icon}")
        slog.info(f"  ▶ 今日操作 : 持仓不动")
    elif sr.get('was_active') and sr.get('action') == 'close':
        _reason_map = {
            'trail_stop': '追踪止盈触发',
            'vote_exit':  f'连续{S_VOTE_W}天牛市投票',
            'max_hold':   f'达到最大持仓天数({sr.get("max_hold")}天)',
        }
        _reason_s = _reason_map.get(sr['close_reason'], sr['close_reason'])
        slog.info(f"【空头策略】 ← 本日平仓 — {sr['signal']} {sr['leverage']}x")
        slog.info(f"  开仓价  : {_p(sr['entry_price'])}    持仓 : {sr['hold_days']} 天")
        slog.info(f"  平仓盈亏: BTC {_pc(sr['gain'])}  →  杠杆 {_pc(sr['pnl_pct'])}")
        _trail_s = (f"已激活  峰值 {_pc(sr['peak'])}  触发线已穿越"
                    if sr['trail_active']
                    else f"未激活  峰值 {_pc(sr['peak'])}")
        slog.info(f"  追踪止盈: {_trail_s}")
        slog.info(f"  ▶ 今日操作 : 平仓 — 原因：{_reason_s}")
    elif sr.get('action') == 'open':
        slog.info(f"【空头策略】 ← 本日开仓 — {sr['signal']} {sr['leverage']}x")
        slog.info(f"  开仓价  : {_p(sr['entry_price'])}    数量 : {sr['qty']} BTC    "
                  f"最大持仓 : {sr['max_hold']} 天")
        slog.info(f"  止损单  : {_p(sr['stop_price'])}  (ID={sr.get('stop_id') or '挂单失败'})")
        slog.info(f"  条件    : G2过滤 ✓  {sr['signal']} 信号 ✓")
        slog.info(f"  ▶ 今日操作 : 开仓")
    else:
        slog.info("【空头策略】 观望中")
        slog.info(f"【空头入场条件检查】")
        _mutex_s_l   = state["long_leg"]["active"]
        _ovr_day_s   = is_override_day(row)
        _sig_orig_s  = get_short_signal(row)
        _sig_cb_s    = _sig_cb_always
        _sig_any_s   = _sig_orig_s or _sig_cb_s
        _mutex_ss    = '✗ 多头持仓中，禁止开空' if _mutex_s_l else '✓ 无仓位冲突'
        _ovr_ss      = (f"⚠ 是 (k14={_k(row,'k14')} k15={_k(row,'k15')})"
                        if _ovr_day_s else '✓ 否')
        _g2_ss       = (f"✓  短组均={_gs_m_s} < 长组均={_gl_m_s}  展宽={_gs_sp_s}"
                        if g2_bear_v
                        else f"✗  短组均={_gs_m_s} ≥ 长组均={_gl_m_s}  展宽={_gs_sp_s}")
        _orig_ss     = (f"✓ {_sig_orig_s[0]}({_sig_orig_s[2]}x)" if _sig_orig_s else '✗ 无')
        _cb_ss       = '✓ CB命中' if _sig_cb_s else '✗ 无CB'
        _any_ss      = (f"✓ 综合: {_sig_any_s[0]}({_sig_any_s[2]}x)"
                        if _sig_any_s else '✗ 无信号')
        slog.info(f"  互斥检查   : {_mutex_ss}")
        slog.info(f"  Override日 : {_ovr_ss}")
        slog.info(f"  GMMA G2空头: {_g2_ss}")
        slog.info(f"  展宽要求   : A≥{GMMA_SP_A} / dual≥{GMMA_SP_DUAL} / L2s≥{GMMA_SP_L2S} / CB≥{GMMA_SP_CB}")
        slog.info(f"  信号检测   : A/dual/L2s={_orig_ss}  Combo Bear={_cb_ss}  → {_any_ss}")
        _can_open_s = (not _mutex_s_l) and g2_bear_v and bool(_sig_any_s) and margin_info['safe']
        slog.info(f"  ▶ 今日操作 : {'→ 满足开仓条件' if _can_open_s else '→ 不开空'}")

    slog.info(_SEP)


# ══════════════════════════════════════════════════════════════════════
# ⑫ 入口
# ══════════════════════════════════════════════════════════════════════

def main():
    init_futures_settings()
    if len(sys.argv) > 1 and sys.argv[1] == "daemon":
        log.info(f"Daemon 模式启动，每天 {DAILY_RUN_TIME} (UTC) 执行")
        schedule.every().day.at(DAILY_RUN_TIME).do(daily_run)
        while True:
            try:
                schedule.run_pending()
                time.sleep(30)
            except KeyboardInterrupt:
                log.info("收到停止信号，退出")
                break
            except Exception as e:
                log.error(f"调度器异常: {e}", exc_info=True)
                time.sleep(60)
    else:
        log.info("单次执行模式")
        daily_run()


if __name__ == "__main__":
    main()
