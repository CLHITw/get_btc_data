"""
binance_trader.py — Binance USDT-M 合约自动交易程序
====================================================
策略：带杠杆的多头月度 + 空头动态组合策略

多头腿（月度）：
  - 每月第一个交易日决策，持有至下月第一个交易日
  - BULL: 4票→1x / 5票→1.5x / 6票→2x   (基于 TOTAL_CAPITAL)
  - NEUTRAL: 0.5x
  - boll_width_z > 3.5 → 多头仓位上限 0.5x
  - 月内止损: 带杠杆净亏损 ≤ -15% 时平仓

空头腿（动态）：
  - 连续3天 BEAR + signal_confirm → 开空
  - bear_n=6 → 2x / 其他 → 1x
  - 止损: 带杠杆净亏损 ≤ -8% 时平仓
  - 平仓: 连续3天 BULL 信号翻转

双向持仓：多头 LONG 和空头 SHORT 同时独立运行

用法：
  python binance_trader.py           # 立即执行一次（测试/手动）
  python binance_trader.py daemon    # 后台常驻，每天 01:15 自动执行

部署（Linux）：
  nohup python binance_trader.py daemon > /root/btc/trader.log 2>&1 &
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

from urllib.parse import urlencode
from datetime import datetime, date
from zoneinfo import ZoneInfo

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ══════════════════════════════════════════════════════════════════════
# ① 配置区（上线前务必修改）
# ══════════════════════════════════════════════════════════════════════

# ┌─────────────────────────────────────────────────────────────────┐
# │  TESTNET 开关：True = 测试网  False = 正式实盘                   │
# │  测试网地址：https://testnet.binancefuture.com                  │
# │  测试网账户充值：https://testnet.binancefuture.com/ → 右上角领取  │
# └─────────────────────────────────────────────────────────────────┘
TESTNET = False   # ← 上实盘前改为 False

# --- Binance API 密钥 ---
# 测试网和正式网密钥不通用，分别填写
if TESTNET:
    API_KEY    = "YOUR_TESTNET_API_KEY"     # ← 填写测试网 Key
    API_SECRET = "YOUR_TESTNET_API_SECRET"  # ← 填写测试网 Secret
else:
    API_KEY    = os.environ.get("BINANCE_API_KEY",    "YOUR_API_KEY_HERE")
    API_SECRET = os.environ.get("BINANCE_API_SECRET", "YOUR_API_SECRET_HERE")

# --- 交易标的 ---
SYMBOL      = "BTCUSDT"
BASE_ASSET  = "BTC"
QUOTE_ASSET = "USDT"

# --- 资金与杠杆 ---
# TOTAL_CAPITAL：分配给本策略的 USDT 本金（1x 仓位名义价值基数）
# 测试网账户有 10000 USDT，建议先用小额 100~500 跑通流程
# 正式网：账户余额保留至少 2 × TOTAL_CAPITAL（应对双腿同时满仓）
TOTAL_CAPITAL  = 300.0    # USDT（测试期间用小额；最小名义价值100，0.5x仓位也能满足）

# MAX_LEVERAGE：Binance 合约设置的杠杆倍数
# 策略最大仓位 2.0x，设 3 留安全边际
MAX_LEVERAGE   = 3

# --- 路径配置 ---
DATA_DIR   = os.path.dirname(os.path.abspath(__file__))
BTC_FILE   = os.path.join(DATA_DIR, "btc.xlsx")
MODEL_DIR  = DATA_DIR
STATE_FILE      = os.path.join(DATA_DIR,
                  "trader_state_testnet.json" if TESTNET else "trader_state.json")
LOG_FILE        = os.path.join(DATA_DIR,
                  "trader_testnet.log"        if TESTNET else "trader.log")
SIGNAL_LOG_FILE = os.path.join(DATA_DIR,
                  "trader_signal_testnet.log" if TESTNET else "trader_signal.log")

# --- 策略参数（与回测保持一致，不要修改）---
LONG_LEV_MAP        = {4: 1.0, 5: 1.5, 6: 2.0}
SHORT_LEV_MAP       = {4: 1.0, 5: 1.0, 6: 2.0}
NEUTRAL_POS         = 0.5
LONG_STOP_LOSS      = -0.15
SHORT_STOP_LOSS     = -0.08
BOLL_OVERFLOW       = 3.5
BEAR_CONSECUTIVE    = 3
BULL_EXIT_CONSECUTIVE = 3

# --- Binance USDT-M 合约 REST API ---
FUTURES_URL = ("https://testnet.binancefuture.com"
               if TESTNET else "https://fapi.binance.com")

# --- 执行时间（UTC，服务器本地时间）---
DAILY_RUN_TIME = "00:08"

# ══════════════════════════════════════════════════════════════════════
# ② 日志配置
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

# 决策信号专用 logger（只记录投票/开平仓结果，供网页左栏展示）
slog = logging.getLogger("signal")
slog.setLevel(logging.INFO)
slog.propagate = False
_sh = logging.FileHandler(SIGNAL_LOG_FILE, encoding="utf-8")
_sh.setFormatter(logging.Formatter("%(asctime)s  %(message)s"))
slog.addHandler(_sh)


# ══════════════════════════════════════════════════════════════════════
# ③ 状态持久化
# ══════════════════════════════════════════════════════════════════════

def default_state() -> dict:
    return {
        "long_leg": {
            "active":       False,
            "pos_ratio":    0.0,      # 实际仓位比例（含杠杆）
            "quantity":     0.0,      # BTC 数量
            "entry_price":  None,
            "entry_date":   None,
            "month":        None,     # 格式 "YYYY-MM"
            "vote":         None,
            "bull_n":       0,
        },
        "short_leg": {
            "active":       False,
            "pos_ratio":    0.0,
            "quantity":     0.0,
            "entry_price":  None,
            "entry_date":   None,
            "vote_window":  [],       # 近3天投票，用于空头逻辑
            "bear_n":       0,
        },
        "last_run_date": None,
    }


def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)
            # 补全旧版状态缺失字段
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


# ══════════════════════════════════════════════════════════════════════
# ④ Binance REST API 封装
# ══════════════════════════════════════════════════════════════════════

# 本地时间与服务器时间偏差（毫秒），启动时同步一次
_TIME_OFFSET: int = 0

def sync_server_time():
    """同步本地时钟与 Binance 服务器时间，消除 -1022 签名错误"""
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
    """对已编码的 query string 做 HMAC-SHA256 签名"""
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
        FUTURES_URL + path,
        params=params,
        headers=_headers(),
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def fapi_post(path: str, params: dict, retries: int = 3) -> dict:
    """
    Binance USDT-M 合约 POST 请求，含自动重试。
    签名规则：对 query string（含 timestamp）做 HMAC，
    将完整参数（含 signature）放在 request body（data=）。
    """
    headers = _headers()
    headers["Content-Type"] = "application/x-www-form-urlencoded"
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            p = dict(params)                 # 每次重试都用新 timestamp/signature
            p["timestamp"] = _timestamp()
            qs  = urlencode(p)
            sig = _sign(qs)
            body = qs + "&signature=" + sig
            resp = requests.post(
                FUTURES_URL + path,
                data=body,
                headers=headers,
                timeout=15,
            )
            data = resp.json() if resp.text.strip() else {}
            if resp.status_code != 200:
                raise RuntimeError(f"Binance API 错误 {resp.status_code}: {data}")
            return data
        except RuntimeError:
            raise                            # 业务错误直接抛，不重试
        except Exception as e:
            last_err = e
            if attempt < retries:
                log.warning(f"  POST {path} 第{attempt}次失败({e})，2秒后重试...")
                time.sleep(2)
    raise RuntimeError(f"POST {path} 重试{retries}次均失败: {last_err}")


# ── 账户与持仓 ──────────────────────────────────────────────────────

def get_futures_balance() -> float:
    """获取 USDT 可用余额"""
    data = fapi_get("/fapi/v2/account", signed=True)
    for asset in data.get("assets", []):
        if asset["asset"] == "USDT":
            return float(asset["availableBalance"])
    return 0.0


def get_position_info(symbol: str = SYMBOL) -> dict:
    """
    返回 {
      'LONG':  {'positionAmt': float, 'entryPrice': float, 'unrealizedProfit': float},
      'SHORT': {'positionAmt': float, 'entryPrice': float, 'unrealizedProfit': float},
    }
    """
    data = fapi_get("/fapi/v2/positionRisk", {"symbol": symbol}, signed=True)
    result = {}
    for p in data:
        side = p.get("positionSide", "BOTH")
        result[side] = {
            "positionAmt":       float(p["positionAmt"]),
            "entryPrice":        float(p["entryPrice"]),
            "unrealizedProfit":  float(p["unRealizedProfit"]),
        }
    return result


def get_current_price(symbol: str = SYMBOL) -> float:
    data = fapi_get("/fapi/v1/ticker/price", {"symbol": symbol})
    return float(data["price"])


# ── 初始化合约设置 ──────────────────────────────────────────────────

def init_futures_settings():
    """
    一次性初始化：
      0. 同步服务器时钟（防止 -1022 签名错误）
      1. 启用双向持仓模式（hedge mode）
      2. 设置杠杆倍数
    重复调用安全。
    测试网注意：positionSideDual POST 接口不可用（-5000），
               请在测试网 UI 手动确认双向持仓已开启后再运行。
    """
    log.info("初始化合约设置...")

    # 0. 同步时钟
    sync_server_time()

    # 双向持仓需在币安账户设置中手动开启（账户 → 偏好设置 → 双向持仓）
    # 程序不调用 positionSideDual 接口，避免重复设置报错

    # 设置杠杆
    try:
        res = fapi_post("/fapi/v1/leverage",
                        {"symbol": SYMBOL, "leverage": MAX_LEVERAGE})
        log.info(f"  ✅ 杠杆已设为 {MAX_LEVERAGE}x (maxNotionalValue={res.get('maxNotionalValue')})")
    except Exception as e:
        log.warning(f"  ⚠️ 杠杆设置失败: {e}")


# ── 下单 ────────────────────────────────────────────────────────────

def get_quantity_precision(symbol: str = SYMBOL) -> int:
    """获取数量精度（小数位数）"""
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
    return 3  # BTC 默认3位小数


_QTY_PRECISION = None

def calc_quantity(notional_usdt: float, price: float) -> float:
    global _QTY_PRECISION
    if _QTY_PRECISION is None:
        _QTY_PRECISION = get_quantity_precision()
    qty = notional_usdt / price
    factor = 10 ** _QTY_PRECISION
    return math.floor(qty * factor) / factor   # 向下取整，防止余额不足


def open_long(pos_ratio: float, price: float) -> float:
    """
    开多头仓位。
    pos_ratio: 策略仓位比例（如 1.5 表示 1.5x TOTAL_CAPITAL 名义价值）
    返回实际成交数量
    """
    notional = TOTAL_CAPITAL * pos_ratio
    qty = calc_quantity(notional, price)
    if qty <= 0:
        log.warning("  ⚠️ 计算数量为0，取消开多")
        return 0.0
    log.info(f"  📈 开多: qty={qty} BTC, pos_ratio={pos_ratio}x, notional≈{notional:.0f} USDT")
    res = fapi_post("/fapi/v1/order", {
        "symbol":       SYMBOL,
        "side":         "BUY",
        "positionSide": "LONG",
        "type":         "MARKET",
        "quantity":     qty,
    })
    log.info(f"     订单ID={res.get('orderId')}  状态={res.get('status')}")
    return qty


def close_long(quantity: float):
    """平多头仓位（SELL LONG）"""
    if quantity <= 0:
        return
    log.info(f"  📉 平多: qty={quantity} BTC")
    res = fapi_post("/fapi/v1/order", {
        "symbol":       SYMBOL,
        "side":         "SELL",
        "positionSide": "LONG",
        "type":         "MARKET",
        "quantity":     quantity,
    })
    log.info(f"     订单ID={res.get('orderId')}  状态={res.get('status')}")


def open_short(pos_ratio: float, price: float) -> float:
    """
    开空头仓位。
    pos_ratio: 绝对值，如 1.0 或 2.0
    返回实际成交数量
    """
    notional = TOTAL_CAPITAL * pos_ratio
    qty = calc_quantity(notional, price)
    if qty <= 0:
        log.warning("  ⚠️ 计算数量为0，取消开空")
        return 0.0
    log.info(f"  📉 开空: qty={qty} BTC, pos_ratio={pos_ratio}x, notional≈{notional:.0f} USDT")
    res = fapi_post("/fapi/v1/order", {
        "symbol":       SYMBOL,
        "side":         "SELL",
        "positionSide": "SHORT",
        "type":         "MARKET",
        "quantity":     qty,
    })
    log.info(f"     订单ID={res.get('orderId')}  状态={res.get('status')}")
    return qty


def close_short(quantity: float):
    """平空头仓位（BUY SHORT）"""
    if quantity <= 0:
        return
    log.info(f"  📈 平空: qty={quantity} BTC")
    res = fapi_post("/fapi/v1/order", {
        "symbol":       SYMBOL,
        "side":         "BUY",
        "positionSide": "SHORT",
        "type":         "MARKET",
        "quantity":     quantity,
    })
    log.info(f"     订单ID={res.get('orderId')}  状态={res.get('status')}")


# ══════════════════════════════════════════════════════════════════════
# ⑤ 数据与策略工具
# ══════════════════════════════════════════════════════════════════════

def update_data_pipeline() -> bool:
    """
    调用现有数据流水线：
      get_data → feature_calculator → kmeans_predict → 写回 btc.xlsx
    """
    try:
        sys.path.insert(0, DATA_DIR)
        from data_process import main as dp_main
        result = dp_main()
        if result is None:
            log.error("数据流水线返回 None，可能是网络或数据问题")
            return False
        log.info(f"数据更新完成: {result.get('date', '?')}")
        return True
    except Exception as e:
        log.error(f"数据流水线异常: {e}", exc_info=True)
        return False


def load_strategy_components():
    """加载策略模块并返回 (df, profiles)"""
    sys.path.insert(0, DATA_DIR)
    from regime_strategy import load_and_prepare, profile_regimes, K_COLS
    df = load_and_prepare(BTC_FILE)
    profiles = profile_regimes(df)
    return df, profiles


def get_latest_row(df: pd.DataFrame) -> pd.Series:
    """返回 btc.xlsx 最新一行（K 值有效的最后一行）"""
    from regime_strategy import K_COLS
    valid = df.dropna(subset=K_COLS)
    return valid.iloc[-1]


def compute_vote(row: pd.Series, profiles: dict) -> tuple:
    """
    返回 (vote, signal_ok, bull_n, bear_n)
    vote: 'BULL'/'BEAR'/'NEUTRAL'/'ABSTAIN'
    """
    from regime_strategy import majority_vote, signal_confirm, K_COLS
    vote = majority_vote(row, profiles, min_agree=4)
    sig_ok = signal_confirm(row, vote) if vote in ("BULL", "BEAR", "NEUTRAL") else False

    votes_list = []
    for k in K_COLS:
        kv = row.get(k)
        if pd.isna(kv):
            continue
        match = profiles[k][profiles[k]["regime"] == int(kv)]
        if len(match) == 0:
            continue
        votes_list.append(match.iloc[0]["type"])

    bull_n = votes_list.count("BULL")
    bear_n = votes_list.count("BEAR")
    return vote, sig_ok, bull_n, bear_n


def is_first_trading_day_of_month(data_date: date, df: pd.DataFrame) -> bool:
    """
    数据日期（最新K线日期）是否是该月第一个有K值的交易日。

    注意：程序在 D+1 凌晨运行，抓到的是 D 日数据。
    因此用数据日期（D）而非程序运行日期（D+1）来判断月初，
    这样 4月1日数据在4月2日被抓到时能正确触发4月月初决策。
    """
    from regime_strategy import K_COLS
    valid = df.dropna(subset=K_COLS).copy()
    valid["ym"] = valid["date"].dt.to_period("M")
    data_ym = pd.Period(data_date, freq="M")
    month_rows = valid[valid["ym"] == data_ym]
    if month_rows.empty:
        return False
    first_date = month_rows.iloc[0]["date"].date()
    return data_date == first_date


# ══════════════════════════════════════════════════════════════════════
# ⑥ 多头月度腿
# ══════════════════════════════════════════════════════════════════════

def handle_long_leg(state: dict, row: pd.Series, profiles: dict,
                    today: date, current_price: float):
    """
    多头腿逻辑（每日执行）：
      - 月初：重新决策，平旧仓 + 按新信号建仓
      - 月中：检查止损
    """
    from regime_strategy import apply_overflow_filter, STRATEGY_MAP

    long = state["long_leg"]
    vote, sig_ok, bull_n, bear_n = compute_vote(row, profiles)

    # 用数据日期（最新K线日期）做月份判断，而非程序运行日期
    row_date = row["date"].date() if hasattr(row["date"], "date") else pd.Timestamp(row["date"]).date()
    data_ym  = row_date.strftime("%Y-%m")

    log.info(f"  多头腿: vote={vote} sig={sig_ok} bull_n={bull_n} data_date={row_date} data_ym={data_ym}")

    is_month_start = is_first_trading_day_of_month(row_date, _df_cache)

    # ── 月初重新决策 ──────────────────────────────────────────────
    if is_month_start:
        log.info("  → 月初决策日")

        # 平掉上月仓位
        if long["active"]:
            log.info(f"  → 平上月多头 qty={long['quantity']}")
            close_long(long["quantity"])
            long.update({"active": False, "pos_ratio": 0.0, "quantity": 0.0,
                         "entry_price": None, "entry_date": None})

        # 决策逻辑（与 strategy_evaluation.py 一致）
        if vote in ("BULL", "NEUTRAL", "BEAR"):
            fv = vote if sig_ok else "ABSTAIN"
        else:
            fv = vote

        base_pos = max(STRATEGY_MAP.get(fv, {"position": 0.0})["position"], 0)
        # overflow filter
        bw = row.get("boll_width_z", np.nan)
        if not np.isnan(bw) and bw > BOLL_OVERFLOW and base_pos > 0:
            base_pos = min(base_pos, 0.5)

        if fv == "BULL" and base_pos > 0:
            lev = LONG_LEV_MAP.get(bull_n, 1.0)
            pos_ratio = base_pos * lev
        elif fv == "NEUTRAL" and base_pos > 0:
            pos_ratio = base_pos   # 0.5，无乘数
        else:
            pos_ratio = 0.0

        if pos_ratio > 0:
            log.info(f"  → 开多: fv={fv} bull_n={bull_n} pos_ratio={pos_ratio}x")
            qty = open_long(pos_ratio, current_price)
            long.update({
                "active":      qty > 0,
                "pos_ratio":   pos_ratio,
                "quantity":    qty,
                "entry_price": current_price,
                "entry_date":  str(row_date),
                "month":       data_ym,
                "vote":        fv,
                "bull_n":      bull_n,
            })
        else:
            log.info(f"  → 月初无多头信号 (fv={fv})")
            long.update({"active": False, "pos_ratio": 0.0, "quantity": 0.0,
                         "month": data_ym})

    # ── 月中：检查止损 ────────────────────────────────────────────
    elif long["active"] and long["entry_price"]:
        raw_ret     = current_price / long["entry_price"] - 1
        levered_ret = raw_ret * long["pos_ratio"]
        log.info(f"  → 持仓中: entry={long['entry_price']:.0f}  "
                 f"now={current_price:.0f}  净收益={levered_ret*100:.1f}%")
        if levered_ret <= LONG_STOP_LOSS:
            log.info(f"  ⚠️ 多头止损触发 ({levered_ret*100:.1f}% ≤ {LONG_STOP_LOSS*100:.0f}%)")
            close_long(long["quantity"])
            long.update({"active": False, "pos_ratio": 0.0, "quantity": 0.0,
                         "entry_price": None})
    else:
        log.info("  → 本月无多头仓位，跳过")


# ══════════════════════════════════════════════════════════════════════
# ⑦ 空头动态腿
# ══════════════════════════════════════════════════════════════════════

def handle_short_leg(state: dict, row: pd.Series, profiles: dict,
                     today: date, current_price: float):
    """
    空头腿逻辑（每日执行）：
      - 维护投票窗口
      - 持仓中：检查止损 / BULL 信号翻转
      - 无仓位：连续3天 BEAR → 开空
    """
    short = state["short_leg"]
    vote, sig_ok, bull_n, bear_n = compute_vote(row, profiles)

    # 更新投票窗口（最多保留 BEAR_CONSECUTIVE 天）
    short["vote_window"].append(vote)
    if len(short["vote_window"]) > BEAR_CONSECUTIVE:
        short["vote_window"].pop(0)

    win = short["vote_window"]
    last_n_bear = win[-BEAR_CONSECUTIVE:] if len(win) >= BEAR_CONSECUTIVE else []
    last_n_bull = win[-BULL_EXIT_CONSECUTIVE:] if len(win) >= BULL_EXIT_CONSECUTIVE else []

    log.info(f"  空头腿: vote={vote} sig={sig_ok} bear_n={bear_n} "
             f"window={win}")

    # ── 持仓中逻辑 ────────────────────────────────────────────────
    if short["active"] and short["entry_price"]:
        raw_pnl     = (current_price / short["entry_price"] - 1) * (-1)  # 空头方向
        levered_pnl = raw_pnl * short["pos_ratio"]
        log.info(f"  → 空头持仓: entry={short['entry_price']:.0f}  "
                 f"now={current_price:.0f}  净收益={levered_pnl*100:.1f}%")

        if levered_pnl <= SHORT_STOP_LOSS:
            log.info(f"  ⚠️ 空头止损触发 ({levered_pnl*100:.1f}% ≤ {SHORT_STOP_LOSS*100:.0f}%)")
            close_short(short["quantity"])
            short.update({"active": False, "pos_ratio": 0.0, "quantity": 0.0,
                          "entry_price": None, "entry_date": None,
                          "vote_window": [], "bear_n": 0})
            return

        if (len(last_n_bull) == BULL_EXIT_CONSECUTIVE
                and all(v == "BULL" for v in last_n_bull)):
            log.info("  → 空头信号翻转（连续 BULL），平空")
            close_short(short["quantity"])
            short.update({"active": False, "pos_ratio": 0.0, "quantity": 0.0,
                          "entry_price": None, "entry_date": None,
                          "bear_n": 0})
            return

        log.info("  → 空头持仓，无止损/翻转，维持")

    # ── 无仓位：尝试开空 ──────────────────────────────────────────
    else:
        if (len(last_n_bear) == BEAR_CONSECUTIVE
                and all(v == "BEAR" for v in last_n_bear)
                and sig_ok):
            lev = SHORT_LEV_MAP.get(bear_n, 1.0)
            pos_ratio = 1.0 * lev
            log.info(f"  → 开空信号: bear_n={bear_n} pos_ratio={pos_ratio}x")
            qty = open_short(pos_ratio, current_price)
            short.update({
                "active":      qty > 0,
                "pos_ratio":   pos_ratio,
                "quantity":    qty,
                "entry_price": current_price,
                "entry_date":  str(today),
                "bear_n":      bear_n,
            })
        else:
            log.info("  → 无空头信号")


# ══════════════════════════════════════════════════════════════════════
# ⑧ 持仓与 Binance 状态同步（防止状态文件与实际持仓不一致）
# ══════════════════════════════════════════════════════════════════════

def sync_state_with_binance(state: dict):
    """
    从 Binance 读取真实持仓，修正状态文件中的数量。
    防止因程序崩溃/重启导致数量不一致。
    """
    try:
        pos = get_position_info()
        long_amt  = abs(pos.get("LONG",  {}).get("positionAmt", 0.0))
        short_amt = abs(pos.get("SHORT", {}).get("positionAmt", 0.0))

        long_entry  = pos.get("LONG",  {}).get("entryPrice", 0.0)
        short_entry = pos.get("SHORT", {}).get("entryPrice", 0.0)

        # 同步多头数量
        if state["long_leg"]["active"] and abs(state["long_leg"]["quantity"] - long_amt) > 1e-6:
            log.info(f"  同步多头数量: 状态={state['long_leg']['quantity']} → Binance={long_amt}")
            state["long_leg"]["quantity"] = long_amt

        # 如果 Binance 已无多头但状态仍显示持仓，修正
        if state["long_leg"]["active"] and long_amt < 1e-6:
            log.warning("  Binance 多头已空仓，修正状态")
            state["long_leg"].update({"active": False, "quantity": 0.0,
                                      "entry_price": None})

        # 如果 Binance 有多头但状态为空（异常开仓），记录警告
        if not state["long_leg"]["active"] and long_amt > 1e-6:
            log.warning(f"  发现 Binance 存在未记录多头持仓 {long_amt} BTC，请手动处理")

        # 同步空头
        if state["short_leg"]["active"] and abs(state["short_leg"]["quantity"] - short_amt) > 1e-6:
            log.info(f"  同步空头数量: 状态={state['short_leg']['quantity']} → Binance={short_amt}")
            state["short_leg"]["quantity"] = short_amt

        if state["short_leg"]["active"] and short_amt < 1e-6:
            log.warning("  Binance 空头已空仓，修正状态")
            state["short_leg"].update({"active": False, "quantity": 0.0,
                                       "entry_price": None})

        if not state["short_leg"]["active"] and short_amt > 1e-6:
            log.warning(f"  发现 Binance 存在未记录空头持仓 {short_amt} BTC，请手动处理")

    except Exception as e:
        log.warning(f"  持仓同步失败（忽略）: {e}")


# ══════════════════════════════════════════════════════════════════════
# ⑨ 每日主执行函数
# ══════════════════════════════════════════════════════════════════════

_df_cache = None   # 当日数据缓存，避免重复加载

def daily_run():
    global _df_cache

    utc_now = datetime.now(ZoneInfo("UTC"))
    today   = datetime.now(ZoneInfo("Europe/Berlin")).date()   # 策略日期仍按柏林日历
    log.info("=" * 70)
    log.info(f"开始每日执行  {utc_now.strftime('%Y-%m-%d %H:%M:%S')} (UTC)")
    log.info("=" * 70)

    # 1. 更新数据
    log.info("【1/5】更新 BTC 数据...")
    ok = update_data_pipeline()
    if not ok:
        log.error("数据更新失败，本次跳过")
        return

    # 2. 加载策略模块
    log.info("【2/5】加载策略...")
    try:
        df, profiles = load_strategy_components()
        _df_cache = df
    except Exception as e:
        log.error(f"策略模块加载失败: {e}", exc_info=True)
        return

    # 3. 获取最新数据行
    try:
        row = get_latest_row(df)
        row_date = row["date"].date() if hasattr(row["date"], "date") else row["date"]
        log.info(f"最新K线: {row_date}  close={row['close']:.0f}  "
                 f"macd_hist_z={row.get('macd_hist_z', float('nan')):.2f}  "
                 f"boll_width_z={row.get('boll_width_z', float('nan')):.2f}")
    except Exception as e:
        log.error(f"读取最新行失败: {e}", exc_info=True)
        return

    # 提前计算投票，供信号日志使用
    try:
        _vote, _sig_ok, _bull_n, _bear_n = compute_vote(row, profiles)
        _neut_n = 6 - _bull_n - _bear_n
        _close  = row['close']
    except Exception:
        _vote, _sig_ok, _bull_n, _bear_n, _neut_n, _close = '?', False, 0, 0, 0, 0

    # 4. 获取当前价格与状态
    log.info("【3/5】查询 Binance 持仓与价格...")
    try:
        current_price = get_current_price()
        log.info(f"当前价格: {current_price:.0f} USDT")
        state = load_state()
        sync_state_with_binance(state)
    except Exception as e:
        log.error(f"Binance 查询失败: {e}", exc_info=True)
        return

    # 5. 执行策略
    log.info("【4/5】执行多头腿...")
    try:
        handle_long_leg(state, row, profiles, today, current_price)
    except Exception as e:
        log.error(f"多头腿执行异常: {e}", exc_info=True)

    log.info("【5/5】执行空头腿...")
    try:
        handle_short_leg(state, row, profiles, today, current_price)
    except Exception as e:
        log.error(f"空头腿执行异常: {e}", exc_info=True)

    # 6. 保存状态
    state["last_run_date"] = str(today)
    save_state(state)

    # 7. 写决策信号日志（左栏）
    ll = state["long_leg"]
    sl = state["short_leg"]
    sig_str = "✓" if _sig_ok else "✗"
    slog.info(f"{'─'*50}")
    slog.info(f"数据日期: {row_date}  收盘: ${_close:,.0f}  当前: ${current_price:,.0f}")
    slog.info(f"投票  BULL={_bull_n}  BEAR={_bear_n}  NEUT={_neut_n}  "
              f"→ {_vote}  信号确认:{sig_str}")
    # 多头
    if ll["active"]:
        ep  = ll["entry_price"] or 0
        pnl = (current_price / ep - 1) * ll["pos_ratio"] if ep else 0
        slog.info(f"[多头] 持仓  {ll['pos_ratio']}x  qty={ll['quantity']} BTC"
                  f"  entry=${ep:,.0f}  浮盈={pnl*100:+.1f}%")
    else:
        slog.info("[多头] 无仓位")
    # 空头
    if sl["active"]:
        ep  = sl["entry_price"] or 0
        pnl = (ep / current_price - 1) * sl["pos_ratio"] if ep else 0
        slog.info(f"[空头] 持仓  {sl['pos_ratio']}x  qty={sl['quantity']} BTC"
                  f"  entry=${ep:,.0f}  浮盈={pnl*100:+.1f}%")
    else:
        slog.info(f"[空头] 无仓位  窗口={sl['vote_window']}")

    log.info("=" * 70)


# ══════════════════════════════════════════════════════════════════════
# ⑩ 入口
# ══════════════════════════════════════════════════════════════════════

def main():
    # 首次启动时初始化合约设置
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
