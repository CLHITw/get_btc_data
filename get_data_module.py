# get_data_module.py

import pandas as pd
import os
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

BTC_FILE = "btc.xlsx"

def get_data():
    """
    获取最新【已收盘】BTC 日线数据（Binance）+ FGI
    追加写入 btc.xlsx（按 date 去重）
    返回：最新一条数据 dict
    """

    # =============================
    # 1️⃣ 昨天（柏林时间）
    # =============================
    berlin_now = datetime.now(ZoneInfo("Europe/Berlin"))
    yesterday = (berlin_now - timedelta(days=1)).date()
    date_str = yesterday.strftime("%Y-%m-%d")

    # =============================
    # 2️⃣ Binance 日线
    # =============================
    try:
        url = "https://api.binance.com/api/v3/klines"
        params = {
            "symbol": "BTCUSDT",
            "interval": "1d",
            "limit": 3
        }
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        klines = resp.json()
    except Exception as e:
        print(f"❌ Binance 获取失败: {e}")
        return None

    yesterday_kline = None
    for k in klines:
        k_date = datetime.utcfromtimestamp(k[0] / 1000).date()
        if k_date == yesterday:
            yesterday_kline = k
            break

    if yesterday_kline is None:
        print("❌ 未找到昨天的 BTC 日线")
        return None

    # =============================
    # 3️⃣ FGI
    # =============================
    try:
        fgi_url = "https://api.alternative.me/fng/?limit=1"
        resp_fgi = requests.get(fgi_url, timeout=10)
        resp_fgi.raise_for_status()
        fgi_json = resp_fgi.json()
        fgi_value = float(fgi_json["data"][0]["value"])
    except Exception:
        fgi_value = float("nan")

    # =============================
    # 4️⃣ 构造数据
    # =============================
    data = {
        "date": date_str,   # 先用 str，后面统一转
        "open": float(yesterday_kline[1]),
        "high": float(yesterday_kline[2]),
        "low": float(yesterday_kline[3]),
        "close": float(yesterday_kline[4]),
        "volume": float(yesterday_kline[5]),
        "quote_volume": float(yesterday_kline[7]),
        "num_trades": int(yesterday_kline[8]),
        "taker_buy_base": float(yesterday_kline[9]),
        "taker_buy_quote": float(yesterday_kline[10]),
        "fgi": fgi_value,
    }

    df_new = pd.DataFrame([data])

    # =============================
    # 5️⃣ 写回 btc.xlsx（关键修复点）
    # =============================
    if os.path.exists(BTC_FILE):
        df_old = pd.read_excel(BTC_FILE)

        # 🔧 核心修复：统一 date 类型
        df_old["date"] = pd.to_datetime(df_old["date"])
        df_new["date"] = pd.to_datetime(df_new["date"])

        df_all = pd.concat([df_old, df_new], ignore_index=True)
        df_all = df_all.drop_duplicates(subset=["date"], keep="last")
        df_all = df_all.sort_values("date").reset_index(drop=True)
    else:
        df_all = df_new
        df_all["date"] = pd.to_datetime(df_all["date"])

    df_all.to_excel(BTC_FILE, index=False)

    return data
