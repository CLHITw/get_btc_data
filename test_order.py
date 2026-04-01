"""
test_order.py — 测试网下单验证脚本
====================================
不依赖任何策略逻辑，直接验证 Binance USDT-M 合约 API：
  1. 时钟同步
  2. 查询账户余额
  3. 查询当前持仓
  4. 开一个极小的 LONG 多头（约 10 USDT 名义价值）
  5. 查询持仓确认已开仓
  6. 立即平仓
  7. 确认已平仓

运行方式：
  python test_order.py
"""

import sys, os, time, hmac, hashlib, math, requests
from urllib.parse import urlencode

# ── 直接复制 binance_trader.py 里的密钥和 URL ──────────────────────
TESTNET    = True
API_KEY    = "YOUR_TESTNET_API_KEY"     # ← 填写测试网 Key
API_SECRET = "YOUR_TESTNET_API_SECRET"  # ← 填写测试网 Secret
SYMBOL     = "BTCUSDT"
FUTURES_URL = ("https://testnet.binancefuture.com"
               if TESTNET else "https://fapi.binance.com")

# 测试用名义价值（约 10 USDT → 买极少量 BTC）
TEST_NOTIONAL = 1000.0  # USDT

# ── API 工具函数 ────────────────────────────────────────────────────

_time_offset = 0

def sync_time():
    global _time_offset
    r = requests.get(FUTURES_URL + "/fapi/v1/time", timeout=5)
    _time_offset = r.json()["serverTime"] - int(time.time() * 1000)
    print(f"  时钟偏差: {_time_offset} ms")

def ts():
    return int(time.time() * 1000) + _time_offset

def sign(qs: str) -> str:
    return hmac.new(API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()

def headers():
    return {"X-MBX-APIKEY": API_KEY,
            "Content-Type": "application/x-www-form-urlencoded"}

def get(path, params=None, signed=False):
    p = params or {}
    if signed:
        p["timestamp"] = ts()
        qs = urlencode(p)
        p["signature"] = sign(qs)
    r = requests.get(FUTURES_URL + path, params=p,
                     headers={"X-MBX-APIKEY": API_KEY}, timeout=10)
    r.raise_for_status()
    return r.json()

def post(path, params):
    p = dict(params)
    p["timestamp"] = ts()
    qs  = urlencode(p)
    sig = sign(qs)
    body = qs + "&signature=" + sig
    r = requests.post(FUTURES_URL + path, data=body,
                      headers=headers(), timeout=15)
    return r.status_code, r.json()

def check(status, data, label):
    if status == 200:
        print(f"  ✅ {label}: OK")
    else:
        print(f"  ❌ {label} 失败 ({status}): {data}")
    return status == 200

# ── 主测试流程 ──────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("Binance 测试网下单验证")
    print("=" * 60)

    # 1. 同步时钟
    print("\n[1] 同步服务器时钟...")
    sync_time()

    # 2. 查询余额
    print("\n[2] 查询 USDT 余额...")
    try:
        acct = get("/fapi/v2/account", signed=True)
        for a in acct.get("assets", []):
            if a["asset"] == "USDT":
                bal = float(a["availableBalance"])
                print(f"  可用余额: {bal:.2f} USDT")
                break
    except Exception as e:
        print(f"  ❌ 查询失败: {e}")
        return

    # 3. 查询当前价格
    print("\n[3] 查询 BTC 当前价格...")
    try:
        price_data = get("/fapi/v1/ticker/price", {"symbol": SYMBOL})
        price = float(price_data["price"])
        print(f"  当前价格: {price:.2f} USDT")
    except Exception as e:
        print(f"  ❌ 查询失败: {e}")
        return

    # 4. 计算最小下单数量（向下取整到 3 位小数）
    qty = math.floor(TEST_NOTIONAL / price * 1000) / 1000
    if qty <= 0:
        qty = 0.001   # 最小保底
    notional = qty * price
    print(f"\n[4] 计划开多: qty={qty} BTC, 名义价值≈{notional:.1f} USDT")

    # 5. 设置杠杆
    print("\n[5] 设置杠杆 3x...")
    s, d = post("/fapi/v1/leverage", {"symbol": SYMBOL, "leverage": 3})
    if not check(s, d, "设置杠杆"):
        return

    # 6. 开多（LONG）
    print(f"\n[6] 开多单: BUY {qty} BTC (positionSide=LONG, MARKET)...")
    s, d = post("/fapi/v1/order", {
        "symbol":       SYMBOL,
        "side":         "BUY",
        "positionSide": "LONG",
        "type":         "MARKET",
        "quantity":     qty,
    })
    if not check(s, d, "开多"):
        print(f"      返回内容: {d}")
        return
    order_id = d.get("orderId")
    print(f"      订单ID: {order_id}  状态: {d.get('status')}")

    # 7. 等待成交
    time.sleep(1)

    # 8. 查询持仓确认
    print("\n[7] 查询持仓确认开仓成功...")
    try:
        pos = get("/fapi/v2/positionRisk", {"symbol": SYMBOL}, signed=True)
        for p in pos:
            if p.get("positionSide") == "LONG":
                amt   = float(p["positionAmt"])
                entry = float(p["entryPrice"])
                upnl  = float(p["unRealizedProfit"])
                print(f"  LONG 持仓: {amt} BTC  入场价: {entry:.2f}  浮盈: {upnl:.4f} USDT")
                if amt > 0:
                    print("  ✅ 开仓成功！")
                else:
                    print("  ⚠️ 持仓数量为0，可能未成交")
    except Exception as e:
        print(f"  ❌ 查询持仓失败: {e}")

    print("\n" + "=" * 60)
    print("开仓完成，请去 testnet.binancefuture.com 查看持仓")
    print("=" * 60)

    ans = input("\n确认看到持仓了吗？输入 y 立即平仓，其他键跳过: ").strip().lower()
    if ans == "y":
        print(f"\n[平仓] SELL {qty} BTC (positionSide=LONG, MARKET)...")
        s, d = post("/fapi/v1/order", {
            "symbol":       SYMBOL,
            "side":         "SELL",
            "positionSide": "LONG",
            "type":         "MARKET",
            "quantity":     qty,
        })
        if check(s, d, "平多"):
            print(f"  订单ID: {d.get('orderId')}  状态: {d.get('status')}")
            print("  ✅ 测试仓位已平，可以开始跑主程序了")
        else:
            print(f"  返回: {d}")


if __name__ == "__main__":
    main()
