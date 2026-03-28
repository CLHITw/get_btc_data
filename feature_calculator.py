# feature_calculator.py

import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings('ignore')


# ===================== 特征列表 =====================
def get_final_features():
    return [
        'rsi_norm',
        'boll_width_z',
        'macd_z', 'macd_hist_z', 'rel_macd_hist_z',
        'volatility_pct_z',
        'volume_pct', 'volume_log_z', 'num_trades_z',
        'taker_buy_base_ratio', 'taker_buy_quote_ratio',
        'dist_ma7_pct_z', 'dist_ma50_pct_z', 'dist_ma99_pct_z', 'dist_ma200_pct_z',
        'atr_pct_z',
        'fginorm'
    ]


# ===================== 基础指标 =====================
def calculate_rsi(close, period=14):
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def calculate_boll(close, period=20, std_dev=2):
    middle = close.rolling(period).mean()
    std = close.rolling(period).std()
    upper = middle + std * std_dev
    lower = middle - std * std_dev
    width = upper - lower
    return upper, lower, width


def calculate_macd(close, fast=12, slow=26, signal=9):
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    signal_line = macd.ewm(span=signal, adjust=False).mean()
    hist = macd - signal_line
    return macd, signal_line, hist


def calculate_atr(high, low, close, period=14):
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    atr = tr.ewm(span=period, adjust=False).mean()
    atr_pct = atr / close
    return atr, atr_pct


def calculate_ma_differences(close):
    ma7 = close.rolling(7).mean()
    ma50 = close.rolling(50).mean()
    ma99 = close.rolling(99).mean()
    ma200 = close.rolling(200).mean()
    return (close / ma7 - 1), (close / ma50 - 1), (close / ma99 - 1), (close / ma200 - 1)


# ===================== 特征生成 =====================
def generate_btc_features(df):
    """
    计算所有特征
    注意：这个函数会计算整个 DataFrame 的特征，以保证滚动窗口计算正确
    """
    df = df.copy()

    # 波动率百分比
    df['volatility_pct'] = (df['close'].rolling(14).std() / df['close'].rolling(14).mean()) * 100
    df['volatility_pct_z'] = (df['volatility_pct'] - df['volatility_pct'].rolling(30).mean()) / df[
        'volatility_pct'].rolling(30).std()

    # Bollinger
    df['boll_upper'], df['boll_lower'], df['boll_width'] = calculate_boll(df['close'])
    df['boll_width_z'] = df['boll_width'] / df['close'].rolling(30).std()

    # MACD
    df['macd'], df['macd_signal'], df['macd_hist'] = calculate_macd(df['close'])
    df['macd_z'] = (df['macd'] - df['macd'].rolling(30).mean()) / df['macd'].rolling(30).std()
    df['macd_hist_z'] = (df['macd_hist'] - df['macd_hist'].rolling(30).mean()) / df['macd_hist'].rolling(30).std()
    df['rel_macd_hist_z'] = df['macd_hist'] / np.abs(df['macd'].replace(0, np.nan))
    df['rel_macd_hist_z'] = (df['rel_macd_hist_z'] - df['rel_macd_hist_z'].rolling(30).mean()) / df[
        'rel_macd_hist_z'].rolling(30).std()

    # RSI
    df['rsi'] = calculate_rsi(df['close'])
    df['rsi_norm'] = df['rsi'] / 100.0

    # 成交量类
    df['volume_pct'] = df['volume'] / df['volume'].rolling(30).mean()
    df['volume_log_z'] = (np.log(df['volume']) - np.log(df['volume']).rolling(30).mean()) / np.log(
        df['volume']).rolling(30).std()
    df['num_trades_z'] = (df['num_trades'] - df['num_trades'].rolling(30).mean()) / df['num_trades'].rolling(30).std()
    df['taker_buy_base_ratio'] = df['taker_buy_base'] / df['volume']
    df['taker_buy_quote_ratio'] = df['taker_buy_quote'] / df['quote_volume']

    # 均线偏离
    df['dist_ma7_pct'], df['dist_ma50_pct'], df['dist_ma99_pct'], df['dist_ma200_pct'] = calculate_ma_differences(
        df['close'])
    for col in ['dist_ma7_pct', 'dist_ma50_pct', 'dist_ma99_pct', 'dist_ma200_pct']:
        df[col + '_z'] = (df[col] - df[col].rolling(30).mean()) / df[col].rolling(30).std()

    # ATR
    df['atr'], df['atr_pct'] = calculate_atr(df['high'], df['low'], df['close'])
    df['atr_pct_z'] = (df['atr_pct'] - df['atr_pct'].rolling(30).mean()) / df['atr_pct'].rolling(30).std()

    # ===================== FGI - 使用前一天的值 =====================
    # fginorm = 前一天的 fgi / 100
    df['fginorm'] = df['fgi'].shift(1) / 100.0
    # 第一行没有前一天数据，用当天的fgi填充
    df['fginorm'] = df['fginorm'].fillna(df['fgi'] / 100.0)

    return df


# ===================== 对外接口 =====================
def feature_calculator(btc_file="btc.xlsx"):
    """
    读取 btc.xlsx，重新计算所有行的特征，写回文件

    注意：虽然新增的可能只有1行，但为了保证滚动窗口计算正确，
         需要重新计算整个 DataFrame 的特征

    Args:
        btc_file (str): BTC数据文件路径

    Returns:
        pd.DataFrame: 更新后的DataFrame
    """
    print(f"📖 读取文件: {btc_file}")
    df = pd.read_excel(btc_file)

    print(f"   原始数据: {len(df)} 行，{len(df.columns)} 列")

    # 确保日期列是 datetime 类型并排序
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)

    print(f"🔧 计算特征（包括滚动窗口指标）...")
    df_updated = generate_btc_features(df)

    # 保证列顺序：原始列 + 特征列
    original_cols = [
        'date', 'open', 'high', 'low', 'close', 'volume',
        'num_trades', 'taker_buy_base', 'taker_buy_quote',
        'quote_volume', 'fgi'
    ]

    final_cols = get_final_features()

    # 确保所有特征列都存在
    for col in final_cols:
        if col not in df_updated.columns:
            df_updated[col] = pd.NA

    # 按指定顺序排列列
    df_updated = df_updated[original_cols + final_cols]

    # 写回文件
    print(f"💾 保存到: {btc_file}")
    df_updated.to_excel(btc_file, index=False)

    print(f"✅ 特征计算完成: {len(df_updated)} 行，{len(df_updated.columns)} 列")
    print(f"   原始列: {len(original_cols)} 个")
    print(f"   特征列: {len(final_cols)} 个\n")

    return df_updated


if __name__ == "__main__":
    # 测试
    df = feature_calculator("btc.xlsx")
    print(f"\n📊 最新3行数据预览:")
    print(df[['date', 'close', 'fgi', 'fginorm', 'rsi_norm']].tail(3))