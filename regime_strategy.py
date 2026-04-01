"""
BTC Regime Switching 策略
===========================
基于 k10~k15 六维 K-means 标签，每月再平衡，多数投票决策。

运行方式：
    python regime_strategy.py

输出：
    - 控制台：Regime 画像表 + 策略映射表 + 逐月回测结果 + 绩效汇总
    - regime_report.xlsx：完整报告（可选，需 openpyxl）
"""

import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

# ── 路径配置 ─────────────────────────────────────────────────────────
DATA_PATH = 'E:/DESK/window_and_profit/main/btc.xlsx'   # 本地路径
# DATA_PATH = '/root/Desktop/btc/get_data/get_btc_data/btc.xlsx'  # 服务器路径

K_COLS  = ['k10', 'k11', 'k12', 'k13', 'k14', 'k15']
Z_COLS  = ['rsi_norm', 'boll_width_z', 'macd_z', 'macd_hist_z',
           'rel_macd_hist_z', 'volatility_pct_z', 'volume_log_z',
           'dist_ma200_pct_z', 'atr_pct_z', 'fginorm']

# ════════════════════════════════════════════════════════════════════
# 1. 数据加载与预处理
# ════════════════════════════════════════════════════════════════════
def load_and_prepare(path: str) -> pd.DataFrame:
    df = pd.read_excel(path)
    df['date']    = pd.to_datetime(df['date'])
    df            = df.sort_values('date').reset_index(drop=True)
    df['fwd_7d']  = df['close'].pct_change(7).shift(-7)
    df['fwd_30d'] = df['close'].pct_change(30).shift(-30)
    return df


# ════════════════════════════════════════════════════════════════════
# 2. Regime 画像（Profiling）
# ════════════════════════════════════════════════════════════════════
def profile_regimes(df: pd.DataFrame) -> dict:
    """
    为每个 k 列的每个 regime 编号计算统计画像，
    并自动归类为 BULL / BEAR / NEUTRAL。

    归类规则（基于数据实证）：
        BULL  : fwd30d > +2.5%  且  win7d > 53%
        BEAR  : fwd30d < -2.0%  或 (fwd30d < -1.0% 且 win7d < 45%)
        NEUTRAL: 其余
    """
    profiles = {}  # {k_col: DataFrame}

    for k in K_COLS:
        rows = []
        valid = df.dropna(subset=[k])

        for regime in sorted(valid[k].unique()):
            sub = valid[valid[k] == regime].copy()

            fwd7d_mean  = sub['fwd_7d'].mean()
            fwd30d_mean = sub['fwd_30d'].mean()
            win7d       = (sub['fwd_7d'] > 0).mean()
            vol_z_mean  = sub['volatility_pct_z'].mean()
            fgi_mean    = sub['fgi'].mean()
            n           = len(sub)

            # 各 Z-score 指标与未来 30 日收益的相关性
            corrs = {}
            for z in Z_COLS:
                pair = sub[[z, 'fwd_30d']].dropna()
                corrs[z] = pair.corr().iloc[0, 1] if len(pair) > 5 else np.nan
            top_z = sorted(corrs.items(), key=lambda x: abs(x[1] if not np.isnan(x[1]) else 0), reverse=True)[:2]
            top_z_str = ' / '.join([f'{z}({v:.2f})' for z, v in top_z])

            # 自动归类
            if fwd30d_mean > 0.025 and win7d > 0.53:
                regime_type = 'BULL'
            elif fwd30d_mean < -0.020 or (fwd30d_mean < -0.010 and win7d < 0.45):
                regime_type = 'BEAR'
            else:
                regime_type = 'NEUTRAL'

            rows.append({
                'k_col':       k,
                'regime':      int(regime),
                'n':           n,
                'fwd7d_%':     round(fwd7d_mean * 100, 2),
                'fwd30d_%':    round(fwd30d_mean * 100, 2),
                'win7d_%':     round(win7d * 100, 1),
                'vol_z':       round(vol_z_mean, 2),
                'fgi':         round(fgi_mean, 1),
                'top_corr':    top_z_str,
                'type':        regime_type,
            })

        profiles[k] = pd.DataFrame(rows)

    return profiles


def print_profiles(profiles: dict):
    print('\n' + '═' * 80)
    print('  STEP 1 ｜ REGIME 画像总览')
    print('═' * 80)
    for k, df_p in profiles.items():
        print(f'\n┌─ {k.upper()} ────────────────────────────────────────────────────')
        bull = df_p[df_p['type'] == 'BULL']['regime'].tolist()
        bear = df_p[df_p['type'] == 'BEAR']['regime'].tolist()
        neut = df_p[df_p['type'] == 'NEUTRAL']['regime'].tolist()
        print(f'│  BULL={bull}  BEAR={bear}  NEUTRAL={neut}')
        print(df_p[['regime', 'n', 'fwd7d_%', 'fwd30d_%', 'win7d_%',
                     'vol_z', 'fgi', 'type', 'top_corr']].to_string(index=False))


# ════════════════════════════════════════════════════════════════════
# 3. 策略映射（Strategy Mapping）
# ════════════════════════════════════════════════════════════════════
STRATEGY_MAP = {
    'BULL': {
        'position':    1.0,            # 100% 多
        'signals':     ['macd_hist_z', 'dist_ma200_pct_z'],
        'signal_rule': 'macd_hist_z > -0.5',   # 不能动量严重转负
        'description': '趋势上涨：macd_hist_z > -0.5 时全仓多头',
    },
    'NEUTRAL': {
        'position':    0.5,
        'signals':     ['rsi_norm', 'boll_width_z'],
        'signal_rule': 'rsi_norm > -1.0',       # RSI 未超卖才入场
        'description': '震荡区间：RSI 未极端超卖时持 50% 仓位',
    },
    'BEAR': {
        'position':    -1.0,                     # 100% 做空
        'signals':     ['macd_hist_z'],
        'signal_rule': 'macd_hist_z < 0.5',      # 动量未转正才做空
        'description': '下跌/恐慌：macd_hist_z < 0.5 时 100% 做空',
    },
}

# ── 顶部过滤层：布林带极度扩张时压缩多头仓位 ───────────────────────
BOLL_OVERFLOW_THRESHOLD = 3.5   # boll_width_z 超过此值，多头最多 50%

def print_strategy_map():
    print('\n' + '═' * 80)
    print('  STEP 2 ｜ 策略映射规则')
    print('═' * 80)
    for rtype, rule in STRATEGY_MAP.items():
        print(f'\n  [{rtype:7s}]  仓位={rule["position"]*100:.0f}%')
        print(f'             信号确认: {rule["signal_rule"]}')
        print(f'             逻辑说明: {rule["description"]}')


# ════════════════════════════════════════════════════════════════════
# 4. 多数投票函数（Majority Vote）
# ════════════════════════════════════════════════════════════════════
def majority_vote(row: pd.Series, profiles: dict, min_agree: int = 4) -> str:
    """
    读取某一天的 k10~k15，查各自对应的 regime_type，
    统计投票结果：
        ≥ min_agree 票 BULL    → 'BULL'
        ≥ min_agree 票 BEAR    → 'BEAR'
        否则                   → 'ABSTAIN'（空仓）
    """
    votes = []
    for k in K_COLS:
        kval = row.get(k)
        if pd.isna(kval):
            continue
        profile_df = profiles[k]
        match = profile_df[profile_df['regime'] == int(kval)]
        if len(match) == 0:
            continue
        votes.append(match.iloc[0]['type'])

    if len(votes) < min_agree:
        return 'ABSTAIN'

    bull_cnt    = votes.count('BULL')
    bear_cnt    = votes.count('BEAR')
    neutral_cnt = votes.count('NEUTRAL')

    if bull_cnt >= min_agree:
        return 'BULL'
    if bear_cnt >= min_agree:
        return 'BEAR'
    # NEUTRAL 也需要 ≥ min_agree 才执行半仓，否则空仓
    if neutral_cnt >= min_agree:
        return 'NEUTRAL'
    return 'ABSTAIN'


def signal_confirm(row: pd.Series, regime_vote: str) -> bool:
    """
    对投票结果做信号二次确认，防止假突破进场。
    """
    if regime_vote == 'BULL':
        mh = row.get('macd_hist_z', np.nan)
        return (not np.isnan(mh)) and (mh > -0.5)

    if regime_vote == 'NEUTRAL':
        rsi = row.get('rsi_norm', np.nan)
        return (not np.isnan(rsi)) and (rsi > -1.0)

    if regime_vote == 'BEAR':
        mh = row.get('macd_hist_z', np.nan)
        return (not np.isnan(mh)) and (mh < 0.5)   # 动量未转正才做空

    return False   # ABSTAIN


def apply_overflow_filter(row: pd.Series, position: float) -> tuple:
    """
    布林带顶部过滤层：
        boll_width_z > BOLL_OVERFLOW_THRESHOLD 且 position > 0
        → 多头仓位最多 50%，防止在极度扩张顶部全仓做多
    返回 (最终仓位, 是否触发过滤)
    """
    if position <= 0:
        return position, False
    bw = row.get('boll_width_z', np.nan)
    if not np.isnan(bw) and bw > BOLL_OVERFLOW_THRESHOLD:
        return min(position, 0.5), True
    return position, False


# ════════════════════════════════════════════════════════════════════
# 5. 动态策略回测（无固定周期，连续N天信号触发开仓/平仓）
# ════════════════════════════════════════════════════════════════════
def run_backtest_dynamic(df: pd.DataFrame, profiles: dict,
                         min_agree: int = 4,
                         bull_entry_consecutive: int = 3,
                         bull_exit_consecutive: int = 3,
                         bear_consecutive: int = 3,
                         stop_loss: float = -0.08) -> pd.DataFrame:
    """
    不按固定周期，完全由信号驱动。
    多头入场、空头平仓、空头入场/多头平仓的连续天数完全独立：
        bull_entry_consecutive : 开多头所需连续 BULL 天数
        bull_exit_consecutive  : 空头平仓所需连续 BULL 天数（独立于多头入场）
        bear_consecutive       : 开空头 / 多头平仓所需连续 BEAR 天数

    平仓条件（满足任一即平）：
        1. 固定止损：持仓亏损 <= stop_loss
        2. 信号翻转：反向信号达到对应天数阈值
    """
    valid = df.dropna(subset=K_COLS).copy().reset_index(drop=True)
    valid['daily_vote'] = valid.apply(
        lambda r: majority_vote(r, profiles, min_agree), axis=1
    )

    max_win      = max(bull_entry_consecutive, bull_exit_consecutive, bear_consecutive)
    trades       = []
    position     = 0.0
    entry_price  = None
    entry_date   = None
    entry_k_vals = None
    entry_k_types= None
    vote_window  = []

    for idx in range(len(valid)):
        row   = valid.iloc[idx]
        today = row['daily_vote']
        price = row['close']

        vote_window.append(today)
        if len(vote_window) > max_win:
            vote_window.pop(0)

        # 各用途独立窗口
        last_bull_entry = vote_window[-bull_entry_consecutive:] if len(vote_window) >= bull_entry_consecutive else []
        last_bull_exit  = vote_window[-bull_exit_consecutive:]  if len(vote_window) >= bull_exit_consecutive  else []
        last_bear       = vote_window[-bear_consecutive:]       if len(vote_window) >= bear_consecutive       else []

        # ── 持仓中：检查两种平仓条件 ──────────────────────────────
        if position != 0:
            # 用日内最坏价格检查止损：多头用low，空头用high
            worst_price = row['low'] if position > 0 else row['high']
            pnl = (worst_price / entry_price - 1) * np.sign(position)

            # 条件1：固定止损
            if pnl <= stop_loss:
                trades.append(_make_trade_record(
                    entry_date, row['date'], entry_price, price,
                    position, entry_k_vals, entry_k_types, 'stop_loss'
                ))
                position = 0.0
                vote_window = []
                continue

            # 条件2：信号翻转平仓
            # 多头平仓：连续 bear_consecutive 天 BEAR
            if position > 0 and len(last_bear) == bear_consecutive and all(v == 'BEAR' for v in last_bear):
                trades.append(_make_trade_record(
                    entry_date, row['date'], entry_price, price,
                    position, entry_k_vals, entry_k_types, 'signal_flip'
                ))
                position = 0.0
            # 空头平仓：连续 bull_exit_consecutive 天 BULL（独立于多头入场条件）
            elif position < 0 and len(last_bull_exit) == bull_exit_consecutive and all(v == 'BULL' for v in last_bull_exit):
                trades.append(_make_trade_record(
                    entry_date, row['date'], entry_price, price,
                    position, entry_k_vals, entry_k_types, 'signal_flip'
                ))
                position = 0.0

        # ── 空仓中：检查开仓条件 ──────────────────────────────────
        if position == 0:
            # 多头入场：连续 bull_entry_consecutive 天 BULL
            if len(last_bull_entry) == bull_entry_consecutive and all(v == 'BULL' for v in last_bull_entry):
                if signal_confirm(row, 'BULL'):
                    pos, _ = apply_overflow_filter(row, 1.0)
                    position     = pos
                    entry_price  = price
                    entry_date   = row['date']
                    entry_k_vals, entry_k_types = _get_k_info(row, profiles)

            elif len(last_bear) == bear_consecutive and all(v == 'BEAR' for v in last_bear):
                if signal_confirm(row, 'BEAR'):
                    position     = -1.0
                    entry_price  = price
                    entry_date   = row['date']
                    entry_k_vals, entry_k_types = _get_k_info(row, profiles)

    # 数据末尾强制平仓
    if position != 0:
        last_row = valid.iloc[-1]
        trades.append(_make_trade_record(
            entry_date, last_row['date'], entry_price, last_row['close'],
            position, entry_k_vals, entry_k_types, 'end_of_data'
        ))

    return pd.DataFrame(trades)


def _get_k_info(row: pd.Series, profiles: dict):
    k_vals  = [int(row[k]) if not pd.isna(row[k]) else 'N/A' for k in K_COLS]
    k_types = []
    for k in K_COLS:
        kv = row.get(k)
        if pd.isna(kv):
            k_types.append('?')
            continue
        pf = profiles[k]
        m  = pf[pf['regime'] == int(kv)]
        k_types.append(m.iloc[0]['type'] if len(m) > 0 else '?')
    return str(k_vals), str(k_types)


def _make_trade_record(entry_date, exit_date, entry_price, exit_price,
                       position, k_vals, k_types, exit_reason):
    btc_ret   = (exit_price - entry_price) / entry_price
    strat_ret = btc_ret * position
    return {
        'entry_date':   entry_date.strftime('%Y-%m-%d'),
        'exit_date':    exit_date.strftime('%Y-%m-%d'),
        'days_held':    (exit_date - entry_date).days,
        'entry_price':  round(entry_price, 0),
        'exit_price':   round(exit_price, 0),
        'direction':    'LONG' if position > 0 else 'SHORT',
        'position_%':   round(abs(position) * 100, 0),
        'k_vals':       k_vals,
        'k_types':      k_types,
        'exit_reason':  exit_reason,
        'btc_ret_%':    round(btc_ret * 100, 2),
        'strat_ret_%':  round(strat_ret * 100, 2),
    }


def performance_summary_dynamic(bt: pd.DataFrame) -> dict:
    """动态策略绩效：以每笔交易为单位统计"""
    if len(bt) == 0:
        return {}
    bt = bt.copy()
    bt['mult'] = 1 + bt['strat_ret_%'] / 100

    cum      = bt['mult'].prod() - 1
    n        = len(bt)
    long_bt  = bt[bt['direction'] == 'LONG']
    short_bt = bt[bt['direction'] == 'SHORT']
    win_rate = (bt['strat_ret_%'] > 0).mean()
    long_wr  = (long_bt['strat_ret_%'] > 0).mean() if len(long_bt)  > 0 else np.nan
    short_wr = (short_bt['strat_ret_%'] > 0).mean() if len(short_bt) > 0 else np.nan

    # 最大回撤
    cum_s  = bt['mult'].cumprod()
    max_dd = ((cum_s - cum_s.cummax()) / cum_s.cummax()).min()

    exit_counts = bt['exit_reason'].value_counts().to_dict()

    return {
        '总交易次数':     n,
        '做多次数':       len(long_bt),
        '做空次数':       len(short_bt),
        '策略累计收益':   f'{cum*100:.1f}%',
        '策略最大回撤':   f'{max_dd*100:.1f}%',
        '整体胜率':       f'{win_rate*100:.1f}%',
        '做多胜率':       f'{long_wr*100:.1f}%',
        '做空胜率':       f'{short_wr*100:.1f}%',
        '平均持仓天数':   f'{bt["days_held"].mean():.1f}',
        '止损退出次数':   exit_counts.get('stop_loss', 0),
        '信号翻转退出':   exit_counts.get('signal_flip', 0),
        '数据末尾平仓':   exit_counts.get('end_of_data', 0),
    }


# ════════════════════════════════════════════════════════════════════
# 5b. 组合策略：多头月度 + 空头动态（可同时持仓）
# ════════════════════════════════════════════════════════════════════
def run_combined_backtest(df: pd.DataFrame, profiles: dict,
                          min_agree: int = 4,
                          consecutive: int = 3,
                          stop_loss: float = -0.08) -> tuple:
    """
    多头腿：月初决策，BULL→+100%，NEUTRAL→+50%，否则 0（不做空）
    空头腿：连续 consecutive 天 BEAR 投票 → -100%，
            触发止损(-8%) 或连续 consecutive 天 BULL 翻转 → 平空
    两腿独立运行，净仓位叠加，同一天可以多空共存。
    返回 (逐月汇总 DataFrame, 逐日明细 DataFrame)
    """
    valid = df.dropna(subset=K_COLS).copy().reset_index(drop=True)
    valid['ym']         = valid['date'].dt.to_period('M')
    valid['daily_vote'] = valid.apply(
        lambda r: majority_vote(r, profiles, min_agree), axis=1
    )
    valid['daily_ret']  = valid['close'].pct_change().fillna(0.0)

    months = sorted(valid['ym'].unique())

    # ── 多头腿：月度决策 → 每日多头仓位 ──────────────────────────
    month_long_pos = {}
    for ym in months:
        month_data   = valid[valid['ym'] == ym]
        row0         = month_data.iloc[0]
        vote         = majority_vote(row0, profiles, min_agree)
        if vote in ('BULL', 'NEUTRAL'):
            confirmed  = signal_confirm(row0, vote)
            fv         = vote if confirmed else 'ABSTAIN'
        else:
            fv = vote
        pos = max(STRATEGY_MAP.get(fv, {'position': 0.0})['position'], 0.0)
        pos, _ = apply_overflow_filter(row0, pos)
        month_long_pos[str(ym)] = pos

    valid['long_pos'] = valid['ym'].astype(str).map(month_long_pos).fillna(0.0)

    # ── 空头腿：动态 3 天 BEAR → 每日空头仓位 ───────────────────
    short_pos_list = [0.0] * len(valid)
    s_pos          = 0.0
    s_entry        = None
    vote_win       = []

    for idx in range(len(valid)):
        row   = valid.iloc[idx]
        price = row['close']
        today = row['daily_vote']

        vote_win.append(today)
        if len(vote_win) > consecutive:
            vote_win.pop(0)
        last_n = vote_win[-consecutive:] if len(vote_win) >= consecutive else []

        if s_pos < 0 and s_entry is not None:
            pnl = (price / s_entry - 1) * np.sign(s_pos)
            if pnl <= stop_loss:                           # 止损
                s_pos, s_entry = 0.0, None
                vote_win = []
            elif len(last_n) == consecutive and all(v == 'BULL' for v in last_n):
                s_pos, s_entry = 0.0, None                # 信号翻转平空

        if s_pos == 0.0 and len(last_n) == consecutive and all(v == 'BEAR' for v in last_n):
            if signal_confirm(row, 'BEAR'):
                s_pos, s_entry = -1.0, price

        short_pos_list[idx] = s_pos

    valid['short_pos'] = short_pos_list
    valid['net_pos']   = valid['long_pos'] + valid['short_pos']

    # ── 每日收益（用前一日仓位计算当日回报）────────────────────
    lp = valid['long_pos'].shift(1).fillna(0.0)
    sp = valid['short_pos'].shift(1).fillna(0.0)
    np_ = valid['net_pos'].shift(1).fillna(0.0)
    valid['long_day_ret']  = valid['daily_ret'] * lp
    valid['short_day_ret'] = valid['daily_ret'] * sp
    valid['net_day_ret']   = valid['daily_ret'] * np_

    # ── 逐月汇总 ─────────────────────────────────────────────────
    monthly = []
    for i, ym in enumerate(months):
        md = valid[valid['ym'] == ym]
        ep = md.iloc[0]['close']
        if i + 1 < len(months):
            xp = valid[valid['ym'] == months[i + 1]].iloc[0]['close']
        else:
            xp = md.iloc[-1]['close']
        btc_r     = (xp - ep) / ep
        long_cum  = (1 + md['long_day_ret']).prod() - 1
        short_cum = (1 + md['short_day_ret']).prod() - 1
        net_cum   = (1 + md['net_day_ret']).prod() - 1
        monthly.append({
            'month':           str(ym),
            'long_pos_%':      round(month_long_pos.get(str(ym), 0) * 100, 0),
            'avg_short_%':     round(md['short_pos'].mean() * 100, 1),
            'btc_ret_%':       round(btc_r * 100, 2),
            'long_ret_%':      round(long_cum * 100, 2),
            'short_ret_%':     round(short_cum * 100, 2),
            'net_ret_%':       round(net_cum * 100, 2),
        })

    return pd.DataFrame(monthly), valid


def performance_summary_combined(monthly: pd.DataFrame, daily: pd.DataFrame) -> dict:
    cum_net   = (1 + daily['net_day_ret']).prod()   - 1
    cum_long  = (1 + daily['long_day_ret']).prod()  - 1
    cum_short = (1 + daily['short_day_ret']).prod() - 1
    cum_btc   = daily['close'].iloc[-1] / daily['close'].iloc[0] - 1

    cum_s  = (1 + daily['net_day_ret']).cumprod()
    max_dd = ((cum_s - cum_s.cummax()) / cum_s.cummax()).min()

    std = daily['net_day_ret'].std()
    sharpe = (daily['net_day_ret'].mean() / std) * (252 ** 0.5) if std > 0 else np.nan

    win_rate = (monthly['net_ret_%'] > 0).mean()

    # 多空共存天数
    overlap = ((daily['long_pos'] > 0) & (daily['short_pos'] < 0)).sum()

    return {
        '策略净累计收益':    f'{cum_net*100:.1f}%',
        '  └ 多头腿贡献':   f'{cum_long*100:.1f}%',
        '  └ 空头腿贡献':   f'{cum_short*100:.1f}%',
        'BTC买入持有':       f'{cum_btc*100:.1f}%',
        '夏普比率(年化)':    f'{sharpe:.2f}',
        '最大回撤':          f'{max_dd*100:.1f}%',
        '月度胜率':          f'{win_rate*100:.1f}%',
        '多空共存天数':      overlap,
    }


# ════════════════════════════════════════════════════════════════════
# 5c. 每月再平衡回测
# ════════════════════════════════════════════════════════════════════
def run_backtest(df: pd.DataFrame, profiles: dict,
                 min_agree: int = 4) -> pd.DataFrame:
    """
    每月第一个有 k 标签的交易日做一次决策，
    持仓到下月第一个决策日（月度持有期）。
    返回逐月交易记录 DataFrame。
    """
    # 只用有完整 k 标签的行
    valid = df.dropna(subset=K_COLS).copy()
    valid['ym'] = valid['date'].dt.to_period('M')

    records = []
    months  = sorted(valid['ym'].unique())

    for i, ym in enumerate(months):
        month_data = valid[valid['ym'] == ym]
        decision_row = month_data.iloc[0]   # 当月第一个交易日

        # ── 多数投票 ──
        vote = majority_vote(decision_row, profiles, min_agree)

        # ── 信号确认 ──
        if vote in ('BULL', 'NEUTRAL', 'BEAR'):
            confirmed  = signal_confirm(decision_row, vote)
            final_vote = vote if confirmed else 'ABSTAIN'
        else:
            final_vote = vote
            confirmed  = False

        position = STRATEGY_MAP.get(final_vote, {'position': 0.0})['position']

        # ── 顶部过滤层 ──
        position, filtered = apply_overflow_filter(decision_row, position)

        # ── 方向标签 ──
        if position > 0:
            direction = 'LONG'
        elif position < 0:
            direction = 'SHORT'
        else:
            direction = 'CASH'

        # ── 计算本月实际收益 ──
        entry_price = decision_row['close']
        if i + 1 < len(months):
            next_month_data = valid[valid['ym'] == months[i + 1]]
            exit_row   = next_month_data.iloc[0]
            exit_price = exit_row['close']
            exit_date  = exit_row['date']
        else:
            exit_row   = month_data.iloc[-1]
            exit_price = exit_row['close']
            exit_date  = exit_row['date']

        btc_return   = (exit_price - entry_price) / entry_price
        strat_return = btc_return * position

        k_vals = {k: (int(decision_row[k]) if not pd.isna(decision_row[k]) else 'N/A')
                  for k in K_COLS}
        k_types_list = []
        for k in K_COLS:
            kv = decision_row.get(k)
            if pd.isna(kv):
                k_types_list.append('?')
                continue
            pf = profiles[k]
            m  = pf[pf['regime'] == int(kv)]
            k_types_list.append(m.iloc[0]['type'] if len(m) > 0 else '?')

        records.append({
            'month':          str(ym),
            'entry_date':     decision_row['date'].strftime('%Y-%m-%d'),
            'exit_date':      exit_date.strftime('%Y-%m-%d'),
            'entry_price':    round(entry_price, 0),
            'exit_price':     round(exit_price, 0),
            'k_vals':         str([k_vals[k] for k in K_COLS]),
            'k_types':        str(k_types_list),
            'vote':           vote,
            'confirmed':      confirmed,
            'boll_filtered':  filtered,
            'final_decision': final_vote,
            'direction':      direction,
            'position_%':     round(position * 100, 0),
            'btc_ret_%':      round(btc_return * 100, 2),
            'strat_ret_%':    round(strat_return * 100, 2),
        })

    return pd.DataFrame(records)


# ════════════════════════════════════════════════════════════════════
# 5b. 按周再平衡回测
# ════════════════════════════════════════════════════════════════════
def run_backtest_weekly(df: pd.DataFrame, profiles: dict,
                        min_agree: int = 4) -> pd.DataFrame:
    """
    每周第一个有 k 标签的交易日做一次决策，
    持仓到下周第一个决策日（周度持有期）。
    返回逐周交易记录 DataFrame。
    """
    valid = df.dropna(subset=K_COLS).copy()
    valid['yw'] = valid['date'].dt.to_period('W')

    records = []
    weeks   = sorted(valid['yw'].unique())

    for i, yw in enumerate(weeks):
        week_data    = valid[valid['yw'] == yw]
        decision_row = week_data.iloc[0]

        vote = majority_vote(decision_row, profiles, min_agree)

        if vote in ('BULL', 'NEUTRAL', 'BEAR'):
            confirmed  = signal_confirm(decision_row, vote)
            final_vote = vote if confirmed else 'ABSTAIN'
        else:
            final_vote = vote
            confirmed  = False

        position = STRATEGY_MAP.get(final_vote, {'position': 0.0})['position']
        position, filtered = apply_overflow_filter(decision_row, position)

        if position > 0:
            direction = 'LONG'
        elif position < 0:
            direction = 'SHORT'
        else:
            direction = 'CASH'

        entry_price = decision_row['close']
        if i + 1 < len(weeks):
            next_week_data = valid[valid['yw'] == weeks[i + 1]]
            exit_row   = next_week_data.iloc[0]
            exit_price = exit_row['close']
            exit_date  = exit_row['date']
        else:
            exit_row   = week_data.iloc[-1]
            exit_price = exit_row['close']
            exit_date  = exit_row['date']

        btc_return   = (exit_price - entry_price) / entry_price
        strat_return = btc_return * position

        k_vals = {k: (int(decision_row[k]) if not pd.isna(decision_row[k]) else 'N/A')
                  for k in K_COLS}
        k_types_list = []
        for k in K_COLS:
            kv = decision_row.get(k)
            if pd.isna(kv):
                k_types_list.append('?')
                continue
            pf = profiles[k]
            m  = pf[pf['regime'] == int(kv)]
            k_types_list.append(m.iloc[0]['type'] if len(m) > 0 else '?')

        records.append({
            'week':           str(yw),
            'entry_date':     decision_row['date'].strftime('%Y-%m-%d'),
            'exit_date':      exit_date.strftime('%Y-%m-%d'),
            'entry_price':    round(entry_price, 0),
            'exit_price':     round(exit_price, 0),
            'k_vals':         str([k_vals[k] for k in K_COLS]),
            'k_types':        str(k_types_list),
            'vote':           vote,
            'confirmed':      confirmed,
            'boll_filtered':  filtered,
            'final_decision': final_vote,
            'direction':      direction,
            'position_%':     round(position * 100, 0),
            'btc_ret_%':      round(btc_return * 100, 2),
            'strat_ret_%':    round(strat_return * 100, 2),
        })

    return pd.DataFrame(records)


# ════════════════════════════════════════════════════════════════════
# 6. 绩效统计
# ════════════════════════════════════════════════════════════════════
def performance_summary(bt: pd.DataFrame, freq: str = 'monthly') -> dict:
    """
    计算策略 vs BTC 买入持有的核心绩效指标。
    freq: 'monthly'（年化因子12）或 'weekly'（年化因子52）
    """
    bt = bt.copy()
    bt['strat_mult'] = 1 + bt['strat_ret_%'] / 100
    bt['btc_mult']   = 1 + bt['btc_ret_%'] / 100

    strat_cum = bt['strat_mult'].prod() - 1
    btc_cum   = bt['btc_mult'].prod() - 1

    ann_factor  = 52 if freq == 'weekly' else 12
    period_name = '周' if freq == 'weekly' else '月'
    n_periods   = len(bt)

    strat_ann = (1 + strat_cum) ** (ann_factor / n_periods) - 1
    btc_ann   = (1 + btc_cum)   ** (ann_factor / n_periods) - 1

    strat_sr = (bt['strat_ret_%'].mean() / bt['strat_ret_%'].std()) * (ann_factor ** 0.5) \
               if bt['strat_ret_%'].std() > 0 else np.nan

    cum_series = bt['strat_mult'].cumprod()
    peak   = cum_series.cummax()
    max_dd = ((cum_series - peak) / peak).min()

    long_bt  = bt[bt['position_%'] > 0]
    short_bt = bt[bt['position_%'] < 0]
    idle     = (bt['position_%'] == 0).sum()
    executed = bt[bt['position_%'] != 0]
    win_rate = (executed['strat_ret_%'] > 0).mean() if len(executed) > 0 else np.nan
    long_wr  = (long_bt['strat_ret_%'] > 0).mean()  if len(long_bt)  > 0 else np.nan
    short_wr = (short_bt['strat_ret_%'] > 0).mean() if len(short_bt) > 0 else np.nan

    return {
        f'总{period_name}数':      n_periods,
        f'做多{period_name}数':    len(long_bt),
        f'做空{period_name}数':    len(short_bt),
        f'空仓{period_name}数':    idle,
        '策略累计收益':             f'{strat_cum*100:.1f}%',
        'BTC累计收益':              f'{btc_cum*100:.1f}%',
        '策略年化收益':             f'{strat_ann*100:.1f}%',
        'BTC年化收益':              f'{btc_ann*100:.1f}%',
        '策略夏普(年化)':           f'{strat_sr:.2f}',
        '策略最大回撤':             f'{max_dd*100:.1f}%',
        '整体执行胜率':             f'{win_rate*100:.1f}%',
        '做多胜率':                 f'{long_wr*100:.1f}%',
        '做空胜率':                 f'{short_wr*100:.1f}%',
    }


def print_backtest(bt: pd.DataFrame, perf: dict, freq: str = 'monthly'):
    label    = '周' if freq == 'weekly' else '月'
    period_col = 'week' if freq == 'weekly' else 'month'
    print('\n' + '═' * 100)
    print(f'  STEP 3 | 逐{label}回测结果')
    print('═' * 100)
    cols = [period_col, 'entry_date', 'entry_price', 'exit_price',
            'k_vals', 'k_types', 'final_decision', 'position_%',
            'btc_ret_%', 'strat_ret_%']
    pd.set_option('display.max_colwidth', 40)
    pd.set_option('display.width', 200)
    print(bt[cols].to_string(index=False))

    print('\n' + '═' * 60)
    print(f'  STEP 4 | 绩效汇总（{label}频）')
    print('═' * 60)
    for k, v in perf.items():
        print(f'  {k:<18} {v}')


# ════════════════════════════════════════════════════════════════════
# 7. 每月再平衡完整操作手册（打印）
# ════════════════════════════════════════════════════════════════════
def print_playbook(profiles: dict):
    print('\n' + '═' * 80)
    print('  STEP 5 ｜ 每月再平衡操作手册')
    print('═' * 80)
    print("""
  【时间节点】
    每月第1个交易日（或新数据到位后的第一天）早晨执行

  【第一步：读取当天 k 标签】
    查看 btc.xlsx 最新一行的 k10~k15 值，例如：
    k_cluster = [7, 1, 11, 2, 11, 3]

  【第二步：查询每个 k 值对应的 Regime 类型】
    对照下方对照表，找到每列 k 值对应的 BULL / BEAR / NEUTRAL：
""")
    for k, pf in profiles.items():
        bull = sorted(pf[pf['type'] == 'BULL']['regime'].tolist())
        bear = sorted(pf[pf['type'] == 'BEAR']['regime'].tolist())
        neut = sorted(pf[pf['type'] == 'NEUTRAL']['regime'].tolist())
        print(f'    {k.upper():4s}  BULL={bull}')
        print(f'          BEAR={bear}')
        print(f'          NEUTRAL={neut}')
        print()

    print("""
  【第三步：多数投票（至少 4/6 同意才执行）】

    统计 BULL / BEAR / NEUTRAL 各得几票：
    ┌─────────────────────────────────────────────────────┐
    │  ≥4 票 BULL    → 进行信号确认（第四步）             │
    │  ≥4 票 BEAR    → 空仓，不操作                       │
    │  ≥4 票 NEUTRAL → 进行信号确认（第四步）             │
    │  未达 4 票      → 空仓，不操作                       │
    └─────────────────────────────────────────────────────┘

  【第四步：信号二次确认（防假突破）】

    若投票结果为 BULL：
        查 macd_hist_z 是否 > -0.5
        [YES] macd_hist_z > -0.5 → 100% 仓位做多
        [NO]  macd_hist_z <= -0.5 → 本月空仓

    若投票结果为 NEUTRAL：
        查 rsi_norm 是否 > -1.0
        [YES] rsi_norm > -1.0 → 50% 仓位做多
        [NO]  rsi_norm <= -1.0 → 本月空仓

  【第五步：执行仓位调整】

    ┌────────────┬──────────┬────────────────────────────┐
    │ 最终决策   │ 仓位     │ 操作                       │
    ├────────────┼──────────┼────────────────────────────┤
    │ BULL       │ 100%     │ 全仓买入/持有 BTC          │
    │ NEUTRAL    │  50%     │ 半仓买入/持有 BTC          │
    │ BEAR       │   0%     │ 清仓/空仓                  │
    │ ABSTAIN    │   0%     │ 空仓观望                   │
    └────────────┴──────────┴────────────────────────────┘

  【第六步：记录当月操作】
    记录：执行日期、k_cluster 值、投票结果、仓位、入场价格
    下月第一个交易日对比出场价格，计算本月收益
""")


# ════════════════════════════════════════════════════════════════════
# 8. 可选：导出 Excel 报告
# ════════════════════════════════════════════════════════════════════
def export_report(profiles: dict,
                  bt_monthly:  pd.DataFrame, perf_monthly:  dict,
                  bt_weekly:   pd.DataFrame, perf_weekly:   dict,
                  bt_dynamic:  pd.DataFrame, perf_dynamic:  dict,
                  comb_monthly: pd.DataFrame, perf_combined: dict,
                  out_path: str = 'regime_report.xlsx'):
    try:
        with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
            # 组合策略（多头月度 + 空头动态）
            comb_monthly.to_excel(writer, sheet_name='组合策略逐月', index=False)
            perf_c = pd.DataFrame(list(perf_combined.items()), columns=['指标', '数值'])
            perf_c.to_excel(writer, sheet_name='组合策略绩效', index=False)

            # 动态策略（对照）
            bt_dynamic.to_excel(writer, sheet_name='动态策略回测', index=False)
            perf_d = pd.DataFrame(list(perf_dynamic.items()), columns=['指标', '数值'])
            perf_d.to_excel(writer, sheet_name='动态策略绩效', index=False)

            # 月度回测（对照）
            bt_monthly.to_excel(writer, sheet_name='月度回测', index=False)
            perf_m = pd.DataFrame(list(perf_monthly.items()), columns=['指标', '数值'])
            perf_m.to_excel(writer, sheet_name='月度绩效', index=False)

            # 周度回测（对照）
            bt_weekly.to_excel(writer, sheet_name='周度回测', index=False)
            perf_w = pd.DataFrame(list(perf_weekly.items()), columns=['指标', '数值'])
            perf_w.to_excel(writer, sheet_name='周度绩效', index=False)

            # Regime 画像
            all_profiles = pd.concat(profiles.values(), ignore_index=True)
            all_profiles.to_excel(writer, sheet_name='Regime画像', index=False)

        print(f'\n  [OK] 报告已导出 -> {out_path}')
    except Exception as e:
        print(f'\n  [FAIL] 报告导出失败: {e}')


# ════════════════════════════════════════════════════════════════════
# 主程序
# ════════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    print('加载数据...')
    df = load_and_prepare(DATA_PATH)
    print(f'数据范围: {df.date.min().date()} ~ {df.date.max().date()}，共 {len(df)} 行')

    # Step 1：画像
    profiles = profile_regimes(df)
    print_profiles(profiles)

    # Step 2：策略映射
    print_strategy_map()

    # Step 3a：组合策略（多头月度 + 空头动态）
    comb_monthly, comb_daily = run_combined_backtest(df, profiles, min_agree=4,
                                                     consecutive=3, stop_loss=-0.08)
    perf_combined = performance_summary_combined(comb_monthly, comb_daily)
    print('\n' + '=' * 80)
    print('  STEP 3 | 组合策略：多头月度 + 空头动态（可同时持仓）')
    print('=' * 80)
    pd.set_option('display.width', 200)
    print(comb_monthly.to_string(index=False))
    print('\n' + '=' * 60)
    print('  STEP 4 | 组合策略绩效')
    print('=' * 60)
    for k, v in perf_combined.items():
        print(f'  {k:<20} {v}')

    # Step 3b：动态策略三组对比（BULL门槛 3/4/5天，BEAR固定3天）
    print('\n' + '=' * 80)
    print('  STEP 3 | 动态策略：BULL门槛对比（BEAR固定3天）')
    print('=' * 80)
    # 多头入场/空头平仓条件独立：bull_entry / bull_exit / bear 分开控制
    # 对比三组：入场门槛 3/4/5 天，空头平仓始终保持 3 天
    dynamic_results = {}
    for bull_entry_n in [3, 4, 5]:
        bt = run_backtest_dynamic(df, profiles, min_agree=4,
                                  bull_entry_consecutive=bull_entry_n,
                                  bull_exit_consecutive=3,
                                  bear_consecutive=3,
                                  stop_loss=-0.08)
        pf = performance_summary_dynamic(bt)
        dynamic_results[bull_entry_n] = (bt, pf)
        print(f'\n--- 多头入场={bull_entry_n}天 / 空头平仓=3天 / BEAR=3天 ---')
        print(bt[['entry_date','exit_date','days_held','direction',
                  'position_%','exit_reason','btc_ret_%','strat_ret_%']].to_string(index=False))

    print('\n' + '=' * 70)
    print('  STEP 4 | 动态策略绩效对比（空头平仓始终3天）')
    print('=' * 70)
    keys = list(dynamic_results[3][1].keys())
    print(f'  {"指标":<20} {"入场3天":>12} {"入场4天":>12} {"入场5天":>12}')
    print('  ' + '-' * 58)
    for key in keys:
        vals = [dynamic_results[n][1].get(key, '-') for n in [3, 4, 5]]
        print(f'  {key:<20} {str(vals[0]):>12} {str(vals[1]):>12} {str(vals[2]):>12}')

    # 用多头入场4天（空头平仓仍3天）作为主版本保存
    bt_dynamic   = dynamic_results[4][0]
    perf_dynamic = dynamic_results[4][1]

    # Step 3b：月度回测（对照组）
    bt_monthly   = run_backtest(df, profiles, min_agree=4)
    perf_monthly = performance_summary(bt_monthly, freq='monthly')
    print_backtest(bt_monthly, perf_monthly, freq='monthly')

    # Step 3c：周度回测（对照组）
    bt_weekly   = run_backtest_weekly(df, profiles, min_agree=4)
    perf_weekly = performance_summary(bt_weekly, freq='weekly')
    print_backtest(bt_weekly, perf_weekly, freq='weekly')

    # Step 5：操作手册
    print_playbook(profiles)

    # 导出 Excel（动态 + 月度 + 周度 + 画像，共 7 个 Sheet）
    export_report(
        profiles,
        bt_monthly,   perf_monthly,
        bt_weekly,    perf_weekly,
        bt_dynamic,   perf_dynamic,
        comb_monthly, perf_combined,
        out_path='E:/DESK/window_and_profit/main/regime_report.xlsx'
    )
