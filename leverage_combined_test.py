import pandas as pd
import numpy as np
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from regime_strategy import (load_and_prepare, profile_regimes, majority_vote,
    signal_confirm, apply_overflow_filter, STRATEGY_MAP, K_COLS)

df = load_and_prepare('btc.xlsx')
profiles = profile_regimes(df)
TOTAL_DAYS = 539

def get_bull_n(row, profiles):
    votes = []
    for k in K_COLS:
        kv = row.get(k)
        if pd.isna(kv): continue
        match = profiles[k][profiles[k]['regime'] == int(kv)]
        if len(match) == 0: continue
        votes.append(match.iloc[0]['type'])
    return votes.count('BULL')


def run_combined_levered(df, profiles,
                         long_lev_map=None,   # {bull_n: multiplier}
                         short_lev=1.0,       # 空头固定杠杆倍数
                         long_stop=-0.15,     # 多头月内止损（含杠杆后）
                         short_stop=-0.08,    # 空头止损
                         min_agree=4,
                         consecutive=3):
    """
    组合策略杠杆版：
      多头腿：月度决策 + 按票数加杠杆 + 月内止损
      空头腿：动态3天BEAR + 固定杠杆 + 止损
    """
    if long_lev_map is None:
        long_lev_map = {4: 1.0, 5: 1.0, 6: 1.0}

    valid = df.dropna(subset=K_COLS).copy().reset_index(drop=True)
    valid['ym']         = valid['date'].dt.to_period('M')
    valid['daily_vote'] = valid.apply(
        lambda r: majority_vote(r, profiles, min_agree), axis=1)
    valid['daily_ret']  = valid['close'].pct_change().fillna(0.0)

    months = sorted(valid['ym'].unique())

    # ── 多头腿：月度决策（含杠杆和月内止损）─────────────────
    long_pos_list = [0.0] * len(valid)

    for i, ym in enumerate(months):
        md_idx = valid[valid['ym'] == ym].index
        row0   = valid.loc[md_idx[0]]
        vote   = majority_vote(row0, profiles, min_agree)
        if vote in ('BULL', 'NEUTRAL', 'BEAR'):
            fv = vote if signal_confirm(row0, vote) else 'ABSTAIN'
        else:
            fv = vote

        base_pos = max(STRATEGY_MAP.get(fv, {'position': 0.0})['position'], 0)
        base_pos, _ = apply_overflow_filter(row0, base_pos)

        # 杠杆：仅对 BULL 仓位应用
        if fv == 'BULL' and base_pos > 0:
            bull_n = get_bull_n(row0, profiles)
            lev    = long_lev_map.get(bull_n, 1.0)
            pos    = base_pos * lev
        else:
            pos = base_pos

        if pos <= 0:
            for idx in md_idx:
                long_pos_list[idx] = 0.0
            continue

        # 月内止损检查（用杠杆后收益衡量）
        entry_p = row0['close']
        stopped = False
        for idx in md_idx:
            row = valid.loc[idx]
            if row['date'] == row0['date']:
                long_pos_list[idx] = pos
                continue
            raw_ret = (row['close'] / entry_p - 1)
            levered_loss = raw_ret * pos          # 杠杆后亏损
            if long_stop is not None and levered_loss <= long_stop:
                stopped = True
                # 止损后剩余月份仓位归零
                for jdx in md_idx[md_idx >= idx]:
                    long_pos_list[jdx] = 0.0
                break
            long_pos_list[idx] = pos
        # 若未止损，已在循环中赋值；若止损，后段已清零

    valid['long_pos'] = long_pos_list

    # ── 空头腿：动态3天BEAR + 杠杆 ──────────────────────────
    short_pos_list = [0.0] * len(valid)
    s_pos  = 0.0
    s_entry = None
    vote_win = []

    for idx in range(len(valid)):
        row   = valid.iloc[idx]
        price = row['close']
        today = row['daily_vote']

        vote_win.append(today)
        if len(vote_win) > consecutive:
            vote_win.pop(0)
        last_n = vote_win[-consecutive:] if len(vote_win) >= consecutive else []

        if s_pos < 0 and s_entry is not None:
            raw_pnl = (price / s_entry - 1) * (-1)      # 空头方向
            levered_pnl = raw_pnl * abs(s_pos)
            if levered_pnl <= short_stop:                # 止损
                s_pos, s_entry = 0.0, None
                vote_win = []
            elif len(last_n) == consecutive and all(v == 'BULL' for v in last_n):
                s_pos, s_entry = 0.0, None              # 信号翻转平空

        if s_pos == 0.0 and len(last_n) == consecutive and all(v == 'BEAR' for v in last_n):
            if signal_confirm(row, 'BEAR'):
                s_pos   = -1.0 * short_lev              # 加杠杆
                s_entry = price

        short_pos_list[idx] = s_pos

    valid['short_pos'] = short_pos_list
    valid['net_pos']   = valid['long_pos'] + valid['short_pos']

    # ── 每日收益 ─────────────────────────────────────────────
    lp  = valid['long_pos'].shift(1).fillna(0.0)
    sp  = valid['short_pos'].shift(1).fillna(0.0)
    np_ = valid['net_pos'].shift(1).fillna(0.0)

    valid['long_day_ret']  = valid['daily_ret'] * lp
    valid['short_day_ret'] = valid['daily_ret'] * sp
    valid['net_day_ret']   = valid['daily_ret'] * np_

    # ── 逐月汇总 ─────────────────────────────────────────────
    monthly = []
    for i, ym in enumerate(months):
        md = valid[valid['ym'] == ym]
        ep = md.iloc[0]['close']
        if i + 1 < len(months):
            xp = valid[valid['ym'] == months[i + 1]].iloc[0]['close']
        else:
            xp = md.iloc[-1]['close']
        btc_r    = (xp - ep) / ep
        long_cum = (1 + md['long_day_ret']).prod() - 1
        sht_cum  = (1 + md['short_day_ret']).prod() - 1
        net_cum  = (1 + md['net_day_ret']).prod() - 1
        monthly.append({
            'month':        str(ym),
            'long_pos_%':   round(md['long_pos'].mean() * 100, 1),
            'short_pos_%':  round(md['short_pos'].mean() * 100, 1),
            'btc_ret_%':    round(btc_r * 100, 2),
            'long_ret_%':   round(long_cum * 100, 2),
            'short_ret_%':  round(sht_cum * 100, 2),
            'net_ret_%':    round(net_cum * 100, 2),
        })

    return pd.DataFrame(monthly), valid


def perf_summary(monthly, daily, name):
    r = daily['net_day_ret']
    lr = daily['long_day_ret']
    sr = daily['short_day_ret']

    cum_net   = (1 + r).prod()  - 1
    cum_long  = (1 + lr).prod() - 1
    cum_short = (1 + sr).prod() - 1
    ann       = (1 + cum_net) ** (365 / TOTAL_DAYS) - 1
    sharpe    = r.mean() / r.std() * np.sqrt(365) if r.std() > 0 else 0
    dd        = ((1 + r).cumprod() / (1 + r).cumprod().cummax() - 1).min()
    m_rets    = monthly['net_ret_%'] / 100
    m_wr      = (m_rets > 0).mean()

    return {
        '方案':       name,
        '净累计收益': f'{cum_net*100:.1f}%',
        '多头贡献':   f'{cum_long*100:.1f}%',
        '空头贡献':   f'{cum_short*100:.1f}%',
        '年化收益':   f'{ann*100:.1f}%',
        '夏普(日频)': round(sharpe, 2),
        '最大回撤':   f'{dd*100:.1f}%',
        '月度胜率':   f'{m_wr*100:.1f}%',
    }


# ════════════════════════════════════════════════════════════
# 测试矩阵
# ════════════════════════════════════════════════════════════

scenarios = [
    # (名称, long_lev_map, short_lev, long_stop, short_stop)
    ('基准（无杠杆）',
     {4:1.0, 5:1.0, 6:1.0}, 1.0, None, -0.08),

    # ── 仅多头加杠杆 ─────────────────────────────────────────
    ('多头：5票1.5x / 6票2x',
     {4:1.0, 5:1.5, 6:2.0}, 1.0, -0.15, -0.08),

    ('多头：6票才2x',
     {4:1.0, 5:1.0, 6:2.0}, 1.0, -0.15, -0.08),

    ('多头：全部2x',
     {4:2.0, 5:2.0, 6:2.0}, 1.0, -0.15, -0.08),

    # ── 仅空头加杠杆 ─────────────────────────────────────────
    ('空头：1.5x',
     {4:1.0, 5:1.0, 6:1.0}, 1.5, None, -0.08),

    ('空头：2x',
     {4:1.0, 5:1.0, 6:1.0}, 2.0, None, -0.08),

    # ── 多空同时加杠杆 ────────────────────────────────────────
    ('多空：多头5票1.5x6票2x + 空头1.5x',
     {4:1.0, 5:1.5, 6:2.0}, 1.5, -0.15, -0.08),

    ('多空：多头5票1.5x6票2x + 空头2x',
     {4:1.0, 5:1.5, 6:2.0}, 2.0, -0.15, -0.08),

    ('多空：多头全2x + 空头2x',
     {4:2.0, 5:2.0, 6:2.0}, 2.0, -0.15, -0.08),
]

results = []
monthly_details = {}

for name, llm, sl, lst, sst in scenarios:
    m_df, d_df = run_combined_levered(
        df, profiles,
        long_lev_map=llm,
        short_lev=sl,
        long_stop=lst,
        short_stop=sst,
    )
    p = perf_summary(m_df, d_df, name)
    results.append(p)
    monthly_details[name] = m_df

summary = pd.DataFrame(results).set_index('方案')
print('=' * 90)
print('  组合策略（月度多头 + 动态空头）杠杆测试')
print('=' * 90)
print(summary.to_string())

# ── 逐月净收益对比（基准 vs 最优方案）─────────────────────
print()
print('=' * 80)
print('  逐月净收益：基准 vs 核心杠杆方案')
print('=' * 80)
base = monthly_details['基准（无杠杆）'][['month', 'btc_ret_%', 'net_ret_%']].copy()
base.columns = ['month', 'BTC', '基准']

for label in ['多头：5票1.5x / 6票2x', '空头：2x', '多空：多头5票1.5x6票2x + 空头2x']:
    col = monthly_details[label]['net_ret_%']
    base[label[:8]] = col.values

print(base.to_string(index=False))
