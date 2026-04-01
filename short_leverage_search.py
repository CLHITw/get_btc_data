import pandas as pd
import numpy as np
import sys
import io
from itertools import product
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from regime_strategy import (load_and_prepare, profile_regimes, majority_vote,
    signal_confirm, apply_overflow_filter, STRATEGY_MAP, K_COLS)

df = load_and_prepare('btc.xlsx')
profiles = profile_regimes(df)
TOTAL_DAYS = 539

# ── 工具：获取票数 ────────────────────────────────────────
def get_votes(row, profiles):
    votes = []
    for k in K_COLS:
        kv = row.get(k)
        if pd.isna(kv): continue
        match = profiles[k][profiles[k]['regime'] == int(kv)]
        if len(match) == 0: continue
        votes.append(match.iloc[0]['type'])
    return votes


# ── Step 1：分析7笔空头的入场信号质量 ─────────────────────
print('=' * 70)
print('  空头7笔交易入场日信号分析')
print('=' * 70)

valid_all = df.dropna(subset=K_COLS).copy().reset_index(drop=True)
valid_all['daily_vote'] = valid_all.apply(
    lambda r: majority_vote(r, profiles, 4), axis=1)
valid_all['daily_ret'] = valid_all['close'].pct_change().fillna(0.0)

# 重跑空头逻辑，记录每笔入场信号
consecutive = 3
s_pos, s_entry, s_date = 0.0, None, None
vote_win = []
short_trades = []

for idx in range(len(valid_all)):
    row   = valid_all.iloc[idx]
    price = row['close']
    today = row['daily_vote']

    vote_win.append(today)
    if len(vote_win) > consecutive:
        vote_win.pop(0)
    last_n = vote_win[-consecutive:] if len(vote_win) >= consecutive else []

    if s_pos < 0 and s_entry is not None:
        raw_pnl = (price / s_entry - 1) * (-1)
        if raw_pnl <= -0.08:
            short_trades[-1]['exit_date'] = row['date']
            short_trades[-1]['exit_price'] = price
            short_trades[-1]['exit_reason'] = 'stop_loss'
            short_trades[-1]['btc_ret'] = (price / s_entry - 1) * 100
            s_pos, s_entry, s_date = 0.0, None, None
            vote_win = []
        elif len(last_n) == consecutive and all(v == 'BULL' for v in last_n):
            short_trades[-1]['exit_date'] = row['date']
            short_trades[-1]['exit_price'] = price
            short_trades[-1]['exit_reason'] = 'signal_flip'
            short_trades[-1]['btc_ret'] = (price / s_entry - 1) * 100
            s_pos, s_entry, s_date = 0.0, None, None

    if s_pos == 0.0 and len(last_n) == consecutive and all(v == 'BEAR' for v in last_n):
        if signal_confirm(row, 'BEAR'):
            votes = get_votes(row, profiles)
            bear_n = votes.count('BEAR')
            bw  = row.get('boll_width_z', np.nan)
            mh  = row.get('macd_hist_z', np.nan)
            mz  = row.get('macd_z', np.nan)
            atr = row.get('atr_pct_z', np.nan)
            s_pos, s_entry, s_date = -1.0, price, row['date']
            short_trades.append({
                'entry_date': row['date'], 'entry_price': price,
                'exit_date': None, 'exit_price': None,
                'exit_reason': 'open', 'btc_ret': None,
                'bear_n': bear_n, 'bw': bw, 'mh': mh, 'mz': mz, 'atr': atr,
            })

st = pd.DataFrame(short_trades)
st['strat_ret_1x'] = -st['btc_ret']
st['result'] = st['strat_ret_1x'].apply(lambda x: 'WIN' if x is not None and x > 0 else 'LOSS')

print(f'{"入场日":>12}  {"结果":>5}  {"熊票数":>5}  {"BTC":>7}  {"1x收益":>8}  {"bw":>5}  {"mh":>5}  {"mz":>5}  {"atr":>5}')
for _, r in st.iterrows():
    if r['btc_ret'] is None: continue
    print(f'  {str(r.entry_date.date()):>10}  {r.result:>5}  {r.bear_n}/6  '
          f'{r.btc_ret:>+6.1f}%  {r.strat_ret_1x:>+7.1f}%  '
          f'{r.bw:>5.2f}  {r.mh:>5.2f}  {r.mz:>5.2f}  {r.atr:>5.2f}')

print()
wins = st[st['result']=='WIN']
loss = st[st['result']=='LOSS']
print(f'胜单均值: bear_n={wins.bear_n.mean():.1f}  bw={wins.bw.mean():.2f}  mz={wins.mz.mean():.2f}')
print(f'败单均值: bear_n={loss.bear_n.mean():.1f}  bw={loss.bw.mean():.2f}  mz={loss.mz.mean():.2f}')


# ── Step 2：组合策略函数（空头按条件杠杆）─────────────────
def run_combined_short_lev(df, profiles,
                            long_lev_map=None,
                            bear_n_lev_map=None,  # {bear_n: multiplier}
                            bw_threshold=None,    # bw >= threshold 才用满杠杆
                            long_stop=None,
                            short_stop=-0.08,
                            min_agree=4,
                            consecutive=3):
    if long_lev_map is None:
        long_lev_map = {4: 1.0, 5: 1.5, 6: 2.0}
    if bear_n_lev_map is None:
        bear_n_lev_map = {4: 1.0, 5: 1.0, 6: 1.0}

    valid = df.dropna(subset=K_COLS).copy().reset_index(drop=True)
    valid['ym']         = valid['date'].dt.to_period('M')
    valid['daily_vote'] = valid.apply(lambda r: majority_vote(r, profiles, min_agree), axis=1)
    valid['daily_ret']  = valid['close'].pct_change().fillna(0.0)
    months = sorted(valid['ym'].unique())

    # ── 多头腿 ────────────────────────────────────────────
    def get_bull_n(row):
        votes = get_votes(row, profiles)
        return votes.count('BULL')

    long_pos_list = [0.0] * len(valid)
    for i, ym in enumerate(months):
        md_idx = valid[valid['ym'] == ym].index
        row0   = valid.loc[md_idx[0]]
        vote   = majority_vote(row0, profiles, min_agree)
        fv     = vote if vote in ('BULL','NEUTRAL','BEAR') and signal_confirm(row0, vote) else (
                 'ABSTAIN' if vote in ('BULL','NEUTRAL','BEAR') else vote)
        base_pos = max(STRATEGY_MAP.get(fv, {'position': 0.0})['position'], 0)
        base_pos, _ = apply_overflow_filter(row0, base_pos)
        if fv == 'BULL' and base_pos > 0:
            bull_n = get_bull_n(row0)
            pos = base_pos * long_lev_map.get(bull_n, 1.0)
        else:
            pos = base_pos
        if pos <= 0:
            continue
        entry_p = row0['close']
        for idx in md_idx:
            row = valid.loc[idx]
            if long_stop is not None and row['date'] > row0['date']:
                if (row['low'] / entry_p - 1) * pos <= long_stop:  # 用日内最低价
                    break
            long_pos_list[idx] = pos

    valid['long_pos'] = long_pos_list

    # ── 空头腿（按 bear_n 和 bw 分级杠杆）────────────────
    short_pos_list = [0.0] * len(valid)
    s_pos, s_entry = 0.0, None
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
            worst_price = row['high']               # 空头最坏价格为日内最高
            raw_pnl = (worst_price / s_entry - 1) * (-1)
            levered_pnl = raw_pnl * abs(s_pos)
            if levered_pnl <= short_stop:
                s_pos, s_entry = 0.0, None
                vote_win = []
            elif len(last_n) == consecutive and all(v == 'BULL' for v in last_n):
                s_pos, s_entry = 0.0, None

        if s_pos == 0.0 and len(last_n) == consecutive and all(v == 'BEAR' for v in last_n):
            if signal_confirm(row, 'BEAR'):
                votes = get_votes(row, profiles)
                bear_n = votes.count('BEAR')
                bw     = row.get('boll_width_z', np.nan)
                lev    = bear_n_lev_map.get(bear_n, 1.0)
                # 可选：bw 未达阈值时降级杠杆
                if bw_threshold is not None and (np.isnan(bw) or bw < bw_threshold):
                    lev = min(lev, 1.0)
                s_pos   = -1.0 * lev
                s_entry = price

        short_pos_list[idx] = s_pos

    valid['short_pos'] = short_pos_list
    valid['net_pos']   = valid['long_pos'] + valid['short_pos']
    lp  = valid['long_pos'].shift(1).fillna(0.0)
    sp  = valid['short_pos'].shift(1).fillna(0.0)
    np_ = valid['net_pos'].shift(1).fillna(0.0)
    valid['long_day_ret']  = valid['daily_ret'] * lp
    valid['short_day_ret'] = valid['daily_ret'] * sp
    valid['net_day_ret']   = valid['daily_ret'] * np_

    r   = valid['net_day_ret']
    cum = (1 + r).prod() - 1
    ann = (1 + cum) ** (365 / TOTAL_DAYS) - 1
    sr  = r.mean() / r.std() * np.sqrt(365) if r.std() > 0 else 0
    eq  = (1 + r).cumprod()
    dd  = (eq / eq.cummax() - 1).min()
    cl  = (1 + valid['long_day_ret']).prod() - 1
    cs  = (1 + valid['short_day_ret']).prod() - 1
    m_r = valid.groupby('ym')['net_day_ret'].apply(lambda x: (1+x).prod()-1)
    mwr = (m_r > 0).mean()

    return {
        'cum': cum, 'ann': ann, 'sharpe': sr, 'dd': dd,
        'long_cum': cl, 'short_cum': cs, 'mwr': mwr,
        'daily': valid,
    }


# ── Step 3：穷举搜索（空头杠杆最高5x，限制DD≤30%）────────
print()
print('=' * 70)
print('  空头分级杠杆穷举（bear_n×bw_threshold，多头固定5票1.5x/6票2x）')
print('  约束：最大回撤 ≤ -30%')
print('=' * 70)

long_lev = {4: 1.0, 5: 1.5, 6: 2.0}
lev_options = [1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0]

best_results = []

for l4, l5, l6 in product(lev_options, lev_options, lev_options):
    if not (l4 <= l5 <= l6):    # 票数越多杠杆越高或相等
        continue
    bnm = {4: l4, 5: l5, 6: l6}
    res = run_combined_short_lev(df, profiles,
                                  long_lev_map=long_lev,
                                  bear_n_lev_map=bnm,
                                  long_stop=-0.15,
                                  short_stop=-0.08)
    if res['dd'] >= -0.30:      # 满足回撤约束
        best_results.append({
            'bear4': l4, 'bear5': l5, 'bear6': l6,
            'cum': res['cum'], 'ann': res['ann'],
            'sharpe': res['sharpe'], 'dd': res['dd'],
            'long_cum': res['long_cum'], 'short_cum': res['short_cum'],
            'mwr': res['mwr'],
        })

best_df = pd.DataFrame(best_results).sort_values('cum', ascending=False)
print(f'满足DD≤30%的方案共 {len(best_df)} 个，Top 15：')
print()
top15 = best_df.head(15)
print(f'  {"4票":>5}  {"5票":>5}  {"6票":>5}  {"累计收益":>9}  {"年化":>7}  {"夏普":>6}  {"最大回撤":>9}  {"月胜率":>7}')
print('  ' + '-' * 65)
for _, r in top15.iterrows():
    print(f'  {r.bear4:>4.1f}x  {r.bear5:>4.1f}x  {r.bear6:>4.1f}x  '
          f'{r.cum*100:>+8.1f}%  {r.ann*100:>+6.1f}%  '
          f'{r.sharpe:>6.2f}  {r.dd*100:>+8.1f}%  {r.mwr*100:>6.1f}%')


# ── Step 4：最优方案详细结果 ──────────────────────────────
best = best_df.iloc[0]
print()
print(f'最优方案: 4票{best.bear4}x / 5票{best.bear5}x / 6票{best.bear6}x')
print(f'  累计: {best.cum*100:.1f}%  夏普: {best.sharpe:.2f}  '
      f'回撤: {best.dd*100:.1f}%  月胜率: {best.mwr*100:.1f}%')
print(f'  多头贡献: {best.long_cum*100:.1f}%  空头贡献: {best.short_cum*100:.1f}%')

# 与基准逐月对比
print()
print('最优方案逐月明细（vs 无杠杆基准 121.2%）:')
opt_res = run_combined_short_lev(df, profiles,
    long_lev_map=long_lev,
    bear_n_lev_map={4: int(best.bear4*2)/2, 5: int(best.bear5*2)/2, 6: int(best.bear6*2)/2},
    long_stop=-0.15, short_stop=-0.08)

base_res = run_combined_short_lev(df, profiles,
    long_lev_map={4:1.0,5:1.0,6:1.0},
    bear_n_lev_map={4:1.0,5:1.0,6:1.0},
    long_stop=None, short_stop=-0.08)

opt_d  = opt_res['daily']
base_d = base_res['daily']
months_list = sorted(opt_d['ym'].unique())

print(f'  {"月份":>8}  {"BTC":>7}  {"基准":>7}  {"最优杠杆":>9}  {"差值":>7}')
for ym in months_list:
    om = opt_d[opt_d['ym']==ym]['net_day_ret']
    bm = base_d[base_d['ym']==ym]['net_day_ret']
    opt_r  = (1+om).prod()-1
    base_r = (1+bm).prod()-1
    btc_r  = (opt_d[opt_d['ym']==ym]['close'].iloc[-1] /
               opt_d[opt_d['ym']==ym]['close'].iloc[0] - 1)
    diff = opt_r - base_r
    mark = ' <-- 加杠杆放大亏损' if diff < -0.05 else (' <-- 加杠杆扩大收益' if diff > 0.05 else '')
    print(f'  {str(ym):>8}  {btc_r*100:>+6.1f}%  {base_r*100:>+6.1f}%  '
          f'{opt_r*100:>+8.1f}%  {diff*100:>+6.1f}%{mark}')
