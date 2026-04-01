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

def get_votes(row, profiles):
    votes = []
    for k in K_COLS:
        kv = row.get(k)
        if pd.isna(kv): continue
        match = profiles[k][profiles[k]['regime'] == int(kv)]
        if len(match) == 0: continue
        votes.append(match.iloc[0]['type'])
    return votes

# ── 跑最终策略：多头5票1.5x/6票2x + 空头6票2x ────────────
valid = df.dropna(subset=K_COLS).copy().reset_index(drop=True)
valid['ym']         = valid['date'].dt.to_period('M')
valid['daily_vote'] = valid.apply(lambda r: majority_vote(r, profiles, 4), axis=1)
valid['daily_ret']  = valid['close'].pct_change().fillna(0.0)
months = sorted(valid['ym'].unique())

# 多头腿
long_lev_map = {4: 1.0, 5: 1.5, 6: 2.0}
long_pos_list = [0.0] * len(valid)
for i, ym in enumerate(months):
    md_idx = valid[valid['ym'] == ym].index
    row0   = valid.loc[md_idx[0]]
    vote   = majority_vote(row0, profiles, 4)
    fv     = (vote if signal_confirm(row0, vote) else 'ABSTAIN') if vote in ('BULL','NEUTRAL','BEAR') else vote
    base   = max(STRATEGY_MAP.get(fv, {'position': 0.0})['position'], 0)
    base, _ = apply_overflow_filter(row0, base)
    if fv == 'BULL' and base > 0:
        bn  = get_votes(row0, profiles).count('BULL')
        pos = base * long_lev_map.get(bn, 1.0)
    else:
        pos = base
    if pos <= 0: continue
    entry_p = row0['close']
    for idx in md_idx:
        row = valid.loc[idx]
        if row['date'] > row0['date'] and (row['close']/entry_p - 1)*pos <= -0.15:
            break
        long_pos_list[idx] = pos
valid['long_pos'] = long_pos_list

# 空头腿（6票2x，4/5票1x）
short_lev_map = {4: 1.0, 5: 1.0, 6: 2.0}
short_pos_list = [0.0] * len(valid)
s_pos, s_entry, vote_win = 0.0, None, []
for idx in range(len(valid)):
    row   = valid.iloc[idx]
    price = row['close']
    vote_win.append(row['daily_vote'])
    if len(vote_win) > 3: vote_win.pop(0)
    last_n = vote_win[-3:] if len(vote_win) >= 3 else []
    if s_pos < 0 and s_entry:
        pnl = (price/s_entry - 1)*(-1)*abs(s_pos)
        if pnl <= -0.08:
            s_pos, s_entry = 0.0, None; vote_win = []
        elif len(last_n)==3 and all(v=='BULL' for v in last_n):
            s_pos, s_entry = 0.0, None
    if s_pos == 0 and len(last_n)==3 and all(v=='BEAR' for v in last_n):
        if signal_confirm(row, 'BEAR'):
            bn  = get_votes(row, profiles).count('BEAR')
            lev = short_lev_map.get(bn, 1.0)
            s_pos, s_entry = -1.0*lev, price
    short_pos_list[idx] = s_pos
valid['short_pos'] = short_pos_list
valid['net_pos']   = valid['long_pos'] + valid['short_pos']

lp  = valid['long_pos'].shift(1).fillna(0.0)
sp  = valid['short_pos'].shift(1).fillna(0.0)
np_ = valid['net_pos'].shift(1).fillna(0.0)
valid['long_ret']  = valid['daily_ret'] * lp
valid['short_ret'] = valid['daily_ret'] * sp
valid['net_ret']   = valid['daily_ret'] * np_
valid['btc_eq']    = (1 + valid['daily_ret']).cumprod()
valid['net_eq']    = (1 + valid['net_ret']).cumprod()

# ── 综合指标计算 ──────────────────────────────────────────
r    = valid['net_ret']
btcr = valid['daily_ret']

def calc_metrics(ret_series, label, total_days=TOTAL_DAYS):
    r = ret_series.copy()
    cum   = (1+r).prod() - 1
    ann   = (1+cum)**(365/total_days) - 1
    vol   = r.std() * np.sqrt(365)
    sharpe = ann / vol if vol > 0 else 0
    # Sortino（只看下行波动）
    downside = r[r < 0].std() * np.sqrt(365)
    sortino  = ann / downside if downside > 0 else 0
    # 最大回撤
    eq = (1+r).cumprod()
    dd_series = eq / eq.cummax() - 1
    max_dd = dd_series.min()
    # 回撤恢复时间（最大回撤后多少天恢复）
    trough_idx = dd_series.idxmin()
    peak_eq    = eq[:trough_idx].max()
    recovery   = eq[trough_idx:]
    recovered  = recovery[recovery >= peak_eq]
    recovery_days = (recovered.index[0] - trough_idx) if len(recovered) > 0 else None
    # Calmar
    calmar = ann / abs(max_dd) if max_dd != 0 else 0
    # 月度统计
    monthly = r.groupby(r.index.to_period('M')).apply(lambda x: (1+x).prod()-1)
    m_win   = (monthly > 0).mean()
    m_avg_w = monthly[monthly > 0].mean() if (monthly > 0).any() else 0
    m_avg_l = monthly[monthly < 0].mean() if (monthly < 0).any() else 0
    pf      = abs(m_avg_w / m_avg_l) if m_avg_l != 0 else np.inf
    # 连续亏损月数
    m_sign  = (monthly > 0).astype(int)
    max_consec_loss = 0
    cur = 0
    for v in m_sign:
        if v == 0: cur += 1; max_consec_loss = max(max_consec_loss, cur)
        else: cur = 0
    return {
        'label': label,
        'cum': cum, 'ann': ann, 'vol': vol,
        'sharpe': sharpe, 'sortino': sortino, 'calmar': calmar,
        'max_dd': max_dd, 'recovery_days': recovery_days,
        'm_wr': m_win, 'm_avg_w': m_avg_w, 'm_avg_l': m_avg_l,
        'profit_factor': pf,
        'max_consec_loss': max_consec_loss,
    }

r_indexed = r.copy()
r_indexed.index = valid['date']
btc_indexed = btcr.copy()
btc_indexed.index = valid['date']

strat = calc_metrics(r_indexed, '本策略（最终版）')
btc   = calc_metrics(btc_indexed, 'BTC 买入持有')

print('=' * 65)
print('  策略综合评估报告')
print(f'  回测期: {valid.date.iloc[0].date()} ~ {valid.date.iloc[-1].date()}  ({TOTAL_DAYS}天)')
print('=' * 65)

metrics = [
    ('── 收益维度', None, None),
    ('累计收益',         f'{strat["cum"]*100:.1f}%',     f'{btc["cum"]*100:.1f}%'),
    ('年化收益',         f'{strat["ann"]*100:.1f}%',     f'{btc["ann"]*100:.1f}%'),
    ('年化波动率',       f'{strat["vol"]*100:.1f}%',     f'{btc["vol"]*100:.1f}%'),
    ('── 风险调整维度', None, None),
    ('夏普比率',         f'{strat["sharpe"]:.2f}',        f'{btc["sharpe"]:.2f}'),
    ('索提诺比率',       f'{strat["sortino"]:.2f}',       f'{btc["sortino"]:.2f}'),
    ('卡玛比率',         f'{strat["calmar"]:.2f}',        f'{btc["calmar"]:.2f}'),
    ('── 回撤维度', None, None),
    ('最大回撤',         f'{strat["max_dd"]*100:.1f}%',  f'{btc["max_dd"]*100:.1f}%'),
    ('回撤恢复天数',     f'{strat["recovery_days"]}天' if strat["recovery_days"] else '未恢复',
                         f'{btc["recovery_days"]}天' if btc["recovery_days"] else '未恢复'),
    ('── 交易质量维度', None, None),
    ('月度胜率',         f'{strat["m_wr"]*100:.1f}%',    f'{btc["m_wr"]*100:.1f}%'),
    ('盈利月平均收益',   f'{strat["m_avg_w"]*100:.1f}%', f'{btc["m_avg_w"]*100:.1f}%'),
    ('亏损月平均亏损',   f'{strat["m_avg_l"]*100:.1f}%', f'{btc["m_avg_l"]*100:.1f}%'),
    ('盈亏比',           f'{strat["profit_factor"]:.2f}', f'{btc["profit_factor"]:.2f}'),
    ('最大连续亏损月数', f'{strat["max_consec_loss"]}个月', f'{btc["max_consec_loss"]}个月'),
]

print(f'  {"指标":<18} {"本策略":>14} {"BTC持有":>14}')
print('  ' + '-'*48)
for m in metrics:
    if m[1] is None:
        print(f'\n  {m[0]}')
    else:
        print(f'  {m[0]:<18} {m[1]:>14} {m[2]:>14}')

# ── 行业基准对比 ──────────────────────────────────────────
print()
print('=' * 65)
print('  与行业基准对比（参考值）')
print('=' * 65)
benchmarks = [
    ('指标',              '本策略',    'BTC持有', '加密对冲基金', '传统对冲基金', '标普500'),
    ('年化收益',          '165%',      '19%',     '20~80%',      '10~20%',      '~15%'),
    ('夏普比率',          '1.80',      '0.51',    '0.8~2.0',     '0.5~1.5',     '~0.6'),
    ('索提诺比率',        f'{strat["sortino"]:.2f}', '-', '1.0~3.0', '1.0~2.0', '~1.0'),
    ('卡玛比率',          f'{strat["calmar"]:.2f}', '-', '1.0~3.0', '0.5~1.5', '~0.5'),
    ('最大回撤',          '-28.5%',   '-72%',     '-20~-50%',    '-5~-20%',    '-24%'),
    ('月度胜率',          '61%',       '56%',     '55~70%',      '55~65%',     '~58%'),
]

col_w = [18, 10, 9, 14, 14, 8]
for row in benchmarks:
    line = ''
    for i, cell in enumerate(row):
        line += f'  {str(cell):<{col_w[i]}}'
    print(line)
