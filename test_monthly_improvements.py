import pandas as pd
import numpy as np
import sys
import io
from collections import Counter
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from regime_strategy import (load_and_prepare, profile_regimes, majority_vote,
    signal_confirm, apply_overflow_filter, STRATEGY_MAP, K_COLS)

df = load_and_prepare('btc.xlsx')
profiles = profile_regimes(df)

TOTAL_DAYS = 539  # 数据天数，用于年化

def get_vote_detail(row, profiles):
    votes = []
    for k in K_COLS:
        kv = row.get(k)
        if pd.isna(kv):
            continue
        match = profiles[k][profiles[k]['regime'] == int(kv)]
        if len(match) == 0:
            continue
        votes.append(match.iloc[0]['type'])
    return votes.count('BULL'), votes.count('BEAR'), votes.count('NEUTRAL'), len(votes)


# ── 原版基准 ──────────────────────────────────────────────
def run_base(df, profiles, min_agree=4):
    valid = df.dropna(subset=K_COLS).copy()
    valid['ym'] = valid['date'].dt.to_period('M')
    records = []
    months = sorted(valid['ym'].unique())
    for i, ym in enumerate(months):
        md = valid[valid['ym'] == ym]
        row0 = md.iloc[0]
        vote = majority_vote(row0, profiles, min_agree)
        if vote in ('BULL', 'NEUTRAL', 'BEAR'):
            fv = vote if signal_confirm(row0, vote) else 'ABSTAIN'
        else:
            fv = vote
        pos = max(STRATEGY_MAP.get(fv, {'position': 0.0})['position'], 0)
        pos, _ = apply_overflow_filter(row0, pos)
        entry_p = row0['close']
        if i + 1 < len(months):
            exit_p = valid[valid['ym'] == months[i + 1]].iloc[0]['close']
        else:
            exit_p = md.iloc[-1]['close']
        strat_r = (exit_p / entry_p - 1) * pos
        records.append({'month': str(ym), 'pos': pos, 'strat_r': strat_r,
                        'vote': fv, 'stopped': False})
    return pd.DataFrame(records)


# ── 改进① 月初3天投票取多数 ──────────────────────────────
def run_v1_3day(df, profiles, min_agree=4):
    valid = df.dropna(subset=K_COLS).copy()
    valid['ym'] = valid['date'].dt.to_period('M')
    records = []
    months = sorted(valid['ym'].unique())
    for i, ym in enumerate(months):
        md = valid[valid['ym'] == ym]
        first3 = md.iloc[:3]
        day_votes = [majority_vote(r, profiles, min_agree) for _, r in first3.iterrows()]
        vc = Counter(v for v in day_votes if v != 'ABSTAIN')
        if vc:
            top_vote, top_cnt = vc.most_common(1)[0]
            vote = top_vote if top_cnt >= 2 else 'ABSTAIN'
        else:
            vote = 'ABSTAIN'
        row0 = first3.iloc[0]
        if vote in ('BULL', 'NEUTRAL', 'BEAR'):
            fv = vote if signal_confirm(row0, vote) else 'ABSTAIN'
        else:
            fv = vote
        pos = max(STRATEGY_MAP.get(fv, {'position': 0.0})['position'], 0)
        pos, _ = apply_overflow_filter(row0, pos)
        entry_p = row0['close']
        if i + 1 < len(months):
            exit_p = valid[valid['ym'] == months[i + 1]].iloc[0]['close']
        else:
            exit_p = md.iloc[-1]['close']
        strat_r = (exit_p / entry_p - 1) * pos
        records.append({'month': str(ym), 'pos': pos, 'strat_r': strat_r,
                        'vote': fv, 'day_votes': str(day_votes), 'stopped': False})
    return pd.DataFrame(records)


# ── 改进② 月内止损 -8% ───────────────────────────────────
def run_v2_stoploss(df, profiles, min_agree=4, stop=-0.08):
    valid = df.dropna(subset=K_COLS).copy()
    valid['ym'] = valid['date'].dt.to_period('M')
    records = []
    months = sorted(valid['ym'].unique())
    for i, ym in enumerate(months):
        md = valid[valid['ym'] == ym]
        row0 = md.iloc[0]
        vote = majority_vote(row0, profiles, min_agree)
        if vote in ('BULL', 'NEUTRAL', 'BEAR'):
            fv = vote if signal_confirm(row0, vote) else 'ABSTAIN'
        else:
            fv = vote
        pos = max(STRATEGY_MAP.get(fv, {'position': 0.0})['position'], 0)
        pos, _ = apply_overflow_filter(row0, pos)
        entry_p = row0['close']
        stopped = False
        exit_p = None
        if pos > 0:
            for _, dr in md.iterrows():
                if dr['date'] <= row0['date']:
                    continue
                if (dr['low'] / entry_p - 1) <= stop:   # 用日内最低价
                    exit_p = dr['low']
                    stopped = True
                    break
        if not stopped:
            if i + 1 < len(months):
                exit_p = valid[valid['ym'] == months[i + 1]].iloc[0]['close']
            else:
                exit_p = md.iloc[-1]['close']
        strat_r = (exit_p / entry_p - 1) * pos
        records.append({'month': str(ym), 'pos': pos, 'strat_r': strat_r,
                        'vote': fv, 'stopped': stopped})
    return pd.DataFrame(records)


# ── 改进③ 按票数调仓位 ───────────────────────────────────
def run_v3_conviction(df, profiles, min_agree=4):
    valid = df.dropna(subset=K_COLS).copy()
    valid['ym'] = valid['date'].dt.to_period('M')
    records = []
    months = sorted(valid['ym'].unique())
    for i, ym in enumerate(months):
        md = valid[valid['ym'] == ym]
        row0 = md.iloc[0]
        vote = majority_vote(row0, profiles, min_agree)
        bull_n, bear_n, neutral_n, total = get_vote_detail(row0, profiles)
        if vote in ('BULL', 'NEUTRAL', 'BEAR'):
            fv = vote if signal_confirm(row0, vote) else 'ABSTAIN'
        else:
            fv = vote
        if fv == 'BULL':
            pos = {4: 0.60, 5: 0.80, 6: 1.00}.get(bull_n, 0.60)
        elif fv == 'NEUTRAL':
            pos = {4: 0.30, 5: 0.40, 6: 0.50}.get(neutral_n, 0.30)
        else:
            pos = 0.0
        pos, _ = apply_overflow_filter(row0, pos)
        entry_p = row0['close']
        if i + 1 < len(months):
            exit_p = valid[valid['ym'] == months[i + 1]].iloc[0]['close']
        else:
            exit_p = md.iloc[-1]['close']
        strat_r = (exit_p / entry_p - 1) * pos
        records.append({'month': str(ym), 'pos': pos, 'strat_r': strat_r,
                        'vote': fv, 'bull_n': bull_n, 'stopped': False})
    return pd.DataFrame(records)


# ── 综合版 ①+②+③ ─────────────────────────────────────────
def run_v_all(df, profiles, min_agree=4, stop=-0.08):
    valid = df.dropna(subset=K_COLS).copy()
    valid['ym'] = valid['date'].dt.to_period('M')
    records = []
    months = sorted(valid['ym'].unique())
    for i, ym in enumerate(months):
        md = valid[valid['ym'] == ym]
        first3 = md.iloc[:3]
        day_votes = [majority_vote(r, profiles, min_agree) for _, r in first3.iterrows()]
        vc = Counter(v for v in day_votes if v != 'ABSTAIN')
        if vc:
            top_vote, top_cnt = vc.most_common(1)[0]
            vote = top_vote if top_cnt >= 2 else 'ABSTAIN'
        else:
            vote = 'ABSTAIN'
        row0 = first3.iloc[0]
        bull_n, bear_n, neutral_n, total = get_vote_detail(row0, profiles)
        if vote in ('BULL', 'NEUTRAL', 'BEAR'):
            fv = vote if signal_confirm(row0, vote) else 'ABSTAIN'
        else:
            fv = vote
        if fv == 'BULL':
            pos = {4: 0.60, 5: 0.80, 6: 1.00}.get(bull_n, 0.60)
        elif fv == 'NEUTRAL':
            pos = {4: 0.30, 5: 0.40, 6: 0.50}.get(neutral_n, 0.30)
        else:
            pos = 0.0
        pos, _ = apply_overflow_filter(row0, pos)
        entry_p = row0['close']
        stopped = False
        exit_p = None
        if pos > 0:
            for _, dr in md.iterrows():
                if dr['date'] <= row0['date']:
                    continue
                if (dr['low'] / entry_p - 1) <= stop:   # 用日内最低价
                    exit_p = dr['low']
                    stopped = True
                    break
        if not stopped:
            if i + 1 < len(months):
                exit_p = valid[valid['ym'] == months[i + 1]].iloc[0]['close']
            else:
                exit_p = md.iloc[-1]['close']
        strat_r = (exit_p / entry_p - 1) * pos
        records.append({'month': str(ym), 'pos': pos, 'strat_r': strat_r,
                        'vote': fv, 'bull_n': bull_n, 'stopped': stopped})
    return pd.DataFrame(records)


# ── 绩效汇总 ─────────────────────────────────────────────
def perf(bt, name):
    r = bt['strat_r']
    active = bt[bt['pos'] > 0]
    cum = (1 + r).prod() - 1
    ann = (1 + cum) ** (365 / TOTAL_DAYS) - 1
    wr  = (active['strat_r'] > 0).mean() if len(active) > 0 else 0
    sr  = (r.mean() / r.std() * np.sqrt(12)) if r.std() > 0 else 0
    dd  = ((1 + r).cumprod() / (1 + r).cumprod().cummax() - 1).min()
    stops = bt['stopped'].sum()
    return {
        '策略': name,
        '累计收益': f'{cum*100:.1f}%',
        '年化收益': f'{ann*100:.1f}%',
        '持仓月胜率': f'{wr*100:.1f}%',
        '夏普(月频)': round(sr, 2),
        '最大回撤': f'{dd*100:.1f}%',
        '止损次数': int(stops),
    }


bt0  = run_base(df, profiles)
bt1  = run_v1_3day(df, profiles)
bt2  = run_v2_stoploss(df, profiles)
bt3  = run_v3_conviction(df, profiles)
bt_a = run_v_all(df, profiles)

results = [
    perf(bt0,  '原版（基准）'),
    perf(bt1,  '① 月初3天投票'),
    perf(bt2,  '② 月内止损-8%'),
    perf(bt3,  '③ 按票数调仓'),
    perf(bt_a, '综合 ①+②+③'),
]
summary = pd.DataFrame(results).set_index('策略')
print('=' * 70)
print('  月度多头策略改进测试')
print('=' * 70)
print(summary.to_string())

# ── 逐月对比 ─────────────────────────────────────────────
print('\n' + '=' * 90)
print('  逐月净收益对比')
print('=' * 90)

valid = df.dropna(subset=K_COLS).copy()
valid['ym'] = valid['date'].dt.to_period('M')
months = sorted(valid['ym'].unique())
btc_rets = []
for i, ym in enumerate(months):
    md = valid[valid['ym'] == ym]
    ep = md.iloc[0]['close']
    if i + 1 < len(months):
        xp = valid[valid['ym'] == months[i + 1]].iloc[0]['close']
    else:
        xp = md.iloc[-1]['close']
    btc_rets.append((xp / ep - 1))

comp = pd.DataFrame({
    'month':   bt0['month'],
    'vote':    bt0['vote'],
    '原版':    bt0['strat_r'],
    '①3天':   bt1['strat_r'],
    '②止损':  bt2['strat_r'],
    '③票数':  bt3['strat_r'],
    '综合':    bt_a['strat_r'],
    'BTC':     btc_rets,
})
for c in ['原版', '①3天', '②止损', '③票数', '综合', 'BTC']:
    comp[c] = comp[c].apply(lambda x: f'{x*100:.1f}%')

print(comp.to_string(index=False))
