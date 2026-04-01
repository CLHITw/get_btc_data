"""
空头特征过滤测试（不限票数）
测试：macd_hist_z<0, volume_log_z>0, 两者组合, macd<-1.0, macd<-1.0+vol>0
"""
import sys, io
import pandas as pd
import numpy as np
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from regime_strategy import (load_and_prepare, profile_regimes, majority_vote,
    signal_confirm, apply_overflow_filter, STRATEGY_MAP, K_COLS)

df = load_and_prepare('btc.xlsx')
profiles = profile_regimes(df)
TOTAL_DAYS = 539
SHORT_STOP = -0.08; BEAR_CONSEC = 3
LONG_LEV_MAP = {4:1.0, 5:1.5, 6:2.0}
SHORT_LEV_MAP = {4:1.0, 5:1.0, 6:2.0}
MIN_AGREE = 4; LONG_STOP = -0.15


def get_counts(row):
    bull_n = bear_n = 0
    for k in K_COLS:
        kv = row.get(k)
        if pd.isna(kv): continue
        match = profiles[k][profiles[k]['regime'] == int(kv)]
        if len(match) == 0: continue
        t = match.iloc[0]['type']
        if t == 'BULL': bull_n += 1
        elif t == 'BEAR': bear_n += 1
    return bull_n, bear_n


def run(df, profiles, midmonth_bear=4, short_min_bear=4,
        short_macd_lt=None, short_vol_gt=None, label=''):

    valid = df.dropna(subset=K_COLS).copy().reset_index(drop=True)
    valid['ym'] = valid['date'].dt.to_period('M')
    valid['daily_vote'] = valid.apply(lambda r: majority_vote(r, profiles, MIN_AGREE), axis=1)
    valid['daily_ret'] = valid['close'].pct_change().fillna(0.0)
    months = sorted(valid['ym'].unique())

    # 多头腿（含方案B）
    long_pos_list = [0.0] * len(valid)
    for i, ym in enumerate(months):
        md_idx = valid[valid['ym'] == ym].index
        row0 = valid.loc[md_idx[0]]
        vote = majority_vote(row0, profiles, MIN_AGREE)
        fv = vote if vote not in ('BULL','NEUTRAL','BEAR') else (
             vote if signal_confirm(row0, vote) else 'ABSTAIN')
        base_pos = max(STRATEGY_MAP.get(fv, {'position': 0.0})['position'], 0)
        base_pos, _ = apply_overflow_filter(row0, base_pos)
        bull_n, _ = get_counts(row0)
        pos = base_pos * LONG_LEV_MAP.get(bull_n, 1.0) if fv == 'BULL' and base_pos > 0 else base_pos
        if pos <= 0:
            for idx in md_idx: long_pos_list[idx] = 0.0
            continue
        entry_p = row0['close']; exited = False; vbuf = []
        for j, idx in enumerate(md_idx):
            row = valid.loc[idx]
            if exited: long_pos_list[idx] = 0.0; continue
            if j == 0: long_pos_list[idx] = pos; vbuf.append(row['daily_vote']); continue
            if (row['low'] / entry_p - 1) * pos <= LONG_STOP:
                exited = True; long_pos_list[idx] = 0.0; continue
            vbuf.append(row['daily_vote'])
            if midmonth_bear > 0 and len(vbuf) >= midmonth_bear:
                if all(v == 'BEAR' for v in vbuf[-midmonth_bear:]):
                    exited = True; long_pos_list[idx] = 0.0; continue
            long_pos_list[idx] = pos
    valid['long_pos'] = long_pos_list

    # 空头腿（含特征过滤）
    short_pos_list = [0.0] * len(valid)
    trade_log = []
    s_pos = 0.0; s_entry = None; s_entry_idx = None
    vote_win = []; cooldown = 0

    for idx in range(len(valid)):
        row = valid.iloc[idx]; price = row['close']; today = row['daily_vote']
        if cooldown > 0: cooldown -= 1
        vote_win.append(today)
        if len(vote_win) > BEAR_CONSEC: vote_win.pop(0)
        last_n = vote_win[-BEAR_CONSEC:] if len(vote_win) >= BEAR_CONSEC else []

        if s_pos < 0 and s_entry is not None:
            lev = abs(s_pos)
            _, bn = get_counts(valid.iloc[s_entry_idx])
            er = valid.iloc[s_entry_idx]
            if (row['high'] / s_entry - 1) * (-1) * lev <= SHORT_STOP:
                pnl = (s_entry / row['close'] - 1) * lev * 100
                trade_log.append({
                    '入场': str(er['date'])[:10], '出场': str(row['date'])[:10],
                    '熊票': bn, 'lev': lev,
                    'macd_h': round(float(er.get('macd_hist_z', np.nan)), 3),
                    'vol_z':  round(float(er.get('volume_log_z', np.nan)), 3),
                    '收益%': round(pnl, 1), '原因': '高点止损'})
                s_pos, s_entry, s_entry_idx = 0.0, None, None
                vote_win = []; cooldown = 1
            elif len(last_n) == BEAR_CONSEC and all(v == 'BULL' for v in last_n):
                pnl = (s_entry / price - 1) * lev * 100
                trade_log.append({
                    '入场': str(er['date'])[:10], '出场': str(row['date'])[:10],
                    '熊票': bn, 'lev': lev,
                    'macd_h': round(float(er.get('macd_hist_z', np.nan)), 3),
                    'vol_z':  round(float(er.get('volume_log_z', np.nan)), 3),
                    '收益%': round(pnl, 1), '原因': '翻转平仓'})
                s_pos, s_entry, s_entry_idx = 0.0, None, None

        if s_pos == 0.0 and cooldown == 0:
            if len(last_n) == BEAR_CONSEC and all(v == 'BEAR' for v in last_n):
                if signal_confirm(row, 'BEAR'):
                    _, bear_n = get_counts(row)
                    ok = (bear_n >= short_min_bear)
                    if ok and short_macd_lt is not None:
                        mh = row.get('macd_hist_z', np.nan)
                        if pd.isna(mh) or mh >= short_macd_lt: ok = False
                    if ok and short_vol_gt is not None:
                        vl = row.get('volume_log_z', np.nan)
                        if pd.isna(vl) or vl <= short_vol_gt: ok = False
                    if ok:
                        lev = SHORT_LEV_MAP.get(bear_n, 1.0)
                        s_pos = -1.0 * lev; s_entry = price; s_entry_idx = idx

        short_pos_list[idx] = s_pos

    # 持仓中
    if s_pos < 0 and s_entry_idx is not None:
        lev = abs(s_pos)
        er = valid.iloc[s_entry_idx]
        _, bn = get_counts(er)
        pnl = (s_entry / valid.iloc[-1]['close'] - 1) * lev * 100
        trade_log.append({
            '入场': str(er['date'])[:10], '出场': '持仓中',
            '熊票': bn, 'lev': lev,
            'macd_h': round(float(er.get('macd_hist_z', np.nan)), 3),
            'vol_z':  round(float(er.get('volume_log_z', np.nan)), 3),
            '收益%': round(pnl, 1), '原因': '持仓中'})

    valid['short_pos'] = short_pos_list
    valid['net_pos'] = valid['long_pos'] + valid['short_pos']
    lp = valid['long_pos'].shift(1).fillna(0.0)
    sp = valid['short_pos'].shift(1).fillna(0.0)
    valid['long_day_ret'] = valid['daily_ret'] * lp
    valid['short_day_ret'] = valid['daily_ret'] * sp
    valid['net_day_ret'] = valid['daily_ret'] * (lp + sp)
    valid['equity'] = (1 + valid['net_day_ret']).cumprod()
    return valid, trade_log


def perf(valid, name):
    r = valid['net_day_ret']; lr = valid['long_day_ret']; sr = valid['short_day_ret']
    cum_net = (1+r).prod()-1; cum_long = (1+lr).prod()-1; cum_short = (1+sr).prod()-1
    ann = (1+cum_net)**(365/TOTAL_DAYS)-1
    shp = r.mean()/r.std()*np.sqrt(365) if r.std()>0 else 0
    eq = (1+r).cumprod()
    lp_ = valid['long_pos'].shift(1).fillna(0.0)
    sp_ = valid['short_pos'].shift(1).fillna(0.0)
    pcl = valid['close'].shift(1)
    intra = (valid['low']/pcl-1)*lp_ + (valid['high']/pcl-1)*sp_
    ieq = eq.shift(1).fillna(1.0)*(1+intra)
    dd = min((eq/eq.cummax()-1).min(), (ieq/eq.cummax()-1).min())
    v2 = valid.copy(); v2['ym'] = v2['date'].dt.to_period('M')
    m_wr = (pd.Series([(1+v2[v2['ym']==ym]['net_day_ret']).prod()-1
                        for ym in sorted(v2['ym'].unique())]) > 0).mean()
    short_n = int((valid['short_pos'].diff().fillna(0) < 0).sum())
    fi = (valid['net_pos'] != 0).idxmax()
    min_eq = valid['equity'].iloc[fi:].min()
    min_intra = ieq.iloc[fi:].min()
    return {
        '方案': name, '净累计': f'{cum_net*100:.1f}%',
        '多头': f'{cum_long*100:.1f}%', '空头': f'{cum_short*100:.1f}%',
        '年化': f'{ann*100:.1f}%', '夏普': round(shp,2),
        'MDD': f'{dd*100:.1f}%', '月胜率': f'{m_wr*100:.1f}%',
        '空头次数': short_n,
        '首仓后最低(收盘)': f'{min_eq:.3f}',
        '曾亏损': '✅否' if min_eq >= 1.0 else f'是({min_eq:.3f})'
    }


scenarios = [
    # (label,              midmonth, min_bear, macd_lt, vol_gt)
    ('基准+B 无过滤',       4, 4, None,  None ),
    ('macd_h < 0',         4, 4,  0.0,  None ),
    ('vol_z > 0',          4, 4, None,   0.0 ),
    ('macd_h<0 & vol_z>0', 4, 4,  0.0,   0.0 ),
    ('macd_h < -1.0',      4, 4, -1.0,  None ),
    ('macd_h<-1.0 & vol_z>0', 4, 4, -1.0, 0.0),
]

print('=' * 100)
print('  空头特征过滤回测（空头不限票数，多头含方案B 4天）')
print('=' * 100)
results = []; all_logs = {}
for (label, mb, smb, sm, sv) in scenarios:
    v, tlog = run(df, profiles, midmonth_bear=mb, short_min_bear=smb,
                  short_macd_lt=sm, short_vol_gt=sv, label=label)
    results.append(perf(v, label))
    all_logs[label] = tlog

print(pd.DataFrame(results).set_index('方案').to_string())

print()
print('=' * 100)
print('  各方案保留的空头交易明细')
print('=' * 100)
for label, _, _, _, _ in scenarios:
    logs = all_logs[label]
    wins = [t for t in logs if t['收益%'] > 2]
    loss = [t for t in logs if t['收益%'] <= 2]
    print(f'\n── {label}  ({len(logs)}笔 | 盈利{len(wins)}笔 | 亏损/微利{len(loss)}笔)')
    if logs:
        print(pd.DataFrame(logs)[['入场','出场','熊票','lev','macd_h','vol_z','收益%','原因']].to_string(index=False))
