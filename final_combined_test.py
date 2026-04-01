"""
最终综合改进测试
================
多头：方案B（月中4天BEAR提前平仓）
空头：① 仅6票开空  ② macd_hist_z < 阈值  ③ 组合
额外：统计从首次开仓起净值是否曾跌破1.0（即是否曾整体亏损）
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

SHORT_STOP    = -0.08
BEAR_CONSEC   = 3
LONG_LEV_MAP  = {4: 1.0, 5: 1.5, 6: 2.0}
SHORT_LEV_MAP = {4: 1.0, 5: 1.0, 6: 2.0}
MIN_AGREE     = 4
LONG_STOP     = -0.15


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


def run(df, profiles,
        midmonth_bear=4,         # 多头月中N天BEAR平仓，0=不启用
        short_min_bear=4,        # 空头最少熊票数（4=原版，6=仅6票）
        short_macd_max=None,     # 空头入场时 macd_hist_z 上限（越负越好）
        label=''):

    valid = df.dropna(subset=K_COLS).copy().reset_index(drop=True)
    valid['ym']         = valid['date'].dt.to_period('M')
    valid['daily_vote'] = valid.apply(lambda r: majority_vote(r, profiles, MIN_AGREE), axis=1)
    valid['daily_ret']  = valid['close'].pct_change().fillna(0.0)
    months = sorted(valid['ym'].unique())

    # ── 多头腿 ───────────────────────────────────────────────────────
    long_pos_list = [0.0] * len(valid)
    for i, ym in enumerate(months):
        md_idx = valid[valid['ym'] == ym].index
        row0   = valid.loc[md_idx[0]]
        vote   = majority_vote(row0, profiles, MIN_AGREE)
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

    # ── 空头腿 ───────────────────────────────────────────────────────
    short_pos_list = [0.0] * len(valid)
    s_pos = 0.0; s_entry = None; vote_win = []; cooldown = 0

    for idx in range(len(valid)):
        row = valid.iloc[idx]; price = row['close']; today = row['daily_vote']
        if cooldown > 0: cooldown -= 1
        vote_win.append(today)
        if len(vote_win) > BEAR_CONSEC: vote_win.pop(0)
        last_n = vote_win[-BEAR_CONSEC:] if len(vote_win) >= BEAR_CONSEC else []

        if s_pos < 0 and s_entry is not None:
            lev = abs(s_pos)
            if (row['high'] / s_entry - 1) * (-1) * lev <= SHORT_STOP:
                s_pos, s_entry = 0.0, None; vote_win = []; cooldown = 1
            elif len(last_n) == BEAR_CONSEC and all(v == 'BULL' for v in last_n):
                s_pos, s_entry = 0.0, None

        if s_pos == 0.0 and cooldown == 0:
            if len(last_n) == BEAR_CONSEC and all(v == 'BEAR' for v in last_n):
                if signal_confirm(row, 'BEAR'):
                    _, bear_n = get_counts(row)
                    ok = bear_n >= short_min_bear
                    if ok and short_macd_max is not None:
                        mh = row.get('macd_hist_z', 0)
                        if pd.isna(mh) or mh > short_macd_max:
                            ok = False
                    if ok:
                        lev = SHORT_LEV_MAP.get(bear_n, 1.0)
                        s_pos = -1.0 * lev; s_entry = price

        short_pos_list[idx] = s_pos

    valid['short_pos'] = short_pos_list
    valid['net_pos']   = valid['long_pos'] + valid['short_pos']
    lp = valid['long_pos'].shift(1).fillna(0.0)
    sp = valid['short_pos'].shift(1).fillna(0.0)
    valid['long_day_ret']  = valid['daily_ret'] * lp
    valid['short_day_ret'] = valid['daily_ret'] * sp
    valid['net_day_ret']   = valid['daily_ret'] * (lp + sp)
    valid['equity']        = (1 + valid['net_day_ret']).cumprod()
    return valid


def perf(valid, name):
    r = valid['net_day_ret']; lr = valid['long_day_ret']; sr = valid['short_day_ret']
    cum_net   = (1+r).prod()-1
    cum_long  = (1+lr).prod()-1
    cum_short = (1+sr).prod()-1
    ann  = (1+cum_net)**(365/TOTAL_DAYS)-1
    shp  = r.mean()/r.std()*np.sqrt(365) if r.std()>0 else 0
    eq   = (1+r).cumprod()
    lp_  = valid['long_pos'].shift(1).fillna(0.0)
    sp_  = valid['short_pos'].shift(1).fillna(0.0)
    pcl  = valid['close'].shift(1)
    intra = (valid['low']/pcl-1)*lp_ + (valid['high']/pcl-1)*sp_
    ieq   = eq.shift(1).fillna(1.0)*(1+intra)
    dd    = min((eq/eq.cummax()-1).min(), (ieq/eq.cummax()-1).min())
    v2 = valid.copy(); v2['ym'] = v2['date'].dt.to_period('M')
    m_wr = (pd.Series([(1+v2[v2['ym']==ym]['net_day_ret']).prod()-1
                        for ym in sorted(v2['ym'].unique())]) > 0).mean()
    short_n = int((valid['short_pos'].diff().fillna(0) < 0).sum())

    # ── 从第一次开仓起的净值分析 ─────────────────────────────────────
    # 找到第一次有仓位的日期
    first_pos_idx = (valid['net_pos'] != 0).idxmax()
    eq_from_first = eq.iloc[first_pos_idx:]
    min_eq  = eq_from_first.min()
    min_date = valid['date'].iloc[eq_from_first.idxmin()]
    ever_below_1 = min_eq < 1.0
    # 日中最低净值（从首仓起）
    intra_from = ieq.iloc[first_pos_idx:]
    min_intra = intra_from.min()

    return {
        '方案':          name,
        '净累计':        f'{cum_net*100:.1f}%',
        '多头贡献':      f'{cum_long*100:.1f}%',
        '空头贡献':      f'{cum_short*100:.1f}%',
        '年化':          f'{ann*100:.1f}%',
        '夏普(日)':      round(shp, 2),
        '最大回撤(MDD)': f'{dd*100:.1f}%',
        '月胜率':        f'{m_wr*100:.1f}%',
        '空头次数':      short_n,
        '首仓后最低净值(收盘)': f'{min_eq:.3f}',
        '首仓后最低净值(日中)': f'{min_intra:.3f}',
        '曾整体亏损':    '⚠️ 是' if ever_below_1 else '✅ 否',
        '最低点日期':    str(min_date)[:10],
    }


# ════════════════════════════════════════════════════════════════════
scenarios = [
    # (label, midmonth, min_bear, macd_max)
    ('原版基准',                         0, 4, None),
    ('多头B(4天)',                        4, 4, None),
    ('多头B + 空头仅6票',                 4, 6, None),
    ('多头B + 空头macd_h<-0.5',          4, 4, -0.5),
    ('多头B + 空头macd_h<-0.8',          4, 4, -0.8),
    ('多头B + 空头macd_h<-1.0',          4, 4, -1.0),
    ('多头B + 空头仅6票 + macd_h<-0.5',  4, 6, -0.5),
    ('多头B + 空头仅6票 + macd_h<-0.8',  4, 6, -0.8),
    ('多头B + 空头仅6票 + macd_h<-1.0',  4, 6, -1.0),
]

print('=' * 130)
print('  最终综合方案对比')
print('=' * 130)
results = []
all_v = {}
for (label, mb, smb, smm) in scenarios:
    v = run(df, profiles, midmonth_bear=mb, short_min_bear=smb, short_macd_max=smm, label=label)
    results.append(perf(v, label))
    all_v[label] = v

summary = pd.DataFrame(results).set_index('方案')
print(summary.to_string())


# ── 净值曲线逐月展示（首仓起） ───────────────────────────────────────
print()
print('=' * 130)
print('  逐月净收益 + 净值（首仓起，1.0=本金）')
print('=' * 130)

sel = ['原版基准', '多头B(4天)', '多头B + 空头仅6票', '多头B + 空头仅6票 + macd_h<-0.8']
base_v = all_v['原版基准']
base_v2 = base_v.copy(); base_v2['ym'] = base_v2['date'].dt.to_period('M')
months = sorted(base_v2['ym'].unique())

rows = []
for i, ym in enumerate(months):
    md = base_v2[base_v2['ym'] == ym]
    ep = md.iloc[0]['close']
    xp = base_v2[base_v2['ym']==months[i+1]].iloc[0]['close'] if i+1<len(months) else md.iloc[-1]['close']
    row = {'month': str(ym), 'BTC%': round((xp/ep-1)*100,1)}
    for label in sel:
        v = all_v[label]
        v2 = v.copy(); v2['ym'] = v2['date'].dt.to_period('M')
        md2 = v2[v2['ym'] == ym]
        m_ret = ((1+md2['net_day_ret']).prod()-1)*100
        eq_end = v2['equity'].iloc[md2.index[-1]]
        row[label[:8]+'%'] = round(m_ret,1)
        row[label[:8]+'净值'] = round(eq_end,3)
    rows.append(row)

comp = pd.DataFrame(rows)
print(comp.to_string(index=False))

# ── 净值曾跌破1.0的详情 ──────────────────────────────────────────────
print()
print('=' * 130)
print('  净值低于1.0（亏损）的时段统计（收盘价）')
print('=' * 130)
for label in sel:
    v = all_v[label]
    eq = v['equity']
    below = v[eq < 1.0]
    if len(below) == 0:
        print(f'\n  ✅ {label}：从未低于1.0，始终盈利')
    else:
        min_eq = eq.min()
        min_date = v['date'].iloc[eq.idxmin()]
        print(f'\n  ⚠️  {label}：共{len(below)}天低于1.0，最低={min_eq:.4f}（{str(min_date)[:10]}）')
        # 找连续低谷段
        below_idx = below.index.tolist()
        segments = []
        seg_start = below_idx[0]
        prev = below_idx[0]
        for bi in below_idx[1:]:
            if bi - prev > 5:
                segments.append((seg_start, prev))
                seg_start = bi
            prev = bi
        segments.append((seg_start, prev))
        for s, e in segments:
            sd = v['date'].iloc[s]; ed = v['date'].iloc[e]
            min_seg = eq.iloc[s:e+1].min()
            print(f'       {str(sd)[:10]} ~ {str(ed)[:10]}  最低净值={min_seg:.4f}')
