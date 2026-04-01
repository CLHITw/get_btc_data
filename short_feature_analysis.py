"""
空头交易特征值分析
==================
目标：找出区分盈利空头 vs 亏损空头的特征值规律
包含：入场时的所有Z-score特征、boll_width_z、RSI、FGI等
同时测试基于特征值过滤的改进方案
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
SHORT_LEV_MAP = {4: 1.0, 5: 1.0, 6: 2.0}
MIN_AGREE     = 4

# 所有可用特征列
FEATURE_COLS = ['rsi_norm', 'boll_width_z', 'macd_z', 'macd_hist_z',
                'rel_macd_hist_z', 'volatility_pct_z', 'volume_log_z',
                'dist_ma200_pct_z', 'atr_pct_z', 'fginorm', 'fgi',
                'rsi', 'boll_width']


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


def extract_short_trades(df, profiles):
    """提取所有空头交易，附上入场时的特征值"""
    valid = df.dropna(subset=K_COLS).copy().reset_index(drop=True)
    valid['ym']         = valid['date'].dt.to_period('M')
    valid['daily_vote'] = valid.apply(lambda r: majority_vote(r, profiles, MIN_AGREE), axis=1)
    valid['daily_ret']  = valid['close'].pct_change().fillna(0.0)

    s_pos = 0.0; s_entry = None; s_bear_n = 0; s_entry_idx = None
    vote_win = []; cooldown = 0
    trades = []

    for idx in range(len(valid)):
        row   = valid.iloc[idx]
        price = row['close']
        today = row['daily_vote']

        if cooldown > 0: cooldown -= 1
        vote_win.append(today)
        if len(vote_win) > BEAR_CONSEC: vote_win.pop(0)
        last_n = vote_win[-BEAR_CONSEC:] if len(vote_win) >= BEAR_CONSEC else []

        if s_pos < 0 and s_entry is not None:
            lev = abs(s_pos)
            raw_pnl = (row['high'] / s_entry - 1) * (-1)
            if raw_pnl * lev <= SHORT_STOP:
                # 止损出场
                entry_row = valid.iloc[s_entry_idx]
                pnl = (s_entry / row['close'] - 1) * lev  # 用收盘价记录实际
                trades.append(_make_trade(entry_row, row, s_bear_n, lev, s_entry,
                                          row['close'], pnl, '日内高点止损', idx - s_entry_idx))
                s_pos, s_entry, s_bear_n, s_entry_idx = 0.0, None, 0, None
                vote_win = []; cooldown = 1
            elif len(last_n) == BEAR_CONSEC and all(v == 'BULL' for v in last_n):
                entry_row = valid.iloc[s_entry_idx]
                pnl = (s_entry / price - 1) * abs(s_pos)
                trades.append(_make_trade(entry_row, row, s_bear_n, abs(s_pos), s_entry,
                                          price, pnl, '翻转平仓', idx - s_entry_idx))
                s_pos, s_entry, s_bear_n, s_entry_idx = 0.0, None, 0, None

        if s_pos == 0.0 and cooldown == 0:
            if len(last_n) == BEAR_CONSEC and all(v == 'BEAR' for v in last_n):
                if signal_confirm(row, 'BEAR'):
                    _, bear_n = get_counts(row)
                    lev = SHORT_LEV_MAP.get(bear_n, 1.0)
                    s_pos = -1.0 * lev; s_entry = price
                    s_bear_n = bear_n; s_entry_idx = idx

    # 持仓中的交易
    if s_pos < 0 and s_entry_idx is not None:
        entry_row = valid.iloc[s_entry_idx]
        exit_row  = valid.iloc[-1]
        lev = abs(s_pos)
        pnl = (s_entry / exit_row['close'] - 1) * lev
        trades.append(_make_trade(entry_row, exit_row, s_bear_n, lev, s_entry,
                                  exit_row['close'], pnl, '持仓中', len(valid) - s_entry_idx))

    return pd.DataFrame(trades)


def _make_trade(entry_row, exit_row, bear_n, lev, entry_p, exit_p, pnl, reason, duration):
    rec = {
        '入场日期':  entry_row['date'].strftime('%Y-%m-%d'),
        '出场日期':  exit_row['date'].strftime('%Y-%m-%d'),
        '熊票':     bear_n,
        '杠杆':     f'{lev:.0f}x',
        '入场价':   round(entry_p),
        '出场价':   round(exit_p),
        '收益%':    round(pnl * 100, 1),
        '持续天':   duration,
        '退出原因': reason,
    }
    # 入场时特征值
    for col in FEATURE_COLS:
        if col in entry_row.index:
            rec[col] = round(entry_row[col], 3) if pd.notna(entry_row[col]) else None
    return rec


trades = extract_short_trades(df, profiles)

print('=' * 120)
print('  空头交易完整记录 + 入场特征值')
print('=' * 120)

display_cols = ['入场日期','出场日期','熊票','杠杆','入场价','出场价','收益%','持续天','退出原因',
                'rsi', 'rsi_norm', 'boll_width', 'boll_width_z', 'fgi', 'fginorm',
                'volatility_pct_z', 'atr_pct_z', 'dist_ma200_pct_z',
                'macd_z', 'macd_hist_z', 'volume_log_z']
display_cols = [c for c in display_cols if c in trades.columns]
print(trades[display_cols].to_string(index=False))


# ── 盈利 vs 亏损 特征对比 ────────────────────────────────────────────
print()
print('=' * 120)
print('  盈利空头 vs 亏损/微利空头 特征均值对比')
print('=' * 120)

feat_cols = [c for c in FEATURE_COLS if c in trades.columns]
trades['盈亏'] = trades['收益%'].apply(lambda x: '盈利' if x > 2 else '亏损/微利')

grp = trades.groupby('盈亏')[feat_cols].mean().T
grp.columns.name = None
if '盈利' in grp.columns and '亏损/微利' in grp.columns:
    grp['差值(盈-亏)'] = grp['盈利'] - grp['亏损/微利']
    grp = grp.sort_values('差值(盈-亏)')
print(grp.round(3).to_string())


# ── 关键特征分布 ─────────────────────────────────────────────────────
print()
print('=' * 120)
print('  每笔交易关键特征一览（排序：收益%从低到高）')
print('=' * 120)
key_cols = ['入场日期','收益%','退出原因','熊票','boll_width_z',
            'fgi','fginorm','volatility_pct_z','dist_ma200_pct_z','atr_pct_z',
            'rsi_norm','macd_hist_z','volume_log_z']
key_cols = [c for c in key_cols if c in trades.columns]
key = trades.sort_values('收益%')[key_cols].copy()
print(key.to_string(index=False))


# ── 基于特征过滤的改进方案回测 ───────────────────────────────────────
print()
print('=' * 120)
print('  基于特征过滤 + 方案B(月中4天BEAR平多头) 改进测试')
print('=' * 120)

LONG_STOP    = -0.15
LONG_LEV_MAP = {4: 1.0, 5: 1.5, 6: 2.0}


def run_full(df, profiles,
             # 空头过滤条件
             short_bw_max=None,        # boll_width_z 上限
             short_bw_min=None,        # boll_width_z 下限（太低说明没波动，不适合做空）
             short_rsi_max=None,       # RSI上限（RSI高说明还在强势，不适合做空）
             short_vol_max=None,       # volatility_pct_z 上限
             short_dist_min=None,      # dist_ma200_pct_z 下限（离均线太近不做空）
             short_fgi_max=None,       # FGI上限（恐慌时做空）
             # 月中再评估
             midmonth_bear=4,
             label=''):

    valid = df.dropna(subset=K_COLS).copy().reset_index(drop=True)
    valid['ym']         = valid['date'].dt.to_period('M')
    valid['daily_vote'] = valid.apply(lambda r: majority_vote(r, profiles, MIN_AGREE), axis=1)
    valid['daily_ret']  = valid['close'].pct_change().fillna(0.0)
    months = sorted(valid['ym'].unique())

    # 多头腿（含方案B）
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
            if midmonth_bear and len(vbuf) >= midmonth_bear:
                if all(v == 'BEAR' for v in vbuf[-midmonth_bear:]):
                    exited = True; long_pos_list[idx] = 0.0; continue
            long_pos_list[idx] = pos

    valid['long_pos'] = long_pos_list

    # 空头腿（含特征过滤）
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
                    # ── 特征过滤 ──────────────────────────────────
                    ok = True
                    bw = row.get('boll_width_z', 0)
                    rsi = row.get('rsi', 50)
                    vol = row.get('volatility_pct_z', 0)
                    dist = row.get('dist_ma200_pct_z', 0)
                    fgi = row.get('fgi', 50)
                    if short_bw_max  is not None and bw   > short_bw_max:  ok = False
                    if short_bw_min  is not None and bw   < short_bw_min:  ok = False
                    if short_rsi_max is not None and rsi  > short_rsi_max: ok = False
                    if short_vol_max is not None and vol  > short_vol_max: ok = False
                    if short_dist_min is not None and dist < short_dist_min: ok = False
                    if short_fgi_max is not None and fgi  > short_fgi_max: ok = False
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
    return valid


def perf(valid, name):
    r = valid['net_day_ret']; lr = valid['long_day_ret']; sr = valid['short_day_ret']
    cum_net = (1+r).prod()-1; cum_long = (1+lr).prod()-1; cum_short = (1+sr).prod()-1
    ann = (1+cum_net)**(365/TOTAL_DAYS)-1
    shp = r.mean()/r.std()*np.sqrt(365) if r.std()>0 else 0
    eq  = (1+r).cumprod()
    lp_ = valid['long_pos'].shift(1).fillna(0.0)
    sp_ = valid['short_pos'].shift(1).fillna(0.0)
    pcl = valid['close'].shift(1)
    intra = (valid['low']/pcl-1)*lp_ + (valid['high']/pcl-1)*sp_
    ieq = eq.shift(1).fillna(1.0)*(1+intra)
    dd  = min((eq/eq.cummax()-1).min(), (ieq/eq.cummax()-1).min())
    v2  = valid.copy(); v2['ym'] = v2['date'].dt.to_period('M')
    m_wr = (pd.Series([(1+v2[v2['ym']==ym]['net_day_ret']).prod()-1
                        for ym in sorted(v2['ym'].unique())]) > 0).mean()
    short_trades = int((valid['short_pos'].diff().fillna(0) < 0).sum())
    return {'方案': name, '净累计': f'{cum_net*100:.1f}%',
            '多头贡献': f'{cum_long*100:.1f}%', '空头贡献': f'{cum_short*100:.1f}%',
            '年化': f'{ann*100:.1f}%', '夏普(日)': round(shp,2),
            '最大回撤': f'{dd*100:.1f}%', '月胜率': f'{m_wr*100:.1f}%',
            '空头次数': short_trades}


filter_scenarios = [
    ('基准+方案B(4天)',            None,  None, None, None, None, None),
    # boll_width_z 过滤（针对高波动假信号）
    ('空头 bw_z < 4.5',           4.5,  None, None, None, None, None),
    ('空头 bw_z < 4.0',           4.0,  None, None, None, None, None),
    ('空头 bw_z < 5.0',           5.0,  None, None, None, None, None),
    # RSI 过滤（RSI过高=强势，不适合做空）
    ('空头 RSI < 60',             None,  None,  60,  None, None, None),
    ('空头 RSI < 55',             None,  None,  55,  None, None, None),
    ('空头 RSI < 50',             None,  None,  50,  None, None, None),
    # FGI 过滤（贪婪时不做空，恐慌时做空）
    ('空头 FGI < 60',             None,  None, None, None, None, 60),
    ('空头 FGI < 50',             None,  None, None, None, None, 50),
    ('空头 FGI < 40',             None,  None, None, None, None, 40),
    # volatility 过滤
    ('空头 vol_z < 2.0',          None,  None, None, 2.0, None, None),
    ('空头 vol_z < 1.5',          None,  None, None, 1.5, None, None),
    # 组合过滤
    ('空头 bw_z<4.5 + RSI<60',    4.5,  None,  60,  None, None, None),
    ('空头 bw_z<4.5 + FGI<60',    4.5,  None, None, None, None,  60),
    ('空头 RSI<60 + FGI<60',      None,  None,  60,  None, None,  60),
    ('空头 bw_z<4.5+RSI<60+FGI<60', 4.5, None, 60, None, None, 60),
]

results = []
for (label, bwmax, bwmin, rsimax, volmax, distmin, fgimax) in filter_scenarios:
    v = run_full(df, profiles,
                 short_bw_max=bwmax, short_bw_min=bwmin,
                 short_rsi_max=rsimax, short_vol_max=volmax,
                 short_dist_min=distmin, short_fgi_max=fgimax,
                 midmonth_bear=4, label=label)
    results.append(perf(v, label))

summary = pd.DataFrame(results).set_index('方案')
print(summary.to_string())
