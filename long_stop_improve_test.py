"""
多头止损改进测试
=================
方案A：按BTC价格跌幅止损（不同仓位用不同阈值）
方案B：月中再评估——出现3天连续BEAR信号时提前平仓
方案C：A+B组合
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


def run_strategy(df, profiles,
                 # ── 原始止损 ─────────────────────────────────────────
                 orig_long_stop=-0.15,     # 杠杆后亏损，原版
                 # ── 方案A：按BTC跌幅止损（不含杠杆）─────────────────
                 btc_stop_map=None,        # {pos: btc跌幅} 如 {2.0:-0.08, 1.5:-0.09, 1.0:-0.10, 0.75:-0.10, 0.5:-0.12}
                 # ── 方案B：月中再评估（N天BEAR信号提前平多头）────────
                 midmonth_bear_exit=False,
                 midmonth_consec=3,        # 连续N天BEAR
                 midmonth_cooldown=0,      # 平仓后本月剩余天数是否空仓
                 label=''):

    valid = df.dropna(subset=K_COLS).copy().reset_index(drop=True)
    valid['ym']         = valid['date'].dt.to_period('M')
    valid['daily_vote'] = valid.apply(
        lambda r: majority_vote(r, profiles, MIN_AGREE), axis=1)
    valid['daily_ret']  = valid['close'].pct_change().fillna(0.0)
    months = sorted(valid['ym'].unique())

    # ── 多头腿 ───────────────────────────────────────────────────────
    long_pos_list  = [0.0] * len(valid)
    long_stop_log  = []   # 记录止损事件

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

        entry_p  = row0['close']
        exited   = False
        exit_reason = None
        vote_buf = []   # 方案B用：记录月内每日投票

        for j, idx in enumerate(md_idx):
            row = valid.loc[idx]

            if exited:
                long_pos_list[idx] = 0.0
                continue

            if j == 0:
                long_pos_list[idx] = pos
                vote_buf.append(row['daily_vote'])
                continue

            # ── 原始止损：杠杆后亏损 ─────────────────────────────────
            lev_loss = (row['low'] / entry_p - 1) * pos
            if lev_loss <= orig_long_stop:
                exited = True
                exit_reason = f'原始止损(杠杆后{orig_long_stop*100:.0f}%)'
                long_stop_log.append({
                    'month': str(ym), 'date': row['date'],
                    'reason': exit_reason, 'pos': pos,
                    'btc_drop': f'{(row["low"]/entry_p-1)*100:.1f}%'
                })
                long_pos_list[idx] = 0.0
                continue

            # ── 方案A：BTC价格跌幅止损 ───────────────────────────────
            if btc_stop_map is not None:
                # 找最接近的仓位档位
                btc_thresh = None
                for p_key in sorted(btc_stop_map.keys(), reverse=True):
                    if pos >= p_key - 0.01:
                        btc_thresh = btc_stop_map[p_key]
                        break
                if btc_thresh is None:
                    btc_thresh = min(btc_stop_map.values())

                btc_drop = (row['low'] / entry_p - 1)
                if btc_drop <= btc_thresh:
                    exited = True
                    exit_reason = f'BTC跌幅止损({btc_thresh*100:.0f}%)'
                    long_stop_log.append({
                        'month': str(ym), 'date': row['date'],
                        'reason': exit_reason, 'pos': pos,
                        'btc_drop': f'{btc_drop*100:.1f}%'
                    })
                    long_pos_list[idx] = 0.0
                    continue

            # ── 方案B：月中BEAR信号再评估 ─────────────────────────────
            if midmonth_bear_exit and j >= 1:
                vote_buf.append(row['daily_vote'])
                if len(vote_buf) >= midmonth_consec:
                    last = vote_buf[-midmonth_consec:]
                    if all(v == 'BEAR' for v in last):
                        exited = True
                        exit_reason = f'月中{midmonth_consec}天BEAR平仓'
                        long_stop_log.append({
                            'month': str(ym), 'date': row['date'],
                            'reason': exit_reason, 'pos': pos,
                            'btc_drop': f'{(row["close"]/entry_p-1)*100:.1f}%'
                        })
                        long_pos_list[idx] = 0.0
                        continue
            elif midmonth_bear_exit:
                vote_buf.append(row['daily_vote'])

            long_pos_list[idx] = pos

    valid['long_pos'] = long_pos_list

    # ── 空头腿（原版不变）────────────────────────────────────────────
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
                    s_pos = -1.0 * SHORT_LEV_MAP.get(bear_n, 1.0); s_entry = price
        short_pos_list[idx] = s_pos

    valid['short_pos'] = short_pos_list
    valid['net_pos']   = valid['long_pos'] + valid['short_pos']
    lp  = valid['long_pos'].shift(1).fillna(0.0)
    sp  = valid['short_pos'].shift(1).fillna(0.0)
    valid['long_day_ret']  = valid['daily_ret'] * lp
    valid['short_day_ret'] = valid['daily_ret'] * sp
    valid['net_day_ret']   = valid['daily_ret'] * (lp + sp)
    return valid, long_stop_log


def perf(valid, name):
    r   = valid['net_day_ret']
    lr  = valid['long_day_ret']
    sr  = valid['short_day_ret']
    cum_net   = (1 + r).prod()  - 1
    cum_long  = (1 + lr).prod() - 1
    cum_short = (1 + sr).prod() - 1
    ann  = (1 + cum_net) ** (365 / TOTAL_DAYS) - 1
    shp  = r.mean() / r.std() * np.sqrt(365) if r.std() > 0 else 0
    eq   = (1 + r).cumprod()
    lp_  = valid['long_pos'].shift(1).fillna(0.0)
    sp_  = valid['short_pos'].shift(1).fillna(0.0)
    pcl  = valid['close'].shift(1)
    intra = (valid['low'] / pcl - 1) * lp_ + (valid['high'] / pcl - 1) * sp_
    ieq   = eq.shift(1).fillna(1.0) * (1 + intra)
    dd    = min((eq / eq.cummax() - 1).min(), (ieq / eq.cummax() - 1).min())
    v2 = valid.copy(); v2['ym'] = v2['date'].dt.to_period('M')
    m_wr = (pd.Series([(1+v2[v2['ym']==ym]['net_day_ret']).prod()-1
                        for ym in sorted(v2['ym'].unique())]) > 0).mean()
    return {
        '方案':     name,
        '净累计':   f'{cum_net*100:.1f}%',
        '多头贡献': f'{cum_long*100:.1f}%',
        '空头贡献': f'{cum_short*100:.1f}%',
        '年化':     f'{ann*100:.1f}%',
        '夏普(日)': round(shp, 2),
        '最大回撤': f'{dd*100:.1f}%',
        '月胜率':   f'{m_wr*100:.1f}%',
    }


def monthly_net(valid):
    v2 = valid.copy(); v2['ym'] = v2['date'].dt.to_period('M')
    months = sorted(v2['ym'].unique())
    rows = []
    for i, ym in enumerate(months):
        md = v2[v2['ym'] == ym]
        ep = md.iloc[0]['close']
        xp = v2[v2['ym']==months[i+1]].iloc[0]['close'] if i+1<len(months) else md.iloc[-1]['close']
        rows.append({'month': str(ym),
                     'BTC%': round((xp/ep-1)*100,1),
                     'net%': round(((1+md['net_day_ret']).prod()-1)*100,1)})
    return pd.DataFrame(rows)


# ════════════════════════════════════════════════════════════════════
print('=' * 110)
print('  多头止损改进方案矩阵')
print('=' * 110)

# 方案A止损映射：{仓位档 -> BTC跌幅阈值}
# 仓位越轻 → 允许跌幅越大（因为损失已经轻了）
# 仓位越重/杠杆 → 止损越紧
A_tight = {2.0: -0.07, 1.5: -0.08, 1.0: -0.10, 0.75: -0.10, 0.5: -0.12}
A_mid   = {2.0: -0.08, 1.5: -0.09, 1.0: -0.12, 0.75: -0.12, 0.5: -0.15}
A_loose = {2.0: -0.10, 1.5: -0.11, 1.0: -0.13, 0.75: -0.13, 0.5: -0.18}
A_flat8 = {2.0: -0.08, 1.5: -0.08, 1.0: -0.08, 0.75: -0.08, 0.5: -0.08}   # 全部8%
A_flat10= {2.0: -0.10, 1.5: -0.10, 1.0: -0.10, 0.75: -0.10, 0.5: -0.10}   # 全部10%
A_flat12= {2.0: -0.12, 1.5: -0.12, 1.0: -0.12, 0.75: -0.12, 0.5: -0.12}   # 全部12%

scenarios = [
    # (label, orig_stop, btc_stop_map, midmonth, consec, cooldown)
    ('原版基准（杠杆后-15%）',       -0.15, None,    False, 3, 0),

    # ── 方案A：BTC跌幅止损 ────────────────────────────────────────────
    ('A 全仓统一BTC跌-8%',           -0.15, A_flat8,  False, 3, 0),
    ('A 全仓统一BTC跌-10%',          -0.15, A_flat10, False, 3, 0),
    ('A 全仓统一BTC跌-12%',          -0.15, A_flat12, False, 3, 0),
    ('A 分档止损(紧) 2x→-7% 0.5x→-12%', -0.15, A_tight, False, 3, 0),
    ('A 分档止损(中) 2x→-8% 0.5x→-15%', -0.15, A_mid,   False, 3, 0),
    ('A 分档止损(宽) 2x→-10% 0.5x→-18%',-0.15, A_loose, False, 3, 0),

    # ── 方案B：月中BEAR信号平仓 ────────────────────────────────────────
    ('B 月中2天BEAR平仓',             -0.15, None,    True,  2, 0),
    ('B 月中3天BEAR平仓',             -0.15, None,    True,  3, 0),
    ('B 月中4天BEAR平仓',             -0.15, None,    True,  4, 0),
    ('B 月中5天BEAR平仓',             -0.15, None,    True,  5, 0),

    # ── 方案C：A+B组合 ────────────────────────────────────────────────
    ('C A(全-10%) + B(月中3天BEAR)',  -0.15, A_flat10, True,  3, 0),
    ('C A(分档中) + B(月中3天BEAR)',  -0.15, A_mid,    True,  3, 0),
    ('C A(全-12%) + B(月中3天BEAR)',  -0.15, A_flat12, True,  3, 0),
    ('C A(全-10%) + B(月中4天BEAR)',  -0.15, A_flat10, True,  4, 0),
]

results = []
all_valid = {}
for (label, os_, bsm, mm, mc, mcd) in scenarios:
    v, slog = run_strategy(df, profiles,
                           orig_long_stop=os_,
                           btc_stop_map=bsm,
                           midmonth_bear_exit=mm,
                           midmonth_consec=mc,
                           label=label)
    results.append(perf(v, label))
    all_valid[label] = (v, slog)

summary = pd.DataFrame(results).set_index('方案')
print(summary.to_string())


# ── 止损触发记录 ─────────────────────────────────────────────────────
print()
print('=' * 110)
print('  止损/提前平仓触发记录')
print('=' * 110)
for label in ['原版基准（杠杆后-15%）',
              'A 全仓统一BTC跌-10%',
              'B 月中3天BEAR平仓',
              'C A(全-10%) + B(月中3天BEAR)']:
    v, slog = all_valid[label]
    print(f'\n── {label} ──')
    if slog:
        print(pd.DataFrame(slog).to_string(index=False))
    else:
        print('  无触发记录')


# ── 逐月净收益对比 ───────────────────────────────────────────────────
print()
print('=' * 110)
print('  逐月净收益对比')
print('=' * 110)

selected = [
    '原版基准（杠杆后-15%）',
    'A 全仓统一BTC跌-10%',
    'A 分档止损(中) 2x→-8% 0.5x→-15%',
    'B 月中3天BEAR平仓',
    'C A(全-10%) + B(月中3天BEAR)',
]
base_m = monthly_net(all_valid['原版基准（杠杆后-15%）'][0])
comp = pd.DataFrame({'month': base_m['month'], 'BTC%': base_m['BTC%']})
for label in selected:
    v, _ = all_valid[label]
    col = label[:8]
    comp[col] = monthly_net(v)['net%'].values
print(comp.to_string(index=False))
