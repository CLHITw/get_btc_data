"""
回撤改进方案回测
================
基于已识别的问题，测试以下改进：
  ① 仅6/6熊票才开空（过滤5票噪音）
  ② 高bw环境过滤（bw < 4.0 才开空）
  ③ 多头持仓时禁止新开空（减少多空冲突）
  ④ ①+②组合
  ⑤ ①+③组合
  ⑥ ①+②+③全组合
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

# ── 参数（与实盘一致）────────────────────────────────────────────
LONG_STOP     = -0.15   # 多头止损（含杠杆后）
SHORT_STOP    = -0.08   # 空头止损（含杠杆后）
BEAR_CONSEC   = 3
LONG_LEV_MAP  = {4: 1.0, 5: 1.5, 6: 2.0}
SHORT_LEV_MAP = {4: 1.0, 5: 1.0, 6: 2.0}
MIN_AGREE     = 4


def get_counts(row, profiles):
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
                 min_bear_n=4,        # 改进①：最少熊票数 (4=原版, 6=仅6票)
                 bw_filter=None,      # 改进②：最大boll_width_z (None=不过滤)
                 no_short_when_long=False,  # 改进③：多头持仓时不开空
                 label=''):
    valid = df.dropna(subset=K_COLS).copy().reset_index(drop=True)
    valid['ym']         = valid['date'].dt.to_period('M')
    valid['daily_vote'] = valid.apply(lambda r: majority_vote(r, profiles, MIN_AGREE), axis=1)
    valid['daily_ret']  = valid['close'].pct_change().fillna(0.0)

    months = sorted(valid['ym'].unique())

    # ── 多头腿（月度决策，按bull_n杠杆，月内止损）────────────────
    long_pos_list = [0.0] * len(valid)
    for i, ym in enumerate(months):
        md_idx = valid[valid['ym'] == ym].index
        row0   = valid.loc[md_idx[0]]
        vote   = majority_vote(row0, profiles, MIN_AGREE)
        fv = vote if vote not in ('BULL','NEUTRAL','BEAR') else (vote if signal_confirm(row0, vote) else 'ABSTAIN')
        base_pos = max(STRATEGY_MAP.get(fv, {'position': 0.0})['position'], 0)
        base_pos, _ = apply_overflow_filter(row0, base_pos)
        if fv == 'BULL' and base_pos > 0:
            bull_n, _ = get_counts(row0, profiles)
            pos = base_pos * LONG_LEV_MAP.get(bull_n, 1.0)
        else:
            pos = base_pos
        if pos <= 0:
            for idx in md_idx: long_pos_list[idx] = 0.0
            continue
        entry_p = row0['close']
        for idx in md_idx:
            row = valid.loc[idx]
            if row['date'] == row0['date']:
                long_pos_list[idx] = pos; continue
            levered_loss = (row['low'] / entry_p - 1) * pos
            if levered_loss <= LONG_STOP:
                for jdx in md_idx[md_idx >= idx]: long_pos_list[jdx] = 0.0
                break
            long_pos_list[idx] = pos
    valid['long_pos'] = long_pos_list

    # ── 空头腿（动态3天BEAR + 过滤条件）────────────────────────────
    short_pos_list = [0.0] * len(valid)
    s_pos   = 0.0
    s_entry = None
    vote_win = []
    cooldown = 0   # 冷却天数

    for idx in range(len(valid)):
        row    = valid.iloc[idx]
        price  = row['close']
        today  = row['daily_vote']
        cur_lp = long_pos_list[idx]

        if cooldown > 0:
            cooldown -= 1

        vote_win.append(today)
        if len(vote_win) > BEAR_CONSEC: vote_win.pop(0)
        last_n = vote_win[-BEAR_CONSEC:] if len(vote_win) >= BEAR_CONSEC else []

        # 持仓止损/平仓检查
        if s_pos < 0 and s_entry is not None:
            worst = row['high']
            raw_pnl = (worst / s_entry - 1) * (-1)
            lev = abs(s_pos)
            levered_pnl = raw_pnl * lev
            if levered_pnl <= SHORT_STOP:
                s_pos, s_entry = 0.0, None
                vote_win = []
                cooldown = 1   # 止损后冷却1天
            elif len(last_n) == BEAR_CONSEC and all(v == 'BULL' for v in last_n):
                s_pos, s_entry = 0.0, None

        # 开空条件
        if s_pos == 0.0 and cooldown == 0:
            if len(last_n) == BEAR_CONSEC and all(v == 'BEAR' for v in last_n):
                if signal_confirm(row, 'BEAR'):
                    _, bear_n = get_counts(row, profiles)
                    # 改进①：熊票数过滤
                    ok_bear_n = (bear_n >= min_bear_n)
                    # 改进②：bw过滤
                    bw = row.get('boll_width_z', 0)
                    ok_bw = (bw_filter is None or bw < bw_filter)
                    # 改进③：多头持仓时不开空
                    ok_long = (not no_short_when_long) or (cur_lp == 0)
                    if ok_bear_n and ok_bw and ok_long:
                        lev   = SHORT_LEV_MAP.get(bear_n, 1.0)
                        s_pos   = -1.0 * lev
                        s_entry = price

        short_pos_list[idx] = s_pos

    valid['short_pos'] = short_pos_list
    valid['net_pos']   = valid['long_pos'] + valid['short_pos']

    lp = valid['long_pos'].shift(1).fillna(0.0)
    sp = valid['short_pos'].shift(1).fillna(0.0)
    np_ = valid['net_pos'].shift(1).fillna(0.0)
    valid['long_day_ret']  = valid['daily_ret'] * lp
    valid['short_day_ret'] = valid['daily_ret'] * sp
    valid['net_day_ret']   = valid['daily_ret'] * np_

    return valid


def perf(valid, name):
    r  = valid['net_day_ret']
    lr = valid['long_day_ret']
    sr = valid['short_day_ret']

    cum_net   = (1 + r).prod()  - 1
    cum_long  = (1 + lr).prod() - 1
    cum_short = (1 + sr).prod() - 1
    ann       = (1 + cum_net) ** (365 / TOTAL_DAYS) - 1
    sharpe    = r.mean() / r.std() * np.sqrt(365) if r.std() > 0 else 0

    eq     = (1 + r).cumprod()
    lp_    = valid['long_pos'].shift(1).fillna(0.0)
    sp_    = valid['short_pos'].shift(1).fillna(0.0)
    prev_cl = valid['close'].shift(1)
    intra_ret = (valid['low'] / prev_cl - 1) * lp_ + (valid['high'] / prev_cl - 1) * sp_
    intra_eq  = eq.shift(1).fillna(1.0) * (1 + intra_ret)
    intra_dd  = intra_eq / eq.cummax() - 1
    close_dd  = eq / eq.cummax() - 1
    dd = min(close_dd.min(), intra_dd.min())

    # 月度胜率
    valid2 = valid.copy()
    valid2['ym'] = valid2['date'].dt.to_period('M')
    months = sorted(valid2['ym'].unique())
    m_rets = []
    for i, ym in enumerate(months):
        md = valid2[valid2['ym'] == ym]
        m_rets.append((1 + md['net_day_ret']).prod() - 1)
    m_wr = (np.array(m_rets) > 0).mean()

    # 空头次数统计
    in_short = False
    short_trades = 0
    short_stops  = 0
    for idx in range(len(valid)):
        sp_val = valid['short_pos'].iloc[idx]
        if not in_short and sp_val < 0:
            in_short = True
            short_trades += 1
            entry_idx = idx
        elif in_short and sp_val == 0:
            in_short = False

    return {
        '方案':       name,
        '净累计':     f'{cum_net*100:.1f}%',
        '多头贡献':   f'{cum_long*100:.1f}%',
        '空头贡献':   f'{cum_short*100:.1f}%',
        '年化':       f'{ann*100:.1f}%',
        '夏普(日)':   round(sharpe, 2),
        '最大回撤':   f'{dd*100:.1f}%',
        '月胜率':     f'{m_wr*100:.1f}%',
        '空头次数':   short_trades,
    }


print('=' * 100)
print('  回撤改进方案回测对比')
print('=' * 100)

scenarios = [
    # (label, min_bear_n, bw_filter, no_short_when_long)
    ('原版基准（4票+无过滤）',    4, None,  False),
    ('①仅6票才开空',              6, None,  False),
    ('②bw<4.0过滤',               4, 4.0,   False),
    ('②bw<3.5过滤',               4, 3.5,   False),
    ('③多头时不开空',             4, None,  True),
    ('①+② 6票且bw<4.0',          6, 4.0,   False),
    ('①+③ 6票+多头时不开空',     6, None,  True),
    ('①+②+③ 全部',               6, 4.0,   True),
]

results = []
for label, mbn, bwf, nswl in scenarios:
    v = run_strategy(df, profiles, min_bear_n=mbn, bw_filter=bwf, no_short_when_long=nswl, label=label)
    results.append(perf(v, label))

summary = pd.DataFrame(results).set_index('方案')
print(summary.to_string())

# ── 逐月净收益对比（基准 vs 最优）──────────────────────────────────
print()
print('=' * 100)
print('  逐月净收益：基准 vs 改进方案')
print('=' * 100)

base_v = run_strategy(df, profiles, min_bear_n=4, bw_filter=None, no_short_when_long=False)
v1     = run_strategy(df, profiles, min_bear_n=6, bw_filter=None, no_short_when_long=False)
v12    = run_strategy(df, profiles, min_bear_n=6, bw_filter=4.0,  no_short_when_long=False)
v123   = run_strategy(df, profiles, min_bear_n=6, bw_filter=4.0,  no_short_when_long=True)

def monthly_rets(valid):
    valid2 = valid.copy()
    valid2['ym'] = valid2['date'].dt.to_period('M')
    months = sorted(valid2['ym'].unique())
    rows = []
    for i, ym in enumerate(months):
        md = valid2[valid2['ym'] == ym]
        ep = md.iloc[0]['close']
        if i + 1 < len(months):
            xp = valid2[valid2['ym'] == months[i+1]].iloc[0]['close']
        else:
            xp = md.iloc[-1]['close']
        btc_r = (xp / ep - 1) * 100
        net_r = ((1 + md['net_day_ret']).prod() - 1) * 100
        rows.append({'month': str(ym), 'btc': round(btc_r, 1), 'net': round(net_r, 1)})
    return pd.DataFrame(rows)

m0   = monthly_rets(base_v)
m1   = monthly_rets(v1)
m12  = monthly_rets(v12)
m123 = monthly_rets(v123)

comp = pd.DataFrame({
    'month':    m0['month'],
    'BTC%':     m0['btc'],
    '原版%':    m0['net'],
    '①6票%':   m1['net'],
    '①②%':    m12['net'],
    '①②③%':  m123['net'],
})
# 标记差异
print(comp.to_string(index=False))
