"""
止盈 + 组合止损 改进方案回测
==============================
改进项：
  CB  — 组合净值回撤超20%时，所有仓位减半（恢复到10%内解除）
  STP — 空头止盈：<6票空头浮盈达到阈值时止盈+冷却
  LTP — 多头止盈：<6票多头月内浮盈达到阈值时止盈剩余月份
  ALL — CB + STP + LTP 全组合

止盈逻辑说明：
  - 6票仓位（高确信）：不设止盈，让利润跑
  - <6票仓位（低确信）：见好就收，止盈后冷却
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

LONG_STOP     = -0.15
SHORT_STOP    = -0.08
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
                 # ── 组合净值熔断 ──────────────────────────────────────
                 circuit_breaker=False,    # 回撤>20% 时仓位减半
                 cb_threshold=-0.20,       # 熔断阈值
                 cb_recover=0.10,          # 回撤收窄至此解除减半
                 # ── 空头止盈 ─────────────────────────────────────────
                 short_tp=None,            # <6票空头止盈阈值(含杠杆)，如0.08
                 short_tp_cooldown=3,      # 止盈后冷却天数
                 short_tp_all=False,       # True=全部空头止盈（含6票）
                 # ── 多头止盈 ─────────────────────────────────────────
                 long_tp=None,             # <6票多头月内止盈阈值(含杠杆)，如0.12
                 long_tp_all=False,        # True=全部多头止盈（含6票）
                 label=''):

    valid = df.dropna(subset=K_COLS).copy().reset_index(drop=True)
    valid['ym']         = valid['date'].dt.to_period('M')
    valid['daily_vote'] = valid.apply(lambda r: majority_vote(r, profiles, MIN_AGREE), axis=1)
    valid['daily_ret']  = valid['close'].pct_change().fillna(0.0)
    months = sorted(valid['ym'].unique())

    # ══════════════════════════════════════════════════════════════════
    # 多头腿（月度决策）
    # ══════════════════════════════════════════════════════════════════
    long_pos_list  = [0.0] * len(valid)
    long_bull_list = [0]   * len(valid)   # 记录每日bull_n，供CB使用

    for i, ym in enumerate(months):
        md_idx = valid[valid['ym'] == ym].index
        row0   = valid.loc[md_idx[0]]
        vote   = majority_vote(row0, profiles, MIN_AGREE)
        fv = vote if vote not in ('BULL','NEUTRAL','BEAR') else (
             vote if signal_confirm(row0, vote) else 'ABSTAIN')
        base_pos = max(STRATEGY_MAP.get(fv, {'position': 0.0})['position'], 0)
        base_pos, _ = apply_overflow_filter(row0, base_pos)
        bull_n, _ = get_counts(row0, profiles)
        if fv == 'BULL' and base_pos > 0:
            pos = base_pos * LONG_LEV_MAP.get(bull_n, 1.0)
        else:
            pos = base_pos

        if pos <= 0:
            for idx in md_idx:
                long_pos_list[idx] = 0.0
                long_bull_list[idx] = bull_n
            continue

        entry_p  = row0['close']
        stopped  = False
        tp_fired = False

        # 是否对此多头设置止盈
        apply_ltp = (long_tp is not None) and (long_tp_all or bull_n < 6)

        for j, idx in enumerate(md_idx):
            row = valid.loc[idx]
            long_bull_list[idx] = bull_n

            if stopped or tp_fired:
                long_pos_list[idx] = 0.0
                continue

            if j == 0:
                long_pos_list[idx] = pos
                continue

            # 止损检查（用日内最低价）
            levered_loss = (row['low'] / entry_p - 1) * pos
            if levered_loss <= LONG_STOP:
                stopped = True
                long_pos_list[idx] = 0.0
                continue

            # 止盈检查（用当日收盘价计算浮盈）
            if apply_ltp:
                levered_gain = (row['close'] / entry_p - 1) * pos
                if levered_gain >= long_tp:
                    tp_fired = True
                    long_pos_list[idx] = 0.0
                    continue

            long_pos_list[idx] = pos

    valid['long_pos']  = long_pos_list
    valid['long_bull'] = long_bull_list

    # ══════════════════════════════════════════════════════════════════
    # 空头腿（动态3天BEAR）
    # ══════════════════════════════════════════════════════════════════
    short_pos_list  = [0.0] * len(valid)
    short_bear_list = [0]   * len(valid)
    s_pos    = 0.0
    s_entry  = None
    s_bear_n = 0
    vote_win = []
    cooldown = 0

    for idx in range(len(valid)):
        row   = valid.iloc[idx]
        price = row['close']
        today = row['daily_vote']

        if cooldown > 0:
            cooldown -= 1

        vote_win.append(today)
        if len(vote_win) > BEAR_CONSEC: vote_win.pop(0)
        last_n = vote_win[-BEAR_CONSEC:] if len(vote_win) >= BEAR_CONSEC else []

        # 持仓管理
        if s_pos < 0 and s_entry is not None:
            worst    = row['high']
            raw_pnl  = (worst / s_entry - 1) * (-1)
            lev      = abs(s_pos)
            lev_pnl  = raw_pnl * lev

            # 止损
            if lev_pnl <= SHORT_STOP:
                s_pos, s_entry, s_bear_n = 0.0, None, 0
                vote_win = []
                cooldown = 1
                short_pos_list[idx] = 0.0
                short_bear_list[idx] = 0
                continue

            # 止盈（用收盘价计算浮盈，仅对<6票或short_tp_all时）
            apply_stp = (short_tp is not None) and (short_tp_all or s_bear_n < 6)
            if apply_stp:
                float_gain = (s_entry / price - 1) * abs(s_pos)  # 空头盈利方向
                if float_gain >= short_tp:
                    s_pos, s_entry, s_bear_n = 0.0, None, 0
                    vote_win = []
                    cooldown = short_tp_cooldown
                    short_pos_list[idx] = 0.0
                    short_bear_list[idx] = 0
                    continue

            # 翻转平仓
            if len(last_n) == BEAR_CONSEC and all(v == 'BULL' for v in last_n):
                s_pos, s_entry, s_bear_n = 0.0, None, 0

        # 开空
        if s_pos == 0.0 and cooldown == 0:
            if len(last_n) == BEAR_CONSEC and all(v == 'BEAR' for v in last_n):
                if signal_confirm(row, 'BEAR'):
                    _, bear_n = get_counts(row, profiles)
                    lev = SHORT_LEV_MAP.get(bear_n, 1.0)
                    s_pos    = -1.0 * lev
                    s_entry  = price
                    s_bear_n = bear_n

        short_pos_list[idx] = s_pos
        short_bear_list[idx] = abs(s_bear_n) if s_pos < 0 else 0

    valid['short_pos']  = short_pos_list
    valid['short_bear'] = short_bear_list

    # ══════════════════════════════════════════════════════════════════
    # 组合净值熔断（每日检查）
    # ══════════════════════════════════════════════════════════════════
    if circuit_breaker:
        daily_ret  = valid['daily_ret']
        lp_arr = pd.Series(valid['long_pos'].values)
        sp_arr = pd.Series(valid['short_pos'].values)

        # 先计算未经熔断的净收益序列，用来判断回撤
        # 实际做法：滚动计算净值，当净值回撤超阈值时记录减半标志
        cb_half = [False] * len(valid)
        eq = 1.0
        peak = 1.0
        halved = False

        for idx in range(len(valid)):
            dd = eq / peak - 1 if peak > 0 else 0
            if not halved and dd <= cb_threshold:
                halved = True
            if halved and dd >= -abs(cb_recover):
                halved = False
            cb_half[idx] = halved

            lp = valid['long_pos'].iloc[idx - 1] if idx > 0 else 0.0
            sp = valid['short_pos'].iloc[idx - 1] if idx > 0 else 0.0
            prev_half = cb_half[idx - 1] if idx > 0 else False
            mult = 0.5 if prev_half else 1.0
            dr = valid['daily_ret'].iloc[idx]
            day_ret = dr * (lp + sp) * mult
            eq = eq * (1 + day_ret)
            if eq > peak: peak = eq

        valid['cb_half'] = cb_half
        # 应用熔断：仓位减半
        valid['long_pos']  = valid.apply(
            lambda r: r['long_pos']  * 0.5 if r['cb_half'] else r['long_pos'],  axis=1)
        valid['short_pos'] = valid.apply(
            lambda r: r['short_pos'] * 0.5 if r['cb_half'] else r['short_pos'], axis=1)

    valid['net_pos'] = valid['long_pos'] + valid['short_pos']
    lp  = valid['long_pos'].shift(1).fillna(0.0)
    sp  = valid['short_pos'].shift(1).fillna(0.0)
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
    ann  = (1 + cum_net) ** (365 / TOTAL_DAYS) - 1
    shp  = r.mean() / r.std() * np.sqrt(365) if r.std() > 0 else 0
    eq   = (1 + r).cumprod()
    lp_  = valid['long_pos'].shift(1).fillna(0.0)
    sp_  = valid['short_pos'].shift(1).fillna(0.0)
    pcl  = valid['close'].shift(1)
    intra = (valid['low'] / pcl - 1) * lp_ + (valid['high'] / pcl - 1) * sp_
    ieq   = eq.shift(1).fillna(1.0) * (1 + intra)
    dd    = min((eq / eq.cummax() - 1).min(), (ieq / eq.cummax() - 1).min())
    valid2 = valid.copy()
    valid2['ym'] = valid2['date'].dt.to_period('M')
    m_wr = (pd.Series([(1 + valid2[valid2['ym']==ym]['net_day_ret']).prod()-1
                        for ym in sorted(valid2['ym'].unique())]) > 0).mean()
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


# ════════════════════════════════════════════════════════════════════
print('=' * 105)
print('  改进方案回测矩阵')
print('=' * 105)

scenarios = [
    # (label,                    CB,    STP,   STP_cd, STP_all, LTP,   LTP_all)
    ('原版基准',                  False, None,  3,      False,   None,  False),

    # ── 组合熔断 ─────────────────────────────────────────────────────
    ('CB  回撤>20%减半',          True,  None,  3,      False,   None,  False),

    # ── 空头止盈（仅<6票）────────────────────────────────────────────
    ('STP 空头<6票止盈+6%',       False, 0.06,  3,      False,   None,  False),
    ('STP 空头<6票止盈+8%',       False, 0.08,  3,      False,   None,  False),
    ('STP 空头<6票止盈+10%',      False, 0.10,  3,      False,   None,  False),

    # ── 空头止盈（全部）──────────────────────────────────────────────
    ('STP 空头全止盈+10%',        False, 0.10,  3,      True,    None,  False),
    ('STP 空头全止盈+15%',        False, 0.15,  3,      True,    None,  False),
    ('STP 空头全止盈+20%',        False, 0.20,  3,      True,    None,  False),
    ('STP 空头全止盈+30%',        False, 0.30,  3,      True,    None,  False),

    # ── 多头止盈（仅<6票）────────────────────────────────────────────
    ('LTP 多头<6票止盈+10%',      False, None,  3,      False,   0.10,  False),
    ('LTP 多头<6票止盈+15%',      False, None,  3,      False,   0.15,  False),
    ('LTP 多头<6票止盈+20%',      False, None,  3,      False,   0.20,  False),

    # ── 多头止盈（全部）──────────────────────────────────────────────
    ('LTP 多头全止盈+20%',        False, None,  3,      False,   0.20,  True),
    ('LTP 多头全止盈+30%',        False, None,  3,      False,   0.30,  True),

    # ── 最优组合 ─────────────────────────────────────────────────────
    ('BEST CB+空<6票STP8%+多<6票LTP15%',
                                  True,  0.08,  3,      False,   0.15,  False),
    ('BEST CB+空全STP20%+多<6票LTP15%',
                                  True,  0.20,  3,      True,    0.15,  False),
]

results = []
for (label, cb, stp, stp_cd, stp_all, ltp, ltp_all) in scenarios:
    v = run_strategy(df, profiles,
                     circuit_breaker=cb, cb_threshold=-0.20, cb_recover=0.10,
                     short_tp=stp, short_tp_cooldown=stp_cd, short_tp_all=stp_all,
                     long_tp=ltp, long_tp_all=ltp_all,
                     label=label)
    results.append(perf(v, label))

summary = pd.DataFrame(results).set_index('方案')
print(summary.to_string())

# ── 逐月对比（原版 vs 选定方案）─────────────────────────────────────
print()
print('=' * 105)
print('  逐月净收益对比（原版 vs 选定改进方案）')
print('=' * 105)

def monthly_net(valid):
    v2 = valid.copy()
    v2['ym'] = v2['date'].dt.to_period('M')
    months = sorted(v2['ym'].unique())
    rows = []
    for i, ym in enumerate(months):
        md = v2[v2['ym'] == ym]
        ep = md.iloc[0]['close']
        xp = v2[v2['ym'] == months[i+1]].iloc[0]['close'] if i+1<len(months) else md.iloc[-1]['close']
        rows.append({'month': str(ym),
                     'BTC%': round((xp/ep-1)*100, 1),
                     'net%': round(((1+md['net_day_ret']).prod()-1)*100, 1)})
    return pd.DataFrame(rows)

base_v = run_strategy(df, profiles)
cb_v   = run_strategy(df, profiles, circuit_breaker=True)
stp_v  = run_strategy(df, profiles, short_tp=0.08, short_tp_cooldown=3)
ltp_v  = run_strategy(df, profiles, long_tp=0.15)
best_v = run_strategy(df, profiles, circuit_breaker=True, short_tp=0.08,
                      short_tp_cooldown=3, long_tp=0.15)

m0    = monthly_net(base_v)
mcb   = monthly_net(cb_v)
mstp  = monthly_net(stp_v)
mltp  = monthly_net(ltp_v)
mbest = monthly_net(best_v)

comp = pd.DataFrame({
    'month':  m0['month'],
    'BTC%':   m0['BTC%'],
    '原版%':  m0['net%'],
    'CB%':    mcb['net%'],
    'STP8%%': mstp['net%'],
    'LTP15%': mltp['net%'],
    '最优%':  mbest['net%'],
})
print(comp.to_string(index=False))
