"""
累计亏损止损 + 追踪止盈 改进测试
==================================
现有止损：日内高点触发（防单日暴涨）→ 保留不变
新增策略：
  CS  — 空头累计亏损止损：收盘浮盈 < -X% 时平仓（防慢慢亏）
  TS  — 空头追踪止盈：浮盈从峰值回撤 Y% 时平仓（锁利润）
  LTS — 多头追踪止盈：月内浮盈从峰值回撤 Z% 时平仓
  组合 — 以上自由组合
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

LONG_STOP     = -0.15   # 多头原始止损（杠杆后）
SHORT_STOP    = -0.08   # 空头日内高点止损（杠杆后）
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
                 # 空头：累计亏损止损（收盘价计算浮盈，杠杆后）
                 short_cum_stop=None,      # 如 -0.12，None=不启用
                 short_cum_cooldown=2,     # 累计止损后冷却天数
                 # 空头：追踪止盈（从峰值浮盈回撤X%平仓）
                 short_trail=None,         # 如 0.08，None=不启用
                 short_trail_cooldown=2,
                 # 多头：追踪止盈（月内从峰值浮盈回撤X%平仓）
                 long_trail=None,          # 如 0.08，None=不启用
                 label=''):

    valid = df.dropna(subset=K_COLS).copy().reset_index(drop=True)
    valid['ym']         = valid['date'].dt.to_period('M')
    valid['daily_vote'] = valid.apply(
        lambda r: majority_vote(r, profiles, MIN_AGREE), axis=1)
    valid['daily_ret']  = valid['close'].pct_change().fillna(0.0)
    months = sorted(valid['ym'].unique())

    # ══════════════════════════════════════════════════════════════════
    # 多头腿（月度决策）
    # ══════════════════════════════════════════════════════════════════
    long_pos_list = [0.0] * len(valid)

    for i, ym in enumerate(months):
        md_idx = valid[valid['ym'] == ym].index
        row0   = valid.loc[md_idx[0]]
        vote   = majority_vote(row0, profiles, MIN_AGREE)
        fv = vote if vote not in ('BULL','NEUTRAL','BEAR') else (
             vote if signal_confirm(row0, vote) else 'ABSTAIN')
        base_pos = max(STRATEGY_MAP.get(fv, {'position': 0.0})['position'], 0)
        base_pos, _ = apply_overflow_filter(row0, base_pos)
        bull_n, _ = get_counts(row0, profiles)
        pos = base_pos * LONG_LEV_MAP.get(bull_n, 1.0) if fv == 'BULL' and base_pos > 0 else base_pos

        if pos <= 0:
            for idx in md_idx: long_pos_list[idx] = 0.0
            continue

        entry_p   = row0['close']
        peak_gain = 0.0   # 追踪止盈：记录月内最高浮盈
        exited    = False

        for j, idx in enumerate(md_idx):
            row = valid.loc[idx]
            if exited:
                long_pos_list[idx] = 0.0
                continue
            if j == 0:
                long_pos_list[idx] = pos
                continue

            # ① 原始止损（日内最低价，杠杆后）
            if (row['low'] / entry_p - 1) * pos <= LONG_STOP:
                exited = True
                long_pos_list[idx] = 0.0
                continue

            # ② 追踪止盈（收盘价计算浮盈，从峰值回撤触发）
            if long_trail is not None:
                cur_gain = (row['close'] / entry_p - 1) * pos
                if cur_gain > peak_gain:
                    peak_gain = cur_gain
                # 只有峰值超过止盈阈值后才激活追踪
                if peak_gain >= long_trail and (peak_gain - cur_gain) >= long_trail * 0.5:
                    exited = True
                    long_pos_list[idx] = 0.0
                    continue

            long_pos_list[idx] = pos

    valid['long_pos'] = long_pos_list

    # ══════════════════════════════════════════════════════════════════
    # 空头腿（动态3天BEAR）
    # ══════════════════════════════════════════════════════════════════
    short_pos_list = [0.0] * len(valid)
    s_pos     = 0.0
    s_entry   = None
    s_peak    = 0.0    # 追踪止盈：记录峰值浮盈
    vote_win  = []
    cooldown  = 0
    exit_log  = []     # 记录每笔空头的退出原因，供分析用

    for idx in range(len(valid)):
        row   = valid.iloc[idx]
        price = row['close']
        today = row['daily_vote']

        if cooldown > 0:
            cooldown -= 1

        vote_win.append(today)
        if len(vote_win) > BEAR_CONSEC: vote_win.pop(0)
        last_n = vote_win[-BEAR_CONSEC:] if len(vote_win) >= BEAR_CONSEC else []

        # ── 持仓管理 ────────────────────────────────────────────────
        if s_pos < 0 and s_entry is not None:
            lev = abs(s_pos)

            # ① 原始日内高点止损（用high）
            raw_spike = (row['high'] / s_entry - 1) * (-1)
            if raw_spike * lev <= SHORT_STOP:
                exit_log.append({'exit_idx': idx, 'reason': '日内高点止损'})
                s_pos, s_entry, s_peak = 0.0, None, 0.0
                vote_win = []
                cooldown = 1
                short_pos_list[idx] = 0.0
                continue

            # ② 收盘累计亏损止损（用close）
            cum_pnl = (s_entry / price - 1) * lev   # 空头盈利方向
            if short_cum_stop is not None and cum_pnl <= short_cum_stop:
                exit_log.append({'exit_idx': idx, 'reason': f'累计亏损止损{short_cum_stop*100:.0f}%'})
                s_pos, s_entry, s_peak = 0.0, None, 0.0
                vote_win = []
                cooldown = short_cum_cooldown
                short_pos_list[idx] = 0.0
                continue

            # ③ 追踪止盈（收盘浮盈从峰值回撤）
            if short_trail is not None:
                if cum_pnl > s_peak:
                    s_peak = cum_pnl
                # 峰值超过trail阈值后，回撤一半则出
                if s_peak >= short_trail and (s_peak - cum_pnl) >= short_trail * 0.5:
                    exit_log.append({'exit_idx': idx, 'reason': f'追踪止盈回撤'})
                    s_pos, s_entry, s_peak = 0.0, None, 0.0
                    vote_win = []
                    cooldown = short_trail_cooldown
                    short_pos_list[idx] = 0.0
                    continue

            # ④ 翻转平仓（3日BULL）
            if len(last_n) == BEAR_CONSEC and all(v == 'BULL' for v in last_n):
                exit_log.append({'exit_idx': idx, 'reason': '翻转平仓'})
                s_pos, s_entry, s_peak = 0.0, None, 0.0

        # ── 开空 ────────────────────────────────────────────────────
        if s_pos == 0.0 and cooldown == 0:
            if len(last_n) == BEAR_CONSEC and all(v == 'BEAR' for v in last_n):
                if signal_confirm(row, 'BEAR'):
                    _, bear_n = get_counts(row, profiles)
                    lev = SHORT_LEV_MAP.get(bear_n, 1.0)
                    s_pos   = -1.0 * lev
                    s_entry = price
                    s_peak  = 0.0

        short_pos_list[idx] = s_pos

    valid['short_pos'] = short_pos_list
    valid['net_pos']   = valid['long_pos'] + valid['short_pos']
    lp  = valid['long_pos'].shift(1).fillna(0.0)
    sp  = valid['short_pos'].shift(1).fillna(0.0)
    np_ = valid['net_pos'].shift(1).fillna(0.0)
    valid['long_day_ret']  = valid['daily_ret'] * lp
    valid['short_day_ret'] = valid['daily_ret'] * sp
    valid['net_day_ret']   = valid['daily_ret'] * np_
    return valid, exit_log


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
    v2    = valid.copy()
    v2['ym'] = v2['date'].dt.to_period('M')
    m_wr = (pd.Series([(1 + v2[v2['ym']==ym]['net_day_ret']).prod()-1
                        for ym in sorted(v2['ym'].unique())]) > 0).mean()
    # 空头出手次数
    short_trades = (valid['short_pos'].diff().fillna(0) < 0).sum()
    return {
        '方案':     name,
        '净累计':   f'{cum_net*100:.1f}%',
        '多头贡献': f'{cum_long*100:.1f}%',
        '空头贡献': f'{cum_short*100:.1f}%',
        '年化':     f'{ann*100:.1f}%',
        '夏普(日)': round(shp, 2),
        '最大回撤': f'{dd*100:.1f}%',
        '月胜率':   f'{m_wr*100:.1f}%',
        '空头入场': int(short_trades),
    }


# ════════════════════════════════════════════════════════════════════
print('=' * 115)
print('  累计亏损止损 + 追踪止盈 方案矩阵')
print('=' * 115)

scenarios = [
    # (label,                        cum_stop, cum_cd, trail, trail_cd, long_trail)
    ('原版基准',                       None,    2,      None,  2,        None),

    # ── 空头：累计亏损止损 ────────────────────────────────────────────
    ('空头累计止损 -8%',               -0.08,   2,      None,  2,        None),
    ('空头累计止损 -10%',              -0.10,   2,      None,  2,        None),
    ('空头累计止损 -12%',              -0.12,   2,      None,  2,        None),
    ('空头累计止损 -15%',              -0.15,   2,      None,  2,        None),
    ('空头累计止损 -18%',              -0.18,   2,      None,  2,        None),

    # ── 空头：追踪止盈（峰值盈利回撤50%触发）─────────────────────────
    ('空头追踪止盈 峰值5%',            None,    2,      0.05,  2,        None),
    ('空头追踪止盈 峰值8%',            None,    2,      0.08,  2,        None),
    ('空头追踪止盈 峰值10%',           None,    2,      0.10,  2,        None),
    ('空头追踪止盈 峰值15%',           None,    2,      0.15,  2,        None),
    ('空头追踪止盈 峰值20%',           None,    2,      0.20,  2,        None),

    # ── 多头：追踪止盈 ───────────────────────────────────────────────
    ('多头追踪止盈 峰值8%',            None,    2,      None,  2,        0.08),
    ('多头追踪止盈 峰值12%',           None,    2,      None,  2,        0.12),
    ('多头追踪止盈 峰值15%',           None,    2,      None,  2,        0.15),
    ('多头追踪止盈 峰值20%',           None,    2,      None,  2,        0.20),

    # ── 组合：空头累计止损 + 追踪止盈 ────────────────────────────────
    ('空头 累计-12% + 追踪峰值10%',    -0.12,   2,      0.10,  2,        None),
    ('空头 累计-15% + 追踪峰值10%',    -0.15,   2,      0.10,  2,        None),
    ('空头 累计-15% + 追踪峰值15%',    -0.15,   2,      0.15,  2,        None),

    # ── 多空组合 ─────────────────────────────────────────────────────
    ('多空 空头累计-12% + 多头追踪15%', -0.12,  2,      None,  2,        0.15),
    ('多空 空头追踪10% + 多头追踪15%',  None,   2,      0.10,  2,        0.15),
    ('多空 空头累计-12%+追踪10% + 多头追踪15%', -0.12, 2, 0.10, 2,      0.15),
]

results = []
for (label, cs, cd, tr, tcd, ltr) in scenarios:
    v, _ = run_strategy(df, profiles,
                        short_cum_stop=cs, short_cum_cooldown=cd,
                        short_trail=tr, short_trail_cooldown=tcd,
                        long_trail=ltr, label=label)
    results.append(perf(v, label))

summary = pd.DataFrame(results).set_index('方案')
print(summary.to_string())

# ── 重点分析：最优方案的空头交易记录 ────────────────────────────────
print()
print('=' * 115)
print('  空头交易明细对比：原版 vs 空头累计止损-12%')
print('=' * 115)

for label, cs, cd, tr, tcd, ltr in [
    ('原版基准',       None,  2, None, 2, None),
    ('累计止损 -12%', -0.12,  2, None, 2, None),
    ('追踪止盈 峰值10%', None, 2, 0.10, 2, None),
]:
    v, elog = run_strategy(df, profiles,
                           short_cum_stop=cs, short_cum_cooldown=cd,
                           short_trail=tr, short_trail_cooldown=tcd,
                           long_trail=ltr)
    print(f'\n── {label} ──')
    # 提取空头交易
    in_short = False
    s_entry_idx = None
    s_entry_price = None
    trades = []
    sp = v['short_pos'].values
    cl = v['close'].values
    dt = v['date'].values

    for i in range(len(v)):
        if not in_short and sp[i] < 0:
            in_short = True
            s_entry_idx = i
            s_entry_price = cl[i]
        elif in_short and sp[i] == 0:
            in_short = False
            exit_price = cl[i - 1]
            lev = abs(sp[s_entry_idx])
            pnl = (s_entry_price / exit_price - 1) * lev * 100
            duration = i - s_entry_idx
            # 找exit reason
            reason = '翻转'
            for e in elog:
                if e['exit_idx'] == i - 1 or e['exit_idx'] == i:
                    reason = e['reason']
                    break
            trades.append({
                '入场日期': pd.Timestamp(dt[s_entry_idx]).strftime('%Y-%m-%d'),
                '出场日期': pd.Timestamp(dt[i-1]).strftime('%Y-%m-%d'),
                '杠杆': f'{lev:.0f}x',
                '入场价': f'${s_entry_price:,.0f}',
                '出场价': f'${exit_price:,.0f}',
                '收益%': f'{pnl:+.1f}%',
                '持续天': duration,
                '退出原因': reason,
            })
    if in_short:
        exit_price = cl[-1]
        lev = abs(sp[s_entry_idx])
        pnl = (s_entry_price / exit_price - 1) * lev * 100
        trades.append({
            '入场日期': pd.Timestamp(dt[s_entry_idx]).strftime('%Y-%m-%d'),
            '出场日期': '持仓中',
            '杠杆': f'{lev:.0f}x',
            '入场价': f'${s_entry_price:,.0f}',
            '出场价': f'${exit_price:,.0f}',
            '收益%': f'{pnl:+.1f}%',
            '持续天': len(v) - s_entry_idx,
            '退出原因': '持仓中',
        })
    print(pd.DataFrame(trades).to_string(index=False))

# ── 逐月对比（聚焦关键月份）─────────────────────────────────────────
print()
print('=' * 115)
print('  逐月净收益：原版 vs 最优改进方案')
print('=' * 115)

def monthly_net(valid):
    v2 = valid.copy()
    v2['ym'] = v2['date'].dt.to_period('M')
    months = sorted(v2['ym'].unique())
    rows = []
    for i, ym in enumerate(months):
        md = v2[v2['ym'] == ym]
        ep = md.iloc[0]['close']
        xp = (v2[v2['ym'] == months[i+1]].iloc[0]['close']
              if i+1 < len(months) else md.iloc[-1]['close'])
        rows.append({'month': str(ym),
                     'BTC%':  round((xp/ep-1)*100, 1),
                     'net%':  round(((1+md['net_day_ret']).prod()-1)*100, 1)})
    return pd.DataFrame(rows)

selected = [
    ('原版基准',            None,  2, None, 2, None),
    ('空头累计-12%',       -0.12,  2, None, 2, None),
    ('空头追踪峰值10%',     None,  2, 0.10, 2, None),
    ('多空最优组合',       -0.12,  2, 0.10, 2, 0.15),
]
dfs = {}
for (label, cs, cd, tr, tcd, ltr) in selected:
    v, _ = run_strategy(df, profiles, short_cum_stop=cs, short_cum_cooldown=cd,
                        short_trail=tr, short_trail_cooldown=tcd, long_trail=ltr)
    dfs[label] = monthly_net(v)

base = dfs['原版基准']
comp = pd.DataFrame({'month': base['month'], 'BTC%': base['BTC%']})
for label in ['原版基准', '空头累计-12%', '空头追踪峰值10%', '多空最优组合']:
    comp[label[:7]] = dfs[label]['net%'].values

print(comp.to_string(index=False))
