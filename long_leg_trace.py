"""
多头腿逐日追踪：聚焦两段下跌期
  ① 2025-10 ~ 2025-11：BTC 12万 → 9万
  ② 2026-01 ~ 2026-03：BTC 9万 → 5万
"""
import sys, io
import pandas as pd
import numpy as np
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from regime_strategy import (load_and_prepare, profile_regimes, majority_vote,
    signal_confirm, apply_overflow_filter, STRATEGY_MAP, K_COLS)

df = load_and_prepare('btc.xlsx')
profiles = profile_regimes(df)

LONG_STOP    = -0.15
LONG_LEV_MAP = {4: 1.0, 5: 1.5, 6: 2.0}
MIN_AGREE    = 4


def get_counts(row):
    bull_n = bear_n = neut_n = 0
    for k in K_COLS:
        kv = row.get(k)
        if pd.isna(kv): continue
        match = profiles[k][profiles[k]['regime'] == int(kv)]
        if len(match) == 0: continue
        t = match.iloc[0]['type']
        if t == 'BULL': bull_n += 1
        elif t == 'BEAR': bear_n += 1
        else: neut_n += 1
    return bull_n, bear_n, neut_n


valid = df.dropna(subset=K_COLS).copy().reset_index(drop=True)
valid['ym'] = valid['date'].dt.to_period('M')
months = sorted(valid['ym'].unique())

# ── 逐月重建多头腿决策 + 逐日记录 ───────────────────────────────────
rows = []

for i, ym in enumerate(months):
    md_idx = valid[valid['ym'] == ym].index
    row0   = valid.loc[md_idx[0]]
    vote   = majority_vote(row0, profiles, MIN_AGREE)
    fv = vote if vote not in ('BULL','NEUTRAL','BEAR') else (
         vote if signal_confirm(row0, vote) else 'ABSTAIN')

    base_pos = max(STRATEGY_MAP.get(fv, {'position': 0.0})['position'], 0)
    base_pos, overflow = apply_overflow_filter(row0, base_pos)
    bull_n, bear_n, neut_n = get_counts(row0)

    pos = base_pos * LONG_LEV_MAP.get(bull_n, 1.0) if fv == 'BULL' and base_pos > 0 else base_pos

    entry_p  = row0['close']
    stopped  = False
    stop_day = None

    for j, idx in enumerate(md_idx):
        row = valid.loc[idx]

        if not stopped and j > 0 and pos > 0:
            levered_loss = (row['low'] / entry_p - 1) * pos
            if levered_loss <= LONG_STOP:
                stopped  = True
                stop_day = row['date']

        active_pos = 0.0 if (stopped and row['date'] >= stop_day) else pos
        float_pnl  = (row['close'] / entry_p - 1) * active_pos if active_pos > 0 else 0.0

        rows.append({
            'date':      row['date'],
            'close':     row['close'],
            'low':       row['low'],
            'month':     str(ym),
            'vote':      fv,
            'bull_n':    bull_n,
            'bear_n':    bear_n,
            'neut_n':    neut_n,
            'overflow':  overflow,
            'pos':       active_pos,
            'entry_p':   entry_p,
            'float_pnl': float_pnl,
            'stopped':   stopped and row['date'] >= stop_day,
            'stop_trig': row['date'] == stop_day,
        })

trace = pd.DataFrame(rows)


def print_period(title, start, end):
    mask = (trace['date'] >= pd.Timestamp(start)) & (trace['date'] <= pd.Timestamp(end))
    sub  = trace[mask].copy()
    print(f'\n{"="*100}')
    print(f'  {title}  ({start} ~ {end})')
    print(f'{"="*100}')
    print(f'{"日期":<12} {"收盘价":>9} {"日内低":>9} {"月份":<8} {"票":>4} '
          f'{"投票结果":<10} {"多头仓位":>6} {"入场价":>9} {"浮动PnL":>9} {"状态":<10}')
    print('-' * 100)

    prev_month = None
    for _, r in sub.iterrows():
        if r['month'] != prev_month:
            # 月初汇总行
            print(f'\n  ▶ {r["month"]}月  投票={r["vote"]}  '
                  f'牛{r["bull_n"]}熊{r["bear_n"]}中{r["neut_n"]}  '
                  f'溢出过滤={r["overflow"]}  入场价=${r["entry_p"]:,.0f}')
            prev_month = r['month']

        status = ''
        if r['stop_trig']:  status = '⚡止损触发'
        elif r['stopped']:  status = '  (已止损)'

        print(f'  {str(r["date"])[:10]:<12} '
              f'${r["close"]:>8,.0f} '
              f'${r["low"]:>8,.0f} '
              f'{r["month"]:<8} '
              f'{int(r["bull_n"]):>2}票 '
              f'{r["vote"]:<10} '
              f'{r["pos"]:>6.2f}x '
              f'${r["entry_p"]:>8,.0f} '
              f'{r["float_pnl"]:>+8.1%} '
              f'{status}')


# ① 2025-10月～2025-11月
print_period('① BTC 12万→9万 下跌期（多头处理）',
             '2025-10-01', '2025-11-30')

# ② 2026-01月～2026-03月
print_period('② BTC 9万→5万 下跌期（多头处理）',
             '2026-01-01', '2026-03-31')

# ── 月度决策汇总表 ───────────────────────────────────────────────────
print(f'\n{"="*100}')
print('  月度多头决策汇总（全部）')
print(f'{"="*100}')
monthly_summary = []
for i, ym in enumerate(months):
    md_idx = valid[valid['ym'] == ym].index
    row0   = valid.loc[md_idx[0]]
    vote   = majority_vote(row0, profiles, MIN_AGREE)
    fv = vote if vote not in ('BULL','NEUTRAL','BEAR') else (
         vote if signal_confirm(row0, vote) else 'ABSTAIN')
    base_pos = max(STRATEGY_MAP.get(fv, {'position': 0.0})['position'], 0)
    base_pos, overflow = apply_overflow_filter(row0, base_pos)
    bull_n, bear_n, neut_n = get_counts(row0)
    pos = base_pos * LONG_LEV_MAP.get(bull_n, 1.0) if fv == 'BULL' and base_pos > 0 else base_pos

    # 检查是否止损
    entry_p = row0['close']
    stopped = False
    for j, idx in enumerate(md_idx):
        if j == 0: continue
        row = valid.loc[idx]
        if pos > 0 and (row['low'] / entry_p - 1) * pos <= LONG_STOP:
            stopped = True
            break

    # 月末收益
    if i + 1 < len(months):
        exit_p = valid[valid['ym'] == months[i+1]].iloc[0]['close']
    else:
        exit_p = valid.loc[md_idx[-1]]['close']
    btc_r    = (exit_p / entry_p - 1) * 100
    strat_r  = (exit_p / entry_p - 1) * pos * 100 if not stopped else 0.0

    monthly_summary.append({
        '月份':     str(ym),
        '入场价':   f'${entry_p:,.0f}',
        '投票':     fv,
        '牛票':     bull_n,
        '仓位':     f'{pos:.2f}x',
        '溢出':     '是' if overflow else '',
        '止损':     '⚡是' if stopped else '',
        'BTC%':     f'{btc_r:+.1f}%',
        '策略%':    f'{strat_r:+.1f}%',
    })

print(pd.DataFrame(monthly_summary).to_string(index=False))
