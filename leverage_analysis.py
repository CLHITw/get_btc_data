import pandas as pd
import numpy as np
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from regime_strategy import (load_and_prepare, profile_regimes, majority_vote,
    signal_confirm, apply_overflow_filter, STRATEGY_MAP, K_COLS,
    run_combined_backtest, performance_summary_combined)

df = load_and_prepare('btc.xlsx')
profiles = profile_regimes(df)

def get_votes(row, profiles):
    votes = []
    for k in K_COLS:
        kv = row.get(k)
        if pd.isna(kv):
            continue
        match = profiles[k][profiles[k]['regime'] == int(kv)]
        if len(match) == 0:
            continue
        votes.append(match.iloc[0]['type'])
    return votes


valid = df.dropna(subset=K_COLS).copy()
valid['ym'] = valid['date'].dt.to_period('M')
months = sorted(valid['ym'].unique())

rows = []
for i, ym in enumerate(months):
    md = valid[valid['ym'] == ym]
    row0 = md.iloc[0]
    votes = get_votes(row0, profiles)
    bull_n    = votes.count('BULL')
    bear_n    = votes.count('BEAR')
    neutral_n = votes.count('NEUTRAL')
    vote = majority_vote(row0, profiles, 4)
    sc   = signal_confirm(row0, vote) if vote in ('BULL', 'NEUTRAL', 'BEAR') else False
    ep = row0['close']
    if i + 1 < len(months):
        xp = valid[valid['ym'] == months[i + 1]].iloc[0]['close']
    else:
        xp = md.iloc[-1]['close']
    btc_r = (xp / ep - 1) * 100
    rows.append({
        'month':     str(ym),
        'vote':      vote,
        'sc':        sc,
        'bull_n':    bull_n,
        'bear_n':    bear_n,
        'neutral_n': neutral_n,
        'btc_r':     round(btc_r, 2),
        'bw':        round(row0.get('boll_width_z', np.nan), 2),
        'mh':        round(row0.get('macd_hist_z', np.nan), 2),
        'mz':        round(row0.get('macd_z', np.nan), 2),
        'atr':       round(row0.get('atr_pct_z', np.nan), 2),
        'rsi':       round(row0.get('rsi_norm', np.nan), 2),
        'entry_p':   ep,
    })

res = pd.DataFrame(rows)

# ─── 1. BULL月：按票数分层 ────────────────────────────────
print('=' * 60)
print('  BULL月：按票数分层')
print('=' * 60)
bull = res[res['vote'] == 'BULL'].copy()
print(f'{"票数":>5}  {"月数":>4}  {"平均BTC":>8}  {"胜率":>6}  {"最大":>7}  {"最小":>7}')
for n in [4, 5, 6]:
    sub = bull[bull['bull_n'] == n]
    if len(sub) == 0:
        continue
    print(f'  {n}/6  {len(sub):>4}  {sub.btc_r.mean():>+7.1f}%  '
          f'{(sub.btc_r>0).mean()*100:>5.0f}%  '
          f'{sub.btc_r.max():>+6.1f}%  {sub.btc_r.min():>+6.1f}%')

print()
print('BULL各月明细 (SC=信号确认通过):')
print(f'{"月份":>8}  {"票数":>4}  {"SC":>4}  {"BTC收益":>8}  {"bw":>5}  {"mh":>5}  {"mz":>5}  {"atr":>5}')
for _, r in bull.iterrows():
    print(f'  {r.month}  {r.bull_n}/6  {"Y" if r.sc else "N":>4}  {r.btc_r:>+7.1f}%  '
          f'{r.bw:>5.2f}  {r.mh:>5.2f}  {r.mz:>5.2f}  {r.atr:>5.2f}')

# ─── 2. BEAR月：入场信号质量 ─────────────────────────────
print()
print('=' * 60)
print('  BEAR月：信号质量分析')
print('=' * 60)
bear = res[res['vote'] == 'BEAR'].copy()
print(f'{"月份":>8}  {"票数":>4}  {"BTC收益":>8}  {"bw":>5}  {"mh":>5}  {"mz":>5}')
for _, r in bear.iterrows():
    print(f'  {r.month}  {r.bear_n}/6  {r.btc_r:>+7.1f}%  '
          f'{r.bw:>5.2f}  {r.mh:>5.2f}  {r.mz:>5.2f}')

# ─── 3. 杠杆回测：按票数调杠杆 ────────────────────────────
print()
print('=' * 60)
print('  杠杆方案测试（多头）')
print('=' * 60)

def run_levered(res, leverage_map, name, stop_monthly=-0.15):
    """
    leverage_map: {bull_n: leverage_multiplier}
    stop_monthly: 月内跌幅止损（比无杠杆更紧）
    """
    valid2 = df.dropna(subset=K_COLS).copy()
    valid2['ym'] = valid2['date'].dt.to_period('M')
    months2 = sorted(valid2['ym'].unique())

    strat_rets = []
    for i, ym in enumerate(months2):
        md = valid2[valid2['ym'] == ym]
        row0 = md.iloc[0]
        votes = get_votes(row0, profiles)
        bull_n = votes.count('BULL')
        vote = majority_vote(row0, profiles, 4)
        if vote in ('BULL', 'NEUTRAL', 'BEAR'):
            fv = vote if signal_confirm(row0, vote) else 'ABSTAIN'
        else:
            fv = vote

        base_pos = max(STRATEGY_MAP.get(fv, {'position': 0.0})['position'], 0)
        base_pos, _ = apply_overflow_filter(row0, base_pos)

        # 应用杠杆（仅对BULL仓位）
        if fv == 'BULL' and base_pos > 0:
            lev = leverage_map.get(bull_n, 1.0)
            pos = base_pos * lev
        else:
            pos = base_pos

        ep = row0['close']
        stopped = False
        exit_p = None

        # 月内止损（杠杆用更紧止损）
        if pos > 0 and stop_monthly is not None:
            for _, dr in md.iterrows():
                if dr['date'] <= row0['date']:
                    continue
                raw_ret = (dr['close'] / ep - 1)
                levered_ret = raw_ret * pos
                if levered_ret <= stop_monthly:
                    exit_p = dr['close']
                    stopped = True
                    break

        if not stopped:
            if i + 1 < len(months2):
                exit_p = valid2[valid2['ym'] == months2[i + 1]].iloc[0]['close']
            else:
                exit_p = md.iloc[-1]['close']

        raw_ret = (exit_p / ep - 1)
        strat_r = raw_ret * pos
        # 杠杆做多最大损失不超过-100%（爆仓上限）
        strat_r = max(strat_r, -1.0)
        strat_rets.append(strat_r)

    r = pd.Series(strat_rets)
    cum = (1 + r).prod() - 1
    ann = (1 + cum) ** (365 / 539) - 1
    sr  = r.mean() / r.std() * np.sqrt(12) if r.std() > 0 else 0
    dd  = ((1 + r).cumprod() / (1 + r).cumprod().cummax() - 1).min()
    wr  = (r[r != 0] > 0).mean()
    return {
        '方案': name,
        '累计收益': f'{cum*100:.1f}%',
        '年化收益': f'{ann*100:.1f}%',
        '月胜率': f'{wr*100:.0f}%',
        '夏普': round(sr, 2),
        '最大回撤': f'{dd*100:.1f}%',
    }

scenarios = [
    ({'4': 1.0, '5': 1.0, '6': 1.0}, '原版（无杠杆）', None),
    ({'4': 1.0, '5': 1.5, '6': 2.0}, '5票1.5x / 6票2x', -0.15),
    ({'4': 1.0, '5': 1.0, '6': 2.0}, '6票才2x', -0.15),
    ({'4': 1.5, '5': 1.5, '6': 2.0}, '全部加杠杆', -0.15),
    ({'4': 1.0, '5': 2.0, '6': 3.0}, '激进：5票2x/6票3x', -0.20),
]

results = []
for lmap_raw, name, stop in scenarios:
    lmap = {int(k): v for k, v in lmap_raw.items()}
    results.append(run_levered(res, lmap, name, stop))

print(pd.DataFrame(results).set_index('方案').to_string())

# ─── 4. BULL月票数分布回顾 ────────────────────────────────
print()
print('=' * 60)
print('  BULL月票数分布 + 大涨月识别')
print('=' * 60)
bull_big = bull[bull['btc_r'] > 10].copy()
bull_sm  = bull[(bull['btc_r'] > 0) & (bull['btc_r'] <= 10)].copy()
bull_neg = bull[bull['btc_r'] <= 0].copy()
print(f'大涨(>10%): {len(bull_big)}月  票数均值={bull_big.bull_n.mean():.1f}  bw均值={bull_big.bw.mean():.2f}')
print(f'小涨(0~10%): {len(bull_sm)}月  票数均值={bull_sm.bull_n.mean():.1f}  bw均值={bull_sm.bw.mean():.2f}')
print(f'亏损:       {len(bull_neg)}月  票数均值={bull_neg.bull_n.mean():.1f}  bw均值={bull_neg.bw.mean():.2f}')
print()
print('大涨月明细:')
print(bull_big[['month','bull_n','btc_r','bw','mh','mz','atr']].to_string(index=False))
print()
print('亏损月明细:')
print(bull_neg[['month','bull_n','btc_r','bw','mh','mz','atr']].to_string(index=False))
