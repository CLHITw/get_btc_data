"""
BTC 价格折线图 + 六维 Cluster 组合标注（最终稳定版）
已修复 PNL 图表显示问题 + 日志接口 500 错误
运行: python btc_chart_app.py
访问: http://<服务器IP>:5000
"""

import sys, json, os, math, colorsys, threading, time, traceback, hmac, hashlib
import requests
from urllib.parse import urlencode
import pandas as pd
import plotly.graph_objects as go
import plotly.utils
from flask import Flask, render_template_string, jsonify

sys.path.insert(0, '/root/Desktop/btc/get_data/get_btc_data')
from regime_strategy import (load_and_prepare, profile_regimes, K_COLS,
                             majority_vote, signal_confirm, apply_overflow_filter, STRATEGY_MAP)

app = Flask(__name__)

# ── 路径配置 ─────────────────────────────────────────────────────────
DATA_PATH        = '/root/Desktop/btc/get_data/get_btc_data/btc.xlsx'
LOG_PATH         = '/root/Desktop/btc/eth_live_1/eth_trader.log'
SIGNAL_LOG_PATH  = '/root/Desktop/btc/get_data/get_btc_data/combined_signal.log'
PNL_LOG_PATH     = '/root/Desktop/btc/get_data/get_btc_data/pnl_records.json'
BACKTEST_PATH    = '/root/Desktop/btc/get_data/get_btc_data/results/full_strategy_backtest.json'
ETH_DATA_PATH    = '/root/Desktop/btc/eth_live_1/data/eth/eth.xlsx'
ETH_SIGNAL_PATH  = '/root/Desktop/btc/eth_live_1/data/eth/best_variant_daily.csv'
ETH_SHORT_PATH   = '/root/Desktop/btc/eth_live_1/data/eth/baseline_short_daily.csv'
LIVE_TRADES_PATH = '/root/Desktop/btc/live_trades.json'
TRADE_LOG_PATH   = '/root/Desktop/btc/trade_log.xlsx'
TRAIL_STOP_LOG_PATH = '/root/Desktop/btc/get_data/get_btc_data/trail_stop.log'
LOG_LINES        = 60

SYMBOLS = [
    'circle', 'square', 'diamond', 'triangle-up', 'triangle-down',
    'star', 'hexagon', 'cross', 'x', 'pentagon',
    'triangle-left', 'triangle-right', 'hexagram', 'hourglass',
    'bowtie', 'asterisk', 'circle-open', 'square-open', 'diamond-open',
    'triangle-up-open', 'star-open', 'hexagon-open'
]

# ── 辅助函数 ─────────────────────────────────────────────────────────
def safe_read_log(path: str, n: int = LOG_LINES):
    """安全读取日志，防止任何异常导致 500 错误"""
    if not os.path.exists(path):
        return [{'level': 'info', 'text': f'{os.path.basename(path)} 文件不存在'}]

    try:
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            lines = f.readlines()
        lines = [l.rstrip() for l in lines if l.strip()][-n:]

        result = []
        for line in lines:
            if 'ERROR' in line.upper():
                level = 'error'
            elif 'WARNING' in line.upper():
                level = 'warning'
            else:
                level = 'info'
            result.append({'level': level, 'text': line})
        return result
    except Exception as e:
        return [{'level': 'error', 'text': f'读取日志失败: {str(e)}'}]


def load_data():
    df = load_and_prepare(DATA_PATH)
    missing_k_cols = [k for k in K_COLS if k not in df.columns]
    if missing_k_cols:
        df = df.copy()
        for k in missing_k_cols:
            df[k] = pd.NA

    profiles = profile_regimes(df)
    bull_n_list, bear_n_list, neut_n_list = [], [], []
    for _, row in df.iterrows():
        counts = {'BULL': 0, 'BEAR': 0, 'NEUTRAL': 0}
        for k in K_COLS:
            kv = row.get(k)
            if pd.isna(kv):
                continue
            match = profiles.get(k, pd.DataFrame())
            if len(match) == 0:
                continue
            match = match[match['regime'] == int(kv)]
            if len(match) == 0:
                continue
            t = match.iloc[0]['type']
            if t in counts:
                counts[t] += 1
        bull_n_list.append(counts['BULL'])
        bear_n_list.append(counts['BEAR'])
        neut_n_list.append(counts['NEUTRAL'])

    df['bull_n'] = bull_n_list
    df['bear_n'] = bear_n_list
    df['neut_n'] = neut_n_list

    def _vote_result(bull, bear, neut):
        if bull >= 4 and bull >= bear and bull >= neut: return 'BULL'
        if bear >= 4 and bear >= bull and bear >= neut: return 'BEAR'
        if neut >= 4 and neut >= bull and neut >= bear: return 'NEUTRAL'
        return 'OTHER'

    df['vote_result'] = [_vote_result(b, be, n)
                         for b, be, n in zip(df['bull_n'], df['bear_n'], df['neut_n'])]
    return df


def compute_equity_curve(df: pd.DataFrame) -> dict:
    """跑完整策略回测，返回每日权益曲线数据供前端使用。"""
    profiles = profile_regimes(df)
    valid = df.dropna(subset=K_COLS).copy().reset_index(drop=True)
    valid['ym'] = valid['date'].dt.to_period('M')
    valid['daily_vote'] = valid.apply(lambda r: majority_vote(r, profiles, 4), axis=1)
    valid['daily_ret'] = valid['close'].pct_change().fillna(0.0)
    months = sorted(valid['ym'].unique())

    def get_votes_local(row):
        votes = []
        for k in K_COLS:
            kv = row.get(k)
            if pd.isna(kv): continue
            match = profiles[k][profiles[k]['regime'] == int(kv)]
            if len(match) == 0: continue
            votes.append(match.iloc[0]['type'])
        return votes

    # 多头腿：5票1.5x / 6票2x
    long_lev_map = {4: 1.0, 5: 1.5, 6: 2.0}
    long_pos_list = [0.0] * len(valid)
    for ym in months:
        md_idx = valid[valid['ym'] == ym].index
        row0 = valid.loc[md_idx[0]]
        vote = majority_vote(row0, profiles, 4)
        fv = (vote if signal_confirm(row0, vote) else 'ABSTAIN') if vote in ('BULL', 'NEUTRAL', 'BEAR') else vote
        base = max(STRATEGY_MAP.get(fv, {'position': 0.0})['position'], 0)
        base, _ = apply_overflow_filter(row0, base)
        if fv == 'BULL' and base > 0:
            bn = get_votes_local(row0).count('BULL')
            pos = base * long_lev_map.get(bn, 1.0)
        else:
            pos = base
        if pos <= 0: continue
        entry_p = row0['close']
        for idx in md_idx:
            row = valid.loc[idx]
            if row['date'] > row0['date'] and (row['close'] / entry_p - 1) * pos <= -0.15:
                break
            long_pos_list[idx] = pos
    valid['long_pos'] = long_pos_list

    # 空头腿：6票2x，4/5票1x
    short_lev_map = {4: 1.0, 5: 1.0, 6: 2.0}
    short_pos_list = [0.0] * len(valid)
    s_pos, s_entry, vote_win = 0.0, None, []
    for idx in range(len(valid)):
        row = valid.iloc[idx]
        price = row['close']
        vote_win.append(row['daily_vote'])
        if len(vote_win) > 3: vote_win.pop(0)
        last_n = vote_win[-3:] if len(vote_win) >= 3 else []
        if s_pos < 0 and s_entry:
            pnl = (price / s_entry - 1) * (-1) * abs(s_pos)
            if pnl <= -0.08:
                s_pos, s_entry = 0.0, None; vote_win = []
            elif len(last_n) == 3 and all(v == 'BULL' for v in last_n):
                s_pos, s_entry = 0.0, None
        if s_pos == 0 and len(last_n) == 3 and all(v == 'BEAR' for v in last_n):
            if signal_confirm(row, 'BEAR'):
                bn = get_votes_local(row).count('BEAR')
                lev = short_lev_map.get(bn, 1.0)
                s_pos, s_entry = -1.0 * lev, price
        short_pos_list[idx] = s_pos
    valid['short_pos'] = short_pos_list
    valid['net_pos'] = valid['long_pos'] + valid['short_pos']

    lp = valid['long_pos'].shift(1).fillna(0.0)
    sp = valid['short_pos'].shift(1).fillna(0.0)
    np_ = valid['net_pos'].shift(1).fillna(0.0)
    valid['net_ret'] = valid['daily_ret'] * np_
    valid['btc_eq'] = (1 + valid['daily_ret']).cumprod()
    valid['net_eq'] = (1 + valid['net_ret']).cumprod()

    return {
        'dates':        [str(d.date()) for d in valid['date']],
        'strategy_pct': [round((v - 1) * 100, 3) for v in valid['net_eq']],
        'btc_pct':      [round((v - 1) * 100, 3) for v in valid['btc_eq']],
        'btc_price':    [round(float(v), 0) for v in valid['close']],
        'net_pos':      [round(float(v), 3) for v in valid['net_pos']],
        'long_pos':     [round(float(v), 3) for v in valid['long_pos']],
        'short_pos':    [round(float(v), 3) for v in valid['short_pos']],
    }


def compute_combos(df: pd.DataFrame):
    valid = df.dropna(subset=K_COLS).copy()
    if len(valid) == 0:
        valid['combo'] = []
        return {}, valid
    valid['combo'] = valid[K_COLS].apply(tuple, axis=1)
    counts = valid['combo'].value_counts()

    keep = {}
    for combo, cnt in counts.items():
        if cnt >= 3:
            keep[combo] = cnt
        elif cnt == 2:
            dates = valid[valid['combo'] == combo]['date'].sort_values().tolist()
            if len(dates) == 2 and (dates[1] - dates[0]).days == 1:
                keep[combo] = cnt

    keep = dict(sorted(keep.items(), key=lambda x: x[1], reverse=True))
    return keep, valid


def gen_colors(n: int):
    colors = []
    for i in range(n):
        hue = i / n
        sat = 0.85 if i % 2 == 0 else 0.70
        val = 0.95 if i % 2 == 0 else 0.78
        r, g, b = colorsys.hsv_to_rgb(hue, sat, val)
        colors.append('#{:02x}{:02x}{:02x}'.format(int(r*255), int(g*255), int(b*255)))
    return colors


def freq_to_size(cnt: int, min_cnt: int, max_cnt: int) -> float:
    if max_cnt == min_cnt:
        return 11.0
    t = math.log(cnt - min_cnt + 1) / math.log(max_cnt - min_cnt + 1)
    return round(7 + t * 8, 1)


def freq_to_opacity(cnt: int, min_cnt: int, max_cnt: int) -> float:
    if max_cnt == min_cnt:
        return 0.75
    t = math.log(cnt - min_cnt + 1) / math.log(max_cnt - min_cnt + 1)
    return round(0.45 + t * 0.55, 2)


def make_time_axis(extra=None):
    axis = dict(
        type='date',
        showgrid=True,
        gridcolor='#21262d',
        zeroline=False,
        autorange=True,
        rangeslider=dict(visible=False),
        rangeselector=dict(
            bgcolor='#161b22',
            activecolor='#30363d',
            bordercolor='#30363d',
            font=dict(size=10, color='#e6edf3'),
            buttons=[
                dict(count=1, label='1M', step='month', stepmode='backward'),
                dict(count=3, label='3M', step='month', stepmode='backward'),
                dict(count=6, label='6M', step='month', stepmode='backward'),
                dict(count=1, label='1Y', step='year', stepmode='backward'),
                dict(step='all', label='ALL'),
            ],
        ),
    )
    if extra:
        axis.update(extra)
    return axis


def make_figure(df: pd.DataFrame, keep: dict, valid: pd.DataFrame):
    fig = go.Figure()

    HOVER_VOTES = 'Bull : %{customdata[5]}票  Bear : %{customdata[6]}票  Neut : %{customdata[7]}票<br>'
    CD_COLS = ['open', 'high', 'low', 'fgi', 'volume', 'bull_n', 'bear_n', 'neut_n']

    def _customdata(frame):
        return frame.reindex(columns=CD_COLS).values

    VOTE_BG = {'BULL': 'rgba(40,167,69,0.88)', 'BEAR': 'rgba(200,45,45,0.88)',
               'NEUTRAL': 'rgba(75,85,95,0.88)', 'OTHER': 'rgba(22,27,34,0.92)'}
    VOTE_BORDER = {'BULL': 'rgba(100,220,120,0.9)', 'BEAR': 'rgba(255,100,100,0.9)',
                   'NEUTRAL': 'rgba(150,160,170,0.9)', 'OTHER': 'rgba(48,54,61,0.9)'}

    def _vbg(v): return VOTE_BG.get(str(v), VOTE_BG['OTHER'])
    def _vborder(v): return VOTE_BORDER.get(str(v), VOTE_BORDER['OTHER'])
    def _hl(series):
        vlist = list(series)
        return dict(bgcolor=[_vbg(v) for v in vlist],
                    bordercolor=[_vborder(v) for v in vlist],
                    font=dict(color='#ffffff', size=12),
                    align='left')

    # 价格折线
    fig.add_trace(go.Scatter(
        x=df['date'], y=df['close'],
        mode='lines',
        name='BTC Close',
        line=dict(color='#F7931A', width=1.6),
        customdata=_customdata(df),
        hoverlabel=_hl(df['vote_result']),
        hovertemplate='<b>%{x|%Y-%m-%d}</b><br>Open : $%{customdata[0]:,.0f}<br>High : $%{customdata[1]:,.0f}<br>Low  : $%{customdata[2]:,.0f}<br>Close: <b>$%{y:,.0f}</b><br>FGI  : %{customdata[3]:.0f}<br>Vol  : %{customdata[4]:,.0f}<br>' + HOVER_VOTES + '<extra>BTC Price</extra>'
    ))

    # GMMA 短组 EWM spans [3,5,8,10,12,15] — 蓝色系
    _gmma_short_spans  = [3, 5, 8, 10, 12, 15]
    _gmma_short_colors = ['#1f6feb', '#388bfd', '#58a6ff', '#79c0ff', '#a5d6ff', '#cae8ff']
    for _i, (_span, _col) in enumerate(zip(_gmma_short_spans, _gmma_short_colors)):
        _ema = df['close'].ewm(span=_span, adjust=False).mean()
        fig.add_trace(go.Scatter(
            x=df['date'], y=_ema,
            mode='lines',
            name='GMMA 短组',
            legendgroup='gmma_short',
            showlegend=(_i == 0),
            line=dict(color=_col, width=0.8),
            hovertemplate=f'<b>%{{x|%Y-%m-%d}}</b><br>GMMA S{_span}: <b>${{y:,.0f}}</b><extra>GMMA S{_span}</extra>'
        ))

    # GMMA 长组 EWM spans [30,35,40,45,50,60] — 红色系
    _gmma_long_spans  = [30, 35, 40, 45, 50, 60]
    _gmma_long_colors = ['#b91c1c', '#dc2626', '#ef4444', '#f87171', '#fca5a5', '#fecaca']
    for _i, (_span, _col) in enumerate(zip(_gmma_long_spans, _gmma_long_colors)):
        _ema = df['close'].ewm(span=_span, adjust=False).mean()
        fig.add_trace(go.Scatter(
            x=df['date'], y=_ema,
            mode='lines',
            name='GMMA 长组',
            legendgroup='gmma_long',
            showlegend=(_i == 0),
            line=dict(color=_col, width=0.8),
            hovertemplate=f'<b>%{{x|%Y-%m-%d}}</b><br>GMMA L{_span}: <b>${{y:,.0f}}</b><extra>GMMA L{_span}</extra>'
        ))

    # 其他组合
    classified_idx = valid[valid['combo'].isin(keep)].index
    unclassified = valid.loc[~valid.index.isin(classified_idx)]
    if len(unclassified) > 0:
        fig.add_trace(go.Scatter(
            x=unclassified['date'], y=unclassified['close'],
            mode='markers',
            name='其他组合',
            marker=dict(symbol='circle', size=5, color='#444c56', opacity=0.4),
            customdata=_customdata(unclassified),
            hoverlabel=_hl(unclassified['vote_result']),
            hovertemplate='<b>%{x|%Y-%m-%d}</b><br>其他低频组合<br>Open : $%{customdata[0]:,.0f}<br>High : $%{customdata[1]:,.0f}<br>Low  : $%{customdata[2]:,.0f}<br>Close: <b>$%{y:,.0f}</b><br>FGI  : %{customdata[3]:.0f}<br>Vol  : %{customdata[4]:,.0f}<br>' + HOVER_VOTES + '<extra></extra>'
        ))

    # 高频组合
    combos_list = list(keep.items())
    n = len(combos_list)
    colors = gen_colors(n)
    counts_only = [c for _, c in combos_list] if len(combos_list) > 0 else [0]
    min_cnt, max_cnt = min(counts_only), max(counts_only)

    for i, (combo, cnt) in enumerate(combos_list):
        sub = valid[valid['combo'] == combo]
        size = freq_to_size(cnt, min_cnt, max_cnt)
        opacity = freq_to_opacity(cnt, min_cnt, max_cnt)
        color = colors[i]
        symbol = SYMBOLS[i % len(SYMBOLS)]
        combo_vals = ','.join(str(int(v)) for v in combo)
        label = f'G{i+1:02d}(×{cnt}) [{combo_vals}]'

        fig.add_trace(go.Scatter(
            x=sub['date'], y=sub['close'],
            mode='markers',
            name=label,
            marker=dict(symbol=symbol, size=size, color=color, opacity=opacity, line=dict(width=0.8, color='#ffffff')),
            customdata=_customdata(sub),
            hoverlabel=_hl(sub['vote_result']),
            hovertemplate='<b>%{x|%Y-%m-%d}</b><br><b>' + label + '</b><br>Open : $%{customdata[0]:,.0f}<br>High : $%{customdata[1]:,.0f}<br>Low  : $%{customdata[2]:,.0f}<br>Close: <b>$%{y:,.0f}</b><br>FGI  : %{customdata[3]:.0f}<br>Vol  : %{customdata[4]:,.0f}<br>' + HOVER_VOTES + '<extra></extra>'
        ))

    fig.update_layout(
        template='plotly_dark',
        paper_bgcolor='#0d1117',
        plot_bgcolor='#161b22',
        font=dict(family='Arial, sans-serif', color='#e6edf3'),
        title=dict(text='BTC Price · 六维 Cluster 组合标注（方案 C）', font=dict(size=14, color='#F7931A'), x=0.01),
        xaxis=make_time_axis(),
        yaxis=dict(showgrid=True, gridcolor='#21262d', zeroline=False, tickprefix='$', tickformat=',.0f'),
        legend=dict(orientation='v', x=1.02, xanchor='left', y=1.0, yanchor='top', bgcolor='rgba(13,17,23,0.85)', font=dict(size=10)),
        hovermode='closest',
        dragmode='pan',
        uirevision='price-chart',
        margin=dict(l=10, r=200, t=46, b=10),
    )
    return fig


def make_eth_figure():
    """ETH 价格折线图 + GMMA + 信号标注 + 每日多空 hover 背景色"""
    # ── 1. 加载价格数据 ───────────────────────────────────────────────
    df = pd.read_excel(ETH_DATA_PATH)
    df.columns = [c.strip().lower() for c in df.columns]
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)

    # ── 2. 提前加载信号文件，计算每天的多空结论 ──────────────────────
    sig = None
    if os.path.exists(ETH_SIGNAL_PATH):
        try:
            sig = pd.read_csv(ETH_SIGNAL_PATH)
            sig.columns = [c.strip().lower() for c in sig.columns]
            sig['date'] = pd.to_datetime(sig['date'])
            sig = sig.sort_values('date').reset_index(drop=True)
        except Exception as e:
            print(f'[WARN] ETH信号读取失败: {e}')
            sig = None

    # 每天的多空结论：LONG / SHORT / NEUTRAL
    stance_map = {}
    if sig is not None:
        lp = sig['scaled_long'].fillna(0) if 'scaled_long' in sig.columns else pd.Series(0.0, index=sig.index)
        sp = sig['short_lev'].fillna(0)   if 'short_lev'   in sig.columns else pd.Series(0.0, index=sig.index)
        _st = pd.Series('NEUTRAL', index=sig.index, dtype=object)
        _st[sp < 0] = 'SHORT'
        _st[lp > 0] = 'LONG'   # 多空同时持仓时以多头为准
        stance_map = dict(zip(sig['date'].dt.date, _st))

    df['_stance'] = df['date'].dt.date.map(stance_map).fillna('NEUTRAL')

    # hover 背景颜色映射
    _BG  = {'LONG': 'rgba(40,167,69,0.88)',  'SHORT': 'rgba(200,45,45,0.88)',  'NEUTRAL': 'rgba(75,85,95,0.88)'}
    _BD  = {'LONG': 'rgba(100,220,120,0.9)', 'SHORT': 'rgba(255,100,100,0.9)', 'NEUTRAL': 'rgba(150,160,170,0.9)'}
    _hl  = dict(
        bgcolor     = [_BG[s] for s in df['_stance']],
        bordercolor = [_BD[s] for s in df['_stance']],
        font=dict(color='#ffffff', size=12), align='left'
    )

    # ── 3. 绘图 ───────────────────────────────────────────────────────
    fig = go.Figure()

    # ETH 价格折线（亮黄色）
    ohlc = [c for c in ['open', 'high', 'low'] if c in df.columns]
    if ohlc:
        fig.add_trace(go.Scatter(
            x=df['date'], y=df['close'], mode='lines',
            name='ETH Close', line=dict(color='#FFD700', width=1.6),
            customdata=df[ohlc].values,
            hoverlabel=_hl,
            hovertemplate='<b>%{x|%Y-%m-%d}</b><br>Open : $%{customdata[0]:,.2f}<br>'
                          'High : $%{customdata[1]:,.2f}<br>Low  : $%{customdata[2]:,.2f}<br>'
                          'Close: <b>$%{y:,.2f}</b><extra>ETH Price</extra>'
        ))
    else:
        fig.add_trace(go.Scatter(
            x=df['date'], y=df['close'], mode='lines',
            name='ETH Close', line=dict(color='#FFD700', width=1.6),
            hoverlabel=_hl,
            hovertemplate='<b>%{x|%Y-%m-%d}</b><br>Close: <b>$%{y:,.2f}</b><extra>ETH Price</extra>'
        ))

    # GMMA 短组 [3,5,8,10,12,15] — 蓝色系
    for _i, (_span, _col) in enumerate(zip(
            [3, 5, 8, 10, 12, 15],
            ['#1f6feb', '#388bfd', '#58a6ff', '#79c0ff', '#a5d6ff', '#cae8ff'])):
        _ema = df['close'].ewm(span=_span, adjust=False).mean()
        fig.add_trace(go.Scatter(
            x=df['date'], y=_ema, mode='lines',
            name='GMMA 短组', legendgroup='eth_gmma_short', showlegend=(_i == 0),
            line=dict(color=_col, width=0.8),
            hovertemplate=f'<b>%{{x|%Y-%m-%d}}</b><br>GMMA S{_span}: <b>${{y:,.2f}}</b><extra>GMMA S{_span}</extra>'
        ))

    # GMMA 长组 [30,35,40,45,50,60] — 红色系
    for _i, (_span, _col) in enumerate(zip(
            [30, 35, 40, 45, 50, 60],
            ['#b91c1c', '#dc2626', '#ef4444', '#f87171', '#fca5a5', '#fecaca'])):
        _ema = df['close'].ewm(span=_span, adjust=False).mean()
        fig.add_trace(go.Scatter(
            x=df['date'], y=_ema, mode='lines',
            name='GMMA 长组', legendgroup='eth_gmma_long', showlegend=(_i == 0),
            line=dict(color=_col, width=0.8),
            hovertemplate=f'<b>%{{x|%Y-%m-%d}}</b><br>GMMA L{_span}: <b>${{y:,.2f}}</b><extra>GMMA L{_span}</extra>'
        ))

    shapes = []
    price_ref = df[['date', 'close']].rename(columns={'close': '_price'})

    # ── 4. 信号标注（复用已加载的 sig） ──────────────────────────────
    if sig is not None:
        try:
            sp2 = sig.merge(price_ref, on='date', how='left')

            def _add_marker(col, name, symbol, color, size=13):
                if col not in sp2.columns:
                    return
                pts = sp2[sp2[col].fillna(False).astype(bool)]
                if len(pts) == 0:
                    return
                fig.add_trace(go.Scatter(
                    x=pts['date'], y=pts['_price'], mode='markers', name=name,
                    marker=dict(symbol=symbol, size=size, color=color,
                                line=dict(width=1, color='#fff')),
                    hovertemplate=f'<b>%{{x|%Y-%m-%d}}</b><br>{name}<br>$%{{y:,.2f}}<extra></extra>'
                ))

            _add_marker('hidden_bull_detect', '隐性看多背离', 'triangle-up',   '#3fb950', 13)
            _add_marker('hidden_bear_detect', '隐性看空背离', 'triangle-down', '#f85149', 13)
            _add_marker('macd_bear_detect',   'MACD顶背离',   'triangle-down', '#d29922', 10)
            _add_marker('gmma_long_active',   'GMMA多头激活', 'diamond',       '#79c0ff',  8)
            _add_marker('gmma_short_active',  'GMMA空头激活', 'diamond',       '#ffa198',  8)

            # 持仓期间背景着色
            lp2   = sig['scaled_long'].fillna(0) if 'scaled_long' in sig.columns else pd.Series(0, index=sig.index)
            sp_lv = sig['short_lev'].fillna(0)   if 'short_lev'   in sig.columns else pd.Series(0, index=sig.index)
            dates = [str(d.date()) for d in sig['date']]
            i = 0
            while i < len(sig):
                is_long  = float(lp2.iloc[i]) > 0
                is_short = float(sp_lv.iloc[i]) < 0
                if not is_long and not is_short:
                    i += 1; continue
                j = i + 1
                while j < len(sig):
                    if (float(lp2.iloc[j]) > 0) == is_long and (float(sp_lv.iloc[j]) < 0) == is_short:
                        j += 1
                    else:
                        break
                shapes.append(dict(
                    type='rect', xref='x', yref='paper',
                    x0=dates[i], x1=dates[min(j, len(dates) - 1)],
                    y0=0, y1=1,
                    fillcolor='rgba(40,167,69,0.10)' if is_long else 'rgba(200,45,45,0.10)',
                    line=dict(width=0)
                ))
                i = j
        except Exception as e:
            print(f'[WARN] ETH信号标注失败: {e}')

    # 基础空头信号（baseline_short_daily.csv）
    if os.path.exists(ETH_SHORT_PATH):
        try:
            sh = pd.read_csv(ETH_SHORT_PATH)
            sh.columns = [c.strip().lower() for c in sh.columns]
            sh['date'] = pd.to_datetime(sh['date'])
            sh = sh.merge(price_ref, on='date', how='left')
            if 'position' in sh.columns:
                active = sh[sh['position'] < 0]
                if len(active) > 0:
                    fig.add_trace(go.Scatter(
                        x=active['date'], y=active['_price'], mode='markers',
                        name='基础空头信号',
                        marker=dict(symbol='square', size=6, color='#ff7b72',
                                    opacity=0.55, line=dict(width=0.5, color='#fff')),
                        customdata=active['position'].abs().values,
                        hovertemplate='<b>%{x|%Y-%m-%d}</b><br>基础空头 %{customdata:.1f}x<br>'
                                      '$%{y:,.2f}<extra></extra>'
                    ))
        except Exception as e:
            print(f'[WARN] ETH基础空头读取失败: {e}')

    fig.update_layout(
        template='plotly_dark',
        paper_bgcolor='#0d1117',
        plot_bgcolor='#161b22',
        font=dict(family='Arial, sans-serif', color='#e6edf3'),
        title=dict(text='ETH Price · GMMA + 信号标注', font=dict(size=14, color='#FFD700'), x=0.01),
        xaxis=make_time_axis(),
        yaxis=dict(showgrid=True, gridcolor='#21262d', zeroline=False, tickprefix='$', tickformat=',.2f'),
        legend=dict(orientation='v', x=1.02, xanchor='left', y=1.0, yanchor='top',
                    bgcolor='rgba(13,17,23,0.85)', font=dict(size=10)),
        hovermode='closest',
        shapes=shapes,
        dragmode='pan',
        uirevision='eth-chart',
        margin=dict(l=10, r=200, t=46, b=10),
    )
    return fig


# ── 缓存 ─────────────────────────────────────────────────────────────
_cache = {}

def refresh_cache():
    try:
        df = load_data()
        keep, valid = compute_combos(df)
        fig = make_figure(df, keep, valid)

        _cache['graph_json'] = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        _cache['total_rows'] = len(df)
        _cache['labeled_rows'] = len(valid)
        _cache['classified'] = valid[valid['combo'].isin(keep)].shape[0]
        _cache['combo_count'] = len(keep)
        _cache['df'] = df
        _cache['equity_curve'] = compute_equity_curve(df)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 数据刷新成功，共 {len(df)} 行")
    except Exception as e:
        print(f"[ERROR] refresh_cache 失败: {e}")
        traceback.print_exc()

    # ETH 图表独立刷新（失败不影响 BTC）
    try:
        eth_fig = make_eth_figure()
        _cache['eth_graph_json'] = json.dumps(eth_fig, cls=plotly.utils.PlotlyJSONEncoder)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] ETH图表刷新成功")
    except Exception as e:
        print(f"[WARN] ETH图表生成失败: {e}")
        _cache['eth_graph_json'] = None

def _auto_refresh():
    """每天 00:10（服务器本地时间）自动重载图表数据"""
    while True:
        now  = time.localtime()
        secs = now.tm_hour * 3600 + now.tm_min * 60 + now.tm_sec
        target = 0 * 3600 + 10 * 60   # 00:10
        wait = target - secs
        if wait <= 0:
            wait += 86400
        time.sleep(wait)
        refresh_cache()

refresh_cache()
threading.Thread(target=_auto_refresh, daemon=True).start()


# ── Binance 实盘盈亏 ──────────────────────────────────────────────────
def _get_env(name):
    """读取环境变量。
    优先从进程环境（os.environ）取；若为空则直接解析 /etc/environment，
    兼容 systemd service 未配置 EnvironmentFile 的情况。
    自动去掉两端引号，兼容有无引号的写法。"""
    val = os.environ.get(name, '').strip().strip('"\'')
    if val:
        return val
    # 回退：直接读 /etc/environment
    try:
        with open('/etc/environment', 'r', encoding='utf-8') as _f:
            for _line in _f:
                _line = _line.strip()
                if _line.startswith(name + '='):
                    return _line[len(name) + 1:].strip().strip('"\'')
    except Exception:
        pass
    return ''

_FAPI = 'https://fapi.binance.com'

def _bget(path, key, secret, params=None):
    """Binance HMAC-SHA256 签名 GET 请求。
    签名必须在 query string 固定后计算，不能让 requests 重排参数。"""
    p = dict(params or {})
    p['timestamp'] = int(time.time() * 1000)
    qs  = urlencode(p)                          # 固定顺序的 query string
    sig = hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
    url = f"{_FAPI}{path}?{qs}&signature={sig}" # 直接拼 URL，绕过 requests 的参数处理
    r   = requests.get(url, headers={'X-MBX-APIKEY': key}, timeout=20)
    if not r.ok:
        raise RuntimeError(f"Binance {r.status_code}: {r.text}")
    data = r.json()
    if isinstance(data, dict) and 'code' in data and int(data['code']) < 0:
        raise RuntimeError(f"Binance API 错误 {data['code']}: {data.get('msg', data)}")
    return data

def _fetch_pos(key, secret, symbol):
    """从 Binance API 获取当前仓位 + 实时价格"""
    data = _bget('/fapi/v2/positionRisk', key, secret, {'symbol': symbol})
    for p in (data if isinstance(data, list) else []):
        if p.get('symbol') == symbol:
            amt = float(p.get('positionAmt', 0))
            return {
                'amt':        amt,
                'entry':      float(p.get('entryPrice', 0)),
                'unrealized': float(p.get('unRealizedProfit', 0)),
                'mark':       float(p.get('markPrice', 0)),
                'leverage':   int(p.get('leverage', 1)),
                'side':       'LONG' if amt > 0 else ('SHORT' if amt < 0 else 'FLAT'),
            }
    return {'amt': 0, 'entry': 0, 'unrealized': 0, 'mark': 0, 'leverage': 1, 'side': 'FLAT'}

def _flat_pos():
    return {'amt': 0, 'entry': 0, 'unrealized': 0, 'mark': 0, 'leverage': 1, 'side': 'FLAT'}


def _find_position_open_time(key, secret, symbol):
    """
    查询最近 7 天的成交记录，找出当前持仓最后一次从零起步的开仓时刻。
    返回 '%Y-%m-%dT%H:%M:%S' 格式字符串，失败返回 None。
    """
    try:
        start_ms = int((time.time() - 7 * 86400) * 1000)
        trades   = _bget('/fapi/v1/userTrades', key, secret,
                         {'symbol': symbol, 'startTime': start_ms, 'limit': 500})
        if not isinstance(trades, list) or not trades:
            return None
        trades   = sorted(trades, key=lambda t: int(t['time']))
        net      = 0.0
        t_open   = None
        for t in trades:
            qty   = float(t.get('qty', 0))
            delta = qty if t.get('buyer') else -qty   # 买+, 卖-
            if abs(net) < 1e-9 and abs(net + delta) > 1e-9:
                # 从零到非零：记录这个时刻为开仓起点
                ms     = int(t['time'])
                t_open = time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(ms / 1000))
            net += delta
        return t_open
    except Exception as e:
        print(f'[WARN] _find_position_open_time({symbol}): {e}')
        return None


# ── 实盘交易记录 ──────────────────────────────────────────────────────
_LIVE_TRADES_EMPTY = lambda: {
    'btc': {'last_side': 'FLAT', 'last_amt': 0.0, 'last_entry': 0.0, 'trades': [], 'snapshots': []},
    'eth': {'last_side': 'FLAT', 'last_amt': 0.0, 'last_entry': 0.0, 'trades': [], 'snapshots': []},
}

def _load_live_trades():
    """读取交易记录。读取失败时返回空结构但不触发覆盖写，保留原文件。"""
    if os.path.exists(LIVE_TRADES_PATH):
        try:
            with open(LIVE_TRADES_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
            # 基本结构校验：两个币种都存在才认为有效
            if isinstance(data, dict) and 'btc' in data and 'eth' in data:
                return data
            print(f'[WARN] live_trades.json 结构异常，当作空文件处理')
        except Exception as e:
            print(f'[WARN] 读取 live_trades.json 失败: {e}，保留原文件不覆盖')
            # 返回 None 表示"读取失败"，与"正常空文件"区分，禁止后续写入
            return None
    return _LIVE_TRADES_EMPTY()


def _save_live_trades(data):
    """原子写入：先写临时文件再重命名，避免写入途中崩溃导致 JSON 截断。
    写入前保留一份滚动备份（.bak），防止数据被意外清空。"""
    if data is None:
        # _load_live_trades 失败时返回 None，此时绝对不覆盖
        return
    try:
        # 滚动备份：只保留最近一份，文件名固定为 .bak（不产生垃圾文件）
        bak = LIVE_TRADES_PATH + '.bak'
        if os.path.exists(LIVE_TRADES_PATH):
            try:
                import shutil
                shutil.copy2(LIVE_TRADES_PATH, bak)
            except Exception:
                pass
        # 原子写入：写临时文件 → rename
        tmp = LIVE_TRADES_PATH + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, LIVE_TRADES_PATH)
    except Exception as e:
        print(f'[WARN] 保存交易记录失败: {e}')


def _append_trade_log(coin, event, trade):
    """将开/平仓事件追加写入 trade_log.xlsx（一次只追加一行）"""
    try:
        row = {
            '时间':      trade.get('open_time') if event == 'OPEN' else trade.get('close_time'),
            '币种':      coin.upper(),
            '事件':      event,
            '方向':      trade.get('side', ''),
            '数量':      trade.get('amt', 0),
            '价格':      trade.get('entry') if event == 'OPEN' else trade.get('close_price'),
            '杠杆':      trade.get('leverage', 1),
            '已实现盈亏': trade.get('realized_pnl', ''),
        }
        df_new = pd.DataFrame([row])
        if os.path.exists(TRADE_LOG_PATH):
            df_old = pd.read_excel(TRADE_LOG_PATH, engine='openpyxl')
            df     = pd.concat([df_old, df_new], ignore_index=True)
        else:
            df = df_new
        df.to_excel(TRADE_LOG_PATH, index=False, engine='openpyxl')
        print(f'[INFO] trade_log.xlsx 已写入 {coin.upper()} {event}')
    except Exception as e:
        print(f'[WARN] 写入 trade_log.xlsx 失败: {e}')


def _init_position_from_api(live_data, coin_key, key, secret, symbol, pos):
    """
    有持仓但无本地记录时，从 Binance 成交历史查出真实开仓时间，
    建立第一条 trade 记录和零起点快照，并写入 xlsx。
    """
    rec      = live_data[coin_key]
    open_t   = _find_position_open_time(key, secret, symbol)
    if not open_t:
        open_t = time.strftime('%Y-%m-%dT%H:%M:%S')   # fallback：用当前时间
    trade = {
        'open_time': open_t, 'close_time': None,
        'side': pos['side'], 'amt': abs(pos['amt']),
        'entry': pos['entry'], 'close_price': None,
        'leverage': pos.get('leverage', 1), 'realized_pnl': None,
    }
    rec['trades'].append(trade)
    # 零起点快照：时间=开仓时间，收益=0
    rec['snapshots'].append({
        'time': open_t, 'mark': pos['entry'],
        'unrealized': 0.0, 'cum_realized': 0.0,
    })
    rec['last_side']  = pos['side']
    rec['last_amt']   = pos['amt']
    rec['last_entry'] = pos['entry']
    _append_trade_log(coin_key, 'OPEN', trade)
    print(f'[INFO] {coin_key.upper()} 初始化：开仓时间={open_t}, 入场价={pos["entry"]}')


def _update_live_trades(data, coin_key, pos):
    """检测平仓/换向/开仓事件，并按分钟级频率持续记录快照"""
    # ── mark=0 表示 API 密钥缺失或请求失败，跳过防止写入无效数据 ──
    if pos.get('mark', 0) <= 0:
        return

    rec        = data[coin_key]
    now_str    = time.strftime('%Y-%m-%dT%H:%M:%S')
    cur_side   = pos['side']
    cur_mark   = pos['mark']
    cur_entry  = pos['entry']
    cur_unreal = pos['unrealized']
    last_side  = rec.get('last_side', 'FLAT')
    changed    = False

    # ── 自愈：API 和本地均为 FLAT，但存在未平仓记录 → 判定为脏数据，自动清除 ──
    if cur_side == 'FLAT' and last_side == 'FLAT':
        phantom = [t for t in rec['trades'] if t.get('close_time') is None]
        if phantom:
            print(f'[WARN] {coin_key.upper()} API持续空仓但有{len(phantom)}条未平仓记录，自动清除')
            rec['trades']        = [t for t in rec['trades'] if t.get('close_time') is not None]
            rec['_pending_open'] = None
            return  # 本次不记快照，状态保持 FLAT

    # 平仓（有仓→FLAT，立即记录，无需防抖）
    if last_side != 'FLAT' and cur_side == 'FLAT':
        rec['_pending_open'] = None   # 清除可能残留的 pending
        for t in reversed(rec['trades']):
            if t.get('close_time') is None:
                entry = t.get('entry', 0)
                amt   = abs(t.get('amt', 0))
                pnl   = (cur_mark - entry) * amt if last_side == 'LONG' else (entry - cur_mark) * amt
                t['close_time']   = now_str
                t['close_price']  = cur_mark
                t['realized_pnl'] = round(pnl, 4)
                _append_trade_log(coin_key, 'CLOSE', t)
                break
        changed = True

    # 换向（先平旧仓再开新仓，方向明确，无需防抖）
    if last_side != 'FLAT' and cur_side != 'FLAT' and last_side != cur_side:
        rec['_pending_open'] = None
        for t in reversed(rec['trades']):
            if t.get('close_time') is None:
                entry = t.get('entry', 0)
                amt   = abs(t.get('amt', 0))
                pnl   = (cur_mark - entry) * amt if last_side == 'LONG' else (entry - cur_mark) * amt
                t['close_time']   = now_str
                t['close_price']  = cur_mark
                t['realized_pnl'] = round(pnl, 4)
                _append_trade_log(coin_key, 'CLOSE', t)
                break
        new_trade = {
            'open_time': now_str, 'close_time': None,
            'side': cur_side, 'amt': abs(pos['amt']),
            'entry': cur_entry, 'close_price': None,
            'leverage': pos.get('leverage', 1), 'realized_pnl': None,
        }
        rec['trades'].append(new_trade)
        _append_trade_log(coin_key, 'OPEN', new_trade)
        changed = True

    # 开仓（FLAT→有仓）— 防抖：需连续两次检测到才记录，过滤 Binance 平仓后的残留脏数据
    if last_side == 'FLAT' and cur_side != 'FLAT':
        pending = rec.get('_pending_open')
        if pending and pending.get('side') == cur_side:
            # 第二次确认 → 真实开仓，正式记录
            new_trade = {
                'open_time': pending.get('detected_at', now_str),
                'close_time': None,
                'side': cur_side, 'amt': abs(pos['amt']),
                'entry': cur_entry, 'close_price': None,
                'leverage': pos.get('leverage', 1), 'realized_pnl': None,
            }
            rec['trades'].append(new_trade)
            rec['_pending_open'] = None
            _append_trade_log(coin_key, 'OPEN', new_trade)
            changed = True
        else:
            # 第一次检测 → 挂 pending，本轮不记录，last_side 保持 FLAT
            rec['_pending_open'] = {
                'side':        cur_side,
                'detected_at': now_str,
                'entry':       cur_entry,
                'amt':         pos['amt'],
                'leverage':    pos.get('leverage', 1),
            }
            print(f'[INFO] {coin_key.upper()} 检测到 FLAT→{cur_side}，等待下一轮确认')
    else:
        # 非 FLAT→有仓 路径：清除残留 pending
        if rec.get('_pending_open') is not None and cur_side == 'FLAT':
            print(f'[INFO] {coin_key.upper()} pending_open 已失效（当前空仓），清除')
            rec['_pending_open'] = None

    # ── 快照 ──
    cum_realized = sum(t.get('realized_pnl') or 0.0 for t in rec['trades'])
    last_snap    = rec['snapshots'][-1] if rec['snapshots'] else None
    do_snap      = False

    if len(rec['trades']) > 0:
        if changed:
            do_snap = True
        elif not last_snap:
            do_snap = True
        else:
            try:
                last_t  = time.mktime(time.strptime(last_snap['time'], '%Y-%m-%dT%H:%M:%S'))
                do_snap = (time.time() - last_t) >= 55
            except Exception:
                do_snap = True

    if do_snap:
        just_opened = (last_side == 'FLAT' and cur_side != 'FLAT' and changed)
        rec['snapshots'].append({
            'time':         now_str,
            'mark':         cur_entry if just_opened else cur_mark,
            'unrealized':   0.0       if just_opened else cur_unreal,
            'cum_realized': round(cum_realized, 4),
        })

    # pending 期间保持 last_side=FLAT，避免下一轮误触发平仓逻辑
    if rec.get('_pending_open') is not None:
        rec['last_amt']   = pos['amt']
        rec['last_entry'] = cur_entry
    else:
        rec['last_side']  = cur_side
        rec['last_amt']   = pos['amt']
        rec['last_entry'] = cur_entry


_live_pnl_cache = {'data': None, 'ts': 0}
_LIVE_TTL = 15   # 页面打开时按更接近实时的频率刷新


def get_live_pnl():
    now = time.time()
    if _live_pnl_cache['data'] is not None and now - _live_pnl_cache['ts'] < _LIVE_TTL:
        return _live_pnl_cache['data']

    btc_key    = _get_env('BTC_RO_API')
    btc_secret = _get_env('BTC_RO_KEY')

    live_data = _load_live_trades()
    result    = {}

    # 读取失败（live_data=None）时跳过所有状态更新，仅返回空结构，不触发覆盖写
    if live_data is None:
        print('[WARN] live_trades.json 读取失败，本轮跳过更新')
        empty = _LIVE_TRADES_EMPTY()
        return {
            'btc': {'trades': [], 'snapshots': [], 'position': _flat_pos(), 'coin': 'BTC'},
            'eth': {'trades': [], 'snapshots': [], 'position': _flat_pos(), 'coin': 'ETH'},
        }

    # ── BTC：使用真实 API ──────────────────────────────────────────────
    try:
        pos = _fetch_pos(btc_key, btc_secret, 'BTCUSDT') if (btc_key and btc_secret) else _flat_pos()
        rec = live_data['btc']

        if pos['side'] != 'FLAT' and len(rec['trades']) == 0:
            # 有持仓但无本地记录 —— 同样需要两次确认，防止脏数据触发误初始化
            pending = rec.get('_pending_open')
            if pending and pending.get('side') == pos['side']:
                _init_position_from_api(live_data, 'btc', btc_key, btc_secret, 'BTCUSDT', pos)
                rec['_pending_open'] = None
            else:
                rec['_pending_open'] = {
                    'side':        pos['side'],
                    'detected_at': time.strftime('%Y-%m-%dT%H:%M:%S'),
                    'entry':       pos.get('entry', 0),
                    'amt':         pos.get('amt', 0),
                    'leverage':    pos.get('leverage', 1),
                }
                print(f'[INFO] BTC 无本地记录检测到 {pos["side"]} 持仓，等待下一轮确认')
        else:
            _update_live_trades(live_data, 'btc', pos)

        result['btc'] = {
            'trades':    rec['trades'],
            'snapshots': rec['snapshots'],
            'position':  pos,
            'coin':      'BTC',
        }
    except Exception as e:
        result['btc'] = {'error': str(e)}

    # ── ETH：不调用 API，仅保留空结构供前端正常渲染 ────────────────────
    eth_rec = live_data.get('eth', {'trades': [], 'snapshots': []})
    result['eth'] = {
        'trades':    eth_rec.get('trades', []),
        'snapshots': eth_rec.get('snapshots', []),
        'position':  _flat_pos(),
        'coin':      'ETH',
    }

    _save_live_trades(live_data)
    _live_pnl_cache['data'] = result
    _live_pnl_cache['ts']   = now
    return result


def _pnl_background_loop():
    """后台每分钟主动拉取仓位、写入快照，不依赖页面是否打开"""
    time.sleep(10)   # 等待启动完成
    while True:
        try:
            _live_pnl_cache['ts'] = 0   # 强制绕过缓存
            get_live_pnl()
        except Exception as e:
            print(f'[WARN] PnL 后台更新失败: {e}')
        time.sleep(60)

threading.Thread(target=_pnl_background_loop, daemon=True).start()


# ── Flask Routes ─────────────────────────────────────────────────────
TEMPLATE = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Gan Crypto Quant</title>
<link rel="icon" type="image/x-icon" href="/favicon.ico">
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: #0d1117; color: #e6edf3; font-family: Arial, sans-serif; height: 100vh; display: flex; flex-direction: column; overflow: hidden; }
  .toolbar { display: flex; align-items: center; gap: 10px; padding: 6px 14px; background: #161b22; border-bottom: 1px solid #30363d; flex-shrink: 0; flex-wrap: wrap; row-gap: 4px; }
  .stat { font-size: 12px; color: #8b949e; white-space: nowrap; }
  .stat b { color: #F7931A; }
  .hint { margin-left: auto; font-size: 11px; color: #444d56; white-space: nowrap; }
  .tab-btns { display: flex; gap: 4px; background: #0d1117; border: 1px solid #30363d; border-radius: 6px; padding: 2px; }
  .tab-btn { font-size: 12px; padding: 4px 12px; border: none; border-radius: 4px; cursor: pointer; background: transparent; color: #8b949e; }
  .tab-btn.active { background: #21262d; color: #e6edf3; }
  .chart-area { flex: 1; position: relative; min-height: 0; }
  #chart, #chart-pnl, #chart-eth { width: 100%; height: 100%; }
  .pnl-shell { width: 100%; height: 100%; display: flex; min-height: 0; }
  .pnl-charts { flex: 1; min-width: 0; display: flex; flex-direction: column; overflow: hidden; }
  .pnl-side-log { width: 360px; border-left: 1px solid #30363d; background: #0f141b; display: flex; flex-direction: column; min-height: 0; flex-shrink: 0; }
  .pnl-side-log .log-header { flex-shrink: 0; }
  .pnl-side-log .log-body { padding: 10px 12px; }
  .log-resize { height: 5px; background: #30363d; cursor: ns-resize; flex-shrink: 0; }
  .log-resize:hover { background: #58a6ff; }
  .log-panel { height: 220px; border-top: 1px solid #30363d; display: flex; overflow: hidden; flex-shrink: 0; }
  .log-col { flex: 1; display: flex; flex-direction: column; overflow: hidden; }
  .log-col + .log-col { border-left: 1px solid #30363d; }
  .log-header { padding: 6px 12px; background: #161b22; border-bottom: 1px solid #21262d; font-size: 12px; }
  .log-body { flex: 1; overflow-y: auto; padding: 8px 12px; font-family: 'Courier New', monospace; font-size: 11.5px; line-height: 1.6; }
  .log-info { color: #8b949e; }
  .log-warning { color: #d29922; }
  .log-error { color: #f85149; }
  @media (max-width: 1180px) {
    .pnl-shell { flex-direction: column; }
    .pnl-side-log { width: 100%; height: 220px; border-left: none; border-top: 1px solid #30363d; }
  }
</style>
</head>
<body>

<div class="toolbar">
  <img src="/favicon.ico" style="width:26px;height:26px;flex-shrink:0;image-rendering:auto;" title="Gan Crypto Quant">
  <span style="font-size:13px;font-weight:700;color:#F7931A;white-space:nowrap;letter-spacing:0.3px;">Gan Crypto Quant</span>
  <span style="width:1px;height:16px;background:#30363d;flex-shrink:0;"></span>
  <span class="stat">总天数 <b>{{ total_rows }}</b></span>
  <span class="stat">有标签 <b>{{ labeled_rows }}</b></span>
  <span class="stat">已分组 <b>{{ classified }}</b></span>
  <span class="stat">组合数 <b>{{ combo_count }}</b></span>
  <div class="tab-btns">
    <button class="tab-btn active" id="tab-price" onclick="switchTab('price')">BTC价格图</button>
    <button class="tab-btn" id="tab-eth" onclick="switchTab('eth')">ETH价格图</button>
    <button class="tab-btn" id="tab-pnl" onclick="switchTab('pnl')">累计收益</button>
  </div>
  <span class="hint">滚轮缩放 · 拖动平移</span>
</div>

<div class="chart-area">
  <div id="chart"></div>
  <div id="chart-pnl" style="display:none; flex-direction:column; overflow:hidden;">
    <div id="pnl-loading" style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);color:#8b949e;z-index:10;display:none;">
      正在加载实盘收益数据...
    </div>
    <div class="pnl-shell">
      <div class="pnl-charts">
        <div id="chart-pnl-btc" style="flex:1; min-height:0;"></div>
        <div style="height:1px; flex-shrink:0; background:#30363d;"></div>
        <div id="chart-pnl-eth" style="flex:1; min-height:0; display:none;"></div>
      </div>
      <div class="pnl-side-log">
        <div class="log-header"><b>日常检测日志</b></div>
        <div class="log-body" id="trail-body"></div>
      </div>
    </div>
  </div>
  <div id="chart-eth" style="display:none;">
    <div id="eth-loading" style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);color:#8b949e;">
      正在加载ETH图表...
    </div>
  </div>
</div>

<div class="log-resize" id="log-resize"></div>
<div class="log-panel" id="log-panel">
  <div class="log-col">
    <div class="log-header"><b>BTC交易决策日志</b> <span id="signal-refresh-time" style="float:right;font-size:10px;color:#666;"></span></div>
    <div class="log-body" id="signal-body"></div>
  </div>
  <div class="log-col">
    <div class="log-header"><b>ETH交易决策日志</b> <span id="maint-refresh-time" style="float:right;font-size:10px;color:#666;"></span></div>
    <div class="log-body" id="maint-body"></div>
  </div>
</div>

<script>
function buildPlotConfig(showModeBar) {
  return {
    responsive: true,
    scrollZoom: true,
    displayModeBar: showModeBar !== false,
    displaylogo: false,
    doubleClick: 'reset+autosize',
    modeBarButtonsToRemove: ['select2d', 'lasso2d', 'hoverClosestCartesian', 'hoverCompareCartesian', 'toggleSpikelines'],
    toImageButtonOptions: {format: 'png', scale: 2}
  };
}

function getVisibleDateRange(gd) {
  var xa = gd && gd._fullLayout && gd._fullLayout.xaxis;
  if (!xa || !Array.isArray(xa.range) || xa.range.length < 2) return null;
  return [String(xa.range[0]), String(xa.range[1])];
}

function calcAxisRange(gd, axisName, xRange) {
  if (!gd || !gd.data || !xRange) return null;
  var x0 = xRange[0].slice(0, 19);
  var x1 = xRange[1].slice(0, 19);
  var yMin = Infinity;
  var yMax = -Infinity;

  gd.data.forEach(function(trace) {
    if (!trace || trace.visible === 'legendonly') return;
    var traceAxis = trace.yaxis || 'y';
    if (traceAxis !== axisName || !Array.isArray(trace.x) || !Array.isArray(trace.y)) return;
    for (var i = 0; i < trace.x.length; i++) {
      var xv = trace.x[i];
      var yv = trace.y[i];
      if (xv == null || yv == null || !isFinite(yv)) continue;
      var key = String(xv).slice(0, 19);
      if (key >= x0 && key <= x1) {
        if (yv < yMin) yMin = yv;
        if (yv > yMax) yMax = yv;
      }
    }
  });

  if (!isFinite(yMin) || !isFinite(yMax)) return null;
  var span = yMax - yMin;
  var pad = span > 0 ? span * 0.08 : Math.max(Math.abs(yMax) * 0.05, 1);
  return [yMin - pad, yMax + pad];
}

function syncVisibleRanges(divId, axes) {
  var gd = document.getElementById(divId);
  var xRange = getVisibleDateRange(gd);
  if (!gd || !xRange) return;

  var update = {};
  axes.forEach(function(axisName) {
    var range = calcAxisRange(gd, axisName, xRange);
    if (range) {
      update[axisName + '.range'] = range;
    }
  });
  if (Object.keys(update).length > 0) {
    Plotly.relayout(gd, update);
  }
}

function attachAdaptiveZoom(divId, axes) {
  var gd = document.getElementById(divId);
  if (!gd || gd.__adaptiveZoomBound) return;
  gd.__adaptiveZoomBound = true;

  gd.on('plotly_relayout', function(e) {
    if (!e) return;
    if (
      e['yaxis.range[0]'] !== undefined || e['yaxis.range[1]'] !== undefined ||
      e['yaxis2.range[0]'] !== undefined || e['yaxis2.range[1]'] !== undefined ||
      e['yaxis.autorange'] !== undefined || e['yaxis2.autorange'] !== undefined
    ) {
      return;
    }
    if (
      e['xaxis.range[0]'] === undefined &&
      e['xaxis.range'] === undefined &&
      e['xaxis.autorange'] === undefined
    ) {
      return;
    }
    requestAnimationFrame(function() {
      syncVisibleRanges(divId, axes);
    });
  });
}

var config = buildPlotConfig(true);
function loadBtcChart() {
  fetch('/api/btc-chart')
    .then(r => r.json())
    .then(data => {
      if (data.error) {
        document.getElementById('chart').innerHTML =
          `<div style="color:#f85149;padding:80px;text-align:center;">鍔犺浇澶辫触: ${data.error}</div>`;
        return;
      }
      Plotly.newPlot('chart', data.data, data.layout, config).then(function() {
        attachAdaptiveZoom('chart', ['y']);
        syncVisibleRanges('chart', ['y']);
      });
    })
    .catch(e => {
      document.getElementById('chart').innerHTML =
        `<div style="color:#f85149;padding:80px;text-align:center;">鍔犺浇澶辫触: ${e}</div>`;
    });
}

loadBtcChart();

// 缩放时自动调整 Y 轴范围，保持折线形态
false && (function() {
  var _closeX = priceData.data[0].x;
  var _closeY = priceData.data[0].y;
  var _t0 = _closeX[0].slice(0,10);  // 最早日期前缀（用于校验格式）

  function calcYRange(x0str, x1str) {
    // 统一截取前10字符做日期比较，兼容带时间的字符串如 "2024-01-01 12:00"
    var d0 = x0str.slice(0,10), d1 = x1str.slice(0,10);
    var yMin = Infinity, yMax = -Infinity;
    for (var i = 0; i < _closeX.length; i++) {
      var d = _closeX[i].slice(0,10);
      if (d >= d0 && d <= d1) {
        var v = _closeY[i];
        if (v != null && isFinite(v)) {
          if (v < yMin) yMin = v;
          if (v > yMax) yMax = v;
        }
      }
    }
    return [yMin, yMax];
  }

  document.getElementById('chart').on('plotly_relayout', function(e) {
    // 忽略 Y 轴自身变化触发的事件，避免循环
    if (e['yaxis.range[0]'] !== undefined || e['yaxis.autorange'] !== undefined) return;

    var x0, x1;
    if (e['xaxis.range[0]'] !== undefined) {
      x0 = e['xaxis.range[0]']; x1 = e['xaxis.range[1]'];
    } else if (e['xaxis.range'] !== undefined) {
      x0 = e['xaxis.range'][0]; x1 = e['xaxis.range'][1];
    } else {
      return;
    }
    if (!x0 || !x1) return;

    var r = calcYRange(String(x0), String(x1));
    if (!isFinite(r[0]) || !isFinite(r[1]) || r[1] <= r[0]) return;
    var pad = (r[1] - r[0]) * 0.05;
    Plotly.relayout('chart', {'yaxis.range': [r[0] - pad, r[1] + pad]});
  });
})();

var pnlLoaded = false;
var ethLoaded = false;

function switchTab(tab) {
  ['chart','chart-pnl','chart-eth'].forEach(function(id) {
    document.getElementById(id).style.display = 'none';
  });
  ['tab-price','tab-pnl','tab-eth'].forEach(function(id) {
    document.getElementById(id).classList.remove('active');
  });
  var logPanel   = document.getElementById('log-panel');
  var logResize  = document.getElementById('log-resize');
  var hideLogs   = (tab === 'pnl');
  logPanel.style.display  = hideLogs ? 'none' : '';
  logResize.style.display = hideLogs ? 'none' : '';
  if (tab === 'price') {
    document.getElementById('chart').style.display = 'block';
    document.getElementById('tab-price').classList.add('active');
    Plotly.Plots.resize('chart');
  } else if (tab === 'pnl') {
    document.getElementById('chart-pnl').style.display = 'flex';
    document.getElementById('tab-pnl').classList.add('active');
    loadTrailLog();
    if (!pnlLoaded) { loadPnlChart(); }
    else { setTimeout(() => { Plotly.Plots.resize('chart-pnl-btc'); Plotly.Plots.resize('chart-pnl-eth'); }, 100); }
  } else if (tab === 'eth') {
    document.getElementById('chart-eth').style.display = 'block';
    document.getElementById('tab-eth').classList.add('active');
    if (!ethLoaded) { loadEthChart(); }
    else { setTimeout(() => Plotly.Plots.resize('chart-eth'), 100); }
  }
}

function loadPnlChart() {
  document.getElementById('pnl-loading').style.display = 'block';
  fetch('/api/live-pnl')
    .then(r => r.json())
    .then(data => {
      document.getElementById('pnl-loading').style.display = 'none';
      renderLivePnl('chart-pnl-btc', data.btc, 'BTC', '#F7931A');
      // renderLivePnl('chart-pnl-eth', data.eth, 'ETH', '#FFD700');  // 暂时隐藏
      setTimeout(() => {
        Plotly.Plots.resize('chart-pnl-btc');
      }, 100);
      pnlLoaded = true;
    })
    .catch(e => {
      document.getElementById('pnl-loading').style.display = 'none';
      document.getElementById('chart-pnl-btc').innerHTML =
        `<div style="color:#f85149;padding:60px;text-align:center;">加载失败: ${e}</div>`;
    });
}

// 累计收益页面近实时刷新
setInterval(function() {
  if (pnlLoaded && document.getElementById('chart-pnl').style.display !== 'none') {
    fetch('/api/live-pnl')
      .then(r => r.json())
      .then(data => {
        renderLivePnl('chart-pnl-btc', data.btc, 'BTC', '#F7931A');
        // renderLivePnl('chart-pnl-eth', data.eth, 'ETH', '#FFD700');  // 暂时隐藏
      })
      .catch(function(){});
  }
}, 15000);

function normalizePnlSnapshots(snaps) {
  var normalized = (snaps || []).map(function(s) {
    return {
      time: String(s && s.time || ''),
      mark: Number(s && s.mark != null ? s.mark : NaN),
      unrealized: Number(s && s.unrealized != null ? s.unrealized : 0),
      cum_realized: Number(s && s.cum_realized != null ? s.cum_realized : 0)
    };
  }).filter(function(s) {
    return !!s.time;
  }).sort(function(a, b) {
    return a.time.localeCompare(b.time);
  });

  var deduped = [];
  normalized.forEach(function(s) {
    if (deduped.length > 0 && deduped[deduped.length - 1].time === s.time) {
      deduped[deduped.length - 1] = s;
    } else {
      deduped.push(s);
    }
  });
  return deduped;
}

function findMarkerPrice(snaps, targetTime, fallback) {
  var best = null;
  (snaps || []).forEach(function(s) {
    if (s.time <= targetTime && isFinite(s.mark) && s.mark > 0) {
      best = s.mark;
    }
  });
  if (best != null) return best;
  return (fallback != null && isFinite(fallback) && fallback > 0) ? fallback : null;
}

function renderLivePnl(divId, d, coin, priceColor) {
  var div = document.getElementById(divId);
  if (!d || d.error) {
    div.innerHTML = '<div style="color:#f85149;padding:40px 20px;text-align:center;font-size:13px;">'
      + coin + ' 实盘数据加载失败<br><span style="color:#8b949e;font-size:11px;">'
      + (d ? d.error : '无数据') + '</span></div>';
    return;
  }

  var pos    = d.position;
  var trades = d.trades || [];
  var pfmt   = coin === 'BTC' ? ',.0f' : ',.4f';

  var markFmt = (pos.mark > 0)
    ? (coin === 'BTC' ? pos.mark.toFixed(0) : pos.mark.toFixed(4))
    : '?';
  var posStr;
  if (pos.side !== 'FLAT') {
    var sideIcon = pos.side === 'LONG' ? '📈 多' : '📉 空';
    var entryFmt = coin === 'BTC' ? pos.entry.toFixed(0) : pos.entry.toFixed(4);
    var unrealStr = (pos.unrealized >= 0 ? '+' : '') + pos.unrealized.toFixed(2) + ' U';
    posStr = sideIcon + ' ' + Math.abs(pos.amt) + ' @ $' + entryFmt
           + '  标记 $' + markFmt + '  未实现 <b>' + unrealStr + '</b>';
  } else {
    posStr = pos.mark > 0 ? ('无持仓  标记价 $' + markFmt) : '无持仓（API密钥未配置）';
  }

  // ── Step 1：过滤无效快照（mark=0 = API 密钥缺失或失败产生的垃圾点）──
  var snaps = normalizePnlSnapshots(d.snapshots || []).filter(function(s) {
    return s.mark > 0;
  });

  // ── Step 2：找第一笔交易的开仓时间，截断之前的无关历史 ──
  var viewStart = null;
  trades.forEach(function(t) {
    if (t.open_time && (viewStart === null || t.open_time < viewStart)) {
      viewStart = t.open_time;
    }
  });
  if (viewStart) {
    snaps = snaps.filter(function(s) { return s.time >= viewStart; });
  }

  if (snaps.length === 0) {
    div.innerHTML = '<div style="color:#8b949e;padding:50px 30px;text-align:center;font-size:13px;">'
      + '<div style="color:#e6edf3;font-size:15px;margin-bottom:14px;">' + coin + ' 实盘收益</div>'
      + '<div>' + posStr + '</div>'
      + '<div style="font-size:11px;margin-top:10px;color:#555;">暂无有效快照（后台每分钟自动记录）</div></div>';
    return;
  }

  // ── Step 3：计算 PnL 时间序列 ──
  var times      = snaps.map(function(s){ return s.time; });
  var markPrices = snaps.map(function(s){ return s.mark; });   // 已过滤 mark=0
  var rawReal    = snaps.map(function(s){
    var r = Number(s.cum_realized != null ? s.cum_realized : 0);
    return isFinite(r) ? r : 0;
  });
  var rawTotal   = snaps.map(function(s, i){
    var u = Number(s.unrealized != null ? s.unrealized : 0);
    if (!isFinite(u)) u = 0;
    return rawReal[i] + u;
  });

  // 归一化：以第一个有效快照为零点
  var base     = rawTotal[0] || 0;
  var totalPnl = rawTotal.map(function(v){ return v - base; });
  var cumReal  = rawReal.map(function(v){ return v - base; });

  var latestTotal = totalPnl[totalPnl.length - 1] || 0;
  var latestReal  = cumReal[cumReal.length - 1]    || 0;
  var pnlColor  = latestTotal >= 0 ? '#3fb950' : '#f85149';
  var fillColor = latestTotal >= 0 ? 'rgba(63,185,80,0.18)' : 'rgba(248,81,73,0.15)';
  var realStr   = (latestReal >= 0 ? '+' : '') + latestReal.toFixed(2) + ' U';
  var title     = coin + ' 实盘收益  已实现 <b>' + realStr + '</b>  |  ' + posStr;

  // ── Step 4：开/平仓标记——只显示 viewStart 之后且价格有效的标记 ──
  var snapTimeMin = snaps.length ? snaps[0].time : null;
  var snapTimeMax = snaps.length ? snaps[snaps.length - 1].time : null;

  var openTimes = [], openPrices = [], openText = [];
  var closeTimes = [], closePrices = [], closeText = [];
  trades.forEach(function(t) {
    // 开仓标记：需要有效的 open_time 和 entry 价格
    var oTime  = t.open_time || '';
    var oEntry = Number(t.entry);
    if (oTime && isFinite(oEntry) && oEntry > 0 &&
        (!snapTimeMin || oTime >= snapTimeMin)) {
      openTimes.push(oTime);
      openPrices.push(oEntry);
      var ep = coin === 'BTC' ? oEntry.toFixed(0) : oEntry.toFixed(4);
      openText.push((t.side === 'LONG' ? '开多' : '开空') + ' ' + t.amt + ' @ $' + ep + '  ' + t.leverage + 'x');
    }
    // 平仓标记：优先用 close_price，再从快照反查，price=0 则跳过
    if (t.close_time && (!snapTimeMin || t.close_time >= snapTimeMin)) {
      var cp = Number(t.close_price);
      if (!isFinite(cp) || cp <= 0) {
        cp = findMarkerPrice(snaps, t.close_time, oEntry);
      }
      if (cp == null || cp <= 0) return;
      closeTimes.push(t.close_time);
      closePrices.push(cp);
      var cpFmt = coin === 'BTC' ? cp.toFixed(0) : cp.toFixed(4);
      var pl = t.realized_pnl != null ? '  ' + (t.realized_pnl >= 0 ? '+' : '') + t.realized_pnl.toFixed(2) + ' U' : '';
      closeText.push('平仓 $' + cpFmt + pl);
    }
  });

  var traces = [
    {
      x: times, y: markPrices, name: coin + '价格',
      type: 'scatter', mode: 'lines', yaxis: 'y2',
      line: {color: priceColor, width: 1.4}, opacity: 0.38, connectgaps: false,
      hovertemplate: coin + ' $%{y:' + pfmt + '}<extra>' + coin + '价格</extra>'
    },
    {
      x: times, y: totalPnl, name: '总收益(含未实现)',
      type: 'scatter', mode: 'lines',
      fill: 'tozeroy', fillcolor: fillColor,
      line: {color: pnlColor, width: 2, shape: 'hv'},
      hovertemplate: '%{x}<br>总收益: <b>%{y:+,.2f} U</b><extra>总收益</extra>'
    },
    {
      x: times, y: cumReal, name: '已实现盈亏',
      type: 'scatter', mode: 'lines',
      line: {color: '#58a6ff', width: 1.5, dash: 'dot', shape: 'hv'},
      hovertemplate: '%{x}<br>已实现: <b>%{y:+,.2f} U</b><extra>已实现</extra>'
    }
  ];
  if (openTimes.length > 0) {
    traces.push({
      x: openTimes, y: openPrices, name: '开仓', yaxis: 'y2',
      type: 'scatter', mode: 'markers', customdata: openText,
      marker: {symbol: 'triangle-up', size: 14, color: '#3fb950', line: {width: 1.5, color: '#fff'}},
      hovertemplate: '<b>%{customdata}</b><extra>开仓</extra>'
    });
  }
  if (closeTimes.length > 0) {
    traces.push({
      x: closeTimes, y: closePrices, name: '平仓', yaxis: 'y2',
      type: 'scatter', mode: 'markers', customdata: closeText,
      marker: {symbol: 'triangle-down', size: 14, color: '#f85149', line: {width: 1.5, color: '#fff'}},
      hovertemplate: '<b>%{customdata}</b><extra>平仓</extra>'
    });
  }

  var layout = {
    template: 'plotly_dark',
    paper_bgcolor: '#0d1117',
    plot_bgcolor: '#161b22',
    font: {family: 'Arial, sans-serif', color: '#e6edf3', size: 11},
    title: {text: title, font: {size: 12, color: pnlColor}, x: 0.01},
    xaxis: {
      type: 'date', showgrid: true, gridcolor: '#21262d', zeroline: false,
      range: [times[0], times[times.length - 1]],
      rangeslider: {visible: false},
      rangeselector: {
        bgcolor: '#161b22',
        activecolor: '#30363d',
        bordercolor: '#30363d',
        font: {size: 10, color: '#e6edf3'},
        buttons: [
          {count: 1, label: '1D', step: 'day', stepmode: 'backward'},
          {count: 7, label: '7D', step: 'day', stepmode: 'backward'},
          {count: 1, label: '1M', step: 'month', stepmode: 'backward'},
          {count: 3, label: '3M', step: 'month', stepmode: 'backward'},
          {step: 'all', label: 'ALL'}
        ]
      }
    },
    yaxis: {
      title: '盈亏 (USDT)', showgrid: true, gridcolor: '#21262d',
      zeroline: true, zerolinecolor: '#555', zerolinewidth: 1,
      tickprefix: '$', tickformat: '+,.2f', autorange: true
    },
    yaxis2: {
      overlaying: 'y', side: 'right',
      tickprefix: '$', tickformat: pfmt, showgrid: false
    },
    legend: {orientation: 'v', x: 0.01, y: 0.99, xanchor: 'left', yanchor: 'top',
             bgcolor: 'rgba(13,17,23,0.75)', bordercolor: '#30363d', borderwidth: 1, font: {size: 10}},
    hovermode: 'x unified',
    dragmode: 'pan',
    uirevision: divId,
    margin: {l: 80, r: 80, t: 46, b: 60}
  };

  var gd = document.getElementById(divId);
  Plotly.react(gd, traces, layout, buildPlotConfig(true)).then(function() {
    attachAdaptiveZoom(divId, ['y', 'y2']);
    syncVisibleRanges(divId, ['y', 'y2']);
  });
}

function loadEthChart() {
  document.getElementById('eth-loading').style.display = 'block';
  fetch('/api/eth-chart')
    .then(r => r.json())
    .then(data => {
      document.getElementById('eth-loading').style.display = 'none';
      if (data.error) {
        document.getElementById('chart-eth').innerHTML =
          `<div style="color:#f85149;padding:80px;text-align:center;">加载失败: ${data.error}</div>`;
        ethLoaded = true; return;
      }
      Plotly.newPlot('chart-eth', data.data, data.layout, config).then(() => {
        setTimeout(() => Plotly.Plots.resize('chart-eth'), 100);
        attachAdaptiveZoom('chart-eth', ['y']);
        syncVisibleRanges('chart-eth', ['y']);
        // ETH 图 Y 轴自动适配
        if (false) document.getElementById('chart-eth').on('plotly_relayout', function(e) {
          if (e['yaxis.range[0]'] !== undefined || e['yaxis.autorange'] !== undefined) return;
          var x0, x1;
          if (e['xaxis.range[0]'] !== undefined) { x0 = e['xaxis.range[0]']; x1 = e['xaxis.range[1]']; }
          else if (e['xaxis.range'] !== undefined) { x0 = e['xaxis.range'][0]; x1 = e['xaxis.range'][1]; }
          else { return; }
          if (!x0 || !x1) return;
          var _ex = document.getElementById('chart-eth').data[0].x;
          var _ey = document.getElementById('chart-eth').data[0].y;
          var d0 = String(x0).slice(0,10), d1 = String(x1).slice(0,10);
          var yMin = Infinity, yMax = -Infinity;
          for (var i = 0; i < _ex.length; i++) {
            var d = String(_ex[i]).slice(0,10);
            if (d >= d0 && d <= d1) {
              var v = _ey[i];
              if (v != null && isFinite(v)) { if (v < yMin) yMin = v; if (v > yMax) yMax = v; }
            }
          }
          if (!isFinite(yMin) || !isFinite(yMax) || yMax <= yMin) return;
          var pad = (yMax - yMin) * 0.05;
          Plotly.relayout('chart-eth', {'yaxis.range': [yMin - pad, yMax + pad]});
        });
      });
      ethLoaded = true;
    })
    .catch(e => {
      document.getElementById('eth-loading').style.display = 'none';
      document.getElementById('chart-eth').innerHTML =
        `<div style="color:#f85149;padding:80px;text-align:center;">加载失败: ${e}</div>`;
    });
}

// 日志加载
function renderLog(lines, bodyId) {
  var html = lines.map(l => `<div class="log-${l.level}">${l.text}</div>`).join('');
  document.getElementById(bodyId).innerHTML = html;
}

function loadSignalLog() {
  fetch('/api/signal-log').then(r => r.json()).then(lines => renderLog(lines, 'signal-body'));
}
function loadMaintLog() {
  fetch('/api/log').then(r => r.json()).then(lines => renderLog(lines, 'maint-body'));
}
function loadTrailLog() {
  fetch('/api/trail-log').then(r => r.json()).then(lines => renderLog(lines, 'trail-body'));
}

loadSignalLog();
loadMaintLog();
loadTrailLog();
setInterval(loadSignalLog, 300000);
setInterval(loadMaintLog, 300000);
setInterval(loadTrailLog, 300000);

window.addEventListener('resize', () => {
  Plotly.Plots.resize('chart');
  if (pnlLoaded) { Plotly.Plots.resize('chart-pnl-btc'); Plotly.Plots.resize('chart-pnl-eth'); }
  if (ethLoaded) Plotly.Plots.resize('chart-eth');
});

// 日志面板拖拽调整高度
(function() {
  var handle = document.getElementById('log-resize');
  var panel  = document.getElementById('log-panel');
  var startY, startH;
  handle.addEventListener('mousedown', function(e) {
    startY = e.clientY;
    startH = panel.offsetHeight;
    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onUp);
    e.preventDefault();
  });
  function onMove(e) {
    var delta = startY - e.clientY;
    var newH  = Math.max(80, Math.min(window.innerHeight - 100, startH + delta));
    panel.style.height = newH + 'px';
    Plotly.Plots.resize('chart');
    if (pnlLoaded) { Plotly.Plots.resize('chart-pnl-btc'); Plotly.Plots.resize('chart-pnl-eth'); }
    if (ethLoaded) Plotly.Plots.resize('chart-eth');
  }
  function onUp() {
    document.removeEventListener('mousemove', onMove);
    document.removeEventListener('mouseup', onUp);
  }
})();
</script>
</body>
</html>
"""

# ── Routes ───────────────────────────────────────────────────────────
_FAVICON_PATH = '/root/Desktop/btc/favicon.ico'

@app.route('/favicon.ico')
def favicon():
    from flask import Response
    try:
        with open(_FAVICON_PATH, 'rb') as f:
            return Response(f.read(), mimetype='image/x-icon',
                            headers={'Cache-Control': 'public, max-age=86400'})
    except Exception:
        return Response(b'', mimetype='image/x-icon')


@app.route('/')
def index():
    return render_template_string(
        TEMPLATE,
        total_rows=_cache.get('total_rows', 0),
        labeled_rows=_cache.get('labeled_rows', 0),
        classified=_cache.get('classified', 0),
        combo_count=_cache.get('combo_count', 0),
    )

@app.route('/api/log')
def api_log():
    return jsonify(safe_read_log(LOG_PATH, LOG_LINES))

@app.route('/api/signal-log')
def api_signal_log():
    return jsonify(safe_read_log(SIGNAL_LOG_PATH, LOG_LINES))

@app.route('/api/trail-log')
def api_trail_log():
    return jsonify(safe_read_log(TRAIL_STOP_LOG_PATH, LOG_LINES))

@app.route('/api/pnl')
def api_pnl():
    try:
        if not os.path.exists(PNL_LOG_PATH):
            return jsonify([])
        with open(PNL_LOG_PATH, 'r', encoding='utf-8') as f:
            return jsonify(json.load(f))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/backtest-curve')
def api_backtest_curve():
    try:
        curve = _cache.get('equity_curve')
        if curve is None:
            return jsonify({'dates': [], 'strategy_pct': [], 'btc_pct': [], 'net_pos': []})
        return jsonify(curve)
    except Exception as e:
        print(f"[ERROR] backtest-curve: {e}")
        return jsonify({'dates': [], 'strategy_pct': [], 'btc_pct': [], 'net_pos': [], 'error': str(e)}), 500


@app.route('/api/eth-chart')
def api_eth_chart():
    try:
        eth_json = _cache.get('eth_graph_json')
        if eth_json is None:
            return jsonify({'error': 'ETH图表数据未就绪，请稍后重试'}), 503
        from flask import Response
        return Response(eth_json, mimetype='application/json')
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/btc-chart')
def api_btc_chart():
    try:
        btc_json = _cache.get('graph_json')
        if btc_json is None:
            df = load_data()
            keep, valid = compute_combos(df)
            fig = make_figure(df, keep, valid)
            btc_json = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
            _cache['graph_json'] = btc_json
        from flask import Response
        return Response(btc_json, mimetype='application/json')
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/live-pnl')
def api_live_pnl():
    try:
        return jsonify(get_live_pnl())
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/live-trades-reset', methods=['POST'])
def api_live_trades_reset():
    """
    重置或手动写入交易记录（用于补录真实开仓信息）。
    POST JSON:
      { "coin": "btc",            # "btc" 或 "eth"
        "action": "reset"         # "reset" = 清空该币种所有记录
      }
    或
      { "coin": "btc",
        "action": "open",         # 手动补录一笔开仓
        "side":       "LONG",     # "LONG" 或 "SHORT"
        "amt":        0.1,        # 开仓数量（正数）
        "entry":      84000.0,    # 开仓均价
        "leverage":   10,
        "open_time":  "2026-04-20T10:30:00"   # 可选，留空用服务器当前时间
      }
    """
    from flask import request
    try:
        body = request.get_json(force=True)
        coin = body.get('coin', '').lower()
        if coin not in ('btc', 'eth'):
            return jsonify({'error': 'coin 必须是 btc 或 eth'}), 400

        data = _load_live_trades()
        action = body.get('action', '')

        if action == 'reset':
            data[coin] = {'last_side': 'FLAT', 'last_amt': 0.0, 'last_entry': 0.0,
                          'trades': [], 'snapshots': []}
            _save_live_trades(data)
            _live_pnl_cache['data'] = None
            return jsonify({'ok': True, 'msg': f'{coin.upper()} 记录已清空'})

        if action == 'open':
            side  = body.get('side', 'LONG').upper()
            amt   = float(body.get('amt', 0))
            entry = float(body.get('entry', 0))
            lev   = int(body.get('leverage', 1))
            t_str = body.get('open_time') or time.strftime('%Y-%m-%dT%H:%M:%S')
            if amt <= 0 or entry <= 0:
                return jsonify({'error': 'amt / entry 必须 > 0'}), 400
            data[coin] = {'last_side': side, 'last_amt': amt if side == 'LONG' else -amt,
                          'last_entry': entry, 'trades': [], 'snapshots': []}
            data[coin]['trades'].append({
                'open_time': t_str, 'close_time': None,
                'side': side, 'amt': amt,
                'entry': entry, 'close_price': None,
                'leverage': lev, 'realized_pnl': None,
            })
            data[coin]['snapshots'].append({
                'time': t_str, 'mark': entry,
                'unrealized': 0.0, 'cum_realized': 0.0,
            })
            _save_live_trades(data)
            _live_pnl_cache['data'] = None
            return jsonify({'ok': True, 'msg': f'{coin.upper()} 开仓记录已写入', 'trade': data[coin]['trades'][-1]})

        return jsonify({'error': f'未知 action: {action}'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    print("BTC Cluster 图表启动 → http://0.0.0.0:5000")
    print(f"回测文件: {BACKTEST_PATH}")
    app.run(host='0.0.0.0', port=5000, debug=False)
