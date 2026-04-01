"""
BTC 价格折线图 + 六维 Cluster 组合标注（方案 C）
运行: python btc_chart_app.py
访问: http://<服务器IP>:5000
"""

import sys, json, os, math, colorsys, threading, time
import pandas as pd
import plotly.graph_objects as go
import plotly.utils
from flask import Flask, render_template_string, request, jsonify

sys.path.insert(0, '/root/Desktop/btc/get_data/get_btc_data')
from regime_strategy import load_and_prepare, profile_regimes, K_COLS

app = Flask(__name__)

# ── 路径配置 ─────────────────────────────────────────────────────────
DATA_PATH        = '/root/Desktop/btc/get_data/get_btc_data/btc.xlsx'
LOG_PATH         = '/root/Desktop/btc/get_data/get_btc_data/trader.log'
SIGNAL_LOG_PATH  = '/root/Desktop/btc/get_data/get_btc_data/trader_signal.log'
PNL_LOG_PATH     = '/root/Desktop/btc/get_data/get_btc_data/pnl_records.json'
LOG_LINES        = 30
# ─────────────────────────────────────────────────────────────────────

SYMBOLS = [
    'circle', 'square', 'diamond', 'triangle-up', 'triangle-down',
    'star', 'hexagon', 'cross', 'x', 'pentagon',
    'triangle-left', 'triangle-right', 'hexagram', 'hourglass',
    'bowtie', 'asterisk', 'circle-open', 'square-open', 'diamond-open',
    'triangle-up-open', 'star-open', 'hexagon-open'
]


def load_data():
    df = load_and_prepare(DATA_PATH)   # 使用 regime_strategy 的加载函数（含 fwd_7d/30d）

    # 计算每日 BULL/BEAR/NEUTRAL 票数
    profiles = profile_regimes(df)
    bull_n_list, bear_n_list, neut_n_list = [], [], []
    for _, row in df.iterrows():
        counts = {'BULL': 0, 'BEAR': 0, 'NEUTRAL': 0}
        for k in K_COLS:
            kv = row.get(k)
            if pd.isna(kv):
                continue
            match = profiles[k][profiles[k]['regime'] == int(kv)]
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

    # 每日投票结果（用于 hover 背景色）
    def _vote_result(bull, bear, neut):
        if bull >= 4 and bull >= bear and bull >= neut:
            return 'BULL'
        if bear >= 4 and bear >= bull and bear >= neut:
            return 'BEAR'
        if neut >= 4 and neut >= bull and neut >= bear:
            return 'NEUTRAL'
        return 'OTHER'
    df['vote_result'] = [_vote_result(b, be, n)
                         for b, be, n in zip(df['bull_n'], df['bear_n'], df['neut_n'])]
    return df


def compute_combos(df: pd.DataFrame):
    """
    过滤规则：
      - 出现 >= 3 次的组合
      - 出现 == 2 次 且两次日期连续（相差1天）的组合
    返回: {combo_tuple: count}，按 count 降序
    """
    valid = df.dropna(subset=K_COLS).copy()
    valid['combo'] = valid[K_COLS].apply(tuple, axis=1)
    counts = valid['combo'].value_counts()

    keep = {}
    for combo, cnt in counts.items():
        if cnt >= 3:
            keep[combo] = cnt
        elif cnt == 2:
            dates = valid[valid['combo'] == combo]['date'].sort_values().tolist()
            if (dates[1] - dates[0]).days == 1:
                keep[combo] = cnt

    # 按出现次数降序排列
    keep = dict(sorted(keep.items(), key=lambda x: x[1], reverse=True))
    return keep, valid


def gen_colors(n: int):
    """
    生成 n 个视觉差异最大的颜色（HSV 均匀分布 + 双层明度交错）。
    返回十六进制颜色列表。
    """
    colors = []
    for i in range(n):
        hue = i / n
        # 奇偶行交替明度：高频组合用明亮色，低频组合颜色略暗
        sat = 0.85 if i % 2 == 0 else 0.70
        val = 0.95 if i % 2 == 0 else 0.78
        r, g, b = colorsys.hsv_to_rgb(hue, sat, val)
        colors.append('#{:02x}{:02x}{:02x}'.format(int(r*255), int(g*255), int(b*255)))
    return colors


def freq_to_size(cnt: int, min_cnt: int, max_cnt: int) -> float:
    """出现越多 → 标记越大（对数缩放，范围 7~15）"""
    if max_cnt == min_cnt:
        return 11.0
    t = math.log(cnt - min_cnt + 1) / math.log(max_cnt - min_cnt + 1)
    return round(7 + t * 8, 1)


def freq_to_opacity(cnt: int, min_cnt: int, max_cnt: int) -> float:
    """出现越多 → 越不透明（0.45 ~ 1.0）"""
    if max_cnt == min_cnt:
        return 0.75
    t = math.log(cnt - min_cnt + 1) / math.log(max_cnt - min_cnt + 1)
    return round(0.45 + t * 0.55, 2)


def make_figure(df: pd.DataFrame, keep: dict, valid: pd.DataFrame):
    fig = go.Figure()

    HOVER_VOTES = (
        'Bull : %{customdata[5]}票  '
        'Bear : %{customdata[6]}票  '
        'Neut : %{customdata[7]}票<br>'
    )
    CD_COLS = ['open', 'high', 'low', 'fgi', 'volume', 'bull_n', 'bear_n', 'neut_n']

    # ── hover 背景色：按投票结果着色 ─────────────────────────────────
    VOTE_BG = {
        'BULL':    'rgba(40,167,69,0.88)',    # 绿
        'BEAR':    'rgba(200,45,45,0.88)',     # 红
        'NEUTRAL': 'rgba(75,85,95,0.88)',      # 灰
        'OTHER':   'rgba(22,27,34,0.92)',      # 暗（默认）
    }
    VOTE_BORDER = {
        'BULL':    'rgba(100,220,120,0.9)',
        'BEAR':    'rgba(255,100,100,0.9)',
        'NEUTRAL': 'rgba(150,160,170,0.9)',
        'OTHER':   'rgba(48,54,61,0.9)',
    }
    def _vbg(v):     return VOTE_BG.get(str(v), VOTE_BG['OTHER'])
    def _vborder(v): return VOTE_BORDER.get(str(v), VOTE_BORDER['OTHER'])
    def _hl(series):
        """生成 hoverlabel dict，bgcolor/bordercolor 均按投票结果逐点着色"""
        vlist = list(series)
        return dict(
            bgcolor=[_vbg(v) for v in vlist],
            bordercolor=[_vborder(v) for v in vlist],
            font=dict(color='#ffffff', size=12),
            align='left',
        )

    # ── 价格折线 ──────────────────────────────────────────────────────
    fig.add_trace(go.Scatter(
        x=df['date'], y=df['close'],
        mode='lines',
        name='BTC Close',
        line=dict(color='#F7931A', width=1.6),
        customdata=df[CD_COLS].values,
        hoverlabel=_hl(df['vote_result']),
        hovertemplate=(
            '<b>%{x|%Y-%m-%d}</b><br>'
            'Open : $%{customdata[0]:,.0f}<br>'
            'High : $%{customdata[1]:,.0f}<br>'
            'Low  : $%{customdata[2]:,.0f}<br>'
            'Close: <b>$%{y:,.0f}</b><br>'
            'FGI  : %{customdata[3]:.0f}<br>'
            'Vol  : %{customdata[4]:,.0f}<br>' +
            HOVER_VOTES +
            '<extra>BTC Price</extra>'
        ),
    ))

    # ── 无法归类的有标签行（灰色小点） ───────────────────────────────
    classified_idx = valid[valid['combo'].isin(keep)].index
    unclassified = valid.loc[~valid.index.isin(classified_idx)]
    if len(unclassified) > 0:
        fig.add_trace(go.Scatter(
            x=unclassified['date'], y=unclassified['close'],
            mode='markers',
            name='其他组合',
            marker=dict(symbol='circle', size=5, color='#444c56', opacity=0.4),
            customdata=unclassified[CD_COLS].values,
            hoverlabel=_hl(unclassified['vote_result']),
            hovertemplate=(
                '<b>%{x|%Y-%m-%d}</b><br>'
                '其他低频组合<br>'
                'Open : $%{customdata[0]:,.0f}<br>'
                'High : $%{customdata[1]:,.0f}<br>'
                'Low  : $%{customdata[2]:,.0f}<br>'
                'Close: <b>$%{y:,.0f}</b><br>'
                'FGI  : %{customdata[3]:.0f}<br>'
                'Vol  : %{customdata[4]:,.0f}<br>' +
                HOVER_VOTES +
                '<extra></extra>'
            ),
        ))

    # ── 按频率生成颜色、大小、透明度 ─────────────────────────────────
    combos_list = list(keep.items())           # [(combo, count), ...]
    n = len(combos_list)
    colors = gen_colors(n)
    counts_only = [c for _, c in combos_list]
    min_cnt, max_cnt = min(counts_only), max(counts_only)

    for i, (combo, cnt) in enumerate(combos_list):
        sub = valid[valid['combo'] == combo]
        size    = freq_to_size(cnt, min_cnt, max_cnt)
        opacity = freq_to_opacity(cnt, min_cnt, max_cnt)
        color   = colors[i]
        symbol  = SYMBOLS[i % len(SYMBOLS)]

        # 图例名称：G01(×53) [2,5,1,3,4,2]
        combo_vals = ','.join(str(int(v)) for v in combo)
        label = f'G{i+1:02d}(×{cnt}) [{combo_vals}]'
        combo_str = 'k10~k15[' + combo_vals + ']'

        fig.add_trace(go.Scatter(
            x=sub['date'], y=sub['close'],
            mode='markers',
            name=label,
            marker=dict(
                symbol=symbol,
                size=size,
                color=color,
                opacity=opacity,
                line=dict(width=0.8, color='#ffffff'),
            ),
            customdata=sub[CD_COLS].values,
            hoverlabel=_hl(sub['vote_result']),
            hovertemplate=(
                '<b>%{x|%Y-%m-%d}</b><br>'
                f'<b>{label}</b>  {combo_str}<br>'
                'Open : $%{customdata[0]:,.0f}<br>'
                'High : $%{customdata[1]:,.0f}<br>'
                'Low  : $%{customdata[2]:,.0f}<br>'
                'Close: <b>$%{y:,.0f}</b><br>'
                'FGI  : %{customdata[3]:.0f}<br>'
                'Vol  : %{customdata[4]:,.0f}<br>' +
                HOVER_VOTES +
                '<extra></extra>'
            ),
        ))

    # ── 布局 ─────────────────────────────────────────────────────────
    fig.update_layout(
        template='plotly_dark',
        paper_bgcolor='#0d1117',
        plot_bgcolor='#161b22',
        font=dict(family='Arial, sans-serif', color='#e6edf3'),
        title=dict(
            text='BTC Price  ·  六维 Cluster 组合标注（方案 C：频率越高 → 标记越大越亮）',
            font=dict(size=14, color='#F7931A'),
            x=0.01, xanchor='left'
        ),
        xaxis=dict(
            showgrid=True, gridcolor='#21262d', zeroline=False,
            rangeslider=dict(visible=True, bgcolor='#161b22', thickness=0.05),
        ),
        yaxis=dict(
            showgrid=True, gridcolor='#21262d', zeroline=False,
            tickprefix='$', tickformat=',.0f', side='left',
        ),
        legend=dict(
            orientation='v',
            x=1.0, xanchor='left',
            y=1.0, yanchor='top',
            bgcolor='rgba(13,17,23,0.80)',
            bordercolor='#30363d', borderwidth=1,
            font=dict(size=10),
            itemsizing='constant',
            tracegroupgap=2,
        ),
        hovermode='closest',
        margin=dict(l=10, r=180, t=46, b=10),
        dragmode='pan',
    )

    return fig


# ── 缓存 ─────────────────────────────────────────────────────────────
_cache = {}

def refresh_cache():
    df           = load_data()
    keep, valid  = compute_combos(df)
    fig          = make_figure(df, keep, valid)
    _cache['graph_json']    = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    _cache['total_rows']    = len(df)
    _cache['labeled_rows']  = len(valid)
    _cache['classified']    = valid[valid['combo'].isin(keep)].shape[0]
    _cache['combo_count']   = len(keep)
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 数据已重载，共 {len(df)} 行")

def _auto_refresh():
    """每天凌晨 2:10（服务器时间）自动重载一次"""
    while True:
        now = time.localtime()
        # 距下次凌晨 2:10 的秒数
        secs_since_midnight = now.tm_hour * 3600 + now.tm_min * 60 + now.tm_sec
        target = 0 * 3600 + 10 * 60   # 00:10
        wait = target - secs_since_midnight
        if wait <= 0:
            wait += 86400
        time.sleep(wait)
        refresh_cache()

# 初始加载
refresh_cache()

# 后台定时刷新线程
threading.Thread(target=_auto_refresh, daemon=True).start()


TEMPLATE = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0">
<title>BTC Cluster Chart</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: #0d1117;
    color: #e6edf3;
    font-family: Arial, sans-serif;
    height: 100dvh;
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }
  .toolbar {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 7px 14px;
    background: #161b22;
    border-bottom: 1px solid #30363d;
    flex-shrink: 0;
    flex-wrap: wrap;
  }
  .stat { font-size: 12px; color: #8b949e; white-space: nowrap; }
  .stat b { color: #F7931A; }
  .hint { margin-left: auto; font-size: 11px; color: #444d56; white-space: nowrap; }
  .tab-btns {
    display: flex;
    gap: 4px;
    background: #0d1117;
    border: 1px solid #30363d;
    border-radius: 6px;
    padding: 2px;
  }
  .tab-btn {
    font-size: 11px;
    padding: 3px 10px;
    border: none;
    border-radius: 4px;
    cursor: pointer;
    background: transparent;
    color: #8b949e;
    transition: background 0.15s, color 0.15s;
  }
  .tab-btn.active {
    background: #21262d;
    color: #e6edf3;
  }
  .chart-area { flex: 0 0 65%; min-height: 0; width: 100%; position: relative; display: flex; flex-direction: column; }
  #chart     { flex: 1 1 0; min-height: 0; }
  #chart-pnl { flex: 1 1 0; min-height: 0; display: none; }
  .log-panel {
    flex: 1 1 0;
    min-height: 0;
    border-top: 1px solid #30363d;
    display: flex;
    flex-direction: row;
    overflow: hidden;
  }
  .log-col {
    flex: 1 1 0;
    min-width: 0;
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }
  .log-col + .log-col { border-left: 1px solid #30363d; }
  .log-header {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 4px 12px;
    background: #161b22;
    border-bottom: 1px solid #21262d;
    flex-shrink: 0;
  }
  .log-header span { font-size: 11px; color: #8b949e; }
  .log-header b { color: #58a6ff; }
  .log-header b.signal-title { color: #3fb950; }
  .log-refresh { font-size: 10px; color: #444d56; margin-left: auto; }
  .log-body {
    flex: 1 1 0;
    overflow-y: auto;
    padding: 6px 12px;
    font-family: 'Courier New', monospace;
    font-size: 11px;
    line-height: 1.7;
  }
  .log-info    { color: #8b949e; }
  .log-warning { color: #d29922; }
  .log-error   { color: #f85149; }
  @media (max-width: 768px) {
    .toolbar { padding: 5px 8px; gap: 6px; }
    .hint { display: none; }
    .chart-area { flex: 0 0 55%; }
  }
</style>
</head>
<body>

<div class="toolbar">
  <span class="stat">总天数 <b>{{ total_rows }}</b></span>
  <span class="stat">有标签 <b>{{ labeled_rows }}</b></span>
  <span class="stat">已分组 <b>{{ classified }}</b></span>
  <span class="stat">组合数 <b>{{ combo_count }}</b>（≥3次 或 连续2次）</span>
  <div class="tab-btns">
    <button class="tab-btn active" id="tab-price" onclick="switchTab('price')">价格图</button>
    <button class="tab-btn"        id="tab-pnl"   onclick="switchTab('pnl')">累计收益</button>
  </div>
  <span class="hint">滚轮/双指缩放 · 拖动平移</span>
</div>

<div class="chart-area">
  <div id="chart"></div>
  <div id="chart-pnl"></div>
</div>

<div class="log-panel">
  <div class="log-col">
    <div class="log-header">
      <span><b class="signal-title">■ 交易决策</b></span>
      <span class="log-refresh" id="signal-refresh-time"></span>
    </div>
    <div class="log-body" id="signal-body"><span class="log-info">加载中...</span></div>
  </div>
  <div class="log-col">
    <div class="log-header">
      <span><b>■ 运维日志</b></span>
      <span class="log-refresh" id="maint-refresh-time"></span>
    </div>
    <div class="log-body" id="maint-body"><span class="log-info">加载中...</span></div>
  </div>
</div>

<script>
// ── 价格图 ───────────────────────────────────────────────────────
var config = {
  responsive: true,
  scrollZoom: true,
  displayModeBar: true,
  modeBarButtonsToRemove: ['select2d','lasso2d','autoScale2d'],
  displaylogo: false
};
var priceData = {{ graph_json | safe }};
Plotly.newPlot('chart', priceData.data, priceData.layout, config);

function resize() {
  var toolbar  = document.querySelector('.toolbar').offsetHeight;
  var logPanel = document.querySelector('.log-panel').offsetHeight;
  var chartH   = window.innerHeight - toolbar - logPanel;
  document.querySelector('.chart-area').style.height = Math.max(chartH, 200) + 'px';
  Plotly.Plots.resize('chart');
  if (document.getElementById('chart-pnl').style.display !== 'none') {
    Plotly.Plots.resize('chart-pnl');
  }
}
window.addEventListener('resize', resize);
resize();

// ── 标签切换 ─────────────────────────────────────────────────────
var pnlLoaded = false;
function switchTab(tab) {
  if (tab === 'price') {
    document.getElementById('chart').style.display     = '';
    document.getElementById('chart-pnl').style.display = 'none';
    document.getElementById('tab-price').classList.add('active');
    document.getElementById('tab-pnl').classList.remove('active');
    Plotly.Plots.resize('chart');
  } else {
    document.getElementById('chart').style.display     = 'none';
    document.getElementById('chart-pnl').style.display = '';
    document.getElementById('tab-price').classList.remove('active');
    document.getElementById('tab-pnl').classList.add('active');
    if (!pnlLoaded) { loadPnlChart(); } else { Plotly.Plots.resize('chart-pnl'); }
  }
}

function loadPnlChart() {
  fetch('/api/pnl')
    .then(r => r.json())
    .then(function(records) {
      if (!records || records.length === 0) {
        document.getElementById('chart-pnl').innerHTML =
          '<div style="color:#8b949e;padding:40px;text-align:center;font-size:13px;">暂无已实现收益记录（首笔平仓后自动显示）</div>';
        return;
      }
      var dates = records.map(function(r){ return r.close_date; });
      // 插入起始点 0%
      var cumVals = [0].concat(records.map(function(r){ return r.cumulative_pct; }));
      var datesFull = [records[0].close_date].concat(dates);

      var colors = records.map(function(r){
        return r.pnl_pct >= 0 ? '#3fb950' : '#f85149';
      });

      var trace = {
        x: datesFull,
        y: cumVals,
        mode: 'lines+markers',
        name: '累计实现收益',
        line: { color: '#58a6ff', width: 2 },
        marker: {
          color: ['#8b949e'].concat(colors),
          size: 8,
        },
        customdata: [null].concat(records),
        hovertemplate: (
          '<b>%{x}</b><br>' +
          '累计收益: <b>%{y:+.1f}%</b><br>' +
          '本次: %{customdata.pnl_pct:+.1f}%  [%{customdata.leg} · %{customdata.reason}]<br>' +
          'entry $%{customdata.entry_price:,.0f} → exit $%{customdata.exit_price:,.0f}' +
          '<extra></extra>'
        ),
      };

      var layout = {
        template: 'plotly_dark',
        paper_bgcolor: '#0d1117',
        plot_bgcolor:  '#161b22',
        font: { family: 'Arial, sans-serif', color: '#e6edf3' },
        title: {
          text: '实际累计收益（已平仓交易，不含浮盈）',
          font: { size: 13, color: '#58a6ff' },
          x: 0.01, xanchor: 'left'
        },
        xaxis: { showgrid: true, gridcolor: '#21262d', zeroline: false },
        yaxis: {
          showgrid: true, gridcolor: '#21262d', zeroline: true,
          zerolinecolor: '#444d56', ticksuffix: '%',
        },
        hovermode: 'closest',
        margin: { l: 60, r: 20, t: 40, b: 40 },
        shapes: [{
          type: 'line', x0: datesFull[0], x1: datesFull[datesFull.length-1],
          y0: 0, y1: 0,
          line: { color: '#444d56', width: 1, dash: 'dot' }
        }],
      };

      Plotly.newPlot('chart-pnl', [trace], layout, config);
      pnlLoaded = true;
    })
    .catch(function(e) {
      document.getElementById('chart-pnl').innerHTML =
        '<div style="color:#f85149;padding:40px;text-align:center;">加载失败: ' + e + '</div>';
    });
}

// ── 日志面板 ─────────────────────────────────────────────────────
function renderLog(lines, bodyId) {
  var html = lines.map(function(l) {
    var cls = 'log-' + l.level;
    var txt = l.text.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    return '<div class="' + cls + '">' + txt + '</div>';
  }).join('');
  var body = document.getElementById(bodyId);
  body.innerHTML = html;
  body.scrollTop = body.scrollHeight;
}

function loadSignalLog() {
  fetch('/api/signal-log')
    .then(r => r.json())
    .then(lines => {
      renderLog(lines, 'signal-body');
      var now = new Date();
      document.getElementById('signal-refresh-time').textContent =
        '更新 ' + now.toLocaleTimeString('zh-CN');
    })
    .catch(e => {
      document.getElementById('signal-body').innerHTML =
        '<span class="log-error">加载失败: ' + e + '</span>';
    });
}

function loadMaintLog() {
  fetch('/api/log')
    .then(r => r.json())
    .then(lines => {
      renderLog(lines, 'maint-body');
      var now = new Date();
      document.getElementById('maint-refresh-time').textContent =
        '更新 ' + now.toLocaleTimeString('zh-CN');
    })
    .catch(e => {
      document.getElementById('maint-body').innerHTML =
        '<span class="log-error">加载失败: ' + e + '</span>';
    });
}

loadSignalLog();
loadMaintLog();
setInterval(loadSignalLog, 300000);
setInterval(loadMaintLog, 300000);
</script>
</body>
</html>
"""


def read_log(n: int = LOG_LINES, path: str = LOG_PATH) -> list:
    if not os.path.exists(path):
        return [{'level': 'info', 'text': f'{os.path.basename(path)} 暂无数据'}]
    try:
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            lines = f.readlines()
        lines = [l.rstrip() for l in lines if l.strip()][-n:]
        result = []
        for line in lines:
            if 'ERROR' in line:
                level = 'error'
            elif 'WARNING' in line:
                level = 'warning'
            else:
                level = 'info'
            result.append({'level': level, 'text': line})
        return result
    except Exception as e:
        return [{'level': 'error', 'text': f'读取日志失败: {e}'}]


@app.route('/')
def index():
    return render_template_string(
        TEMPLATE,
        graph_json=_cache['graph_json'],
        total_rows=_cache['total_rows'],
        labeled_rows=_cache['labeled_rows'],
        classified=_cache['classified'],
        combo_count=_cache['combo_count'],
    )


@app.route('/api/log')
def api_log():
    return jsonify(read_log())

@app.route('/api/signal-log')
def api_signal_log():
    return jsonify(read_log(LOG_LINES, SIGNAL_LOG_PATH))


@app.route('/api/pnl')
def api_pnl():
    if not os.path.exists(PNL_LOG_PATH):
        return jsonify([])
    try:
        with open(PNL_LOG_PATH, 'r', encoding='utf-8') as f:
            records = json.load(f)
        return jsonify(records)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    print(f"数据: {os.path.abspath(DATA_PATH)}")
    print(f"保留组合: {_cache['combo_count']} 种  |  覆盖: {_cache['classified']}/{_cache['labeled_rows']} 天")
    print("启动 → http://0.0.0.0:5000")
    app.run(host='0.0.0.0', port=5000, debug=False)
