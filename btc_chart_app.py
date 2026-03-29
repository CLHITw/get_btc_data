"""
BTC 价格折线图 + 六维 Cluster 组合标注（方案 C）
运行: python btc_chart_app.py
访问: http://<服务器IP>:5000
"""

import json, os, math, colorsys
import pandas as pd
import plotly.graph_objects as go
import plotly.utils
from flask import Flask, render_template_string, request

app = Flask(__name__)

# ── 数据路径 ─────────────────────────────────────────────────────────
DATA_PATH = '/root/Desktop/btc/get_data/get_btc_data/btc.xlsx'
# ─────────────────────────────────────────────────────────────────────

K_COLS = ['k10', 'k11', 'k12', 'k13', 'k14', 'k15']

SYMBOLS = [
    'circle', 'square', 'diamond', 'triangle-up', 'triangle-down',
    'star', 'hexagon', 'cross', 'x', 'pentagon',
    'triangle-left', 'triangle-right', 'hexagram', 'hourglass',
    'bowtie', 'asterisk', 'circle-open', 'square-open', 'diamond-open',
    'triangle-up-open', 'star-open', 'hexagon-open'
]


def load_data():
    df = pd.read_excel(DATA_PATH)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)
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

    # ── 价格折线 ──────────────────────────────────────────────────────
    fig.add_trace(go.Scatter(
        x=df['date'], y=df['close'],
        mode='lines',
        name='BTC Close',
        line=dict(color='#F7931A', width=1.6),
        customdata=df[['open', 'high', 'low', 'fgi', 'volume']].values,
        hovertemplate=(
            '<b>%{x|%Y-%m-%d}</b><br>'
            'Open : $%{customdata[0]:,.0f}<br>'
            'High : $%{customdata[1]:,.0f}<br>'
            'Low  : $%{customdata[2]:,.0f}<br>'
            'Close: <b>$%{y:,.0f}</b><br>'
            'FGI  : %{customdata[3]:.0f}<br>'
            'Vol  : %{customdata[4]:,.0f}'
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
            customdata=unclassified[['open', 'high', 'low', 'fgi', 'volume']].values,
            hovertemplate=(
                '<b>%{x|%Y-%m-%d}</b><br>'
                '其他低频组合<br>'
                'Open : $%{customdata[0]:,.0f}<br>'
                'High : $%{customdata[1]:,.0f}<br>'
                'Low  : $%{customdata[2]:,.0f}<br>'
                'Close: <b>$%{y:,.0f}</b><br>'
                'FGI  : %{customdata[3]:.0f}<br>'
                'Vol  : %{customdata[4]:,.0f}'
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

        # 图例名称：G01(×53) 这样
        label = f'G{i+1:02d}(×{cnt})'
        # 悬停显示完整组合
        combo_str = 'k_cluster[' + ', '.join(str(int(v)) for v in combo) + ']'

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
            customdata=sub[['open', 'high', 'low', 'fgi', 'volume']].values,
            hovertemplate=(
                '<b>%{x|%Y-%m-%d}</b><br>'
                f'<b>{label}</b>  {combo_str}<br>'
                'Open : $%{customdata[0]:,.0f}<br>'
                'High : $%{customdata[1]:,.0f}<br>'
                'Low  : $%{customdata[2]:,.0f}<br>'
                'Close: <b>$%{y:,.0f}</b><br>'
                'FGI  : %{customdata[3]:.0f}<br>'
                'Vol  : %{customdata[4]:,.0f}'
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
            tickprefix='$', tickformat=',.0f', side='right',
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
        hovermode='x unified',
        margin=dict(l=10, r=130, t=46, b=10),
        dragmode='pan',
    )

    return fig


# ── 预加载 ───────────────────────────────────────────────────────────
_df    = load_data()
_keep, _valid = compute_combos(_df)
_fig   = make_figure(_df, _keep, _valid)
_graph_json = json.dumps(_fig, cls=plotly.utils.PlotlyJSONEncoder)

# 统计信息
_total_rows     = len(_df)
_labeled_rows   = len(_valid)
_classified     = _valid[_valid['combo'].isin(_keep)].shape[0]
_combo_count    = len(_keep)


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
  .stat {
    font-size: 12px;
    color: #8b949e;
    white-space: nowrap;
  }
  .stat b { color: #F7931A; }
  .hint {
    margin-left: auto;
    font-size: 11px;
    color: #444d56;
    white-space: nowrap;
  }
  #chart { flex: 1 1 0; min-height: 0; width: 100%; }

  @media (max-width: 768px) {
    .toolbar { padding: 5px 8px; gap: 6px; }
    .hint { display: none; }
  }
</style>
</head>
<body>

<div class="toolbar">
  <span class="stat">总天数 <b>{{ total_rows }}</b></span>
  <span class="stat">有标签 <b>{{ labeled_rows }}</b></span>
  <span class="stat">已分组 <b>{{ classified }}</b></span>
  <span class="stat">组合数 <b>{{ combo_count }}</b>
    （≥3次 或 连续2次）</span>
  <span class="hint">滚轮/双指缩放 · 拖动平移 · 点击图例显示/隐藏</span>
</div>

<div id="chart"></div>

<script>
var gd = document.getElementById('chart');
var data   = {{ graph_json | safe }};
var config = {
  responsive: true,
  scrollZoom: true,
  displayModeBar: true,
  modeBarButtonsToRemove: ['select2d','lasso2d','autoScale2d'],
  displaylogo: false
};
Plotly.newPlot('chart', data.data, data.layout, config);

function resize() {
  var h = window.innerHeight - document.querySelector('.toolbar').offsetHeight;
  gd.style.height = h + 'px';
  Plotly.Plots.resize('chart');
}
window.addEventListener('resize', resize);
resize();
</script>
</body>
</html>
"""


@app.route('/')
def index():
    return render_template_string(
        TEMPLATE,
        graph_json=_graph_json,
        total_rows=_total_rows,
        labeled_rows=_labeled_rows,
        classified=_classified,
        combo_count=_combo_count,
    )


if __name__ == '__main__':
    print(f"数据: {os.path.abspath(DATA_PATH)}")
    print(f"保留组合: {_combo_count} 种  |  覆盖: {_classified}/{_labeled_rows} 天")
    print("启动 → http://0.0.0.0:5000")
    app.run(host='0.0.0.0', port=5000, debug=False)
