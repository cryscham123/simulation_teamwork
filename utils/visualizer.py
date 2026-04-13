import pandas as pd
import plotly.graph_objects as go
from typing import List, Any


COLOR_MAP = {
    'setup':     'rgb(0, 200, 83)',
    'working':   'rgb(46, 137, 205)',
    'repairing': 'rgb(255, 140, 0)',
    'pm':        'rgb(180, 0, 255)',
}


def create_gantt_chart(logs: List[Any],
                       max_time: float,
                       title: str = "반도체 공정 시뮬레이션 간트 차트") -> go.Figure:
    """
    EventLogger 로그로부터 머신 기준 Gantt Chart 생성.

    Args:
        logs    : event_logger.logs (list of dict)
        max_time: 시뮬레이션 종료 시각
        title   : 차트 제목

    Returns:
        Plotly Figure 객체
    """
    df = pd.DataFrame(logs)

    # machine 이벤트만 사용, finish 없는 행 제거
    df_m = df[df['resource'] == 'machine'].copy()
    df_m = df_m.dropna(subset=['finish'])
    df_m = df_m[df_m['finish'] > df_m['start']]

    if df_m.empty:
        fig = go.Figure()
        fig.update_layout(title=title + " (데이터 없음)")
        return fig

    machine_ids = sorted(df_m['id'].unique(), key=lambda x: str(x))
    y_labels = [str(mid) for mid in machine_ids]

    fig = go.Figure()
    added = set()

    for event_type, color in COLOR_MAP.items():
        events = df_m[df_m['event'] == event_type]
        if events.empty:
            continue

        show_legend = event_type not in added
        added.add(event_type)

        x_vals, y_vals, base_vals, hover_texts = [], [], [], []

        for _, row in events.iterrows():
            duration = row['finish'] - row['start']
            x_vals.append(duration)
            y_vals.append(str(row['id']))
            base_vals.append(row['start'])
            desc = row.get('description') or ''
            hover_texts.append(
                f"Machine: {row['id']}<br>"
                f"Event: {row['event']}<br>"
                f"Start: {row['start']:.2f}<br>"
                f"Finish: {row['finish']:.2f}<br>"
                f"Duration: {duration:.2f}<br>"
                f"{desc}"
            )

        fig.add_trace(go.Bar(
            x=x_vals,
            y=y_vals,
            base=base_vals,
            orientation='h',
            name=event_type,
            marker_color=color,
            hovertext=hover_texts,
            hoverinfo='text',
            legendgroup=event_type,
            showlegend=show_legend,
        ))

    fig.update_layout(
        title=title,
        barmode='overlay',
        xaxis=dict(title='Time', range=[0, max_time + 1]),
        yaxis=dict(
            title='Machine ID',
            categoryorder='array',
            categoryarray=y_labels,
        ),
        hovermode='closest',
        height=max(400, len(machine_ids) * 60 + 150),
        legend=dict(title='Event Type'),
    )

    return fig
