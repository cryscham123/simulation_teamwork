import pandas as pd
import plotly.figure_factory as ff
import plotly.graph_objects as go
from typing import List, Any

def create_gantt_chart(logs: List[Any],
                           max_time:float,
                           title: str = "반도체 공정 시뮬레이션 간트 차트",
                           exclude_event_types: List[str] = None) -> go.Figure:
    """
    Job 리스트로부터 Gantt Chart를 생성

    Args:
        jobs: Job 인스턴스 리스트
        max_time: 시뮬레이션 최대 시간
        title: 차트 제목
        bar_margin: 간트 차트 bar 사이 간격


    Returns:
        Plotly Figure 객체
    """
    # 모든 Job의 이벤트 로그 수집
    df_events = pd.DataFrame(logs)

    _exclude = set(exclude_event_types) if exclude_event_types else set()
    gantt_data = []

    for id, events in df_events[df_events['resource'] == 'machine'].groupby('id'):
        for _, event in events.sort_values('start').iterrows():
            if event['start'] == event['finish']:
                continue
            if event['event'] in _exclude:
                continue
            gantt_data.append({
                'Task': id,
                'Start': event['start'],
                'Finish': (event['finish'] if event['finish'] is not None else max_time),
                'Resource': f"{event['event']}",
                'Description': f"{event['description']}"
            })


    df_gantt = pd.DataFrame(gantt_data)

    # ff.create_gantt은 colors 딕셔너리 키가 'Resource' 컬럼의 실제 값과 정확히 일치해야 함.
    # 시뮬레이션 종류(Stage I/II)에 따라 등장하는 이벤트가 달라지므로 동적으로 필터링.
    _all_colors = {
        "waiting":   'rgb(220, 220, 220)',
        "setup":     'rgb(0, 200, 83)',
        "working":   'rgb(46, 137, 205)',
        "breakdown": 'rgb(255, 65, 54)',
        "repairing": 'rgb(255, 140, 0)',
        "pm":        'rgb(148, 103, 189)',
    }
    _present = set(df_gantt['Resource'].unique()) if not df_gantt.empty else set()
    colors = {k: v for k, v in _all_colors.items() if k in _present}
    # 미등록 이벤트 타입은 기본색으로 추가
    for evt in _present:
        if evt not in colors:
            colors[evt] = 'rgb(150, 150, 150)'

    fig = ff.create_gantt(
        df_gantt,
        colors=colors,
        index_col='Resource',
        show_colorbar=True,
        group_tasks=True,
        showgrid_x=True,
        showgrid_y=True,
        title=title,
    )
    _target_order = ["breakdown", "repairing", "pm", "working", "setup", "waiting"]
    fig.data = sorted(
        fig.data,
        key=lambda x: _target_order.index(x.name) if x.name in _target_order else 999
    )

    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Machine ID",
        hovermode='closest',
        height=max(400, len(df_gantt['Task'].unique()) * 50),
        xaxis=dict(range=[0, max_time + 1], type='linear', dtick=5),
        legend=dict(traceorder='reversed'),
        bargap=1,
        bargroupgap=1
    )

    return fig
