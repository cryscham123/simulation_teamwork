import pandas as pd
import plotly.figure_factory as ff
import plotly.graph_objects as go
from typing import List, Any

def create_gantt_chart(logs: List[Any], 
                           max_time:float, 
                           title: str = "Job Gantt Chart",
                           bar_margin: float = 0.3) -> go.Figure:
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

    gantt_data = []

    pd.set_option('display.max_rows', None)  # 모든 행 출력
    for id, events in df_events.groupby('id'):
        for _, event in events.sort_values('start').iterrows():
            if event['start'] == event['finish']:
                continue
            gantt_data.append({
                'Task': id,
                'Start': event['start'],
                'Finish': (event['finish'] if event['finish'] is not None else max_time) - bar_margin,
                'Resource': f"{event['event']}",
                'Description': f"{event['description']}"
            })

    df_gantt = pd.DataFrame(gantt_data)

    colors = {
            "waiting": 'rgb(220, 220, 220)',  # 밝은 회색
            "setup": 'rgb(0, 200, 83)', # 초록색
            "working": 'rgb(46, 137, 205)',  # 파란색
            "breakdown": 'rgb(255, 65, 54)',  # 빨간색
            "repairing": 'rgb(255, 140, 0)'  # 주황색
    }

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
    target_order = ["breakdown", "repairing", "working", "setup", "waiting"]

    fig.data = sorted(
        fig.data, 
        key=lambda x: target_order.index(x.name) if x.name in target_order else 999
    )

    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Job ID",
        hovermode='closest',
        height=max(400, len(df_gantt['Task'].unique()) * 50),
        xaxis=dict(range=[0, max_time + 1], type='linear', dtick=5),
        legend=dict(traceorder='reversed'),
        bargap=1,
        bargroupgap=1
    )

    return fig
