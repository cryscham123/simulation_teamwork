import pandas as pd
import plotly.figure_factory as ff
import plotly.graph_objects as go
from typing import List, Any

def create_gantt_chart(logs: List[Any], 
                           max_time:float, 
                           title: str = "반도체 공정 시뮬레이션 간트 차트") -> go.Figure:
    """
    Job 리스트로부터 Gantt Chart를 생성

    Args:
        jobs: Job 인스턴스 리스트
        max_time: 시뮬레이션 최대 시간
        title: 차트 제목

    Returns:
        Plotly Figure 객체
    """
    # 모든 Job의 이벤트 로그 수집
    df_events = pd.DataFrame(logs)

    gantt_data = []

    for id, events in df_events[df_events['resource'] == 'machine'].groupby('id'):
        for _, event in events.sort_values('start').iterrows():
            if event['start'] == event['finish']:
                continue
            job_id = event['description'].split('\n')[0].split(': ')[1] if event['description'] else ""
            gantt_data.append({
                'Task': id,
                'Start': event['start'],
                'Finish': event['finish'],
                'Resource': f"{event['event']}-{job_id}" if event['event'] == 'working' else event['event'],
                'Description': f"{event['description']}"
            })

    df_gantt = pd.DataFrame(gantt_data)

    colors = {}
    for resource in df_gantt['Resource'].unique():
        if resource == "waiting":
            colors[resource] = 'rgb(220, 220, 220)'  # 밝은 회색
        elif resource == "setup":
            colors[resource] = 'rgb(0, 200, 83)' # 초록색
        elif "working" in resource:
            colors[resource] = f'rgb(0, {200 - int(resource.split("-")[1][1:]) * 20}, 255)'  # 파란색 계열
        elif resource == "repairing":
            colors[resource] = 'rgb(255, 65, 54)'  # 빨간색
        else:
            colors[resource] = 'rgb(255, 140, 0)'  # 주황색

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
    jobs = df_events[df_events['resource'] == 'job']['id'].sort_values().unique()
    target_order = ["repairing", "PM", "waiting", "setup"] + [f"working-{i}" for i in jobs]

    fig.data = sorted(
        fig.data, 
        key=lambda x: target_order.index(x.name) if x.name in target_order else 999
    )

    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Machine ID",
        hovermode='closest',
        height=max(400, len(df_gantt['Task'].unique()) * 50),
        xaxis=dict(range=[0, max_time + 1], type='linear', dtick=5)
    )

    return fig
