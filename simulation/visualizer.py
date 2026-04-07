import pandas as pd
import plotly.figure_factory as ff
import plotly.graph_objects as go
from typing import List, Any

def create_job_gantt_chart(jobs: List[Any], max_time:float, title: str = "Job Gantt Chart") -> go.Figure:
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
    all_events = []
    for job in jobs:
        events = job.event_log
        all_events.extend(events)
    df_events = pd.DataFrame(all_events)

    gantt_data = []

    pd.set_option('display.max_rows', None)  # 모든 행 출력
    for job_id, job_events in df_events.groupby('job_id'):
        for i, (_, event) in enumerate(job_events.sort_values('time').iterrows()):
            if i != 0:
                gantt_data[-1]['Finish'] = event['time'] if gantt_data[-1]['Resource'] != 'interrupt' else gantt_data[-1]['Start'] + 1
                if gantt_data[-1]['Start'] == gantt_data[-1]['Finish']:
                    gantt_data.pop()
            if event['event_type'] == 'completed':
                break
            gantt_data.append({
                'Task': job_id,
                'Start': event['time'],
                'Resource': f"{event['event_type']}",
                'Description': f"{event['description']}"
            })
        else:
            gantt_data[-1]['Finish'] = max_time

    df_gantt = pd.DataFrame(gantt_data)

    colors = {}
    for resource in df_gantt['Resource'].unique():
        if resource == "waiting":
            colors[resource] = 'rgb(220, 220, 220)'  # 밝은 회색
        elif resource == "setup":
            colors[resource] = 'rgb(0, 200, 83)' # 초록색
        elif resource == "working":
            colors[resource] = 'rgb(46, 137, 205)'  # 파란색
        else:
            colors[resource] = 'rgb(255, 65, 54)'  # 빨간색

    # Gantt Chart 생성
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

    # 레이아웃 업데이트
    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Job ID",
        hovermode='closest',
        height=max(400, len(df_gantt['Task'].unique()) * 50),
        xaxis=dict(range=[0, max_time + 1], type='linear')
    )

    return fig