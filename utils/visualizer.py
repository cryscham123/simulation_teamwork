import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
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
    #for id, events in df_events.groupby('id'):
        for _, event in events.sort_values('start').iterrows():
            if event['start'] == event['finish']:
                continue
            job_id = event['description'].split('\n')[0].split(': ')[1] if event['description'] else ""
            gantt_data.append({
                'Task': id,
                'Start': event['start'],
                'Finish': event['finish'],
                'Resource': f"{event['event']}-{job_id}" if event['event'] == 'working' and event['resource'] == 'machine' else event['event'],
                'Description': f"{event['description']}"
            })

    df_gantt = pd.DataFrame(gantt_data)

    df_gantt['Duration'] = df_gantt['Finish'] - df_gantt['Start']
    color_map = {}
    for res in df_gantt['Resource'].unique():
        if res == "waiting": color_map[res] = 'rgb(220, 220, 220)'
        elif res == "setup": color_map[res] = 'rgb(0, 200, 83)'
        elif res == "repairing": color_map[res] = 'rgb(255, 65, 54)'
        elif res == "PM": color_map[res] = 'rgb(255, 140, 0)'
        elif res == 'qtime_over': color_map[res] = 'rgb(255, 0, 255)'
        elif "working-" in res:
            val = int(res.split("-")[1][1:]) % 5 
            color_map[res] = f'rgb(0, {200 - val * 30}, 255)'
        else:
            color_map[res] = 'rgb(0, 0, 255)'

    jobs = df_events[df_events['resource'] == 'job']['id'].sort_values().unique()
    target_order = ["repairing", "PM", "waiting", "setup", "qtime_over"] + [f"working-{i}" for i in jobs]
    sorted_tasks = sorted(df_gantt['Task'].unique())

    fig = px.bar(
        df_gantt,
        base="Start",
        x="Duration",
        y="Task",
        color="Resource",
        color_discrete_map=color_map,
        orientation='h',
        category_orders={
            "Resource": target_order,
            "Task": sorted_tasks
        },
        title=title
    )
    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Machine ID",
        xaxis=dict(range=[0, max_time + 1], dtick=10),
        height=max(400, len(df_gantt['Task'].unique()) * 50)
    )
    fig.update_traces(width=0.3)
    return fig
