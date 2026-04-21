import pandas as pd
import os
from typing import List


def save_event_log(logs: List[dict], filepath: str) -> None:
    """EventLogger.logs를 CSV로 저장"""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    pd.DataFrame(logs).to_csv(filepath, index=False)


def load_event_log(filepath: str) -> pd.DataFrame:
    return pd.read_csv(filepath)


def save_schedule_summary(jobs: list, filepath: str) -> None:
    """Job 인스턴스 리스트로부터 완료 요약 CSV 저장"""
    records = [
        {
            'job_id': job.id,
            'is_completed': job.is_completed,
            'completed_time': job.completed_time,
            'total_qtime_over': job.total_qtime_over,
        }
        for job in jobs
    ]
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    pd.DataFrame(records).to_csv(filepath, index=False)


def load_schedule_summary(filepath: str) -> pd.DataFrame:
    return pd.read_csv(filepath)
