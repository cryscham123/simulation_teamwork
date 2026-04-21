import simpy
import pandas as pd
from typing import Optional

from algorithms.base import Algorithm
from simulation.scheduler import Scheduler
from simulation.machine import Machine
from utils import EventLogger


class RuleBasedDispatch(Algorithm):
    """
    Machine 중심 복합 Dispatching Rule.

    변경 전: 하나의 Job이 여러 Machine을 평가 → Machine 선택
    변경 후: 하나의 Machine이 자신의 Queue에 있는 여러 Job을 평가 → Job 선택

    우선순위 튜플: (qtime_urgency, spt, cr)
    - qtime_urgency: 잔여 QTime이 작을수록(위급) 낮은 값 → 우선
    - spt: 처리 시간이 짧을수록 낮은 값 → 우선
    - cr: Critical Ratio가 낮을수록 위급 → 우선
    """

    def select_job(self, machine_context: dict, waiting_jobs: list) -> Optional[dict]:
        """
        Machine의 Queue에서 우선순위가 가장 높은 Job 선택.
        Queue에 Job이 1개이면 즉시 반환(스코어 계산 생략).
        """
        if not waiting_jobs:
            return None
        if len(waiting_jobs) == 1:
            return waiting_jobs[0]

        scored = [
            (self._score(machine_context, jctx), jctx)
            for jctx in waiting_jobs
        ]
        scored.sort(key=lambda x: x[0])
        return scored[0][1]

    def _score(self, machine_ctx: dict, job_ctx: dict) -> tuple:
        """
        Machine 관점에서 Job의 우선순위 점수를 계산.
        낮은 점수 = 높은 우선순위.

        Args:
            machine_ctx: {'machine': Machine, 'machine_id': int, 'now': float}
            job_ctx: job_context dict (route_job_to_machine이 채운 정보)
        """
        now: float = machine_ctx['now']
        machine: Machine = machine_ctx['machine']

        # --- 우선순위 1: QTime 긴급도 (잔여 시간이 작을수록 위급) ---
        max_qtime = job_ctx.get('max_qtime', float('inf'))
        prev_finish = job_ctx.get('prev_op_finish', now)
        qtime_remaining = max_qtime - (now - prev_finish)

        # --- 우선순위 2: SPT — 이 Machine에서의 처리 시간 ---
        pt = machine.get_process_time(job_ctx['op_id'])

        # --- 우선순위 3: CR — Critical Ratio (낮을수록 위급) ---
        remaining_pt = max(job_ctx.get('remaining_process_time', 1.0), 1.0)
        cr = (job_ctx['due_date'] - now) / remaining_pt

        return (qtime_remaining, pt, cr)


class RuleBasedStage1Scheduler(Scheduler):
    """
    RuleBasedDispatch 알고리즘을 사용하는 Stage I 스케줄러.

    - route_job_to_machine: job_context에 remaining_process_time을 추가 후 EFT 라우팅
    - request_job: Machine이 유휴 상태일 때 알고리즘으로 최적 Job 선택
    """

    def __init__(self, env: simpy.Environment, machine_df: pd.DataFrame,
                 operations_df: pd.DataFrame, machine_failure_df: pd.DataFrame,
                 setup_times_df: pd.DataFrame, op_machine_df: pd.DataFrame,
                 jobs_df: pd.DataFrame, qtime_constraints_df: pd.DataFrame,
                 event_logger: EventLogger, algorithm: Algorithm):
        # Stage I는 순수 스케줄링 실험 — Machine 고장/수리 없음
        super().__init__(env, machine_df, operations_df, machine_failure_df,
                         setup_times_df, op_machine_df, event_logger,
                         enable_failures=False)
        self._algorithm = algorithm
        self._jobs_df = jobs_df.set_index('job_id')
        self._qtime_df = qtime_constraints_df

    def route_job_to_machine(self, job_context: dict):
        """
        job_context에 remaining_process_time(CR 계산용)을 보강한 뒤
        부모의 EFT 라우팅으로 위임.
        """
        job_id = job_context['job_id']
        op_seq = job_context['op_seq']
        job_context['remaining_process_time'] = (
            self._estimate_remaining_process_time(job_id, op_seq)
        )
        yield self._env.process(super().route_job_to_machine(job_context))

    def request_job(self, machine_id: int) -> Optional[dict]:
        """
        Machine이 유휴 상태일 때 호출.
        Queue가 비었으면 이벤트 리셋 후 None 반환.
        Queue에 Job이 있으면 알고리즘(Q-Time → SPT → CR)으로 최적 Job 선택.
        """
        queue = self._machine_queues[machine_id]
        if not queue:
            self._machine_job_events[machine_id] = self._env.event()
            return None

        machine = self._machines[machine_id]
        machine_ctx = {
            'machine':    machine,
            'machine_id': machine_id,
            'now':        self._env.now,
        }

        selected = self._algorithm.select_job(machine_ctx, queue)
        if selected is not None:
            queue.remove(selected)
        return selected

    def _estimate_remaining_process_time(self, job_id: int, from_op_seq: int) -> float:
        """from_op_seq 이상의 잔여 op에 대해 최소 처리 시간 합산으로 잔여 시간 추정."""
        try:
            job_ops = self._op_table.loc[job_id]
        except KeyError:
            return 1.0

        remaining_ops = job_ops[job_ops.index >= from_op_seq]
        total = 0.0
        for _, row in remaining_ops.iterrows():
            op_rows = self._op_machine_df[self._op_machine_df['op_id'] == row['op_id']]
            if not op_rows.empty:
                total += float(op_rows['process_time'].min())
        return max(total, 1.0)
