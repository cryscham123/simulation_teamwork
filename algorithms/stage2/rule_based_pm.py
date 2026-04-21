"""
Stage II: 누적 고장률 기반 동적 PM(예방 정비) 타이밍 결정

PM 결정 흐름도
─────────────────────────────────────────────────────────────────────
Master Trigger: Λ(last_repair, now) >= threshold
  아니면 → NO_PM

  Step 0  idle_available >= t_PM?
          YES → ADVANCE_PM_FREE (비용 없이 즉시 PM)

  Step 1  postpone_finish(현재 Job + PM 완료 시각) > Next Job QTime 데드라인?
          YES(QTime 위반) → Step 3으로 이동 (Step 2 생략)
          NO  → Step 2로 이동

  Step 2  cost_advance < cost_postpone?
          YES(선행이 이득) → Step 3으로 이동
          NO  → POSTPONE_PM (작업 우선, 나중에 PM)

  Step 3  advance_start(= now + t_PM) > Current Job QTime 데드라인?
          YES → FORCED_POSTPONE_PM (QTime 보호 — 강제 미루기)
          NO  → ADVANCE_PM (작업 잠시 대기, PM 먼저 수행)
─────────────────────────────────────────────────────────────────────
"""

import math
import simpy
import pandas as pd
from algorithms.base import Algorithm
from algorithms.stage1.rule_based import RuleBasedStage1Scheduler
from simulation.scheduler import Scheduler
from simulation.machine import Machine
from utils import EventLogger


# ─────────────────────────────────────────────────────────────────────────────
# PM 결정 결과 상수
# 문자열로 정의해 machine.py에서 circular import 없이 비교 가능
# ─────────────────────────────────────────────────────────────────────────────
class PMDecision:
    NO_PM              = 'NO_PM'
    ADVANCE_PM_FREE    = 'ADVANCE_PM_FREE'
    ADVANCE_PM         = 'ADVANCE_PM'
    POSTPONE_PM        = 'POSTPONE_PM'
    FORCED_POSTPONE_PM = 'FORCED_POSTPONE_PM'


# ─────────────────────────────────────────────────────────────────────────────
# PM 타이밍 결정 정책
# ─────────────────────────────────────────────────────────────────────────────
class RuleBasedPMPolicy:
    """
    누적 고장 기댓값(Λ) 기반 동적 PM 타이밍 결정 정책.

    Args:
        threshold: PM을 고려하기 시작할 누적 고장 기댓값 임계치(ε).
                   Λ(last_repair, now) >= ε 이어야 PM 로직 실행.
    """

    def __init__(self, threshold: float):
        self.threshold = threshold

    def decide(self, machine: Machine, current_job_ctx: dict, scheduler) -> str:
        """
        PM 수행 여부 및 타이밍 결정.

        Args:
            machine: 결정 대상 Machine 인스턴스
            current_job_ctx: Queue에서 방금 꺼낸 Job의 context dict
            scheduler: Scheduler 인스턴스 (Queue 조회 및 env 접근)

        Returns:
            PMDecision 클래스의 문자열 상수 중 하나
        """
        now: float = scheduler._env.now

        # ── Master Trigger ────────────────────────────────────────────────────
        if not machine.needs_pm(now, self.threshold):
            return PMDecision.NO_PM

        t_pm: float = machine.pm_duration
        t_mr: float = machine.repair_time

        # 누적 고장 기댓값 → 고장 발생 확률 근사 (포아송 과정)
        hazard     = machine.cumulative_hazard(machine.last_repair_time, now)
        p_breakdown = 1.0 - math.exp(-hazard)

        # 현재 Job이 도착하기 전까지 머신이 유휴했던 시간
        idle_available: float = max(0.0, now - machine.idle_since)

        # ── Step 0: IDLE_FIT — 유휴 시간만으로 PM이 가능하면 공짜 PM ────────
        if idle_available >= t_pm:
            return PMDecision.ADVANCE_PM_FREE

        # ── 현재 Job 파라미터 ─────────────────────────────────────────────────
        job_type = current_job_ctx['job_type']
        op_id    = current_job_ctx['op_id']

        cur_setup = machine.get_setup_time(job_type)
        cur_pt    = machine.get_process_time(op_id)

        # 현재 Job QTime 데드라인
        cur_max_qtime      = current_job_ctx.get('max_qtime', float('inf'))
        cur_prev_finish    = current_job_ctx.get('prev_op_finish', now)
        cur_qtime_deadline = cur_prev_finish + cur_max_qtime

        # 현재 Job은 이미 Queue에서 제거됨 → queue[0]이 Next Job
        machine_queue = scheduler._machine_queues[machine.id]
        next_job_ctx  = machine_queue[0] if machine_queue else None

        # Postpone 시나리오: 현재 Job 처리 → PM 완료 예상 시각
        postpone_finish = now + cur_setup + cur_pt + t_pm

        # ── Step 1: Postpone 시 Next Job QTime 위반 여부 ─────────────────────
        next_qtime_violated = False
        if next_job_ctx is not None:
            next_max_qtime      = next_job_ctx.get('max_qtime', float('inf'))
            next_prev_finish    = next_job_ctx.get('prev_op_finish', now)
            next_qtime_deadline = next_prev_finish + next_max_qtime
            next_qtime_violated = postpone_finish > next_qtime_deadline

        if not next_qtime_violated:
            # ── Step 2: 비용 비교 ──────────────────────────────────────────────
            # Advance 비용: PM 선행으로 인한 추가 대기 시간(유휴 시간 초과분)
            cost_advance  = max(0.0, t_pm - idle_available)
            # Postpone 비용: 고장 시 예상 수리 손실 (E[T_MR] × P_breakdown)
            cost_postpone = p_breakdown * t_mr

            if cost_advance >= cost_postpone:
                # 미루는 것이 더 경제적
                return PMDecision.POSTPONE_PM
            # cost_advance < cost_postpone → 당기는 것이 이득 → Step 3으로

        # ── Step 3: Advance 시 Current Job QTime 위반 여부 ───────────────────
        # PM을 먼저 수행하면 현재 Job이 실제로 시작되는 시각
        advance_start = now + t_pm

        if advance_start > cur_qtime_deadline:
            # PM을 당기면 현재 Job 폐기 → QTime 보호를 위해 강제로 미루기
            return PMDecision.FORCED_POSTPONE_PM

        # PM 선행이 안전하고 경제적
        return PMDecision.ADVANCE_PM


# ─────────────────────────────────────────────────────────────────────────────
# Stage II 스케줄러
# ─────────────────────────────────────────────────────────────────────────────
class Stage2RuleBasedPMScheduler(RuleBasedStage1Scheduler):
    """
    Stage II 스케줄러: RuleBasedStage1Scheduler + 동적 PM 예방 정비.

    Stage I과 동일한 Q-Time → SPT → CR 디스패칭 규칙을 유지하면서:
      - enable_failures=True: 고장/MR 시뮬레이션 활성화
      - 각 Machine에 RuleBasedPMPolicy 부착

    Args:
        pm_threshold (float): PM 검토 시작 임계치(ε).
                              낮을수록 PM을 더 자주 고려.
                              기본값 1.0 (누적 고장 기댓값 1회 이상이면 PM 고려).
    """

    def __init__(self, env: simpy.Environment, machine_df: pd.DataFrame,
                 operations_df: pd.DataFrame, machine_failure_df: pd.DataFrame,
                 setup_times_df: pd.DataFrame, op_machine_df: pd.DataFrame,
                 jobs_df: pd.DataFrame, qtime_constraints_df: pd.DataFrame,
                 event_logger: EventLogger, algorithm: Algorithm,
                 pm_threshold: float = 0.105):

        # Stage I __init__은 enable_failures=False 하드코딩 → 기반 Scheduler 직접 호출
        Scheduler.__init__(
            self, env, machine_df, operations_df, machine_failure_df,
            setup_times_df, op_machine_df, event_logger,
            enable_failures=True          # Stage II: 고장 시뮬레이션 ON
        )

        # Stage I 필드 수동 설정 (super().__init__을 우회했으므로)
        self._algorithm = algorithm
        self._jobs_df   = jobs_df.set_index('job_id')
        self._qtime_df  = qtime_constraints_df

        # PM 정책을 모든 Machine에 주입
        pm_policy = RuleBasedPMPolicy(threshold=pm_threshold)
        for machine in self._machines.values():
            machine._pm_policy = pm_policy
