"""
algorithms/rule_based/scheduler.py
====================================
RuleBasedScheduler — Makespan 최소화 기반 PM Re-scheduling 스케줄러

논문: Wang et al. (2024) "Joint optimization of FJSSP and PM under
      high-frequency production switching"
      의 advance-postpone balancing method를 Makespan 관점으로 재해석.

전체 로직 흐름:
──────────────────────────────────────────────────────────────────────
  초기 FJSP 스케줄 생성 (기본 Scheduler의 FilterStore 기반 EFT 방식)
          ↓
  시뮬레이션 시간 t 진행
          ↓
  ∫λ(s)ds ≥ ε ?  →  NO: 머신 그대로 반환
          ↓ YES
  t*_rk가 현재 진행 예정 Op와 충돌?
          ↓
  [Case A] idle 구간 ≥ pm_duration → IDLE_FIT  → ΔC_max = 0, 즉시 PM
  [Case B] ΔC_max(advance) ≤ ΔC_max(delay)   → ADVANCE → PM 먼저 수행
  [Case C] ΔC_max(advance) >  ΔC_max(delay)  → DELAY   → pm_pending=True 표시
          ↓
  [DELAY 후처리] put_back_machine() 호출 시
    → pm_pending=True 이면 PM 수행 후 스토어 반환 (별도 프로세스)
──────────────────────────────────────────────────────────────────────
"""

import simpy
from enum import Enum, auto
from typing import Tuple
import pandas as pd

from simulation.scheduler import Scheduler
from simulation.machine import Machine
from utils import EventLogger


# ─────────────────────────────────────────────────────────────────────
# PM 결정 열거형
# ─────────────────────────────────────────────────────────────────────

class PMDecision(Enum):
    IDLE_FIT = auto()   # 유휴 구간에 PM 삽입 — Makespan 영향 없음
    ADVANCE  = auto()   # PM을 작업 전에 당겨 수행
    DELAY    = auto()   # PM을 작업 완료 후로 미룸 (pm_pending 표시)
    NO_PM    = auto()   # PM 불필요


# ─────────────────────────────────────────────────────────────────────
# RuleBasedScheduler
# ─────────────────────────────────────────────────────────────────────

class RuleBasedScheduler(Scheduler):
    """
    Makespan 최소화 기반 PM Re-scheduling 스케줄러.

    기본 Scheduler를 상속하여 get_matched_machine / put_back_machine 을
    오버라이드. PM 판단·실행 로직이 두 메서드에 집중되어 있으므로
    Job 클래스(job.py)는 수정하지 않아도 된다.
    """

    def __init__(
        self,
        env: simpy.Environment,
        machine_df: pd.DataFrame,
        operations_df: pd.DataFrame,
        machine_failure_df: pd.DataFrame,
        setup_times_df: pd.DataFrame,
        op_machine_df: pd.DataFrame,
        event_logger: EventLogger,
        pm_epsilon: float = 0.4,
    ):
        """
        Args:
            event_logger: 이벤트 기록 인스턴스
            pm_epsilon  : PM 판단 임계치 ε
                          누적 고장 기댓값 Λ(last_repair_time, now) ≥ ε 이면 PM 필요
        """
        super().__init__(
            env, machine_df, operations_df,
            machine_failure_df, setup_times_df, op_machine_df,
            event_logger
        )
        self._pm_epsilon = pm_epsilon
        self._pm_log: list[dict] = []     # PM 수행 이력 (분석용)

    # ──────────────────────────────────────────────────────────────────
    # ΔC_max 계산 — ADVANCE 시나리오
    # ──────────────────────────────────────────────────────────────────

    def _delta_cmax_advance(
        self, machine: Machine, op_start: float, op_duration: float
    ) -> Tuple[float, float]:
        """
        PM을 작업 전에 당겼을 때의 Makespan 순증가량 계산.

        유휴 시간(idle_available)만큼은 PM 시간을 흡수할 수 있으므로
        실제 지연 = max(0, pm_duration - idle_available).

        Args:
            machine    : 대상 기계
            op_start   : 작업 예정 시작 시간 (= env.now)
            op_duration: 작업 소요 시간 (setup + process)

        Returns:
            (delta, pm_start_time)
            delta        : Makespan 순증가량
            pm_start_time: PM이 실제 시작될 시점
        """
        idle_available = max(op_start - machine.idle_since, 0.0)
        delta          = max(0.0, machine.pm_duration - idle_available)

        # 유휴 구간을 최대한 활용한 PM 시작 시점
        pm_start = max(op_start - machine.pm_duration, machine.idle_since)
        return delta, pm_start

    # ──────────────────────────────────────────────────────────────────
    # ΔC_max 계산 — DELAY 시나리오
    # ──────────────────────────────────────────────────────────────────

    def _delta_cmax_delay(
        self, machine: Machine, op_start: float, op_duration: float
    ) -> float:
        """
        PM을 작업 완료 후로 미뤘을 때의 기대 Makespan 증가량 계산.

        작업 구간 [op_start, op_start + op_duration] 동안
        기대 고장 횟수 × repair_time 만큼 추가 지연이 발생할 수 있음.

        ΔC_max(delay) = repair_time × Λ(op_start, op_start + op_duration)

        Args:
            machine    : 대상 기계
            op_start   : 작업 예정 시작 시간
            op_duration: 작업 소요 시간

        Returns:
            float: 기대 Makespan 증가량
        """
        op_end          = op_start + op_duration
        lambda_integral = machine.cumulative_hazard(op_start, op_end)
        return machine.repair_time * lambda_integral

    # ──────────────────────────────────────────────────────────────────
    # PM 시점 결정 (3-way 분기)
    # ──────────────────────────────────────────────────────────────────

    def _decide_pm_timing(
        self, machine: Machine, op_start: float, op_duration: float
    ) -> PMDecision:
        """
        IDLE_FIT / ADVANCE / DELAY 중 Makespan 최소화 관점에서 결정.

        Case A: 유휴 시간 ≥ pm_duration          → IDLE_FIT (비용 0)
        Case B: ΔC_max(advance) ≤ ΔC_max(delay) → ADVANCE
        Case C: ΔC_max(advance) >  ΔC_max(delay) → DELAY

        Args:
            machine    : 대상 기계
            op_start   : 작업 예정 시작 시간 (= env.now)
            op_duration: 작업 소요 시간

        Returns:
            PMDecision 열거형
        """
        idle_available = max(op_start - machine.idle_since, 0.0)

        # ── Case A: 유휴 시간으로 PM 전부 흡수 → Makespan 무영향 ─────────
        if idle_available >= machine.pm_duration:
            print(f'{round(op_start, 2)}\t'
                  f'Machine {machine.id} → PM Decision: IDLE_FIT '
                  f'(idle={idle_available:.2f} ≥ pm_dur={machine.pm_duration})')
            return PMDecision.IDLE_FIT

        # ── Case B / C: ΔC_max 비교 ──────────────────────────────────────
        delta_adv, _ = self._delta_cmax_advance(machine, op_start, op_duration)
        delta_del    = self._delta_cmax_delay(machine, op_start, op_duration)

        print(f'{round(op_start, 2)}\t'
              f'Machine {machine.id} → PM Decision: '
              f'ΔC_max(Adv)={delta_adv:.4f}  ΔC_max(Del)={delta_del:.4f}')

        # 동점이면 Advance 우선 (불확실성 조기 제거)
        if delta_adv <= delta_del:
            return PMDecision.ADVANCE
        else:
            return PMDecision.DELAY

    # ──────────────────────────────────────────────────────────────────
    # [오버라이드] get_matched_machine — PM Advance/IDLE_FIT 처리
    # ──────────────────────────────────────────────────────────────────

    def get_matched_machine(self, job_id: int, op_seq: int):
        """
        유휴 머신을 선택하고, PM 필요 시 ADVANCE/IDLE_FIT이면 즉시 PM 수행 후 반환.
        DELAY면 pm_pending=True만 표시하고 머신을 그대로 반환.

        Job.run()은 이 메서드의 반환값만 사용하므로 Job 코드 수정 불필요.

        Args:
            job_id : 작업 ID
            op_seq : 작업 시퀀스

        Yields:
            Machine: (PM 완료 후 또는 그대로) 할당된 머신
        """
        # ── 1. 기본 머신 획득 (부모 클래스와 동일) ────────────────────────
        op_group = self._op_table.loc[(job_id, op_seq), 'op_group']
        machine: Machine = yield self._machine_store[op_group].get(
            lambda x: x.is_idle()
        )

        now = self._env.now

        # ── 2. 해당 Op의 예상 소요 시간 (setup + process) ─────────────────
        # setup time은 job_type 정보 없이 최악 케이스(최대값)로 근사
        # 실제 환경에서는 job_type을 Scheduler로 전달하여 정확히 계산 가능
        op_duration = self._get_estimated_op_duration(machine, job_id, op_seq)

        # ── 3. PM 필요 여부 판단 (Λ ≥ ε) ────────────────────────────────
        if not machine.needs_pm(now, self._pm_epsilon):
            return machine  # PM 불필요 → 바로 반환

        accumulated = machine.cumulative_hazard(machine.last_repair_time, now)
        print(f'{round(now, 2)}\t'
              f'Machine {machine.id} needs PM '
              f'(Λ={accumulated:.4f} ≥ ε={self._pm_epsilon})')

        # ── 4. PM 시점 결정 ───────────────────────────────────────────────
        decision = self._decide_pm_timing(machine, now, op_duration)

        if decision in (PMDecision.IDLE_FIT, PMDecision.ADVANCE):
            # ── Case A / B: 작업 전 즉시 PM 수행 ────────────────────────
            print(f'{round(now, 2)}\t'
                  f'Machine {machine.id} → {decision.name}: PM before job')
            yield self._env.process(machine.perform_pm())
            self._pm_log.append({
                'machine_id': machine.id,
                'decision'  : decision.name,
                'pm_start'  : now,
                'pm_end'    : self._env.now,
                'job_id'    : job_id,
                'op_seq'    : op_seq,
            })

        else:
            # ── Case C: DELAY — pm_pending 표시 후 머신 그대로 반환 ───────
            print(f'{round(now, 2)}\t'
                  f'Machine {machine.id} → DELAY: PM after job '
                  f'(job_id={job_id}, op_seq={op_seq})')
            machine.pm_pending = True

        return machine

    # ──────────────────────────────────────────────────────────────────
    # [오버라이드] put_back_machine — DELAY PM 후처리
    # ──────────────────────────────────────────────────────────────────

    def put_back_machine(self, machine: Machine):
        """
        Job이 Op를 완료하고 머신을 반환할 때 호출.

        pm_pending=True(DELAY 결정)이면:
          → PM 수행 후 스토어 반환 (별도 SimPy 프로세스로 비동기 처리)
        pm_pending=False이면:
          → 즉시 스토어 반환 (기존 동작과 동일)

        Args:
            machine: 반환할 머신
        """
        machine.idle_since = self._env.now

        if machine.pm_pending:
            # PM 완료 후 스토어 반환 — 별도 프로세스로 처리하여 non-blocking
            self._env.process(self._delayed_pm_and_return(machine))
        else:
            self._machine_store[machine.group].put(machine)

    def _delayed_pm_and_return(self, machine: Machine):
        """
        DELAY 결정된 PM을 작업 완료 직후 수행하고 머신을 스토어에 반환.

        비동기 프로세스로 실행되므로 Job의 다음 Op 탐색을 블록하지 않음.
        (다른 Job은 이 PM이 끝날 때까지 해당 머신을 사용할 수 없음 —
         스토어에 반환되지 않았으므로 FilterStore.get()이 대기 상태 유지)

        Args:
            machine: PM 수행 후 반환할 머신
        """
        pm_start = self._env.now
        yield self._env.process(machine.perform_pm())
        pm_end = self._env.now

        self._pm_log.append({
            'machine_id': machine.id,
            'decision'  : 'DELAY',
            'pm_start'  : pm_start,
            'pm_end'    : pm_end,
        })

        # PM 완료 후 스토어 반환
        self._machine_store[machine.group].put(machine)

    # ──────────────────────────────────────────────────────────────────
    # 내부 헬퍼
    # ──────────────────────────────────────────────────────────────────

    def _get_estimated_op_duration(
        self, machine: Machine, job_id: int, op_seq: int
    ) -> float:
        """
        ΔC_max 계산을 위한 Op 예상 소요 시간 추정.
        = process_time (setup_time은 보수적으로 0 가정 — 이미 idle 구간에 반영됨)

        더 정밀한 계산이 필요하면 job_type을 Scheduler에 전달하여
        get_setup_time()을 호출하도록 확장 가능.

        Args:
            machine: 대상 기계
            job_id : 작업 ID
            op_seq : 작업 시퀀스

        Returns:
            float: 예상 Op 소요 시간
        """
        op_id = self._op_table.loc[(job_id, op_seq), 'op_id']
        return machine.get_process_time(op_id)

    # ──────────────────────────────────────────────────────────────────
    # 분석용 메서드
    # ──────────────────────────────────────────────────────────────────

    def get_pm_summary(self) -> pd.DataFrame:
        """PM 수행 이력을 DataFrame으로 반환 (KPI 분석용)"""
        if not self._pm_log:
            return pd.DataFrame(columns=[
                'machine_id', 'decision', 'pm_start', 'pm_end'
            ])
        return pd.DataFrame(self._pm_log)
