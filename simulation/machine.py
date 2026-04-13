import simpy
import random
import pandas as pd
import math
from typing import Dict, Any
from utils import EventLogger

class Machine:
    def __init__(self, env: simpy.Environment, id: int, group: str,
                 failure_info: Dict[str, Any], setup_time_info: pd.DataFrame,
                 process_time_info: pd.DataFrame,
                 event_logger: EventLogger):
        """
        Machine 초기화

        Args:
            env              : SimPy 환경
            id               : 머신 ID
            group            : 머신 그룹
            failure_info     : 고장 정보 딕셔너리
            setup_time_info  : 셋업 시간 정보 DataFrame
            process_time_info: 프로세싱 시간 정보 DataFrame
            event_logger     : 이벤트 기록 인스턴스
        """
        self.__env          = env
        self.__id           = id
        self.group          = group
        self.__event_logger = event_logger
        self.__is_repairing = False

        # 가정: Machine은 한 번에 하나의 작업만 처리할 수 있다.
        self.resource = simpy.PreemptiveResource(env, capacity=1)

        # 고장 관련 파라미터
        self.__base_hazard          = failure_info['base_hazard']
        self.__hazard_increase_rate = failure_info['hazard_increase_rate']
        self.__repair_time          = failure_info['repair_time']
        self.__pm_duration          = failure_info['pm_duration']

        # 시간 정보
        self.__setup_times   = setup_time_info
        self.__process_times = process_time_info

        # 머신 상태
        self.__is_repaired      = True
        self.__last_repair_time = 0.0     # 마지막 PM/MR 완료 시점
        self.__last_job_type    = None

        # PM 관련 신규 상태
        # Delay 결정 시: Job 완료 후 PM을 수행해야 함을 표시하는 플래그
        self.pm_pending  = False
        # 기계가 유휴 상태가 된 시점 (ΔC_max(advance) 계산 시 idle time 산출용)
        self.idle_since  = 0.0

        # 고장 프로세스 시작
        env.process(self.__breakdown())

    # ──────────────────────────────────────────────────────────────────────
    # Properties (public 접근자)
    # ──────────────────────────────────────────────────────────────────────

    @property
    def id(self) -> int:
        """머신 ID 반환"""
        return self.__id

    @property
    def last_repair_time(self) -> float:
        """마지막 PM/MR 완료 시점 (고장률 시계 기준점)"""
        return self.__last_repair_time

    @property
    def pm_duration(self) -> float:
        """PM 소요 시간"""
        return self.__pm_duration

    @property
    def repair_time(self) -> float:
        """돌발 고장 수리(MR) 소요 시간"""
        return self.__repair_time

    # ──────────────────────────────────────────────────────────────────────
    # 고장률 / PM 판단 메서드
    # ──────────────────────────────────────────────────────────────────────

    def hazard_at(self, sim_time: float) -> float:
        """
        sim_time 시점의 순간 고장률 λ(sim_time)
        τ = sim_time - last_repair_time (정비 후 경과 시간)
        λ(τ) = base_hazard + hazard_increase_rate * τ
        """
        tau = max(sim_time - self.__last_repair_time, 0.0)
        return self.__base_hazard + self.__hazard_increase_rate * tau

    def cumulative_hazard(self, t_start: float, t_end: float) -> float:
        """
        누적 고장 기댓값 Λ(t_start, t_end) 계산
        = ∫_{t_start}^{t_end} (h0 + hr * (s - last_repair_time)) ds
        = h0*(Δt) + hr/2 * (τ_end² - τ_start²)

        Args:
            t_start: 구간 시작 시뮬레이션 시간
            t_end  : 구간 종료 시뮬레이션 시간

        Returns:
            float: 해당 구간의 기대 고장 횟수
        """
        if t_end <= t_start:
            return 0.0
        L         = self.__last_repair_time
        tau_start = max(t_start - L, 0.0)
        tau_end   = max(t_end   - L, 0.0)
        result = (self.__base_hazard * (tau_end - tau_start)
                  + 0.5 * self.__hazard_increase_rate * (tau_end**2 - tau_start**2))
        return max(result, 0.0)

    def needs_pm(self, current_time: float, epsilon: float) -> bool:
        """
        last_repair_time 이후 current_time까지 누적 고장 기댓값 ≥ ε 이면 True

        Args:
            current_time: 현재 시뮬레이션 시간
            epsilon     : PM 판단 임계치

        Returns:
            bool: PM 필요 여부
        """
        accumulated = self.cumulative_hazard(self.__last_repair_time, current_time)
        return accumulated >= epsilon

    # ──────────────────────────────────────────────────────────────────────
    # PM SimPy 프로세스
    # ──────────────────────────────────────────────────────────────────────

    def perform_pm(self):
        """
        PM(예방정비) 실행 SimPy 제너레이터.
        완료 후 last_repair_time을 현재 시각으로 갱신 (고장률 시계 리셋).
        PM은 "as good as new" 복원 — 논문 가정 3 준수.
        """
        pm_start = self.__env.now
        print(f'{round(pm_start, 2)}\t'
              f'Machine {self.__id} PM started (duration={self.__pm_duration})')

        pm_idx = self.__event_logger.log_event_start(
            id=self.__id, event='pm', resource='machine', description='PM'
        )
        yield self.__env.timeout(self.__pm_duration)
        self.__event_logger.log_event_finish(pm_idx)

        # ── 핵심: last_repair_time 갱신 → 누적 고장 기댓값 리셋 ──────────
        self.__last_repair_time = self.__env.now
        self.__last_job_type    = None      # 셋업 정보도 초기화
        self.idle_since         = self.__env.now
        self.pm_pending         = False

        print(f'{round(self.__env.now, 2)}\t'
              f'Machine {self.__id} PM finished '
              f'(last_repair_time → {self.__last_repair_time:.2f})')

    # ──────────────────────────────────────────────────────────────────────
    # 기존 내부 프로세스 (버그 수정)
    # ──────────────────────────────────────────────────────────────────────

    def __breakdown(self):
        """
        머신 돌발 고장 프로세스.
        [수정] 매 루프 시작 시점의 env.now를 기준으로 λ를 재계산하여
               시간이 지날수록 고장률이 올라가는 동작을 올바르게 구현.
        """
        while True:
            # ── 매 루프마다 현재 시점 기준 λ 재계산 ─────────────────────
            current_time = self.__env.now
            lam = self.hazard_at(current_time)  # λ(now)

            # 지수분포로 다음 고장까지 대기 (λ는 순간값 — 근사)
            time_to_failure = random.expovariate(lam)
            yield self.__env.timeout(time_to_failure)

            # 아직 수리 중이면 이번 고장 이벤트 스킵
            if not self.__is_repaired:
                continue

            self.__is_repaired = False
            with self.resource.request(priority=-1, preempt=True) as req:
                yield req
                print(f'{round(self.__env.now, 2)}\t'
                      f'Machine {self.__id} broke down')
                repair_idx = self.__event_logger.log_event_start(
                    id=self.__id, event='repairing', resource='machine', description='MR'
                )
                yield self.__env.process(self.__repair())
                self.__event_logger.log_event_finish(repair_idx)
            self.__is_repaired = True

    def __repair(self):
        """
        돌발 고장 수리(MR) 프로세스.
        [논문 가정] MR은 고장률 특성을 변경하지 않으므로
                   last_repair_time을 갱신하지 않음.
                   (PM만 "as good as new" 복원)
        """
        yield self.__env.timeout(self.__repair_time)
        # MR 후 셋업 정보는 초기화
        self.__last_job_type = None
        print(f'{round(self.__env.now, 2)}\t'
              f'Machine {self.__id} repaired (MR — hazard rate unchanged)')

    # ──────────────────────────────────────────────────────────────────────
    # 기존 공개 메서드 (변경 없음)
    # ──────────────────────────────────────────────────────────────────────

    def is_idle(self) -> bool:
        """머신이 가용 가능한 상태인지 확인"""
        return self.resource.count < self.resource.capacity

    def get_setup_time(self, job_type: str) -> float:
        if self.__last_job_type is None or job_type == self.__last_job_type:
            return 0
        setup_time_row = self.__setup_times[
            (self.__setup_times['from_job_type'] == self.__last_job_type) &
            (self.__setup_times['to_job_type'] == job_type)
        ]
        return setup_time_row['setup_time'].iloc[0]

    def get_process_time(self, op_id: int) -> float:
        process_time_row = self.__process_times[
            self.__process_times['op_id'] == op_id
        ]
        return process_time_row['process_time'].iloc[0]

    def setup(self, job_type: str, op_id: int = None, job_id: int = None):
        yield self.__env.timeout(self.get_setup_time(job_type))
        self.__last_job_type = job_type

    def work(self, op_id: int, job_id: int = None):
        yield self.__env.timeout(self.get_process_time(op_id))
