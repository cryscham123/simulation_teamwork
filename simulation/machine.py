import simpy
import random
import math
import pandas as pd
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
            env: SimPy 환경
            id: 머신 ID
            group: 머신 그룹
            failure_info: 고장 정보 딕셔너리
            setup_time_info: 셋업 시간 정보 DataFrame
            process_time_info: 프로세싱 시간 정보 DataFrame
            event_logger: 이벤트 기록 인스턴스
        """
        self.__env = env
        self.__id = id
        self.group = group
        self.__event_logger = event_logger
        self.__is_repairing = False

        # 가정: Machine은 한 번에 하나의 작업만 처리할 수 있다.
        self.resource = simpy.PreemptiveResource(env, capacity=1)

        # 고장 관련 파라미터
        self.__base_hazard = failure_info['base_hazard']
        self.__hazard_increase_rate = failure_info['hazard_increase_rate']
        self.__repair_time = failure_info['repair_time']
        # PM 관련 파라미터
        self.__pm_duration = failure_info['pm_duration']

        # 시간 정보
        self.__setup_times = setup_time_info
        self.__process_times = process_time_info

        # 머신 상태
        self.__last_job_type = None

        # Stage II 대비 속성
        self.__last_repair_time: float = 0.0
        self.idle_since: float = 0.0
        self.pm_pending: bool = False

        # EFT 라우팅용: 이 머신이 현재 작업을 완료할 예상 시각
        self.estimated_free_time: float = 0.0
        # Interrupt 처리를 위한 현재 서브프로세스 참조
        self.__sub_process = None

        # Stage II PM 관련
        # PM 정책 인스턴스 — Stage II 스케줄러가 외부에서 주입 (None이면 PM 없음)
        self._pm_policy = None
        # PM 진행 중 플래그 — down()이 PM 중 발생한 고장 이벤트를 무시하게 함
        self._pm_in_progress: bool = False
        # Scheduler가 관리하는 이 Machine의 down() 프로세스 참조 (고장 클럭 리셋용)
        self._down_process = None

    @property
    def id(self) -> int:
        """머신 ID 반환"""
        return self.__id

    @property
    def is_repairing(self) -> bool:
        return self.__is_repairing

    @property
    def last_repair_time(self) -> float:
        return self.__last_repair_time

    @property
    def pm_duration(self) -> float:
        return self.__pm_duration

    @property
    def repair_time(self) -> float:
        return self.__repair_time

    def cumulative_hazard(self, t_start: float, t_end: float) -> float:
        """누적 고장 기댓값 Λ(t_start, t_end) = h0*Δt + hr/2*(τ_end² - τ_start²)"""
        h0 = self.__base_hazard
        hr = self.__hazard_increase_rate
        tau_start = t_start - self.__last_repair_time
        tau_end = t_end - self.__last_repair_time
        dt = t_end - t_start
        return h0 * dt + hr / 2 * (tau_end ** 2 - tau_start ** 2)

    def needs_pm(self, current_time: float, epsilon: float) -> bool:
        """Stage II PM 판단용: Λ(last_repair_time, current_time) >= epsilon"""
        return self.cumulative_hazard(self.__last_repair_time, current_time) >= epsilon

    def __calculate_hazard(self, tau_start: float = 0.0) -> float:
        """
        현재 기계 나이(tau_start)를 고려한 고장까지 잔여 시간 샘플링.
        λ(τ) = h0 + hr*τ 의 역 CDF:
          Δt = (-(h0+hr*τ_s) + sqrt((h0+hr*τ_s)² - 2*hr*log(U))) / hr
        """
        h0 = self.__base_hazard
        hr = self.__hazard_increase_rate
        u = random.random()
        effective_h0 = h0 + hr * tau_start
        return (-effective_h0 + math.sqrt(effective_h0 ** 2 - 2 * hr * math.log(u))) / hr

    def down(self):
        """머신 중단 프로세스 — 매 호출 시 현재 기계 나이 기준으로 λ 재계산"""
        is_broken = True
        try:
            tau_start = self.__env.now - self.__last_repair_time
            yield self.__env.timeout(self.__calculate_hazard(tau_start))
            # MR 수리 중이거나 PM 진행 중이면 고장 아님
            if self.__is_repairing or self._pm_in_progress:
                is_broken = False
        except simpy.Interrupt:
            # PM 완료 후 고장 클럭 리셋을 위한 인터럽트 — 고장 아님
            is_broken = False

        return self, is_broken

    def repair(self):
        """MR 수리 프로세스 — last_repair_time 갱신 없음 (MR은 기계 나이 유지)"""
        self.__is_repairing = True
        idx = self.__event_logger.log_event_start(self.__id, 'repairing', 'machine', None)
        yield self.__env.timeout(self.__repair_time)
        self.__event_logger.log_event_finish(idx)
        # 수리시 setup 정보도 초기화
        self.__is_repairing = False
        self.__last_job_type = None

    def is_idle(self) -> bool:
        """
        머신이 가용 가능한 상태인지 확인.
        수리중인지 아닌지를 판별하는 용도로 사용
        """
        return self.resource.count < self.resource.capacity

    def get_setup_time(self, job_type: str) -> float:
        """
        주어진 작업 타입에 대한 셋업 시간 반환

        Args:
            job_type: 작업 타입

        Returns:
            float: 셋업 시간
        """
        # 이전 작업이 없을 경우 setup time은 없다고 가정
        if self.__last_job_type is None or job_type == self.__last_job_type:
            return 0

        setup_time_row = self.__setup_times[
            (self.__setup_times['from_job_type'] == self.__last_job_type) &
            (self.__setup_times['to_job_type'] == job_type)
        ]
        return setup_time_row['setup_time'].iloc[0]

    def get_process_time(self, op_id: int) -> float:
        """
        주어진 작업 ID에 대한 처리 시간 반환

        Args:
            op_id: 작업 ID

        Returns:
            float: 처리 시간
        """
        process_time_row = self.__process_times[
            self.__process_times['op_id'] == op_id
        ]
        return process_time_row['process_time'].iloc[0]

    def setup(self, job_type: str, op_id: int, job_id: int):
        """
        머신 셋업 프로세스

        Args:
            job_type: 작업 타입
        """
        idx = -1
        try:
            idx = self.__event_logger.log_event_start(self.__id, 'setup', 'machine', 'job: {job_id}\noperation: {op_id}')
            yield self.__env.timeout(self.get_setup_time(job_type))
            self.__event_logger.log_event_finish(idx)
            self.__last_job_type = job_type
        except simpy.Interrupt:
            self.__event_logger.log_event_finish(idx)

    def work(self, op_id: int, job_id: int):
        """
        작업 처리 프로세스

        Args:
            op_id: 작업 ID
        """
        idx = -1
        try:
            idx = self.__event_logger.log_event_start(self.__id, 'working', 'machine', f'job: {job_id}\noperation: {op_id}')
            yield self.__env.timeout(self.get_process_time(op_id))
            self.__event_logger.log_event_finish(idx)
        except simpy.Interrupt:
            self.__event_logger.log_event_finish(idx)

    def _do_pm(self, scheduler):
        """
        예방 정비(PM) 수행 프로세스.
        - 고장 클럭(_down_process) 일시 억제 후 PM 실행
        - PM 완료 시 last_repair_time 갱신 (기계 나이 리셋)
        - _down_process를 인터럽트해 _chk_machine_broken이 새로운 고장 클럭으로 재시작하도록 유도
        """
        
        self._pm_in_progress = True
        idx = self.__event_logger.log_event_start(self.__id, 'pm', 'machine', None)
        yield self.__env.timeout(self.__pm_duration)
        self.__event_logger.log_event_finish(idx)

        # 기계 나이 리셋 (MR과 달리 PM은 last_repair_time 갱신)
        self.__last_repair_time = self.__env.now
        self.__last_job_type = None
        self._pm_in_progress = False

        # 기존 고장 클럭 인터럽트 → _chk_machine_broken이 새 클럭을 fresh한 나이로 재시작
        if self._down_process is not None and self._down_process.is_alive:
            self._down_process.interrupt()

    def run(self, scheduler):
        """
        Machine Pull 디스패칭 루프.
        유휴 상태가 되면 자신의 Queue에서 scheduler.request_job()으로 Job을 가져와 처리.

        Stage II: _pm_policy가 설정된 경우 PM 타이밍을 동적으로 결정.
          - ADVANCE_PM_FREE / ADVANCE_PM : Job 처리 전 PM 수행
          - POSTPONE_PM                  : Job 처리 후 PM 수행
          - FORCED_POSTPONE_PM           : Job만 처리 (PM 생략 — QTime 보호)
          - NO_PM                        : PM 없이 바로 처리

        고장(Interrupt) 발생 시 job_ctx의 done_event로 상태('failed'/'requeue')를 전달.
        """
        while True:
            job_ctx = scheduler.request_job(self.__id)
            if job_ctx is None:
                # Queue가 비었으면 신규 Job 도착 이벤트를 기다림
                yield scheduler.get_job_event(self.__id)
                continue

            done_event = job_ctx['done_event']
            op_id: int = job_ctx['op_id']
            job_type: str = job_ctx['job_type']
            job_id: int = job_ctx['job_id']
            priority: int = job_ctx.get('priority', 0)
            is_in_work = False

            # Job의 대기(waiting) 로그 종료 콜백 호출
            if 'end_wait_fn' in job_ctx:
                job_ctx['end_wait_fn']()

            # ── PM 결정 (Stage II) ──────────────────────────────────────────
            # resource.count > 0 이면 repair 프로세스가 자원 점유 중
            # → repair 중 PM 중복 실행 방지
            pm_decision = 'NO_PM'
            if self._pm_policy is not None and self.resource.count == 0:
                pm_decision = self._pm_policy.decide(self, job_ctx, scheduler)

            # PM 포함 여부를 EFT 예상 완료 시각에 반영
            pm_extra = (
                self.__pm_duration
                if pm_decision in ('ADVANCE_PM_FREE', 'ADVANCE_PM', 'POSTPONE_PM')
                else 0.0
            )
            self.estimated_free_time = (
                self.__env.now
                + pm_extra
                + self.get_setup_time(job_type)
                + self.get_process_time(op_id)
            )

            # ── ADVANCE: Job 처리 전 PM ──────────────────────────────────────
            if pm_decision in ('ADVANCE_PM_FREE', 'ADVANCE_PM'):
                yield self.__env.process(self._do_pm(scheduler))

            # ── Job 처리 (Setup → Work) ──────────────────────────────────────
            j_setup_idx = -1
            j_work_idx = -1
            job_succeeded = False

            try:
                with self.resource.request(priority=priority, preempt=False) as req:
                    yield req

                    # Setup
                    j_setup_idx = self.__event_logger.log_event_start(
                        job_id, 'setup', 'job',
                        f'machine: {self.__id}\noperation: {op_id}'
                    )
                    self.__sub_process = self.__env.process(
                        self.setup(job_type, op_id, job_id)
                    )
                    yield self.__sub_process
                    self.__event_logger.log_event_finish(j_setup_idx)

                    # Setup 완료 → QTime 모니터 중단
                    if 'qtime_interrupt_fn' in job_ctx:
                        job_ctx['qtime_interrupt_fn']()

                    # Work
                    is_in_work = True
                    j_work_idx = self.__event_logger.log_event_start(
                        job_id, 'working', 'job',
                        f'machine: {self.__id}\noperation: {op_id}'
                    )
                    self.__sub_process = self.__env.process(
                        self.work(op_id, job_id)
                    )
                    yield self.__sub_process
                    self.__event_logger.log_event_finish(j_work_idx)
                    self.__sub_process = None

                    done_event.succeed({'status': 'done'})
                    job_succeeded = True

            except simpy.Interrupt:
                # Machine 고장으로 인한 선점 인터럽트
                if self.__sub_process is not None:
                    self.__sub_process.interrupt()
                    self.__sub_process = None
                if not is_in_work:
                    self.__event_logger.log_event_finish(j_setup_idx)
                else:
                    self.__event_logger.log_event_finish(j_work_idx)
                if not done_event.triggered:
                    done_event.succeed({'status': 'failed' if is_in_work else 'requeue'})

            # ── POSTPONE: Job 처리 후 PM (성공한 경우 + repair 미점유 시만) ──
            if pm_decision == 'POSTPONE_PM' and job_succeeded and self.resource.count == 0:
                yield self.__env.process(self._do_pm(scheduler))

            self.estimated_free_time = self.__env.now
            self.idle_since = self.__env.now

