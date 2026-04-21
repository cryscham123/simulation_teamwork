import simpy
import pandas as pd
from typing import Dict, List, Optional
from .machine import Machine
from utils import EventLogger


class Scheduler:
    """
    양방향 큐(Queue) 기반 스케줄러.
    - Push: route_job_to_machine → EFT 룰로 Machine 선택 → transport_job으로 Queue에 삽입
    - Pull: Machine.run() → request_job 호출 → Queue에서 최적 Job 선택(기본: FIFO)
    """

    def __init__(self, env: simpy.Environment, machine_df: pd.DataFrame,
                 operations_df: pd.DataFrame, machine_failure_df: pd.DataFrame,
                 setup_times_df: pd.DataFrame, op_machine_df: pd.DataFrame,
                 event_logger: EventLogger, enable_failures: bool = True):
        self._env = env
        self._broken_chk_events = []
        self._op_machine_df = op_machine_df

        # op 완료 시각 추적 — (job_id, op_seq) -> finish_time
        self._op_finish_times: Dict = {}

        # Machine 인스턴스 저장소
        self._machines: Dict[int, Machine] = {}
        # Machine별 Job 대기열 (job_context dict 리스트)
        self._machine_queues: Dict[int, list] = {}
        # Machine별 "신규 Job 도착" 알림 이벤트
        self._machine_job_events: Dict[int, simpy.Event] = {}
        # op_group → [Machine, ...] 역인덱스 (EFT 탐색 효율화)
        self._group_to_machines: Dict[str, List[Machine]] = {}

        for machine_id, row in machine_df.set_index('machine_id').iterrows():
            machine_group = row['machine_group']

            failure_info = machine_failure_df[
                machine_failure_df['machine_id'] == machine_id
            ].iloc[0].to_dict()

            setup_time_info = setup_times_df[
                setup_times_df['machine_group'] == machine_group
            ]

            process_time_info = op_machine_df[
                op_machine_df['machine_id'] == machine_id
            ]

            machine = Machine(
                env=env,
                id=machine_id,
                group=machine_group,
                failure_info=failure_info,
                setup_time_info=setup_time_info,
                process_time_info=process_time_info,
                event_logger=event_logger
            )

            self._machines[machine_id] = machine
            self._machine_queues[machine_id] = []
            self._machine_job_events[machine_id] = env.event()
            self._group_to_machines.setdefault(machine_group, []).append(machine)

            # Machine Pull 루프 시작
            env.process(machine.run(self))

            if enable_failures:
                down_proc = env.process(machine.down())
                self._broken_chk_events.append(down_proc)
                machine._down_process = down_proc  # PM 완료 후 고장 클럭 리셋에 사용

        if enable_failures:
            env.process(self._chk_machine_broken())

        self._op_table = operations_df.sort_values(
            ['job_id', 'op_seq']
        ).set_index(['job_id', 'op_seq'])

    # -------------------------------------------------------------------------
    # Machine 고장/수리 관련 (기존 로직 유지)
    # -------------------------------------------------------------------------

    def _chk_machine_broken(self):
        while True:
            broken_machines = yield self._env.any_of(self._broken_chk_events)
            for event in broken_machines:
                machine, is_broken = event.value
                self._broken_chk_events.remove(event)
                if is_broken:
                    self._env.process(self._machine_repair(machine))
                # 고장 클럭 재시작 (PM 완료 후 fresh한 기계 나이로 재계산됨)
                new_down = self._env.process(machine.down())
                self._broken_chk_events.append(new_down)
                machine._down_process = new_down

    def _machine_repair(self, machine: Machine):
        """
        MR 수리 프로세스.
        priority=-1 선점으로 현재 처리 중인 Job을 중단시키고 수리.
        수리 완료 후 Machine.run() 루프가 자연스럽게 재개됨.
        """
        with machine.resource.request(priority=-1, preempt=True) as req:
            yield req
            yield self._env.process(machine.repair())

    # -------------------------------------------------------------------------
    # Push 로직: Job → Machine Queue
    # -------------------------------------------------------------------------

    def route_job_to_machine(self, job_context: dict):
        """
        EFT(Earliest Finish Time) 룰로 최적 Machine을 선택한 뒤
        transport_job 프로세스를 통해 해당 Machine의 Queue에 삽입.

        job_context 필수 키: job_id, op_seq, op_id, job_type,
                             due_date, priority, done_event
        """
        job_id = job_context['job_id']
        op_seq = job_context['op_seq']
        op_id = job_context['op_id']

        # op_group 조회 및 전파
        op_group = self._op_table.loc[(job_id, op_seq), 'op_group']
        job_context['op_group'] = op_group

        # QTime 계산에 필요한 이전 op 완료 시각 설정 (없으면 현재 시각)
        if 'prev_op_finish' not in job_context:
            job_context['prev_op_finish'] = self._op_finish_times.get(
                (job_id, op_seq - 1), self._env.now
            )

        # EFT 기준 최적 Machine 선택 (수리 중 Machine 제외, 없으면 전체 허용)
        candidates = self._group_to_machines.get(op_group, [])
        available = [m for m in candidates if not m.is_repairing] or candidates

        best_machine = min(
            available,
            key=lambda m: self._estimate_finish_time(m, op_id)
        )
        job_context['assigned_machine_id'] = best_machine.id

        yield self._env.process(self.transport_job(job_context, best_machine))

    def transport_job(self, job_context: dict, target_machine: Machine):
        """
        OHT 물류 플레이스홀더.
        현재는 즉시(timeout=0) 이동하며, 추후 이 메서드만 수정해 AGV/OHT 로직 추가 가능.
        """
        yield self._env.timeout(0)  # OHT 이동 시간 (현재 0, 향후 확장)
        self._machine_queues[target_machine.id].append(job_context)
        # Machine이 대기 중이라면 이벤트로 깨움
        evt = self._machine_job_events[target_machine.id]
        if not evt.triggered:
            evt.succeed()

    # -------------------------------------------------------------------------
    # Pull 로직: Machine → Job 선택
    # -------------------------------------------------------------------------

    def request_job(self, machine_id: int) -> Optional[dict]:
        """
        Machine이 유휴 상태가 될 때 호출.
        Queue가 비었으면 이벤트를 리셋하고 None 반환.
        기본 구현: FIFO (서브클래스에서 알고리즘 기반 우선순위 룰로 오버라이드).
        """
        queue = self._machine_queues[machine_id]
        if not queue:
            # 이벤트 리셋 — 다음 Job 도착 시 transport_job이 다시 트리거
            self._machine_job_events[machine_id] = self._env.event()
            return None
        return queue.pop(0)  # 기본: FIFO

    def get_job_event(self, machine_id: int) -> simpy.Event:
        """Machine이 빈 Queue 대기 시 yield할 이벤트 반환."""
        return self._machine_job_events[machine_id]

    # -------------------------------------------------------------------------
    # 공통 유틸리티
    # -------------------------------------------------------------------------

    def _estimate_finish_time(self, machine: Machine, op_id: int) -> float:
        """EFT 추정: 현재 예상 완료 시각 + Queue 잔여 처리 시간 + 신규 Job 처리 시간."""
        pt = machine.get_process_time(op_id)
        free_time = max(machine.estimated_free_time, self._env.now)
        queue_load = sum(
            machine.get_process_time(ctx['op_id'])
            for ctx in self._machine_queues[machine.id]
        )
        return free_time + queue_load + pt

    def notify_op_finish(self, job_id: int, op_seq: int):
        """Job이 op 완료 직후 호출 — QTime 잔여 계산에 사용."""
        self._op_finish_times[(job_id, op_seq)] = self._env.now
