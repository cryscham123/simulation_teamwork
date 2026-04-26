from numpy import inf
import simpy
import pandas as pd
from .machine import Machine
from utils import EventLogger
from typing import Dict
from .job import Job
import random
import os

class Scheduler:
    """시뮬레이션 환경의 스케줄러 클래스"""

    def __init__(self,
                 env: simpy.Environment,
                 data: Dict[str, pd.DataFrame],
                 event_logger: EventLogger,
                 pm_hazard_threshold: float,
                 qtime_urgency_factor: float):
        """
        Scheduler 초기화

        Args:
            env: SimPy 환경
            data: 시뮬레이션에 필요한 데이터 딕셔너리
            event_logger: 이벤트 기록 인스턴스
            pm_hazard_threshold: PM 고장 확률 임계값
            qtime_urgency_factor: QTime 긴급도 가중치
        """
        self.__env = env
        self.__qutime_urgency_factor = qtime_urgency_factor
        self.__machines = []
        self.machine_events = simpy.Store(env, capacity=float('inf'))

        # 머신 인스턴스 생성 및 스토어에 추가
        for machine_id, row in data['machines'].set_index('machine_id').iterrows():
            machine_group = row['machine_group']

            # 해당 머신의 고장 정보 가져오기
            machine_failure_df = data['machine_failure']
            failure_info = machine_failure_df[
                machine_failure_df['machine_id'] == machine_id
            ].iloc[0].to_dict()

            # 해당 머신 그룹의 셋업 시간 정보 가져오기
            setup_times_df = data['setup_times']
            setup_time_info = setup_times_df[
                setup_times_df['machine_group'] == machine_group
            ]

            # 해당 머신의 처리 시간 정보 가져오기
            op_machine_df = data['operation_machine_map']
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
                pm_hazard_threshold=pm_hazard_threshold,
                event_logger=event_logger,
                event_queue=self.machine_events
            )
            machine.down_process = env.process(machine.down())
            machine.pm_process = env.process(machine.PM())
            machine.run_process = env.process(machine.run())
            self.__machines.append(machine)
        env.process(self.__chk_machine_event())

        self.__jobs = []
        self.job_events = simpy.Store(env, capacity=float('inf'))
        for _, job_info in data['jobs'].iterrows():
            # 해당 작업의 operation 정보 가져오기
            job_operations = data['operations'].loc[
                data['operations']['job_id'] == job_info['job_id'],
            ].sort_values('op_seq')

            job = Job(
                env=env,
                job_info=job_info.to_dict(),
                op_info=job_operations,
                event_logger=event_logger,
                event_queue=self.job_events
            )
            self.__jobs.append(job)
            env.process(job.release())
        self.job_chk_process = env.process(self.__chk_job_waiting(len(self.__jobs)))

    def __chk_machine_event(self):
        """
        머신 고장 체크 프로세스
        """
        while True:
            machine = yield self.machine_events.get()
            if machine.cur_state == Machine.State.REPAIRING:
                if machine.pm_process.is_alive:
                    machine.pm_process.interrupt()
                if machine.repair_process is not None and machine.repair_process.is_alive:
                    machine.repair_process.interrupt()
            self.__env.process(self.__repair_and_reschedule_machine(machine))

    def __repair_and_reschedule_machine(self, machine: Machine):
        """
        머신 수리 프로세스

        Args:
            machine: 수리할 머신
        """
        machine.repair_process = self.__env.process(machine.repair())
        status = yield machine.repair_process
        # PM에 성공하면 머신 고장 확률 초기화
        if status == Machine.RepairStatus.SUCCESS_PM:
            machine.down_process.interrupt()
        elif status == Machine.RepairStatus.FAILED_PM:
            return
        machine.down_process = self.__env.process(machine.down())
        machine.pm_process = self.__env.process(machine.PM())

    def __chk_job_waiting(self, num_jobs: int):
        """
        작업 대기 체크 프로세스
        """
        terminated_jobs = 0
        while terminated_jobs < num_jobs:
            job = yield self.job_events.get()
            # 작업 완료 시 시뮬레이션에서 제외
            if job.cur_state == Job.State.COMPLETED:
                terminated_jobs += 1
                continue
            # 작업 대기 상태 혹은 대기, 세팅, 작업 도중 기계 고장 시 다시 매칭 시도
            self.__env.process(self.__matching_machine(job))

    def __matching_machine(self, job: Job):
        """
        작업과 매칭되는 머신을 찾아 작업 실행 프로세스 시작

        Args:
            job: 매칭할 작업
        """
        job.start_qtime_chk()
        # 이 로직은 phase1에서 처리하도록 변경 예정
        # 임시로 scheduler에서 처리되도록 구현한 상태
        target = self.__match_job_machine(job, self.__machines, os.getenv('MACHINE_CHOICE', 'random'))
        yield target.put_job(job)
        self.__env.process(job.operation_completed())

    def __match_job_machine(self, job: Job, machines: list, choice_method: str):
        """
        작업과 매칭되는 머신 선택

        Args:
            job: 매칭할 작업
            machines: 머신 리스트
            choice_method: 머신 선택 방법 (예: 'random', 'shortest')

        Returns:
            Machine: 선택된 머신
        """
        if choice_method == 'random':
            target = [x for x in self.__machines if x.group == job.get_op_group()]
            target = target[random.randint(0, len(target)-1)]
        else:
            candidates = [
                m for m in machines
                if m.group == job.get_op_group()
            ]
            op_id = job.get_current_operation()

            avg_proc = sum(m.get_process_time(op_id) for m in candidates) / len(candidates)
            urgency_threshold = avg_proc * self.__qutime_urgency_factor
            # 얘는 뭐에 쓰임?
            _is_urgent = job.get_remain_qtime() < urgency_threshold

            # 작업이 언제 시작할 지 모르기 때문에, setup time은 정확하지 않음.
            return min(candidates, key=lambda m: m.get_process_time(op_id) + 1000000000000000 * (int(not m.is_idle()) + m.queue_size()))

    def get_simulation_info(self):
        """
        임시 함수
        나중에 event log에서 모든 정보를 추출할 수 있도록 변경 예정
        """
        completed_cnt = 0
        completed_in_due_date = 0
        total_qtime_violation = 0.0
        total_waiting_time = 0.0
        for job in self.__jobs:
            print(f"Job ID: {job.id}\tQTime Violation: {round(job.total_qtime_over, 3)}\t대기 시간: {round(job.total_waiting_time, 3)}\t완료 시간: {round(job.completed_time, 3) if job.completed_time > 0.0 else '미완료'}")
            completed_cnt += int(job.completed_time > 0.0)
            completed_in_due_date = int(job.is_in_due_date())
            total_qtime_violation += job.total_qtime_over
            total_waiting_time += job.total_waiting_time
        print(f"시뮬레이션 시간: {round(self.__env.now, 3)}\n총 작업 수: {len(self.__jobs)}\n완료된 작업 수: {completed_cnt}\n기한 안에 완료된 작업 수: {completed_in_due_date}\n총 QTime 위반 시간: {round(total_qtime_violation, 3)}\n총 대기 시간: {round(total_waiting_time, 3)}")
