from numpy import inf
import simpy
import pandas as pd
from .machine import Machine
from utils import EventLogger
from algorithms import Algorithm
from typing import Dict
from .job import Job

class Scheduler:
    """시뮬레이션 환경의 스케줄러 클래스"""

    def __init__(self, env: simpy.Environment, data: Dict[str, pd.DataFrame], event_logger: EventLogger, algorithm: Algorithm = None):
        """
        Scheduler 초기화

        Args:
            env: SimPy 환경
            data: 시뮬레이션에 필요한 데이터 딕셔너리
            event_logger: 이벤트 기록 인스턴스
            algorithm: 작업과 머신 매칭 알고리즘 (기본값: None)
        """
        self.__algorithm = algorithm
        self.__env = env
        # 머신 그룹별로 FilterStore 생성
        self.__machine_store = simpy.FilterStore(env, capacity=float('inf'))
        self.__machine_events = []

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
                event_logger=event_logger
            )
            machine.down_process = env.process(machine.down())
            machine.pm_process = env.process(machine.PM(self.__algorithm.calculate_PM_time(machine) if self.__algorithm else inf))
            self.__machine_events += [machine.down_process, machine.pm_process]
            self.__machine_store.put(machine)
        env.process(self.__chk_machine_event())

        self.__jobs = []
        self.__chk_job_waiting_events = []
        for _, job_info in data['jobs'].iterrows():
            # 해당 작업의 operation 정보 가져오기
            job_operations = data['operations'].loc[
                data['operations']['job_id'] == job_info['job_id'],
            ].sort_values('op_seq')

            job = Job(
                env=env,
                job_info=job_info.to_dict(),
                op_info=job_operations,
                event_logger=event_logger
            )
            self.__jobs.append(job)
            self.__chk_job_waiting_events.append(env.process(job.run()))
        self.job_chk_process = env.process(self.__chk_job_waiting(len(self.__chk_job_waiting_events)))

    def __chk_machine_event(self):
        """
        머신 고장 체크 프로세스
        """
        while True:
            events = yield self.__env.any_of(self.__machine_events)
            for event in events:
                if event.name == '__machine_repair':
                    self.__machine_events.remove(event)
                    continue
                machine = event.value
                self.__machine_events.remove(machine.down_process)
                self.__machine_events.remove(machine.pm_process)
                if machine.cur_state == Machine.State.REPAIRING:
                    machine.pm_process.interrupt()
                else:
                    machine.down_process.interrupt()
                self.__machine_events.append(self.__env.process(self.__machine_repair(machine)))

    def __machine_repair(self, machine: Machine):
        """
        머신 수리 프로세스

        Args:
            machine: 수리할 머신
        """
        with machine.resource.request(priority=-1, preempt=True) as req:
            yield req
            yield self.__machine_store.get(lambda x: x.id == machine.id)
            yield self.__env.process(machine.repair())
        machine.down_process = self.__env.process(machine.down())
        machine.pm_process = self.__env.process(machine.PM(self.__algorithm.calculate_PM_time(machine) if self.__algorithm else inf))
        self.__machine_events += [machine.down_process, machine.pm_process]
        self.__machine_store.put(machine)

    def __chk_job_waiting(self, num_jobs: int):
        """
        작업 대기 체크 프로세스
        """
        terminated_jobs = 0
        while terminated_jobs < num_jobs:
            waiting_jobs = yield self.__env.any_of(self.__chk_job_waiting_events)
            for event in waiting_jobs:
                if event.name == '__matching_machine':
                    self.__chk_job_waiting_events.remove(event)
                    continue
                job, status = event.value
                self.__chk_job_waiting_events.remove(event)
                # 작업 완료 시 시뮬레이션에서 제외
                if status == Job.State.COMPLETED:
                    terminated_jobs += 1
                    continue
                # 작업 대기 상태 혹은 세팅 도중 기계 고장 시 다시 매칭 시도
                self.__chk_job_waiting_events.append(self.__env.process(self.__matching_machine(job)))

    def __matching_machine(self, job: Job):
        """
        작업과 매칭되는 머신을 찾아 작업 실행 프로세스 시작

        Args:
            job: 매칭할 작업
        """
        qtime_process = self.__env.process(job.chk_qtime())
        # 이 로직은 phase1에서 처리하도록 변경 예정
        # 임시로 scheduler에서 처리되도록 구현한 상태
        if self.__algorithm is None:
            target = yield self.__machine_store.get(lambda x: x.group == job.get_op_group() and x.is_idle())
        else:
            target = yield self.__env.process(self.__algorithm.match_job_machine(job, self.__machine_store))
        process = self.__env.process(job.run(target, qtime_process))
        self.__chk_job_waiting_events.append(process)
        yield process
        self.__machine_store.put(target)

    def get_simulation_info(self):
        """
        임시 함수
        나중에 event log에서 모든 정보를 추출할 수 있도록 변경 예정
        """
        completed_cnt = 0
        completed_in_due_date = 0
        total_qtime_violation = 0.0
        for job in self.__jobs:
            print(f"Job ID: {job.id}\tQTime Violation: {round(job.total_qtime_over, 3)}\t완료 시간: {round(job.completed_time, 3) if job.completed_time > 0.0 else '미완료'}")
            completed_cnt += int(job.completed_time > 0.0)
            completed_in_due_date = int(job.is_in_due_date())
            total_qtime_violation += job.total_qtime_over
        print(f"시뮬레이션 시간: {round(self.__env.now, 3)}\n총 작업 수: {len(self.__jobs)}\n완료된 작업 수: {completed_cnt}\n기한 안에 완료된 작업 수: {completed_in_due_date}\n총 QTime 위반 시간: {round(total_qtime_violation, 3)}")
