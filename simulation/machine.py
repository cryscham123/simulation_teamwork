import simpy
from math import inf
import random
import math
import pandas as pd
from typing import Dict, Any
from utils import EventLogger
from enum import Enum
from .job import Job
import os

class Machine:
    class State(Enum):
        IDLE = 0
        SETUP = 1
        WORKING = 2
        REPAIRING = 3
        PM = 4

    class RepairStatus(Enum):
        SUCCESS_PM = 0
        FAILED_PM = 1

    def __init__(self, env: simpy.Environment, id: int, group: str,
                 failure_info: Dict[str, Any], setup_time_info: pd.DataFrame,
                 process_time_info: pd.DataFrame, pm_hazard_threshold: float,
                 event_logger: EventLogger, event_queue: simpy.Store):
        """
        Machine 초기화

        Args:
            env: SimPy 환경
            id: 머신 ID
            group: 머신 그룹
            failure_info: 고장 정보 딕셔너리
            setup_time_info: 셋업 시간 정보 DataFrame
            process_time_info: 프로세싱 시간 정보 DataFrame
            pm_hazard_threshold: PM 고장 확률 임계값
            event_logger: 이벤트 기록 인스턴스
            event_queue: 머신 이벤트를 기록할 queue
        """
        self.__env = env
        self.__id = id
        self.group = group
        self.__event_logger = event_logger
        self.__event_queue = event_queue

        self.__resource = simpy.PreemptiveResource(env, capacity=1)
        self.__queue = simpy.FilterStore(env, capacity=float('inf'))

        # 고장 관련 파라미터
        self.__base_hazard = failure_info['base_hazard']
        self.__hazard_increase_rate = failure_info['hazard_increase_rate']
        self.__repair_time = failure_info['repair_time']
        self.__pm_duration = failure_info['pm_duration']
        self.__pm_hazard_threshold = pm_hazard_threshold

        # 시간 정보
        self.__setup_times = setup_time_info
        self.__process_times = process_time_info

        # 머신 상태
        self.__last_job_type = None
        self.__event_idx = -1
        self.__repair_idx = -1
        self.__PM_idx = -1
        self.cur_state = Machine.State.IDLE
        self.down_process = None
        self.pm_process = None
        self.run_process = None
        self.repair_process = None

    def __del__(self):
        self.__event_logger.log_event_finish(self.__event_idx)
        self.__event_logger.log_event_finish(self.__PM_idx)
        self.__event_logger.log_event_finish(self.__repair_idx)

    @property
    def id(self) -> int:
        """머신 ID 반환"""
        return self.__id

    @property
    def queue(self) -> simpy.FilterStore:
        """머신 대기열 반환"""
        return self.__queue

    def put_job(self, job: Job):
        """작업을 머신의 대기열에 추가. 발음에 주의하자."""
        return self.__queue.put(job)

    def queue_size(self):
        return len(self.__queue.items)

    def __calculate_hazard(self):
        """
        기존 base.py에서 처리하던걸 다시 machine으로 이관.
        csv 값을 통해 원하는 동작 처리 가능
        """
        if os.getenv('DOWN_ACTIVE', 'True').lower() == 'false':
            return inf
        h0 = self.__base_hazard
        hr = self.__hazard_increase_rate
        u = random.random()

        if hr > 0:
            return (-h0 + math.sqrt((h0 ** 2) - 2 * hr * math.log(u))) / hr
        if h0 > 0:
            return -math.log(u) / h0
        return inf

    def down(self):
        """머신 중단 프로세스"""
        try:
            yield self.__env.timeout(self.__calculate_hazard())
            if self.cur_state in [Machine.State.PM, Machine.State.REPAIRING]:
                return
        except simpy.Interrupt:
            # 예방 보전 성공으로 인한 인터럽트 발생
            return
        self.__event_queue.put((self, Machine.State.REPAIRING))

    def __calculate_PM_time(self):
        if os.getenv('PM_ACTIVE', 'True').lower() == 'false':
            return inf
        h0 = self.__base_hazard
        hr = self.__hazard_increase_rate
        thr = self.__pm_hazard_threshold
        if hr > 0:
            t_star = (-h0 + math.sqrt((h0 ** 2) + 2.0 * hr * thr)) / hr
        elif h0 > 0:
            t_star = thr / h0
        else:
            t_star = inf
        return t_star

    def PM(self):
        """예방 보전 프로세스"""
        try:
            yield self.__env.timeout(self.__calculate_PM_time())
            if self.cur_state in [Machine.State.PM, Machine.State.REPAIRING]:
                return
        except simpy.Interrupt:
            # 머신 고장으로 인한 인터럽트 발생
            return
        self.__event_queue.put((self, Machine.State.PM))

    def repair(self, required: State):
        """
        머신 수리 프로세스
        나중에 리팩토링 예정.
        """
        preempt, reason, time = (True, 'repairing', self.__repair_time) if required == Machine.State.REPAIRING else (False, 'PM', self.__pm_duration)
        try:
            with self.__resource.request(priority=-1, preempt=preempt) as req:
                yield req
                self.__last_job_type = None
                if reason == 'PM':
                    self.cur_state = Machine.State.PM
                    self.__PM_idx = self.__event_logger.log_event_start(self.__id, reason, 'machine', None)
                else:
                    self.cur_state = Machine.State.REPAIRING
                    self.__repair_idx = self.__event_logger.log_event_start(self.__id, reason, 'machine', None)
                yield self.__env.timeout(time)
                # 수리시 setup 정보도 초기화
                self.cur_state = Machine.State.IDLE
                ret = self.RepairStatus.SUCCESS_PM if reason == 'PM' else None
        except simpy.Interrupt:
            ret = self.RepairStatus.FAILED_PM if reason == 'PM' else None
        if reason == 'PM':
            self.__event_logger.log_event_finish(self.__PM_idx)
            self.__PM_idx = -1
        else:
            self.__event_logger.log_event_finish(self.__repair_idx)
            self.__repair_idx = -1
        return ret

    def is_idle(self) -> bool:
        """
        머신이 가용 가능한 상태인지 확인.
        수리중인지 아닌지를 판별하는 용도로 사용
        """
        return self.__resource.count < self.__resource.capacity

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

    def run(self):
        """
        머신의 메인 프로세스

        Args:
            criteria: 작업 선택 기준
        """
        while True:
            is_completed = False
            job = None
            try:
                # machine이 job을 고르지 않음. FIFO로 처리됨.
                job = yield self.__queue.get()
                with self.__resource.request(priority=0, preempt=False) as req:
                    yield req
                    job.waiting_end()
                    op_id = job.get_current_operation()

                    self.cur_state = Machine.State.SETUP
                    job.set_state(Job.State.SETUP)
                    self.__event_idx = self.__event_logger.log_event_start(self.__id, 'setup', 'machine', f'job: {job.id}\noperation: {op_id}')
                    yield self.__env.timeout(self.get_setup_time(job.job_type))
                    self.__last_job_type = job.job_type
                    self.__event_logger.log_event_finish(self.__event_idx)

                    job.interrupt_qtime()

                    self.cur_state = Machine.State.WORKING
                    job.set_state(Job.State.WORKING)
                    self.__event_idx = self.__event_logger.log_event_start(self.__id, 'working', 'machine', f'job: {job.id}\noperation: {op_id}')
                    yield self.__env.timeout(self.get_process_time(op_id))

                    self.cur_state = Machine.State.IDLE
                    is_completed = True
            except simpy.Interrupt:
                items = self.__queue.items.copy()
                self.__queue.items.clear()
                for item in items:
                    item.operation_end_signal.put(False)
            self.__event_logger.log_event_finish(self.__event_idx)
            if job is not None:
                job.operation_end_signal.put(is_completed)
            self.__event_idx = -1
