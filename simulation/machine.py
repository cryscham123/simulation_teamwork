import simpy
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
                 process_time_info: pd.DataFrame,
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
            event_logger: 이벤트 기록 인스턴스
            event_queue: 머신 이벤트를 기록할 queue
        """
        self.__env = env
        self.__id = id
        self.group = group
        self.__event_logger = event_logger
        self.__event_queue = event_queue

        self.__resource = simpy.PreemptiveResource(env, capacity=1)
        # 대기열은 무제한으로 가정.
        self.__queue = simpy.FilterStore(env, capacity=float(os.getenv('MACHINE_QUEUE_CAPACITY', 'inf')))

        # 고장 관련 파라미터
        self.__base_hazard = failure_info['base_hazard']
        self.__hazard_increase_rate = failure_info['hazard_increase_rate']
        self.__repair_time = failure_info['repair_time']
        self.__pm_duration = failure_info['pm_duration']

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

    def calculate_hazard(self):
        h0 = self.__base_hazard
        hr = self.__hazard_increase_rate
        u = random.random()

        return (-h0 + math.sqrt(h0**2 - 2*hr*math.log(u))) / hr

    def down(self, time_to_fail: float):
        """머신 중단 프로세스"""
        try:
            yield self.__env.timeout(time_to_fail)
            if self.cur_state == Machine.State.REPAIRING:
                return
            self.cur_state = Machine.State.REPAIRING
        except simpy.Interrupt:
            # 예방 보전 성공으로 인한 인터럽트 발생
            return
        self.__event_queue.put(self)

    def PM(self, time_to_PM: float):
        """예방 보전 프로세스"""
        try:
            yield self.__env.timeout(time_to_PM)
            if self.cur_state in [Machine.State.PM, Machine.State.REPAIRING]:
                return
        except simpy.Interrupt:
            # 머신 고장으로 인한 인터럽트 발생
            return
        self.__event_queue.put(self)

    def repair(self):
        """
        머신 수리 프로세스
        나중에 리팩토링 예정.
        """
        priority, preempt, reason, time = (-2, True, 'repairing', self.__repair_time) if self.cur_state == Machine.State.REPAIRING else (-1, False, 'PM', self.__pm_duration)
        try:
            with self.__resource.request(priority=priority, preempt=preempt) as req:
                yield req
                if reason == 'PM':
                    self.cur_state = Machine.State.PM
                    self.__PM_idx = self.__event_logger.log_event_start(self.__id, reason, 'machine', None)
                else:
                    self.__repair_idx = self.__event_logger.log_event_start(self.__id, reason, 'machine', None)
                yield self.__env.timeout(time)
                # 수리시 setup 정보도 초기화
                self.cur_state = Machine.State.IDLE
                self.__last_job_type = None
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

    def run(self, criteria):
        """
        머신의 메인 프로세스

        Args:
            criteria: 작업 선택 기준
        """
        while True:
            is_completed = False
            job = None
            try:
                # queue가 비어있으면 제일 빨리 도착하는 작업 선택
                # 그렇지 않을 경우 우선순위가 가장 높은 작업 선택
                if criteria is not None:
                    self.__queue.items.sort(key=lambda x: criteria(x, self), reverse=True)
                # 이 부분이 이상해 보일 수 있는데, simpy get 메소드가 interrupt 발생 시 동작이 이상해지는 경우가 있어서 밖으로 뺐음.
                job = yield self.__queue.get()
                with self.__resource.request(priority=0, preempt=False) as req:
                    yield req
                    job.waiting_end()
                    op_id = job.get_current_operation()

                    self.cur_state = Machine.State.SETUP
                    self.__event_idx = self.__event_logger.log_event_start(self.__id, 'setup', 'machine', f'job: {job.id}\noperation: {op_id}')
                    yield self.__env.timeout(self.get_setup_time(job.job_type))
                    self.__last_job_type = job.job_type
                    self.__event_logger.log_event_finish(self.__event_idx)

                    job.interrupt_qtime()

                    self.cur_state = Machine.State.WORKING
                    self.__event_idx = self.__event_logger.log_event_start(self.__id, 'working', 'machine', f'job: {job.id}\noperation: {op_id}')
                    yield self.__env.timeout(self.get_process_time(op_id))

                    self.cur_state = Machine.State.IDLE
                    is_completed = True
            except simpy.Interrupt:
                items = self.__queue.items.copy()
                self.__queue.items.clear()
                for item in items:
                    item.operation_end_signal.put(False)
                # self.__gueue.get()을 밖으로 뺐기 때문에, 고장이 발생했을 때 수리가 완료되기까지 기다렸다가 대기열의 작업을 빼오기 위해 아래처럼 짬.
                self.__event_logger.log_event_finish(self.__event_idx)
                self.__event_idx = -1
                yield self.repair_process
            self.__event_logger.log_event_finish(self.__event_idx)
            if job is not None:
                job.operation_end_signal.put(is_completed)
            self.__event_idx = -1
