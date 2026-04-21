from numpy import inf
import simpy
import pandas as pd
from typing import Dict, Any
from utils import EventLogger
from enum import Enum
from .machine import Machine

class Job:
    class State(Enum):
        WAITING = 1
        SETUP = 2
        WORKING = 3
        COMPLETED = 4

    def __init__(self, env: simpy.Environment, job_info: Dict[str, Any],
                 op_info: pd.DataFrame, event_logger: EventLogger):
        """
        Job 초기화

        Args:
            env: SimPy 환경
            job_info: 작업 정보 딕셔너리
            op_info: 작업 operation 정보 DataFrame
            event_logger: 이벤트 기록 인스턴스
        """
        self.__env = env
        self.__id = job_info['job_id']
        self.__type = job_info['job_type']
        self.__release_time = job_info['release_time']
        self.__due_date = job_info['due_date']
        self.__priority = job_info['priority']
        self.__qtime = op_info['qtime'].astype(float).values
        self.__qtime[0] = float(inf) # 첫 번째 operation에 대한 qtime은 고려하지 않는다.
        self.__op_seq = op_info[['op_id', 'op_seq']].values
        self.__op_group = op_info[['op_group', 'op_seq']].values
        self.__event_logger = event_logger
        self.__completed_time = 0.0

        # 프로세스 상태 관리
        self.__cur_seq = 0
        self.__sub_process = None
        self.__is_over_qtime = False
        self.__cur_event_idx = -1
        self.is_released = False

        self.__qtime_over_time_start = 0.0
        self.total_qtime_over = 0.0

    def __del__(self):
        if self.__is_over_qtime:
            self.total_qtime_over += self.__env.now - self.__qtime_over_time_start
        self.__event_logger.log_event_finish(self.__cur_event_idx)

    @property
    def id(self):
        return self.__id

    @property
    def completed_time(self):
        return self.__completed_time

    @property
    def cur_seq(self):
        return self.__cur_seq

    def is_in_due_date(self):
        return self.completed_time > 0.0 and self.__due_date < self.__completed_time

    def get_op_group(self):
        """
        현재 operation에 대한 그룹 정보 반환
        """
        if self.__cur_seq == 0:
            return None
        return self.__op_group[self.__cur_seq - 1][0]

    def chk_qtime(self):
        """
        QTime 체크 프로세스
        """
        try:
            yield self.__env.timeout(self.__qtime[self.__cur_seq - 1])
            self.__is_over_qtime = True
            # qtime 초과 시간 기록
            self.__qtime_over_time_start = self.__env.now

        except simpy.Interrupt:
            pass

    def __interrupt_qtime(self, qtime_process: simpy.Process):
        """
        QTime 체크 프로세스 중단
        """
        if not self.__is_over_qtime:
            qtime_process.interrupt()
            return
        self.total_qtime_over += self.__env.now - self.__qtime_over_time_start
        self.__is_over_qtime = False

    def run(self, machine: Machine=None, qtime_process: simpy.Process=None):
        """작업 실행 메인 프로세스"""
        cur_state = self.State.WAITING
        if self.__cur_seq == 0:
            yield self.__env.timeout(self.__release_time)
            self.is_released = True
        else:
            op_id, _ = self.__op_seq[self.__cur_seq - 1]
            try:
                # machine의 resource를 점유한 상태로 로직 시작
                with machine.resource.request(priority=self.__priority, preempt=False) as req:
                    yield req

                    # wating 종료
                    self.__event_logger.log_event_finish(self.__cur_event_idx)

                    # setup 단계
                    cur_state = self.State.SETUP
                    self.__cur_event_idx = self.__event_logger.log_event_start(id=self.id, event='setup', description=f'machine: {machine.id}\noperation: {op_id}', resource='job')
                    self.__sub_process = self.__env.process(machine.setup(self.__type, op_id, self.__id))
                    yield self.__sub_process
                    self.__event_logger.log_event_finish(self.__cur_event_idx)

                    # setup이 완료되면 qtime check 종료.
                    self.__interrupt_qtime(qtime_process)

                    # work 단계
                    cur_state = self.State.WORKING
                    self.__cur_event_idx = self.__event_logger.log_event_start(id=self.id, event='working', description=f'machine: {machine.id}\noperation: {op_id}', resource='job')
                    self.__sub_process = self.__env.process(machine.work(op_id, self.__id))
                    yield self.__sub_process
                    self.__sub_process = None
                    self.__event_logger.log_event_finish(self.__cur_event_idx)

            except simpy.Interrupt:
                # Machine breakdown으로 인한 interrupt
                if self.__sub_process is not None:
                    self.__sub_process.interrupt()
                    self.__event_logger.log_event_finish(self.__cur_event_idx)
                return self, cur_state
        cur_state = self.State.WAITING
        self.__cur_seq += 1
        if self.__cur_seq > len(self.__op_seq):
            cur_state = self.State.COMPLETED
            self.__completed_time = self.__env.now
            self.__cur_event_idx = -1
            return self, cur_state
        # wating 시작
        self.__cur_event_idx = self.__event_logger.log_event_start(id=self.id, event='waiting', resource='job')
        return self, cur_state
