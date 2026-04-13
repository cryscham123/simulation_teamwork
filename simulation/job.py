from numpy import inf
import simpy
import pandas as pd
from typing import Dict, Any, Optional
from .scheduler import Scheduler
from .machine import Machine
from utils import EventLogger

class Job:
    def __init__(self, env: simpy.Environment, job_info: Dict[str, Any],
                 op_info: pd.DataFrame, scheduler: Scheduler, event_logger: EventLogger):
        """
        Job 초기화

        Args:
            env: SimPy 환경
            job_info: 작업 정보 딕셔너리
            op_info: 작업 operation 정보 DataFrame
            scheduler: 스케줄러 인스턴스
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
        self.__scheduler = scheduler
        self.__event_logger = event_logger
        self.__is_completed = False
        self.__completed_time = 0.0

        # 프로세스 상태 관리
        self.__sub_process = None
        self.__cur_machine: Optional[Machine] = None
        self.__is_over_qtime = False
        self.__process = env.process(self.run())

        self.__qtime_over_time_start = 0.0
        self.total_qtime_over = 0.0

    @property
    def id(self):
        return self.__id

    @property
    def is_completed(self):
        return self.__is_completed

    @property
    def completed_time(self):
        return self.__completed_time

    @property
    def process(self):
        return self.__process

    def is_in_due_date(self):
        return self.__is_completed and self.__due_date < self.__completed_time

    def __chk_qtime(self, seq: int):
        """
        QTime 체크 프로세스

        Args:
            seq: 작업 시퀀스
        """
        try:
            yield self.__env.timeout(self.__qtime[seq - 1])
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
        self.total_qtime_over = self.calculate_qtime_over(self.__env.now)
        self.__is_over_qtime = False

    def calculate_qtime_over(self, cur_time: float):
        """
        QTime 초과 시간 계산 메서드
        """
        if self.__is_over_qtime:
            return self.total_qtime_over + (cur_time - self.__qtime_over_time_start)
        return self.total_qtime_over


    def run(self):
        """작업 실행 메인 프로세스"""
        # release time만큼 기다려준다.
        yield self.__env.timeout(self.__release_time)

        for op_id, seq in self.__op_seq:
            while True:
                is_in_work = False
                # qtime 타이머를 켜고 프로세스 시작
                qtime_process = self.__env.process(self.__chk_qtime(seq))
                # 가용 가능한 machine 선택
                idx = self.__event_logger.log_event_start(id=self.id, event='waiting', resource='job')
                self.__cur_machine = yield self.__env.process(self.__scheduler.get_matched_machine(self.__id, seq))
                self.__event_logger.log_event_finish(idx)
                try:
                    # machine의 resource를 점유한 상태로 로직 시작
                    with self.__cur_machine.resource.request(priority=self.__priority, preempt=False) as req:
                        yield req

                        # setup 단계
                        idx = self.__event_logger.log_event_start(id=self.__cur_machine.id, event='setup', description=f'job: {self.__id}\noperation: {op_id}', resource='machine')
                        self.__sub_process = self.__env.process(self.__cur_machine.setup(self.__type, op_id, self.__id))
                        yield self.__sub_process
                        self.__event_logger.log_event_finish(idx)

                        # setup이 완료되면 qtime check 종료.
                        self.__interrupt_qtime(qtime_process)

                        # work 단계
                        is_in_work = True
                        idx = self.__event_logger.log_event_start(id=self.__cur_machine.id, event='working', description=f'job: {self.__id}\noperation: {op_id}', resource='machine')
                        self.__sub_process = self.__env.process(self.__cur_machine.work(op_id, self.__id))
                        yield self.__sub_process
                        self.__event_logger.log_event_finish(idx)
                        is_in_work = False
                        self.__sub_process = None
                        break

                except simpy.Interrupt:
                    # Machine breakdown으로 인한 interrupt
                    if self.__sub_process is not None:
                        self.__sub_process.interrupt()
                    self.__event_logger.log_event_finish(idx)
                    self.__scheduler.put_back_machine(self.__cur_machine)
                    if is_in_work:
                        return
            self.__scheduler.put_back_machine(self.__cur_machine)
            self.__cur_machine = None
        else:
            self.__is_completed = True
            self.__completed_time = self.__env.now
