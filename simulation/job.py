from numpy import inf
import simpy
import pandas as pd
from typing import Dict, Any, Optional
from .scheduler import Scheduler
from .machine import Machine


class Job:
    def __init__(self, env: simpy.Environment, job_info: Dict[str, Any],
                 op_info: pd.DataFrame, scheduler: Scheduler):
        """
        Job 초기화

        Args:
            env: SimPy 환경
            job_info: 작업 정보 딕셔너리
            op_info: 작업 operation 정보 DataFrame
            scheduler: 스케줄러 인스턴스
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
        self.__is_completed = False
        self.__completed_time = 0.0

        # 프로세스 상태 관리
        self.__cur_machine: Optional[Machine] = None
        self.__qtime_over_time_start = 0.0
        self.__total_qtime_over = 0.0
        self.__is_over_qtime = False
        env.process(self.run())

        # 이벤트 로그
        self.__event_log = []

    @property
    def id(self):
        return self.__id

    @property
    def event_log(self):
        return self.__event_log

    @property
    def total_qtime_over(self):
        return self.__total_qtime_over

    @property
    def is_completed(self):
        return self.__is_completed

    @property
    def completed_time(self):
        return self.__completed_time

    def log_event(self, event_type: str, op_id: Optional[int] = None, machine_id: Optional[int] = None, reason: Optional[str] = None):
        self.__event_log.append({
            'job_id': self.__id,
            'event_type': event_type,
            'description': f"Job {self.__id} - {event_type}" 
                + (f" on Machine {machine_id}" if machine_id is not None else "")
                + (f" for Operation {op_id}" if op_id is not None else "")
                + (f" due to {reason}" if reason is not None else ""),
            'time': self.__env.now
        })

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
        self.__total_qtime_over = self.calculate_qtime_over(self.__env.now)
        self.__is_over_qtime = False

    def calculate_qtime_over(self, cur_time: float):
        """
        QTime 초과 시간 계산 메서드
        """
        if self.__is_over_qtime:
            return self.__total_qtime_over + (cur_time - self.__qtime_over_time_start)
        return self.__total_qtime_over


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
                self.log_event(event_type='waiting', op_id=op_id)
                self.__cur_machine = yield self.__env.process(self.__scheduler.get_matched_machine(self.__id, seq))
                try:
                    # machine의 resource를 점유한 상태로 로직 시작
                    with self.__cur_machine.resource.request(priority=self.__priority, preempt=False) as req:
                        yield req
                        self.log_event(event_type='allocated', op_id=op_id, machine_id=self.__cur_machine.id)

                        # setup 단계
                        self.log_event(event_type='setup', op_id=op_id, machine_id=self.__cur_machine.id)
                        yield self.__env.process(self.__cur_machine.setup(self.__type))

                        # setup이 완료되면 qtime check 종료.
                        self.__interrupt_qtime(qtime_process)

                        # work 단계
                        is_in_work = True
                        self.log_event(event_type='working', op_id=op_id, machine_id=self.__cur_machine.id)
                        yield self.__env.process(self.__cur_machine.work(op_id))
                        is_in_work = False
                        break

                except simpy.Interrupt:
                    # Machine breakdown으로 인한 interrupt
                    self.log_event(event_type='interrupt', op_id=op_id, machine_id=self.__cur_machine.id, reason='machine breakdown')
                    self.__scheduler.put_back_machine(self.__cur_machine)
                    # 작업 중 고장이 발생하면 폐기
                    if is_in_work:
                        self.log_event(event_type='completed')
                        return 

            self.__scheduler.put_back_machine(self.__cur_machine)
            self.__cur_machine = None
        else:
            self.log_event(event_type='completed')
            self.__is_completed = True
            self.__completed_time = self.__env.now
