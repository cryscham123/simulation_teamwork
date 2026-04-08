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
        self.__qtime = op_info['qtime'].values
        self.__op_seq = op_info[['op_id', 'op_seq']].values
        self.__scheduler = scheduler

        # 프로세스 상태 관리
        self.__job_process = env.process(self.run())
        self.__sub_process: Optional[simpy.Process] = None
        self.__cur_machine: Optional[Machine] = None

        # 이벤트 로그
        self.__event_log = []

    @property
    def event_log(self):
        return self.__event_log

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
            self.__qtime_start = self.__env.now
            yield self.__env.timeout(self.__qtime[seq - 1])
            # qtime 초과시 현재 작업을 중단한다.
            self.__job_process.interrupt()
        except simpy.Interrupt:
            pass

    def __interrupt_qtime(self, qtime_process: simpy.Process):
        """
        QTime 체크 프로세스 중단
        """
        try:
            if qtime_process.is_alive:
                qtime_process.interrupt()
        except (RuntimeError, AttributeError):
            # 프로세스가 이미 종료되었거나 interrupt할 수 없는 상태
            pass


    def run(self):
        """작업 실행 메인 프로세스"""
        # release time만큼 기다려준다.
        yield self.__env.timeout(self.__release_time)

        try:
            for op_id, seq in self.__op_seq:
                while True:
                    is_in_work = False
                    # qtime 타이머를 켜고 프로세스 시작
                    qtime_process = self.__env.process(self.__chk_qtime(seq))
                    # 가용 가능한 machine 선택
                    self.log_event(event_type='waiting', op_id=op_id)
                    self.__cur_machine = yield self.__env.process(
                        self.__scheduler.get_matched_machine(self.__id, seq)
                    )
                    # machine이 할당되고, setup 단계 전까지 가면 qtime check 종료.
                    self.__interrupt_qtime(qtime_process)
                    try:
                        # machine의 resource를 점유한 상태로 로직 시작
                        with self.__cur_machine.resource.request(priority=self.__priority, preempt=False) as req:
                            yield req
                            self.log_event(event_type='allocated', op_id=op_id, machine_id=self.__cur_machine.id)

                            # Setup 단계
                            self.log_event(event_type='setup', op_id=op_id, machine_id=self.__cur_machine.id)
                            self.__sub_process = self.__env.process(
                                self.__cur_machine.setup(self.__type)
                            )
                            yield self.__sub_process

                            # Work 단계
                            is_in_work = True
                            self.log_event(event_type='working', op_id=op_id, machine_id=self.__cur_machine.id)
                            self.__sub_process = self.__env.process(
                                self.__cur_machine.work(op_id)
                            )
                            yield self.__sub_process
                            is_in_work = False
                            break

                    except simpy.Interrupt:
                        # Machine breakdown으로 인한 interrupt
                        self.log_event(event_type='interrupt', op_id=op_id, machine_id=self.__cur_machine.id, reason='machine breakdown')
                        self.__sub_process = None
                        self.__scheduler.put_back_machine(self.__cur_machine)
                        # 작업 중 고장이 발생하면 폐기
                        if is_in_work:
                            self.log_event(event_type='completed')
                            return 

                self.__scheduler.put_back_machine(self.__cur_machine)
                self.__cur_machine = None
                self.__sub_process = None
            else:
                self.log_event(event_type='completed')

        except simpy.Interrupt:
            # Qtime 초과로 인한 job discard
            self.log_event(event_type='interrupt', reason='qtime exceeded')
            self.log_event(event_type='completed')
            if self.__sub_process:
                self.__sub_process.interrupt()
            if self.__cur_machine:
                self.__scheduler.put_back_machine(self.__cur_machine)

