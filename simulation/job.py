import simpy
import pandas as pd
from typing import Dict, Any, Optional
from .scheduler import Scheduler
from .machine import Machine

print('카푸치노1234')
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
        self.__current_stage: Optional[str] = None  # 'setup' 또는 'work' 구분용

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

    def run(self):
        """작업 실행 메인 프로세스"""
        # release time만큼 기다려준다.
        yield self.__env.timeout(self.__release_time)

        try:
            for op_id, seq in self.__op_seq:
                # qtime 타이머를 켜고 프로세스 시작
                qtime_process = self.__env.process(self.__chk_qtime(seq))

                # Operation 처리 (machine breakdown으로 인한 interrupt 처리)
                operation_completed = False
                while not operation_completed:
                    try:
                        # 가용 가능한 machine 선택
                        self.__cur_machine = yield self.__env.process(
                            self.__scheduler.get_matched_machine(self.__id, seq)
                        )
                        # machine이 할당되고, setup 단계 전까지 가면 qtime check 종료.
                        # qtime_process가 이미 종료되었을 수 있으므로 try-except로 처리
                        try:
                            if qtime_process.is_alive:
                                qtime_process.interrupt()
                        except (RuntimeError, AttributeError):
                            # 프로세스가 이미 종료되었거나 interrupt할 수 없는 상태
                            pass
                        # machine의 resource를 점유한 상태로 로직 시작
                        with self.__cur_machine.resource.request(
                            priority=self.__priority, preempt=False
                        ) as req:
                            yield req

                            # Setup 단계
                            print(f'{round(self.__env.now, 2)}\t'
                                  f'Job {self.__id} starts setup for operation {op_id} '
                                  f'on machine {self.__cur_machine.id}')
                            self.__current_stage = 'setup'
                            self.__sub_process = self.__env.process(
                                self.__cur_machine.setup(self.__type)
                            )
                            yield self.__sub_process

                            # Work 단계
                            print(f'{round(self.__env.now, 2)}\t'
                                  f'Job {self.__id} starts processing operation {op_id} '
                                  f'on machine {self.__cur_machine.id}')
                            self.__current_stage = 'work'
                            self.__sub_process = self.__env.process(
                                self.__cur_machine.work(op_id)
                            )
                            yield self.__sub_process

                            print(f'{round(self.__env.now, 2)}\t'
                                  f'Job {self.__id} finished operation {op_id} '
                                  f'on machine {self.__cur_machine.id}')
                            operation_completed = True

                    except simpy.Interrupt:
                        # Machine breakdown으로 인한 interrupt
                        if self.__current_stage == 'setup':
                            # Setup 중 고장: 재시도
                            print(f'{round(self.__env.now, 2)}\t'
                                  f'Job {self.__id} interrupted during setup '
                                  f'on machine {self.__cur_machine.id}, '
                                  f'will retry operation {op_id}')
                            # 고장난 Machine을 반환하고 다시 대기
                            self.__sub_process = None
                            self.__scheduler.put_back_machine(self.__cur_machine)
                        elif self.__current_stage == 'work':
                            # Work 중 고장: job 폐기
                            print(f'{round(self.__env.now, 2)}\t'
                                  f'Job {self.__id} interrupted during work '
                                  f'on machine {self.__cur_machine.id}, job discarded')
                            # 고장난 Machine을 반환하고 종료
                            self.__sub_process = None
                            self.__scheduler.put_back_machine(self.__cur_machine)
                            return 

                self.__scheduler.put_back_machine(self.__cur_machine)
                self.__cur_machine = None
                self.__sub_process = None
                self.__current_stage = None

        except simpy.Interrupt:
            # Qtime 초과로 인한 job discard
            print(f'{round(self.__env.now, 2)}\t'
                  f'Job {self.__id} discarded due to qtime violation')
            if self.__sub_process:
                self.__sub_process.interrupt()
            if self.__cur_machine:
                self.__scheduler.put_back_machine(self.__cur_machine)

