from numpy import inf
import simpy
import pandas as pd
from typing import Dict, Any
from .scheduler import Scheduler
from utils import EventLogger

class Job:
    def __init__(self, env: simpy.Environment, job_info: Dict[str, Any],
                 op_info: pd.DataFrame, qtime_df: pd.DataFrame,
                 scheduler: Scheduler, event_logger: EventLogger):
        """
        Job 초기화

        Args:
            env: SimPy 환경
            job_info: 작업 정보 딕셔너리
            op_info: 작업 operation 정보 DataFrame
            qtime_df: 이 Job의 qtime 제약 DataFrame (qtime_constraints.csv에서 필터링)
            scheduler: 스케줄러 인스턴스
            event_logger: 이벤트 기록 인스턴스
        """
        self.__env = env
        self.__id = job_info['job_id']
        self.__type = job_info['job_type']
        self.__release_time = job_info['release_time']
        self.__due_date = job_info['due_date']
        self.__priority = job_info['priority']

        # qtime_constraints.csv 기반으로 qtime 배열 구성
        num_ops = len(op_info)
        self.__qtime = [float(inf)] * num_ops
        if qtime_df is not None and not qtime_df.empty:
            for _, row in qtime_df.iterrows():
                to_seq = int(row['to_op_seq'])
                if 1 <= to_seq <= num_ops:
                    self.__qtime[to_seq - 1] = float(row['max_qtime'])
        self.__qtime[0] = float(inf)  # 첫 번째 operation에 대한 qtime은 고려하지 않는다.

        self.__op_seq = op_info[['op_id', 'op_seq']].values
        self.__scheduler = scheduler
        self.__event_logger = event_logger
        self.__is_completed = False
        self.__completed_time = 0.0

        # 프로세스 상태 관리
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
            if qtime_process.is_alive:
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
        """
        작업 실행 메인 프로세스.
        각 op마다:
          1. QTime 타이머 시작 (op당 1회)
          2. job_context 빌드 후 route_job_to_machine으로 Machine Queue에 Push
          3. done_event 대기 — Machine.run()이 처리 완료 시 트리거
          4. 결과: 'done' → 다음 op / 'failed' → Job 폐기 / 'requeue' → 재라우팅
        """
        yield self.__env.timeout(self.__release_time)

        for op_id, seq in self.__op_seq:
            # QTime 타이머는 op당 1회 생성 — 재라우팅(requeue) 시에도 계속 유지
            qtime_process = self.__env.process(self.__chk_qtime(seq))

            while True:
                done_event = self.__env.event()

                # --- 대기(waiting) 로그 시작, Machine이 Queue에서 꺼낼 때 종료 ---
                wait_idx = self.__event_logger.log_event_start(
                    id=self.id, event='waiting', resource='job'
                )

                job_context = {
                    'job_id':    self.__id,
                    'job_type':  self.__type,
                    'op_id':     op_id,
                    'op_seq':    seq,
                    'due_date':  self.__due_date,
                    'release_time': self.__release_time,
                    'priority':  self.__priority,
                    # QTime 제약 정보 (dispatching score 계산용)
                    'max_qtime': self.__qtime[seq - 1],
                    # Machine이 Queue에서 꺼낼 때 호출 → waiting 로그 종료
                    'end_wait_fn':        lambda wi=wait_idx: self.__event_logger.log_event_finish(wi),
                    # Setup 완료 후 Machine이 호출 → QTime 모니터 중단
                    'qtime_interrupt_fn': lambda qp=qtime_process: self.__interrupt_qtime(qp),
                    'done_event': done_event,
                }

                # EFT 라우팅 → transport_job → Machine Queue에 Push
                yield self.__env.process(
                    self.__scheduler.route_job_to_machine(job_context)
                )

                # Machine이 처리 완료 또는 고장 시 done_event 트리거
                result = yield done_event

                status = result['status']
                if status == 'failed':
                    # work 중 고장 → Job 폐기
                    return
                elif status == 'requeue':
                    # setup 중 고장 → 다른 Machine으로 재라우팅 (QTime 타이머 유지)
                    continue
                else:
                    # 정상 완료
                    break

            self.__scheduler.notify_op_finish(self.__id, seq)

        self.__is_completed = True
        self.__completed_time = self.__env.now
