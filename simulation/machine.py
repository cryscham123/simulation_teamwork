import simpy
import random
import math
import pandas as pd
from typing import Dict, Any
from utils import EventLogger
from enum import Enum

class Machine:
    class State(Enum):
        IDLE = 0
        SETUP = 1
        WORKING = 2
        REPAIRING = 3
        PM = 4

    def __init__(self, env: simpy.Environment, id: int, group: str,
                 failure_info: Dict[str, Any], setup_time_info: pd.DataFrame,
                 process_time_info: pd.DataFrame,
                 event_logger: EventLogger):
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
        """
        self.__env = env
        self.__id = id
        self.group = group
        self.__event_logger = event_logger

        # 가정: Machine은 한 번에 하나의 작업만 처리할 수 있다.
        self.resource = simpy.PreemptiveResource(env, capacity=1)

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
        self.cur_state = Machine.State.IDLE
        self.down_process = None
        self.pm_process = None
        self.run_process = None

    def __del__(self):
        self.__event_logger.log_event_finish(self.__event_idx)

    @property
    def id(self) -> int:
        """머신 ID 반환"""
        return self.__id

    def calculate_hazard(self):
        """일단은 남겨 놓음. rule-based 알고리즘에 옮겨주길 바람."""
        h0 = self.__base_hazard
        hr = self.__hazard_increase_rate
        u = random.random()

        return (-h0 + math.sqrt(h0**2 - 2*hr*math.log(u))) / hr

    def down(self, time_to_fail: float):
        """머신 중단 프로세스"""
        try:
            # 머신 고장
            yield self.__env.timeout(time_to_fail)
            if self.cur_state in [Machine.State.PM, Machine.State.REPAIRING]:
                return self
            self.cur_state = Machine.State.REPAIRING
        except simpy.Interrupt:
            # 예방 보전 성공으로 인한 인터럽트 발생
            pass
        return self

    def PM(self, time_to_PM: float):
        """예방 보전 프로세스"""
        try:
            yield self.__env.timeout(time_to_PM)
            if self.cur_state in [Machine.State.PM, Machine.State.REPAIRING]:
                return self
            # 예방 보전 시작 전, 현재 작업이 있다면 완료될 때까지 대기
            if self.cur_state != Machine.State.IDLE:
                yield self.run_process
            self.cur_state = Machine.State.PM
        except simpy.Interrupt:
            # 머신 고장으로 인한 인터럽트 발생
            pass
        return self

    def repair(self):
        """머신 수리 프로세스"""
        reason, time = ('repairing', self.__repair_time) if self.cur_state == Machine.State.REPAIRING else ('PM', self.__pm_duration)
        idx = self.__event_logger.log_event_start(self.__id, reason, 'machine', None)
        yield self.__env.timeout(time)
        self.__event_logger.log_event_finish(idx)
        # 수리시 setup 정보도 초기화
        self.cur_state = Machine.State.IDLE
        self.__last_job_type = None

    def is_idle(self) -> bool:
        """
        머신이 가용 가능한 상태인지 확인.
        수리중인지 아닌지를 판별하는 용도로 사용
        """
        return self.resource.count < self.resource.capacity

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

    def setup(self, job_type: str, op_id: int, job_id: int):
        """
        머신 셋업 프로세스

        Args:
            job_type: 작업 타입
        """
        self.cur_state = Machine.State.SETUP
        self.__event_idx = -1
        try:
            self.__event_idx = self.__event_logger.log_event_start(self.__id, 'setup', 'machine', f'job: {job_id}\noperation: {op_id}')
            yield self.__env.timeout(self.get_setup_time(job_type))
            self.__event_logger.log_event_finish(self.__event_idx)
            self.__last_job_type = job_type
        except simpy.Interrupt:
            self.__event_logger.log_event_finish(self.__event_idx)

    def work(self, op_id: int, job_id: int):
        """
        작업 처리 프로세스

        Args:
            op_id: 오퍼레이션 ID
            job_id: 작업 ID
        """
        self.cur_state = Machine.State.WORKING
        process_time = self.get_process_time(op_id)
        try:
            self.__event_idx = self.__event_logger.log_event_start(self.__id, 'working', 'machine', f'job: {job_id}\noperation: {op_id}')
            yield self.__env.timeout(process_time)
            self.__event_logger.log_event_finish(self.__event_idx)
            self.cur_state = Machine.State.IDLE
        except simpy.Interrupt:
            self.__event_logger.log_event_finish(self.__event_idx)
