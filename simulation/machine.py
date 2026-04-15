import simpy
import random
import math
import pandas as pd
from typing import Dict, Any
from utils import EventLogger

class Machine:
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
        self.__is_repairing = False

        # 가정: Machine은 한 번에 하나의 작업만 처리할 수 있다.
        self.resource = simpy.PreemptiveResource(env, capacity=1)

        # 고장 관련 파라미터
        self.__base_hazard = failure_info['base_hazard']
        self.__hazard_increase_rate = failure_info['hazard_increase_rate']
        self.__repair_time = failure_info['repair_time']
        # PM 관련 로직은 현재 구현되어 있지 않다.
        self.__pm_duration = failure_info['pm_duration']

        # 시간 정보
        self.__setup_times = setup_time_info
        self.__process_times = process_time_info

        # 머신 상태
        self.__last_job_type = None
        self.__event_idx = -1

    def __del__(self):
        self.__event_logger.log_event_finish(self.__event_idx)

    @property
    def id(self) -> int:
        """머신 ID 반환"""
        return self.__id

    def __calculate_hazard(self):
        h0 = self.__base_hazard
        hr = self.__hazard_increase_rate
        u = random.random()

        return (-h0 + math.sqrt(h0**2 - 2*hr*math.log(u))) / hr

    def down(self):
        """머신 중단 프로세스"""
        is_broken = True
        try:
            # 머신 고장
            yield self.__env.timeout(self.__calculate_hazard())
            if self.__is_repairing:
                is_broken = False
        except simpy.Interrupt:
            # 예방 보전 성공으로 인한 인터럽트 발생
            is_broken = False

        return self, is_broken

    def repair(self):
        """머신 수리 프로세스"""
        self.__is_repairing = True
        idx = self.__event_logger.log_event_start(self.__id, 'repairing', 'machine', None)
        yield self.__env.timeout(self.__repair_time)
        self.__event_logger.log_event_finish(idx)
        # 수리시 setup 정보도 초기화
        self.__is_repairing = False
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
        process_time = self.get_process_time(op_id)
        try:
            self.__event_idx = self.__event_logger.log_event_start(self.__id, 'working', 'machine', f'job: {job_id}\noperation: {op_id}')
            yield self.__env.timeout(process_time)
            self.__event_logger.log_event_finish(self.__event_idx)
        except simpy.Interrupt:
            self.__event_logger.log_event_finish(self.__event_idx)

