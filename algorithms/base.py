from abc import ABC, abstractmethod
from simulation import Job, Machine
from typing import List

class Algorithm(ABC):
    """알고리즘의 기본 클래스"""

    @abstractmethod
    def match_job_machine(self, job: Job, machine_list: List[Machine]) -> Machine:
        """
        주어진 작업과 작업 순서에 매칭되는 머신 반환

        Args:
            job: 매칭할 작업 인스턴스
            machine_list: 모든 머신 리스트

        Returns:
            Machine: 매칭된 머신 인스턴스
        """
        pass

    @abstractmethod
    def calculate_PM_time(self, machine: Machine) -> float:
        """
        주어진 머신에 대한 예방 보전 시간 계산

        Args:
            machine: 예방 보전을 수행할 머신 인스턴스

        Returns:
            float: 예방 보전 시간
        """
        pass
