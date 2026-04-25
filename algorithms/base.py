from abc import ABC, abstractmethod
from simulation import Job, Machine
from typing import List

class Algorithm(ABC):
    """알고리즘의 기본 클래스"""

    @abstractmethod
    def get_job_criteria_score(self, job: Job, machine: Machine) -> float:
        """
        주어진 작업에 대한 우선순위 기준 계산
        내림차순으로 높은 값이 우선순위가 높음

        Args:
            job: 우선순위 기준을 계산할 작업 인스턴스
            machine: 우선순위 기준 계산에 참조할 머신 인스턴스

        Returns:
            float: 계산된 우선순위 기준 값
        """
        pass

    @abstractmethod
    def match_job_machine(self, job: Job, machine_list: List[Machine]) -> Machine:
        """
        주어진 작업과 작업 순서에 매칭되는 머신 반환
        down된 job도 후보로 매칭될 수 있다고 가정

        Args:
            job: 매칭할 작업 인스턴스
            machine_list: 모든 머신 리스트

        Returns:
            Machine: 매칭된 머신 인스턴스
        """
        pass

    @abstractmethod
    def calculate_down_time(self, machine: Machine) -> float:
        """
        주어진 머신에 대한 고장 시간 계산

        Args:
            machine: 고장 시간을 계산할 머신 인스턴스

        Returns:
            float: 다음 고장까지 남은 시간
        """
        pass

    @abstractmethod
    def calculate_PM_time(self, machine: Machine) -> float:
        """
        주어진 머신에 대한 예방 보전 시간 계산

        Args:
            machine: 예방 보전을 수행할 머신 인스턴스

        Returns:
            float: 다음 예방 보전까지 남은 시간
        """
        pass
