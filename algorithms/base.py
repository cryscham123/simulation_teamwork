from abc import ABC, abstractmethod
from simpy import FilterStore
from simulation import Job, Machine

class Algorithm(ABC):
    """알고리즘의 기본 클래스"""

    @abstractmethod
    def match_job_machine(self, job: Job, machine_list: FilterStore) -> Machine:
        """
        주어진 작업과 작업 순서에 매칭되는 머신 반환
        특별한 알고리즘 없이 가장 빨리 유휴 상태로 전환된 아무 머신을 선택

        Args:
            job: 매칭할 작업 인스턴스
            machine_list: 매칭 가능한 머신 리스트 (FilterStore)

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
