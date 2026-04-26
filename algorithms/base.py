from abc import ABC, abstractmethod
from simulation import Job, Machine
from typing import List

class Algorithm(ABC):
    """알고리즘의 기본 클래스"""

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
