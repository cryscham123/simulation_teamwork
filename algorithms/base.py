from abc import ABC, abstractmethod
from typing import Optional


class Algorithm(ABC):
    """
    모든 스케줄링 알고리즘이 반드시 구현해야 하는 인터페이스.
    Scheduler는 이 추상 클래스만 알고, 구체 알고리즘은 모름.

    기존 (Job 중심): Job이 여러 Machine을 평가해 1개 선택
    변경 (Machine 중심): Machine이 자신의 Queue에 있는 여러 Job을 평가해 1개 선택
    """

    @abstractmethod
    def select_job(self, machine_context: dict, waiting_jobs: list) -> Optional[dict]:
        """
        Machine이 자신의 Queue에서 처리할 Job을 선택.

        Args:
            machine_context: Machine의 현재 상태 정보 딕셔너리
                - machine: Machine 인스턴스
                - machine_id: int
                - now: 현재 시뮬레이션 시각
            waiting_jobs: 대기 중인 job_context dict 목록
                각 dict는 job_id, op_id, op_seq, due_date, release_time,
                priority, max_qtime, prev_op_finish, remaining_process_time 등 포함

        Returns:
            선택된 job_context dict, 없으면 None
        """
        pass
