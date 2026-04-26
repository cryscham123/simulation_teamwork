from typing import List

from algorithms.base import Algorithm
from simulation import Job, Machine

class Rule1(Algorithm):
    def __init__(self, qtime_urgency_factor: float):
        """
        Args:
            env: SimPy 환경 (PM 절대 시각 계산에 사용).
            qtime_urgency_factor: Remaining Q-time 이 후보 평균 processing time
                * factor 미만이면 Q-time urgent 로 판단.
            pm_hazard_threshold: 누적 고장률 PM 임계치. 기본 0.15 (= 15%).
        """
        self.__qutime_urgency_factor = qtime_urgency_factor

    def match_job_machine(self, job: Job, machine_list: List[Machine]) -> Machine:
        candidates = [
            m for m in machine_list
            if m.group == job.get_op_group()
        ]
        op_id = job.get_current_operation()

        avg_proc = sum(m.get_process_time(op_id) for m in candidates) / len(candidates)
        urgency_threshold = avg_proc * self.__qutime_urgency_factor
        # 얘는 뭐에 쓰임?
        _is_urgent = job.get_remain_qtime() < urgency_threshold

        # 작업이 언제 시작할 지 모르기 때문에, setup time은 정확하지 않음.
        return min(candidates, key=lambda m: m.get_process_time(op_id) + 1000000000000000 * (int(not m.is_idle()) + m.queue_size()))
