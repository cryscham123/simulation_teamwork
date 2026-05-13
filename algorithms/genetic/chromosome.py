from dataclasses import dataclass
from typing import List, Optional, Tuple


@dataclass
class Chromosome:
    # 길이 = 전체 operation 개수. 값 = feasible_machine_table[i]의 인덱스.
    # 예: machine[2]=1 → i=2번 op는 feasible_machine_table[2][1] 머신에서 처리
    machine: List[int]

    # 길이 = 머신 개수. 값 = PM_LEVELS의 인덱스 (encoder.py에서 정의).
    # 예: pm[0]=2 → 0번 머신의 PM threshold는 PM_LEVELS[2]
    pm: List[int]

    # 길이 = 전체 operation 개수. 각 operation의 dispatching 우선순위 점수.
    # 값의 범위: 0 ~ 99 (정수, 높을수록 높은 우선순위)
    # 예: operation_priority[0]=85 → 0번 op는 우선순위 85점
    # Stocker에서 candidates 중 operation_priority 합이 높은 job을 선택
    operation_priority: List[int]

    # 평가 전 None, 평가 후 (makespan, qtime_violation). 둘 다 작을수록 좋음.
    fitness: Optional[Tuple[float, float]] = None
