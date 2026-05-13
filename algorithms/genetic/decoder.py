from dataclasses import dataclass
from typing import Dict, List

from .chromosome import Chromosome
from .encoder import EncodedData


@dataclass
class SimulationInput:
    """GA가 결정한 의사결정을 시뮬레이터에 주입하는 데이터 묶음."""

    # op_id → 그 op를 처리할 machine_id.
    # 예: {'J1_O1': 'M2', 'J1_O2': 'M4', ...}
    op_machine: Dict[str, str]

    # machine_id → 그 머신의 PM threshold 값.
    # 예: {'M1': 0.2, 'M2': 0.1, ...}
    pm_thresholds: Dict[str, float]

    # op_id → 그 op의 dispatching 우선순위 점수 (0~99).
    # 예: {'J1_O1': 85, 'J1_O2': 62, ...}
    # Stocker에서 candidates 중 operation_priority 합이 높은 job을 선택
    operation_priority: Dict[str, int]


def decode(chromo: Chromosome, encoded: EncodedData) -> SimulationInput:
    """염색체를 시뮬레이션 입력 형태로 변환."""
    # machine[i] = j → i번 op는 feasible_machine_table[i][j] 머신에서 처리
    op_machine = {
        encoded.operation_index_table[i]: encoded.feasible_machine_table[i][gene]
        for i, gene in enumerate(chromo.machine)
    }

    # pm[m] = k → m번 머신의 threshold는 pm_levels[k]
    pm_thresholds = {
        encoded.machine_index_table[m]: encoded.pm_levels[gene]
        for m, gene in enumerate(chromo.pm)
    }

    # operation_priority[i] = priority_score → i번 op의 dispatching 우선순위
    operation_priority = {
        encoded.operation_index_table[i]: score
        for i, score in enumerate(chromo.operation_priority)
    }

    return SimulationInput(
        op_machine=op_machine,
        pm_thresholds=pm_thresholds,
        operation_priority=operation_priority,
    )
