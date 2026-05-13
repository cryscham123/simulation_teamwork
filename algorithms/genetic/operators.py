import random
from typing import Callable, List, Tuple

from .chromosome import Chromosome
from .encoder import EncodedData


def random_chromosome(encoded: EncodedData) -> Chromosome:
    """무작위 초기 염색체 1개 생성."""
    n_machines = len(encoded.machine_index_table)
    n_levels = len(encoded.pm_levels)
    n_operations = len(encoded.operation_index_table)

    # machine: 각 op별로 feasible 머신 중 하나를 무작위 선택
    machine = [random.randrange(len(feasible)) for feasible in encoded.feasible_machine_table]

    # pm: 각 머신별로 PM 레벨 중 하나를 무작위 선택
    pm = [random.randrange(n_levels) for _ in range(n_machines)]

    # operation_priority: 각 operation별로 0~len(operation_index_table) 사이의 우선순위 점수를 무작위 선택
    operation_priority = random.sample(range(n_operations), n_operations)

    return Chromosome(machine=machine, pm=pm, operation_priority=operation_priority)


### Selection
def tournament_select(population: List[Chromosome],
                      fitness_fn: Callable[[Chromosome], float],
                      k: int = 3) -> Chromosome:
    """k명 무작위 추출 후 fitness_fn 기준 최소값(=가장 좋음) 선발."""
    candidates = random.sample(population, k)
    return min(candidates, key=fitness_fn)


#### Crossover

def crossover(parent1: Chromosome,
              parent2: Chromosome,
              rate: float = 0.8) -> Tuple[Chromosome, Chromosome]:
    """염색체 부분별로 다른 crossover. rate 확률로 교차, 아니면 부모 그대로 복사."""
    if random.random() >= rate:
        return _copy(parent1), _copy(parent2)

    mac_c1, mac_c2 = _uniform_crossover(parent1.machine, parent2.machine)
    pm_c1, pm_c2 = _uniform_crossover(parent1.pm, parent2.pm)
    op_c1, op_c2 = _pmx_crossover(parent1.operation_priority, parent2.operation_priority)

    return (
        Chromosome(machine=mac_c1, pm=pm_c1, operation_priority=op_c1),
        Chromosome(machine=mac_c2, pm=pm_c2, operation_priority=op_c2),
    )


def _copy(chromo: Chromosome) -> Chromosome:
    return Chromosome(
        machine=list(chromo.machine),
        pm=list(chromo.pm),
        operation_priority=list(chromo.operation_priority),
    )


def _uniform_crossover(p1: List[int], p2: List[int]) -> Tuple[List[int], List[int]]:
    """Uniform Crossover. 각 위치마다 50% 확률로 부모 교환."""
    c1, c2 = [], []
    for g1, g2 in zip(p1, p2):
        if random.random() < 0.5:
            c1.append(g1)
            c2.append(g2)
        else:
            c1.append(g2)
            c2.append(g1)
    return c1, c2

def _pmx_crossover(p1: List[int], p2: List[int]) -> Tuple[List[int], List[int]]:
    """Partially Mapped Crossover (PMX) 구현."""
    size = len(p1)
    # 두 개의 교차점 선택
    cx1, cx2 = sorted(random.sample(range(size), 2))

    # 자식 초기화
    c1, c2 = [None] * size, [None] * size

    # 1단계: 교차 구간 복사
    c1[cx1:cx2+1] = p2[cx1:cx2+1]
    c2[cx1:cx2+1] = p1[cx1:cx2+1]

    # 2단계: 나머지 구간 채우기 및 중복 해결
    def fill_and_repair(child, parent, mapping_source, mapping_target):
        for i in range(size):
            if cx1 <= i <= cx2:
                continue
            
            val = parent[i]
            # 교차 구간에 이미 존재하는 값이라면 매핑 테이블을 따라 대체값 탐색
            while val in mapping_source:
                # mapping_source에서의 위치를 찾아 mapping_target의 값으로 변경
                idx = mapping_source.index(val)
                val = mapping_target[idx]
            child[i] = val

    # 구간 매핑 정보 추출
    segment1 = p1[cx1:cx2+1]
    segment2 = p2[cx1:cx2+1]

    fill_and_repair(c1, p1, segment2, segment1)
    fill_and_repair(c2, p2, segment1, segment2)

    return c1, c2

#### Mutation

def mutate(chromo: Chromosome,
           encoded: EncodedData,
           rate: float = 0.1) -> Chromosome:
    """각 부분 독립적으로 rate 확률로 변이. 새 Chromosome 반환 (원본 보존)."""
    new_mac = list(chromo.machine)
    new_pm = list(chromo.pm)
    new_op = list(chromo.operation_priority)

    if random.random() < rate:
        _random_reset_machine(new_mac, encoded.feasible_machine_table)
    if random.random() < rate:
        _random_reset(new_pm, len(encoded.pm_levels))
    if random.random() < rate:
        _random_swap(new_op)

    return Chromosome(machine=new_mac, pm=new_pm, operation_priority=new_op)


def _random_reset_machine(genes: List[int],
                           feasible_table: List[List[str]]) -> None:
    """machine 유전자 한 위치를 다른 feasible 인덱스로 재설정."""
    i = random.randrange(len(genes))
    feasible_count = len(feasible_table[i])
    if feasible_count < 2:
        return
    new_val = random.randrange(feasible_count)
    while new_val == genes[i]:
        new_val = random.randrange(feasible_count)
    genes[i] = new_val


def _random_reset(genes: List[int], n_values: int) -> None:
    """일반 정수 유전자 한 위치를 다른 값으로 재설정."""
    if n_values < 2:
        return
    i = random.randrange(len(genes))
    new_val = random.randrange(n_values)
    while new_val == genes[i]:
        new_val = random.randrange(n_values)
    genes[i] = new_val


def _random_swap(genes: List[int]) -> None:
    """순열 유지를 위한 Swap 변이: 두 위치의 값을 서로 바꿈."""
    idx1, idx2 = random.sample(range(len(genes)), 2)
    genes[idx1], genes[idx2] = genes[idx2], genes[idx1]
