"""
[SimPy 연동 방식]
  - 각 염색체 평가 시 SimPy 시뮬레이션을 실행
  - 순서 염색체(sequence) → job의 release_time 미세 조정으로 처리 우선순위 반영
    (sequence 상 앞선 job에 더 빠른 release_time 부여, 기존 값 ±epsilon 범위)
  - 장비 선택 염색체(machine_assignment) → SimPy Scheduler의 FilterStore 방식 특성상
    직접 반영 불가. 독립 평가(v1) 또는 향후 Scheduler 확장 시 활용.
  - 시뮬레이션 stdout을 캡처하여 완료/위반/makespan 메트릭 추출

[SimPy 시뮬레이션 확률적 특성]
  - 머신 고장(Weibull-like hazard)이 난수 기반이므로 동일 염색체라도
    실행마다 결과가 다를 수 있음
  - 필요 시 n_sim_runs > 1로 여러 번 실행 후 평균 적합도를 사용할 수 있음

[데이터 입력 형식 - DataLoader.load_all_data() 반환 딕셔너리]
  - machines:              machine_id, machine_group
  - jobs:                  job_id, job_type, release_time, due_date, priority
  - operations:            job_id, op_id, op_seq, op_group, qtime
  - machine_failure:       machine_id, base_hazard, hazard_increase_rate, repair_time, pm_duration
  - setup_times:           machine_group, from_job_type, to_job_type, setup_time
  - operation_machine_map: machine_id, op_id, process_time
"""

import random
import os
import sys
import io
import re
import contextlib
from copy import deepcopy
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Any

import numpy as np
import pandas as pd

# 프로젝트 루트를 sys.path에 추가 (단독 실행 / 다른 경로에서 실행 시 필요)
# 이 파일 위치: algorithms/genetic/ga_simul_04.py
# 프로젝트 루트: ../../ (두 단계 위)
# 경로땜에 오류나서 ai가 추가 하라는데 뭔진 모르겠음
_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(os.path.dirname(_this_dir))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import simpy
from simulation import DataLoader, Scheduler, Job  # noqa: E402


# 염색체 정의
@dataclass
class Chromosome:
    """
    유전 알고리즘 염색체

    Attributes:
        sequence (List[str]): job-based 순서 염색체.
            각 job_id가 operation 수만큼 반복. 예: ['J1','J2','J1','J3','J2']
        machine_assignment (Dict[Tuple, str]): 장비 선택 염색체.
            {(job_id, op_seq): machine_id} 형태.
            ※ SimPy Scheduler는 내부적으로 장비를 선택하므로 직접 반영되지 않음.-> 이거 수정해야됨
        fitness (float): 적합도 (높을수록 우수).
    """
    sequence: List[str]
    machine_assignment: Dict[Tuple[str, int], str]
    fitness: float = 0.0


# 유전 알고리즘 클래스 (SimPy 연동)
class GeneticAlgorithmWithSim:
    """
    반도체 공정 스케줄링 유전 알고리즘 (SimPy 완전 연결 버전)

    각 염색체 평가마다 SimPy 시뮬레이션을 실행하고,
    시뮬레이션 결과(완료율, q-time 위반, makespan 등)로 적합도를 산출
    """

    def __init__(
        self,
        data: Dict[str, pd.DataFrame],
        population_size: int = 20,
        n_generations: int = 30,
        crossover_rate: float = 0.85,
        mutation_rate: float = 0.15,
        tournament_size: int = 3,
        simul_time: float = 1000.0,
        n_sim_runs: int = 1,
    ):
        """
        Args:
            data: DataLoader.load_all_data() 반환값
            population_size: 개체군 크기
            n_generations: 총 세대 수
            crossover_rate: 교차 확률
            mutation_rate: 돌연변이 확률
            tournament_size: 토너먼트 선택 크기
            simul_time: SimPy 시뮬레이션 실행 시간 상한
            n_sim_runs: 확률적 시뮬레이션 반복 횟수 (1이면 단일 실행)
        """
        self.data = data
        self.population_size = population_size
        self.n_generations = n_generations
        self.crossover_rate = crossover_rate
        self.mutation_rate = mutation_rate
        self.tournament_size = tournament_size
        self.simul_time = simul_time
        self.n_sim_runs = n_sim_runs

        self._preprocess_data()
        self.history: List[float] = []

    # 데이터 전처리
    def _preprocess_data(self):
        """CSV 데이터를 GA에서 빠르게 조회할 수 있도록 전처리"""
        jobs_df = self.data['jobs']
        ops_df = self.data['operations']
        op_machine_df = self.data['operation_machine_map']

        self.jobs: Dict[str, Dict] = jobs_df.set_index('job_id').to_dict('index')

        self.job_ops: Dict[str, List[Dict]] = {}
        for job_id, grp in ops_df.groupby('job_id'):
            self.job_ops[job_id] = (
                grp.sort_values('op_seq')[['op_id', 'op_seq', 'op_group', 'qtime']]
                .to_dict('records')
            )

        self.op_info_map: Dict[Tuple, Dict] = {}
        for job_id, ops in self.job_ops.items():
            for op in ops:
                self.op_info_map[(job_id, op['op_seq'])] = op

        self.op_valid_machines: Dict[str, List[str]] = {}
        for op_id, grp in op_machine_df.groupby('op_id'):
            self.op_valid_machines[op_id] = grp['machine_id'].tolist()

        self.job_ids: List[str] = list(self.jobs.keys())
        self.sequence_template: List[str] = []
        for job_id in self.job_ids:
            n_ops = len(self.job_ops.get(job_id, []))
            self.sequence_template.extend([job_id] * n_ops)

    # 염색체 생성
    def _create_random_chromosome(self) -> Chromosome:
        """무작위 유효 염색체 생성"""
        sequence = self.sequence_template.copy()
        random.shuffle(sequence)

        machine_assignment: Dict[Tuple[str, int], str] = {}
        for job_id, ops in self.job_ops.items():
            for op in ops:
                valid = self.op_valid_machines.get(op['op_id'], [])
                if valid:
                    machine_assignment[(job_id, op['op_seq'])] = random.choice(valid)

        return Chromosome(sequence=sequence, machine_assignment=machine_assignment)

    def initialize_population(self) -> List[Chromosome]:
        """초기 개체군 생성"""
        return [self._create_random_chromosome() for _ in range(self.population_size)]

    # SimPy 시뮬레이션 실행 및 결과 파싱
    def _sequence_to_release_offsets(self, sequence: List[str]) -> Dict[str, float]:
        """
        순서 염색체를 job별 release_time 조정 오프셋으로 변환

        [로직]
        시퀀스 내 각 job_id의 첫 번째 등장 위치(position)를 기준으로
        작은 epsilon(=0.001)을 곱한 오프셋을 부여합니다.
        같은 release_time을 가진 job들 사이의 처리 순서를 제어하기 위한 미세 조정입니다.

        Returns:
            {job_id: offset} - 기존 release_time에 더할 값
        """
        first_pos: Dict[str, int] = {}
        for i, job_id in enumerate(sequence):
            if job_id not in first_pos:
                first_pos[job_id] = i
        epsilon = 0.001
        return {job_id: pos * epsilon for job_id, pos in first_pos.items()}

    def _run_simulation(self, chromosome: Chromosome) -> Tuple[str, float]:
        """
        SimPy 시뮬레이션 실행 (stdout 캡처)

        [수행 과정]
        1. 순서 염색체 → release_time 미세 조정 (job 처리 우선순위 반영)
        2. SimPy Environment + Scheduler + Job 생성 (simulation/ 패키지 사용)
        3. env.run(until=simul_time) 실행, stdout 캡처
        4. 캡처된 로그 문자열 반환

        Args:
            chromosome: 평가할 염색체

        Returns:
            (output_log, actual_run_time): 시뮬레이션 로그 문자열, 실제 종료 시간
        """
        offsets = self._sequence_to_release_offsets(chromosome.sequence)

        # release_time 미세 조정 (원본 DataFrame 불변 유지)
        # float으로 변환 후 오프셋 적용 (int64 컬럼에 float 대입 시 TypeError 방지)
        jobs_df = self.data['jobs'].copy()
        jobs_df['release_time'] = jobs_df['release_time'].astype(float)
        jobs_df['release_time'] += jobs_df['job_id'].map(offsets).fillna(0.0)

        captured = io.StringIO()
        env = simpy.Environment()

        scheduler = Scheduler(
            env=env,
            machine_df=self.data['machines'],
            operations_df=self.data['operations'],
            machine_failure_df=self.data['machine_failure'],
            setup_times_df=self.data['setup_times'],
            op_machine_df=self.data['operation_machine_map'],
            preferred_machines=chromosome.machine_assignment,
        )

        for _, job_row in jobs_df.iterrows():
            job_info = job_row.to_dict()
            job_ops_df = (
                self.data['operations']
                .loc[self.data['operations']['job_id'] == job_info['job_id'],
                     ['op_id', 'op_seq', 'qtime']]
                .sort_values('op_seq')
            )
            with contextlib.redirect_stdout(captured):
                Job(env=env, job_info=job_info, op_info=job_ops_df, scheduler=scheduler)

        with contextlib.redirect_stdout(captured):
            env.run(until=self.simul_time)

        return captured.getvalue(), env.now

    def _parse_metrics(self, log: str, sim_end_time: float) -> Dict[str, Any]:
        """
        SimPy 시뮬레이션 로그 파싱 → 성능 지표 추출

        [파싱 패턴]
        - "finished operation"   → 완료된 operation 카운트
        - "discarded due to qtime violation" → qtime 위반 job 카운트
        - "broke down" / "repaired"         → 머신 고장/수리 카운트
        - 완료된 operation의 최대 시간 → makespan

        Returns:
            {
              'n_completed_ops': int,
              'n_completed_jobs': int,
              'n_qtime_violations': int,
              'n_breakdowns': int,
              'makespan': float,
              'log': str,  # 원본 로그
            }
        """
        completed_ops: Dict[str, int] = {}   # job_id → 완료 op 수
        qtime_violated: set = set()
        makespan = 0.0
        n_breakdowns = 0

        for line in log.splitlines():
            parts = line.split('\t', 1)
            if len(parts) < 2:
                continue
            try:
                t = float(parts[0].strip())
            except ValueError:
                continue
            msg = parts[1].strip()

            # 완료된 operation
            m = re.match(r'Job (\S+) finished operation', msg)
            if m:
                jid = m.group(1)
                completed_ops[jid] = completed_ops.get(jid, 0) + 1
                makespan = max(makespan, t)
                continue

            # qtime 위반으로 폐기
            m = re.match(r'Job (\S+) discarded due to qtime violation', msg)
            if m:
                qtime_violated.add(m.group(1))
                continue

            # 머신 고장
            if 'broke down' in msg:
                n_breakdowns += 1

        # 모든 op을 완료한 job 수
        op_counts = {j: len(ops) for j, ops in self.job_ops.items()}
        completed_jobs = {
            jid for jid, cnt in completed_ops.items()
            if cnt >= op_counts.get(jid, 0)
        }

        return {
            'n_completed_ops': sum(completed_ops.values()),
            'n_completed_jobs': len(completed_jobs),
            'n_qtime_violations': len(qtime_violated),
            'n_breakdowns': n_breakdowns,
            'makespan': makespan if makespan > 0 else sim_end_time,
            'log': log,
        }

    # 적합도 평가
    def evaluate(self, chromosome: Chromosome) -> float:
        """
        SimPy 시뮬레이션 기반 적합도 평가

        Fitness = 0.50 × completed_rate         job을 일단 끝내는 것이 최우선
                + 0.30 × makespan_score         빠른 처리
                + 0.20 × (1 - violation_rate)   q-time 위반 페널티
                
                최악: 0.0  (job 하나도 완료 못함, 위반 최대, makespan 최대)
                최선: 1.0  (모든 job 완료, 위반 없음, makespan 최소)
        """
        scores = []
        for _ in range(self.n_sim_runs):
            log, end_time = self._run_simulation(chromosome)
            m = self._parse_metrics(log, end_time)

            n_jobs = len(self.job_ids)
            completed_rate = m['n_completed_jobs'] / n_jobs if n_jobs > 0 else 0.0   
            makespan_score = 1.0 / (1.0 + m['makespan'] / max(self.simul_time, 1.0))
            violation_rate = (
                m['n_qtime_violations'] / n_jobs if n_jobs > 0 else 0.0
            #tardiness 추가할수도? 고민중 추후 논의
            )

            score = (
                0.50 * completed_rate
                + 0.30 * makespan_score
                + 0.20 * (1.0 - violation_rate)
            )
            scores.append(score)

        return float(np.mean(scores))

    # 교차 (OX) / 돌연변이
    def _ox_crossover(self, p1: List[str], p2: List[str]) -> Tuple[List[str], List[str]]:
        """Order Crossover (OX) - 반복 원소 포함 순열 대응 버전"""
        n = len(p1)
        if n < 2:
            return p1[:], p2[:]
        a, b = sorted(random.sample(range(n), 2))

        total_needed: Dict[str, int] = {}
        for g in p1:
            total_needed[g] = total_needed.get(g, 0) + 1

        def ox(pa: List[str], pb: List[str]) -> List[str]:
            child = [None] * n
            child[a:b + 1] = pa[a:b + 1]
            placed: Dict[str, int] = {}
            for g in child[a:b + 1]:
                placed[g] = placed.get(g, 0) + 1
            remaining = []
            for g in pb:
                if placed.get(g, 0) < total_needed.get(g, 0):
                    remaining.append(g)
                    placed[g] = placed.get(g, 0) + 1
            positions = list(range(b + 1, n)) + list(range(a))
            for pos, gene in zip(positions, remaining):
                child[pos] = gene
            return child  # type: ignore

        return ox(p1, p2), ox(p2, p1)

    def crossover(
        self, parent1: Chromosome, parent2: Chromosome
    ) -> Tuple[Chromosome, Chromosome]:
        """교차: 순서(OX) + 장비선택(단일점)"""
        if random.random() > self.crossover_rate:
            return deepcopy(parent1), deepcopy(parent2)

        c1_seq, c2_seq = self._ox_crossover(parent1.sequence, parent2.sequence)

        keys = list(parent1.machine_assignment.keys())
        if len(keys) > 1:
            cut = random.randint(1, len(keys) - 1)
            c1_ma = {
                k: (parent1.machine_assignment[k] if i < cut else parent2.machine_assignment[k])
                for i, k in enumerate(keys)
            }
            c2_ma = {
                k: (parent2.machine_assignment[k] if i < cut else parent1.machine_assignment[k])
                for i, k in enumerate(keys)
            }
        else:
            c1_ma = deepcopy(parent1.machine_assignment)
            c2_ma = deepcopy(parent2.machine_assignment)

        return (
            Chromosome(sequence=c1_seq, machine_assignment=c1_ma),
            Chromosome(sequence=c2_seq, machine_assignment=c2_ma),
        )

    def mutate(self, chromosome: Chromosome) -> Chromosome:
        """돌연변이: 순서(스왑) + 장비선택(무작위 재선택)"""
        chrom = deepcopy(chromosome)

        if random.random() < self.mutation_rate and len(chrom.sequence) >= 2:
            i, j = random.sample(range(len(chrom.sequence)), 2)
            chrom.sequence[i], chrom.sequence[j] = chrom.sequence[j], chrom.sequence[i]

        for key in chrom.machine_assignment:
            if random.random() < self.mutation_rate:
                job_id, op_seq = key
                op_info = self.op_info_map.get((job_id, op_seq))
                if op_info:
                    valid = self.op_valid_machines.get(op_info['op_id'], [])
                    if valid:
                        chrom.machine_assignment[key] = random.choice(valid)

        return chrom

    def tournament_selection(self, population: List[Chromosome]) -> Chromosome:
        """토너먼트 선택"""
        candidates = random.sample(population, min(self.tournament_size, len(population)))
        return max(candidates, key=lambda c: c.fitness)

    # 출력 함수

    def _print_sim_log(self, log: str):
        print("\n[공정 시뮬레이션 - SimPy]")
        print("-" * 60)
        print(log.strip())

    def _print_kpi(self, metrics: Dict[str, Any], generation: int):
        n_jobs = len(self.job_ids)
        print(f"\n[세대 {generation} 정보 요약]")
        print(f"  완료된 작업 수    : {metrics['n_completed_jobs']} / {n_jobs}")
        print(f"  Q-time 위반 건수  : {metrics['n_qtime_violations']} 건")
        print(f"  머신 고장 횟수    : {metrics['n_breakdowns']} 회")
        print(f"  메이크스팬        : {round(metrics['makespan'], 2)}")

    def _print_history(self):
        """세대별 최적 적합도 변화 (최근 10세대)"""
        print("\n  [세대별 최적 적합도 변화]")
        recent = self.history[-10:]
        offset = len(self.history) - len(recent)
        for i, f in enumerate(recent):
            gen_num = offset + i + 1
            print(f"  세대 {gen_num:4d}: {f:.4f}")

    # 메인 실행

    def run(self) -> Chromosome:
        """
        유전 알고리즘 실행 (SimPy 연동)

        각 세대마다:
          1. SimPy 시뮬레이션으로 적합도 평가 (stdout 캡처)
          2. 최적 개체의 시뮬레이션 로그 + KPI 출력
          3. 세대별 적합도 변화 출력
          4. 선택 → 교차 → 돌연변이 → 다음 세대 생성 (엘리트 보존)

        Returns:
            best_overall (Chromosome): 전체 최적 염색체
        """
        print("=" * 60)
        print(" 반도체 공정 스케줄링 유전 알고리즘")
        print("=" * 60)
        print(f"  개체군 크기      : {self.population_size}")
        print(f"  세대 수          : {self.n_generations}")
        print(f"  교차율           : {self.crossover_rate}")
        print(f"  돌연변이율       : {self.mutation_rate}")
        print(f"  시뮬레이션 시간  : {self.simul_time}")
        print(f"  시뮬 반복 횟수   : {self.n_sim_runs}")
        print(f"  Job 수           : {len(self.job_ids)}")
        print(f"  총 Operation 수  : {len(self.sequence_template)}")
        print("=" * 60)

        population = self.initialize_population()
        best_overall: Optional[Chromosome] = None

        for gen in range(1, self.n_generations + 1):
            # ── 적합도 평가 ──────────────────────────────────────
            print(f"\n[세대 {gen} 평가 중...]", end=' ', flush=True)
            for chrom in population:
                chrom.fitness = self.evaluate(chrom)
            print("완료")

            best_gen = max(population, key=lambda c: c.fitness)
            self.history.append(best_gen.fitness)

            if best_overall is None or best_gen.fitness > best_overall.fitness:
                best_overall = deepcopy(best_gen)

            # ── 세대 결과 출력 ──────────────────────────────────
            print(f"\n{'=' * 60}")
            print(f"  세대 {gen} / {self.n_generations}")
            print(f"{'=' * 60}")

            # 최적 개체 시뮬레이션 로그 재실행 (출력용)
            log, end_time = self._run_simulation(best_gen)
            metrics = self._parse_metrics(log, end_time)

            self._print_sim_log(log)
            self._print_kpi(metrics, gen)

            print(f"\n  현재 세대 최적 적합도  : {round(best_gen.fitness, 4)}")
            print(f"  전체 누적 최적 적합도  : {round(best_overall.fitness, 4)}")
            self._print_history()
            # ────────────────────────────────────────────────────

            # 다음 세대 생성 (엘리트 1개 보존)
            new_pop: List[Chromosome] = [deepcopy(best_gen)]
            while len(new_pop) < self.population_size:
                p1 = self.tournament_selection(population)
                p2 = self.tournament_selection(population)
                c1, c2 = self.crossover(p1, p2)
                new_pop.append(self.mutate(c1))
                if len(new_pop) < self.population_size:
                    new_pop.append(self.mutate(c2))
            population = new_pop

        # ── 최종 결과 ──────────────────────────────────────────
        print(f"\n{'=' * 60}")
        print("  최종 결과 (전체 최적 염색체 - SimPy 시뮬레이션)")
        print(f"{'=' * 60}")
        print(f"  전체 최적 적합도: {round(best_overall.fitness, 4)}")

        final_log, final_end = self._run_simulation(best_overall)
        final_metrics = self._parse_metrics(final_log, final_end)
        self._print_sim_log(final_log)
        self._print_kpi(final_metrics, '최종')

        return best_overall
    
if __name__ == '__main__':
    from dotenv import load_dotenv

    load_dotenv()
    BASE_DATA_PATH = os.getenv('BASE_DATA_PATH', 'data')
    SIMUL_TIME = float(os.getenv('SIMUL_TIME', 1000))

    # simulation 패키지의 DataLoader로 데이터 로드
    loader = DataLoader(BASE_DATA_PATH)
    data = loader.load_all_data()

    ga = GeneticAlgorithmWithSim(
        data=data,
        population_size=20,
        n_generations=30,
        crossover_rate=0.85,
        mutation_rate=0.15,
        tournament_size=3,
        simul_time=SIMUL_TIME,
        n_sim_runs=1,
    )

    best = ga.run()
    print("\n완료.")