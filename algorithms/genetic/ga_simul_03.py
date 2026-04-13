import random
import os
import sys
from copy import deepcopy
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Any

import numpy as np
import pandas as pd

_this_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(os.path.dirname(_this_dir))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


@dataclass
class Chromosome:
    sequence: List[str]
    machine_assignment: Dict[Tuple[str, int], str]
    fitness: float = 0.0


class GeneticAlgorithm:

    def __init__(
        self,
        data: Dict[str, pd.DataFrame],
        population_size=30,
        n_generations=50,
        crossover_rate=0.85,
        mutation_rate=0.15,
        tournament_size=3,
        simul_time=1000.0,
    ):
        self.data = data
        self.population_size = population_size
        self.n_generations = n_generations
        self.crossover_rate = crossover_rate
        self.mutation_rate = mutation_rate
        self.tournament_size = tournament_size
        self.simul_time = simul_time

        self._preprocess_data()
        self.history: List[float] = []

    def _preprocess_data(self):
        jobs_df = self.data['jobs']
        ops_df = self.data['operations']
        machines_df = self.data['machines']
        op_machine_df = self.data['operation_machine_map']
        setup_times_df = self.data['setup_times']

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

        self.machine_process_times: Dict[Tuple, float] = {
            (row['machine_id'], row['op_id']): row['process_time']
            for _, row in op_machine_df.iterrows()
        }

        self.setup_time_map: Dict[Tuple, float] = {
            (row['machine_group'], row['from_job_type'], row['to_job_type']): row['setup_time']
            for _, row in setup_times_df.iterrows()
        }

        self.machine_group: Dict[str, str] = (
            machines_df.set_index('machine_id')['machine_group'].to_dict()
        )

        self.job_ids: List[str] = list(self.jobs.keys())
        self.sequence_template: List[str] = []
        for job_id in self.job_ids:
            n_ops = len(self.job_ops.get(job_id, []))
            self.sequence_template.extend([job_id] * n_ops)

    def _get_setup_time(self, machine_id, from_job_type, to_job_type):
        if from_job_type is None or from_job_type == to_job_type:
            return 0.0
        group = self.machine_group.get(machine_id, '')
        return self.setup_time_map.get((group, from_job_type, to_job_type), 0.0)

    def _create_random_chromosome(self) -> Chromosome:
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
        return [self._create_random_chromosome() for _ in range(self.population_size)]

    def decode(self, chromosome: Chromosome) -> List[Dict]:
        sequence = chromosome.sequence
        machine_assignment = chromosome.machine_assignment

        job_op_counter: Dict[str, int] = {}
        machine_avail: Dict[str, float] = {}
        machine_last_type: Dict[str, str] = {}
        job_ready: Dict[str, float] = {}
        job_discarded: set = set()

        schedule: List[Dict] = []

        for job_id in sequence:
            if job_id in job_discarded:
                continue

            job_op_counter[job_id] = job_op_counter.get(job_id, 0) + 1
            op_seq = job_op_counter[job_id]

            op_info = self.op_info_map.get((job_id, op_seq))
            if op_info is None:
                continue

            op_id = op_info['op_id']
            qtime = op_info['qtime']
            job_info = self.jobs[job_id]
            job_type = job_info['job_type']

            if op_seq == 1:
                ready_time = float(job_info['release_time'])
            else:
                ready_time = job_ready.get(job_id, float(job_info['release_time']))

            machine_id = machine_assignment.get((job_id, op_seq))
            if machine_id is None:
                continue

            m_avail = machine_avail.get(machine_id, 0.0)
            assign_time = max(ready_time, m_avail)

            wait_time = assign_time - ready_time
            if wait_time > qtime:
                job_discarded.add(job_id)
                schedule.append({
                    'job_id': job_id, 'op_seq': op_seq, 'op_id': op_id,
                    'machine_id': machine_id,
                    'ready_time': ready_time, 'assign_time': assign_time,
                    'setup_start': assign_time, 'work_start': assign_time,
                    'end_time': assign_time,
                    'wait_time': wait_time, 'qtime_limit': qtime,
                    'setup_time': 0.0, 'process_time': 0.0,
                    'status': 'qtime_violated',
                })
                continue

            from_type = machine_last_type.get(machine_id)
            setup_time = self._get_setup_time(machine_id, from_type, job_type)
            process_time = self.machine_process_times.get((machine_id, op_id), 0.0)

            work_start = assign_time + setup_time
            end_time = work_start + process_time

            machine_avail[machine_id] = end_time
            machine_last_type[machine_id] = job_type
            job_ready[job_id] = end_time

            schedule.append({
                'job_id': job_id, 'op_seq': op_seq, 'op_id': op_id,
                'machine_id': machine_id,
                'ready_time': ready_time, 'assign_time': assign_time,
                'setup_start': assign_time, 'work_start': work_start,
                'end_time': end_time,
                'wait_time': wait_time, 'qtime_limit': qtime,
                'setup_time': setup_time, 'process_time': process_time,
                'status': 'completed',
            })

        return schedule

    def evaluate(self, chromosome: Chromosome) -> float:
        schedule = self.decode(chromosome)
        if not schedule:
            return 0.0

        completed = [s for s in schedule if s['status'] == 'completed']
        violated  = [s for s in schedule if s['status'] == 'qtime_violated']

        op_counts = {j: len(ops) for j, ops in self.job_ops.items()}
        done_ops: Dict[str, int] = {}
        for s in completed:
            done_ops[s['job_id']] = done_ops.get(s['job_id'], 0) + 1
        completed_jobs = {j for j, cnt in done_ops.items() if cnt == op_counts.get(j, 0)}

        n_jobs = len(self.job_ids)
        total_ops = len(self.sequence_template)

        completed_rate = len(completed_jobs) / n_jobs if n_jobs > 0 else 0.0
        violation_rate = len(violated) / total_ops if total_ops > 0 else 0.0

        makespan = max((s['end_time'] for s in completed), default=0.0)
        makespan_score = 1.0 / (1.0 + makespan / max(self.simul_time, 1.0))

        total_tard = 0.0
        for job_id in completed_jobs:
            due = float(self.jobs[job_id]['due_date'])
            job_end = max(s['end_time'] for s in completed if s['job_id'] == job_id)
            total_tard += max(0.0, job_end - due)
        avg_tard = total_tard / len(completed_jobs) if completed_jobs else makespan
        tardiness_score = 1.0 / (1.0 + avg_tard / max(self.simul_time, 1.0))

        fitness = (
            0.50 * completed_rate
            + 0.25 * makespan_score
            + 0.15 * (1.0 - violation_rate)
            + 0.10 * tardiness_score
        )
        return fitness

    def _ox_crossover(self, p1: List[str], p2: List[str]) -> Tuple[List[str], List[str]]:
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

            return child

        return ox(p1, p2), ox(p2, p1)

    def crossover(self, parent1: Chromosome, parent2: Chromosome) -> Tuple[Chromosome, Chromosome]:
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
        candidates = random.sample(population, min(self.tournament_size, len(population)))
        return max(candidates, key=lambda c: c.fitness)

    def _print_schedule(self, schedule: List[Dict]):
        print("\n[공정 시뮬레이션 로그]")
        print("-" * 60)

        events: List[Tuple[float, str]] = []
        for s in schedule:
            jid, oid, mid = s['job_id'], s['op_id'], s['machine_id']
            if s['status'] == 'completed':
                events.append((s['ready_time'],
                                f"Job {jid} is waiting for machine for operation {oid}"))
                events.append((s['setup_start'],
                                f"Job {jid} starts setup for operation {oid} on machine {mid}"))
                events.append((s['work_start'],
                                f"Job {jid} starts processing operation {oid} on machine {mid}"))
                events.append((s['end_time'],
                                f"Job {jid} finished operation {oid} on machine {mid}"))
            else:
                events.append((s['ready_time'],
                                f"Job {jid} is waiting for machine for operation {oid}"))
                events.append((s['assign_time'],
                                f"Job {jid} discarded due to qtime violation"))

        for t, msg in sorted(events, key=lambda x: x[0]):
            print(f"{round(t, 2)}\t{msg}")

    def _print_kpi(self, schedule: List[Dict], generation):
        completed = [s for s in schedule if s['status'] == 'completed']
        violated  = [s for s in schedule if s['status'] == 'qtime_violated']

        op_counts = {j: len(ops) for j, ops in self.job_ops.items()}
        done_ops: Dict[str, int] = {}
        for s in completed:
            done_ops[s['job_id']] = done_ops.get(s['job_id'], 0) + 1
        completed_jobs = {j for j, cnt in done_ops.items() if cnt == op_counts.get(j, 0)}

        n_jobs = len(self.job_ids)
        makespan = max((s['end_time'] for s in completed), default=0.0)

        total_tard = 0.0
        for jid in completed_jobs:
            due = float(self.jobs[jid]['due_date'])
            jend = max(s['end_time'] for s in completed if s['job_id'] == jid)
            total_tard += max(0.0, jend - due)
        avg_tard = total_tard / len(completed_jobs) if completed_jobs else 0.0

        busy: Dict[str, float] = {}
        for s in completed:
            m = s['machine_id']
            busy[m] = busy.get(m, 0.0) + s['setup_time'] + s['process_time']
        avg_util = (
            sum(busy.values()) / (makespan * len(busy))
            if makespan > 0 and busy else 0.0
        )

        print(f"\n[세대 {generation} KPI 요약]")
        print(f"  완료된 작업 수    : {len(completed_jobs)} / {n_jobs}")
        print(f"  Q-time 위반 건수  : {len(violated)} 건")
        print(f"  메이크스팬        : {round(makespan, 2)}")
        print(f"  평균 지연도       : {round(avg_tard, 2)}")
        print(f"  평균 장비 가동률  : {round(avg_util * 100, 1)} %")

    def _print_history(self):
        print("\n  [세대별 최적 적합도 변화]")
        recent = self.history[-10:]
        offset = len(self.history) - len(recent)
        for i, f in enumerate(recent):
            gen_num = offset + i + 1
            print(f"  세대 {gen_num:4d}: {f:.4f}")

    def run(self) -> Chromosome:
        print("=" * 60)
        print("  반도체 공정 스케줄링 유전 알고리즘 (독립 실행 v1)")
        print("=" * 60)
        print(f"  개체군 크기    : {self.population_size}")
        print(f"  세대 수        : {self.n_generations}")
        print(f"  교차율         : {self.crossover_rate}")
        print(f"  돌연변이율     : {self.mutation_rate}")
        print(f"  Job 수         : {len(self.job_ids)}")
        print(f"  총 Operation 수: {len(self.sequence_template)}")
        print("=" * 60)

        population = self.initialize_population()
        best_overall: Optional[Chromosome] = None

        for gen in range(1, self.n_generations + 1):
            for chrom in population:
                chrom.fitness = self.evaluate(chrom)

            best_gen = max(population, key=lambda c: c.fitness)
            self.history.append(best_gen.fitness)

            if best_overall is None or best_gen.fitness > best_overall.fitness:
                best_overall = deepcopy(best_gen)

            print(f"\n{'=' * 60}")
            print(f"  세대 {gen} / {self.n_generations}")
            print(f"{'=' * 60}")

            best_schedule = self.decode(best_gen)
            self._print_schedule(best_schedule)
            self._print_kpi(best_schedule, gen)

            print(f"\n  현재 세대 최적 적합도  : {round(best_gen.fitness, 4)}")
            print(f"  전체 누적 최적 적합도  : {round(best_overall.fitness, 4)}")
            self._print_history()

            new_pop: List[Chromosome] = [deepcopy(best_gen)]
            while len(new_pop) < self.population_size:
                p1 = self.tournament_selection(population)
                p2 = self.tournament_selection(population)
                c1, c2 = self.crossover(p1, p2)
                new_pop.append(self.mutate(c1))
                if len(new_pop) < self.population_size:
                    new_pop.append(self.mutate(c2))
            population = new_pop

        print(f"\n{'=' * 60}")
        print("  최종 결과 (전체 최적 염색체)")
        print(f"{'=' * 60}")
        print(f"  전체 최적 적합도: {round(best_overall.fitness, 4)}")
        final_schedule = self.decode(best_overall)
        self._print_schedule(final_schedule)
        self._print_kpi(final_schedule, '최종')

        return best_overall
