import os
import random
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Any

import pandas as pd
import simpy


# =========================
# 유전알고리즘 파라미터
# =========================
POP_SIZE = 120
GENERATIONS = 150
TOURNAMENT_K = 5
CROSSOVER_RATE = 0.9
MUTATION_RATE_SEQ = 0.20
MUTATION_RATE_MAC = 0.35
ELITE_SIZE = 3
RANDOM_SEED = 42

# 목적함수 가중치
W_MAKESPAN = 1.0
W_QTIME = 1000.0
W_UNFINISHED = 100000.0
W_BREAKDOWN_DISCARD = 50000.0


@dataclass
class Individual:
    # 순서 유전자: 작업 ID를 공정 수만큼 반복하여 저장
    seq_gene: List[str]
    # 장비 유전자: 각 공정(op_id)을 어떤 장비에서 처리할지 지정
    machine_gene: Dict[str, str]
    fitness: float = float('inf')
    decoded: Optional[dict] = None


class LocalMachine:
    """GA 파일 내부에서만 사용하는 간단한 머신 클래스"""

    def __init__(self,
                 env: simpy.Environment,
                 machine_id: str,
                 machine_group: str,
                 process_time_info: pd.DataFrame,
                 setup_time_info: pd.DataFrame,
                 failure_info: Optional[Dict[str, Any]] = None):
        self.env = env
        self.id = machine_id
        self.group = machine_group
        self.resource = simpy.PreemptiveResource(env, capacity=1)

        self._process_times = process_time_info
        self._setup_times = setup_time_info
        self._last_job_type = None

        # 상대 코드 연결 지점:
        # 다른 사람이 만든 machine.py의 고장 로직과 동일하게 맞추고 싶다면
        # 아래 failure_info 부분을 그 파일의 데이터 구조에 맞춰 연결하면 된다.
        self._failure_enabled = failure_info is not None
        self._is_repaired = True
        self._last_repair_time = 0.0
        self._base_hazard = 0.0
        self._hazard_increase_rate = 0.0
        self._repair_time = 0.0

        if failure_info is not None:
            self._base_hazard = float(failure_info.get('base_hazard', 0.0))
            self._hazard_increase_rate = float(failure_info.get('hazard_increase_rate', 0.0))
            self._repair_time = float(failure_info.get('repair_time', 0.0))
            env.process(self._breakdown())

    def _breakdown(self):
        """고장 프로세스"""
        while True:
            lam = self._base_hazard + self._hazard_increase_rate * max(0.0, self.env.now - self._last_repair_time)
            if lam <= 0:
                return

            yield self.env.timeout(random.expovariate(lam))

            if not self._is_repaired:
                continue

            self._is_repaired = False
            with self.resource.request(priority=-1, preempt=True) as req:
                yield req
                print(f'{round(self.env.now, 2)}\tMachine {self.id} broke down')
                yield self.env.timeout(self._repair_time)
                self._last_repair_time = self.env.now
                self._last_job_type = None
                self._is_repaired = True
                print(f'{round(self.env.now, 2)}\tMachine {self.id} repaired')

    def is_idle(self) -> bool:
        """현재 사용 가능한 상태인지 확인"""
        return self.resource.count < self.resource.capacity

    def get_setup_time(self, job_type: str) -> float:
        """직전 품종과 현재 품종에 따른 셋업 시간 반환"""
        if self._last_job_type is None or self._last_job_type == job_type:
            return 0.0

        row = self._setup_times[
            (self._setup_times['from_job_type'] == self._last_job_type) &
            (self._setup_times['to_job_type'] == job_type)
        ]
        if row.empty:
            return 0.0
        return float(row['setup_time'].iloc[0])

    def get_process_time(self, op_id: str) -> float:
        """현재 머신에서 해당 공정의 처리 시간 반환"""
        row = self._process_times[self._process_times['op_id'] == op_id]
        if row.empty:
            raise ValueError(f'처리시간 정보가 없습니다. op_id={op_id}, machine_id={self.id}')
        return float(row['process_time'].iloc[0])

    def setup(self, job_type: str):
        """셋업 수행"""
        setup_time = self.get_setup_time(job_type)
        yield self.env.timeout(setup_time)
        self._last_job_type = job_type

    def work(self, op_id: str):
        """가공 수행"""
        process_time = self.get_process_time(op_id)
        yield self.env.timeout(process_time)


class GAControlledScheduler:
    """GA 염색체 정보를 이용하여 장비를 할당하는 스케줄러"""

    def __init__(self,
                 env: simpy.Environment,
                 machine_df: pd.DataFrame,
                 operation_machine_df: pd.DataFrame,
                 setup_times_df: pd.DataFrame,
                 dispatch_priority: Dict[Tuple[str, int], int],
                 preferred_machine: Dict[Tuple[str, int], str],
                 machine_failure_df: Optional[pd.DataFrame] = None):
        self.env = env
        self.dispatch_priority = dispatch_priority
        self.preferred_machine = preferred_machine

        self.machine_store = {
            group: simpy.FilterStore(env, capacity=float('inf'))
            for group in machine_df['machine_group'].unique()
        }

        self.machine_dict: Dict[str, LocalMachine] = {}
        failure_lookup: Dict[str, Dict[str, Any]] = {}

        if machine_failure_df is not None and not machine_failure_df.empty:
            failure_lookup = machine_failure_df.set_index('machine_id').to_dict('index')

        for _, row in machine_df.iterrows():
            machine_id = row['machine_id']
            machine_group = row['machine_group']

            process_time_info = operation_machine_df[
                operation_machine_df['machine_id'] == machine_id
            ]
            setup_time_info = setup_times_df[
                setup_times_df['machine_group'] == machine_group
            ]
            failure_info = failure_lookup.get(machine_id)

            machine = LocalMachine(
                env=env,
                machine_id=machine_id,
                machine_group=machine_group,
                process_time_info=process_time_info,
                setup_time_info=setup_time_info,
                failure_info=failure_info,
            )
            self.machine_dict[machine_id] = machine
            self.machine_store[machine_group].put(machine)

    def get_machine(self, job_id: str, op_seq: int, op_group: str):
        """
        GA가 지정한 장비를 우선 사용하고,
        해당 장비가 유휴가 아닐 경우에는 동일 그룹 내 다른 유휴 장비를 대기 후 사용한다.
        """
        preferred_id = self.preferred_machine.get((job_id, op_seq))
        priority_value = self.dispatch_priority.get((job_id, op_seq), 10**9)

        # 상대 코드 연결 지점:
        # 다른 사람이 만든 scheduler.py의 get_matched_machine을 그대로 쓰고 싶다면
        # 여기의 선택 규칙만 외부 스케줄러 내부 로직으로 옮기면 된다.
        # 지금은 GA 파일만 수정해야 하므로 동일 역할을 여기서 직접 수행한다.

        if preferred_id is not None:
            preferred_machine = yield self.machine_store[op_group].get(
                lambda x: x.id == preferred_id and x.is_idle()
            )
            return preferred_machine, priority_value

        target = yield self.machine_store[op_group].get(lambda x: x.is_idle())
        return target, priority_value

    def put_back_machine(self, machine: LocalMachine):
        """사용이 끝난 장비를 다시 스토어에 반환"""
        self.machine_store[machine.group].put(machine)


class GASimJob:
    """GA 염색체 기준으로 SimPy에서 직접 실행되는 작업 클래스"""

    def __init__(self,
                 env: simpy.Environment,
                 job_info: Dict[str, Any],
                 op_info: pd.DataFrame,
                 scheduler: GAControlledScheduler,
                 qtime_limit: Dict[Tuple[str, int, int], float],
                 result_log: List[Dict[str, Any]],
                 job_summary: Dict[str, Dict[str, Any]]):
        self.env = env
        self.job_id = job_info['job_id']
        self.job_type = job_info['job_type']
        self.release_time = float(job_info['release_time'])
        self.due_date = float(job_info['due_date'])
        self.priority = int(job_info['priority'])
        self.op_info = op_info.sort_values('op_seq').reset_index(drop=True)
        self.scheduler = scheduler
        self.qtime_limit = qtime_limit
        self.result_log = result_log
        self.job_summary = job_summary

        self.prev_op_end_time: Dict[int, float] = {}
        self.qtime_violation_count = 0
        self.qtime_violation_total = 0.0
        self.finished = False
        self.discarded_by_breakdown = False
        self.discarded_by_qtime = False

        env.process(self.run())

    def run(self):
        """작업 전체 공정 실행"""
        yield self.env.timeout(self.release_time)

        for _, op_row in self.op_info.iterrows():
            op_id = op_row['op_id']
            op_seq = int(op_row['op_seq'])
            op_group = op_row['op_group']

            # q-time 위반 여부는 '이전 공정 종료 ~ 현재 공정 시작' 대기시간으로 평가한다.
            q_violation = 0.0
            wait_time = 0.0
            q_limit = None

            machine, request_priority = yield self.env.process(
                self.scheduler.get_machine(self.job_id, op_seq, op_group)
            )

            with machine.resource.request(priority=request_priority, preempt=False) as req:
                try:
                    yield req
                except simpy.Interrupt:
                    self.discarded_by_breakdown = True
                    self._finalize_discard()
                    return

                start_time = self.env.now

                if op_seq >= 2:
                    prev_end = self.prev_op_end_time[op_seq - 1]
                    wait_time = start_time - prev_end
                    q_limit = self.qtime_limit.get((self.job_id, op_seq - 1, op_seq))
                    if q_limit is not None:
                        q_violation = max(0.0, wait_time - q_limit)
                        if q_violation > 0:
                            self.qtime_violation_count += 1
                            self.qtime_violation_total += q_violation

                setup_time = machine.get_setup_time(self.job_type)
                process_time = machine.get_process_time(op_id)

                try:
                    if setup_time > 0:
                        yield self.env.process(machine.setup(self.job_type))
                    else:
                        # 셋업시간이 0이어도 마지막 품종은 현재 작업 기준으로 갱신한다.
                        machine._last_job_type = self.job_type

                    yield self.env.process(machine.work(op_id))
                except simpy.Interrupt:
                    self.discarded_by_breakdown = True
                    self.scheduler.put_back_machine(machine)
                    self._finalize_discard()
                    return

                end_time = self.env.now
                self.prev_op_end_time[op_seq] = end_time

                self.result_log.append({
                    'job_id': self.job_id,
                    'op_id': op_id,
                    'op_seq': op_seq,
                    'machine_id': machine.id,
                    'machine_group': machine.group,
                    'job_type': self.job_type,
                    'release_time': self.release_time,
                    'request_priority': request_priority,
                    'setup_time': setup_time,
                    'process_time': process_time,
                    'start_time': start_time,
                    'end_time': end_time,
                    'wait_time_from_prev_op': wait_time,
                    'qtime_limit': q_limit,
                    'qtime_violation': q_violation,
                })

                self.scheduler.put_back_machine(machine)

        self.finished = True
        self.job_summary[self.job_id] = {
            'job_id': self.job_id,
            'finished': True,
            'discarded_by_qtime': False,
            'discarded_by_breakdown': False,
            'qtime_violation_count': self.qtime_violation_count,
            'qtime_violation_total': self.qtime_violation_total,
            'completion_time': self.prev_op_end_time[max(self.prev_op_end_time.keys())] if self.prev_op_end_time else None,
            'due_date': self.due_date,
            'tardiness': max(0.0, (self.prev_op_end_time[max(self.prev_op_end_time.keys())] - self.due_date)) if self.prev_op_end_time else None,
        }

    def _finalize_discard(self):
        """중도 폐기 시 요약 정보 기록"""
        self.job_summary[self.job_id] = {
            'job_id': self.job_id,
            'finished': False,
            'discarded_by_qtime': self.discarded_by_qtime,
            'discarded_by_breakdown': self.discarded_by_breakdown,
            'qtime_violation_count': self.qtime_violation_count,
            'qtime_violation_total': self.qtime_violation_total,
            'completion_time': None,
            'due_date': self.due_date,
            'tardiness': None,
        }


class GAScheduler:
    def __init__(self,
                 jobs_path: Optional[str] = None,
                 machines_path: Optional[str] = None,
                 operations_path: Optional[str] = None,
                 op_machine_map_path: Optional[str] = None,
                 qtime_path: Optional[str] = None,
                 setup_path: Optional[str] = None,
                 machine_failure_path: Optional[str] = None):
        random.seed(RANDOM_SEED)

        self.jobs_path = self._resolve_path(jobs_path, ['jobs.csv', os.path.join('schema', 'jobs.csv')])
        self.machines_path = self._resolve_path(machines_path, ['machines.csv', os.path.join('schema', 'machines.csv')])
        self.operations_path = self._resolve_path(operations_path, ['operations.csv', os.path.join('schema', 'operations.csv')])
        self.op_machine_map_path = self._resolve_path(op_machine_map_path, ['operation_machine_map.csv', os.path.join('schema', 'operation_machine_map.csv')])
        self.qtime_path = self._resolve_path(qtime_path, ['qtime_constraints.csv', os.path.join('schema', 'qtime_constraints.csv')])
        self.setup_path = self._resolve_path(setup_path, ['setup_times.csv', os.path.join('schema', 'setup_times.csv')])
        self.machine_failure_path = self._resolve_optional_path(machine_failure_path, ['machine_failure.csv', os.path.join('schema', 'machine_failure.csv')])

        self.jobs = pd.read_csv(self.jobs_path)
        self.machines = pd.read_csv(self.machines_path)
        self.operations = pd.read_csv(self.operations_path)
        self.op_machine_map = pd.read_csv(self.op_machine_map_path)
        self.qtimes = pd.read_csv(self.qtime_path)
        self.setup = pd.read_csv(self.setup_path)
        self.machine_failure = pd.read_csv(self.machine_failure_path) if self.machine_failure_path else None

        self._build_lookup()

    def _resolve_path(self, user_path: Optional[str], fallback_candidates: List[str]) -> str:
        """필수 파일 경로 탐색"""
        candidates = []
        if user_path:
            candidates.append(user_path)
        candidates.extend(fallback_candidates)

        for path in candidates:
            if path and os.path.exists(path):
                return path

        raise FileNotFoundError(f'필수 파일을 찾을 수 없습니다. 후보 경로: {candidates}')

    def _resolve_optional_path(self, user_path: Optional[str], fallback_candidates: List[str]) -> Optional[str]:
        """선택 파일 경로 탐색"""
        candidates = []
        if user_path:
            candidates.append(user_path)
        candidates.extend(fallback_candidates)

        for path in candidates:
            if path and os.path.exists(path):
                return path
        return None

    def _build_lookup(self):
        """데이터 전처리 및 조회용 딕셔너리 생성"""
        self.jobs = self.jobs.sort_values('job_id').reset_index(drop=True)
        self.machines = self.machines.sort_values('machine_id').reset_index(drop=True)
        self.operations = self.operations.sort_values(['job_id', 'op_seq']).reset_index(drop=True)
        self.op_machine_map = self.op_machine_map.sort_values(['op_id', 'machine_id']).reset_index(drop=True)

        self.job_ids = self.jobs['job_id'].tolist()
        self.machine_ids = self.machines['machine_id'].tolist()

        self.job_info = self.jobs.set_index('job_id').to_dict('index')
        self.job_type_of = {row['job_id']: row['job_type'] for _, row in self.jobs.iterrows()}
        self.release_of = {row['job_id']: float(row['release_time']) for _, row in self.jobs.iterrows()}
        self.due_of = {row['job_id']: float(row['due_date']) for _, row in self.jobs.iterrows()}
        self.priority_of = {row['job_id']: int(row['priority']) for _, row in self.jobs.iterrows()}

        self.ops_by_job: Dict[str, List[dict]] = {}
        self.num_ops_per_job: Dict[str, int] = {}
        self.op_id_by_job_seq: Dict[Tuple[str, int], str] = {}
        self.op_group_of: Dict[str, str] = {}

        for _, row in self.operations.iterrows():
            d = row.to_dict()
            job_id = row['job_id']
            op_seq = int(row['op_seq'])
            op_id = row['op_id']
            self.ops_by_job.setdefault(job_id, []).append(d)
            self.op_id_by_job_seq[(job_id, op_seq)] = op_id
            self.op_group_of[op_id] = row['op_group']

        self.num_ops_per_job = {job_id: len(op_list) for job_id, op_list in self.ops_by_job.items()}
        self.total_num_ops = len(self.operations)

        self.machine_group_of = {row['machine_id']: row['machine_group'] for _, row in self.machines.iterrows()}
        self.feasible_machines: Dict[str, List[str]] = {}
        self.proc_time: Dict[Tuple[str, str], float] = {}

        for _, row in self.op_machine_map.iterrows():
            op_id = row['op_id']
            machine_id = row['machine_id']
            process_time = float(row['process_time'])
            self.feasible_machines.setdefault(op_id, []).append(machine_id)
            self.proc_time[(op_id, machine_id)] = process_time

        self.qtime_limit: Dict[Tuple[str, int, int], float] = {}
        for _, row in self.qtimes.iterrows():
            key = (row['job_id'], int(row['from_op_seq']), int(row['to_op_seq']))
            self.qtime_limit[key] = float(row['max_qtime'])

        self.setup_time: Dict[Tuple[str, str, str], float] = {}
        for _, row in self.setup.iterrows():
            key = (row['machine_group'], row['from_job_type'], row['to_job_type'])
            self.setup_time[key] = float(row['setup_time'])

    def create_random_individual(self) -> Individual:
        """랜덤 개체 생성"""
        seq_gene: List[str] = []
        for job_id in self.job_ids:
            seq_gene.extend([job_id] * self.num_ops_per_job[job_id])
        random.shuffle(seq_gene)

        machine_gene: Dict[str, str] = {}
        for op_id in self.operations['op_id'].tolist():
            feasible = self.feasible_machines[op_id]
            machine_gene[op_id] = random.choice(feasible)

        ind = Individual(seq_gene=seq_gene, machine_gene=machine_gene)
        self.evaluate(ind)
        return ind

    def decode(self, ind: Individual) -> dict:
        """
        염색체를 바로 시뮬레이션용 우선순위 정보로 변환한다.

        반환값 설명
        - dispatch_priority: 각 (job_id, op_seq)가 몇 번째로 등장했는지
        - preferred_machine: 각 (job_id, op_seq)를 어떤 장비에서 처리할지
        """
        next_op_index = {job_id: 1 for job_id in self.job_ids}
        dispatch_priority: Dict[Tuple[str, int], int] = {}
        preferred_machine: Dict[Tuple[str, int], str] = {}

        for order_idx, job_id in enumerate(ind.seq_gene, start=1):
            op_seq = next_op_index[job_id]
            if op_seq > self.num_ops_per_job[job_id]:
                continue

            op_id = self.op_id_by_job_seq[(job_id, op_seq)]
            chosen_machine = ind.machine_gene.get(op_id)

            if chosen_machine not in self.feasible_machines[op_id]:
                chosen_machine = random.choice(self.feasible_machines[op_id])
                ind.machine_gene[op_id] = chosen_machine

            dispatch_priority[(job_id, op_seq)] = order_idx
            preferred_machine[(job_id, op_seq)] = chosen_machine
            next_op_index[job_id] += 1

        return {
            'dispatch_priority': dispatch_priority,
            'preferred_machine': preferred_machine,
        }

    def run_simulation(self, ind: Individual) -> dict:
        """염색체를 SimPy 환경에서 실제 실행하여 성능 계산"""
        decoded = self.decode(ind)
        env = simpy.Environment()
        result_log: List[Dict[str, Any]] = []
        job_summary: Dict[str, Dict[str, Any]] = {}

        scheduler = GAControlledScheduler(
            env=env,
            machine_df=self.machines,
            operation_machine_df=self.op_machine_map,
            setup_times_df=self.setup,
            dispatch_priority=decoded['dispatch_priority'],
            preferred_machine=decoded['preferred_machine'],
            machine_failure_df=self.machine_failure,
        )

        # 상대 코드 연결 지점:
        # 다른 사람이 만든 job.py를 직접 사용하려면
        # 아래 GASimJob 생성부를 외부 Job 클래스 호출로 바꾸고,
        # q-time 기록/결과 수집 로직만 주입하면 된다.
        for _, job_row in self.jobs.iterrows():
            job_id = job_row['job_id']
            op_info = self.operations[self.operations['job_id'] == job_id].copy()
            GASimJob(
                env=env,
                job_info=job_row.to_dict(),
                op_info=op_info,
                scheduler=scheduler,
                qtime_limit=self.qtime_limit,
                result_log=result_log,
                job_summary=job_summary,
            )

        env.run()

        schedule_df = pd.DataFrame(result_log)
        if not schedule_df.empty:
            schedule_df = schedule_df.sort_values(['start_time', 'machine_id', 'job_id']).reset_index(drop=True)
            makespan = float(schedule_df['end_time'].max())
        else:
            makespan = float('inf')

        job_summary_df = pd.DataFrame(job_summary.values())
        if job_summary_df.empty:
            unfinished_job_count = len(self.job_ids)
            breakdown_discard_count = 0
            total_qtime_violation = 0.0
            qtime_violation_count = 0
        else:
            unfinished_job_count = int((~job_summary_df['finished']).sum())
            breakdown_discard_count = int(job_summary_df['discarded_by_breakdown'].sum())
            total_qtime_violation = float(job_summary_df['qtime_violation_total'].sum())
            qtime_violation_count = int(job_summary_df['qtime_violation_count'].sum())

        return {
            'dispatch_priority': decoded['dispatch_priority'],
            'preferred_machine': decoded['preferred_machine'],
            'schedule_df': schedule_df,
            'job_summary_df': job_summary_df,
            'makespan': makespan,
            'total_qtime_violation': total_qtime_violation,
            'qtime_violation_count': qtime_violation_count,
            'unfinished_job_count': unfinished_job_count,
            'breakdown_discard_count': breakdown_discard_count,
        }

    def evaluate(self, ind: Individual) -> float:
        """시뮬레이션 결과를 이용해 적합도 계산"""
        decoded = self.run_simulation(ind)
        fitness = (
            W_MAKESPAN * decoded['makespan'] +
            W_QTIME * decoded['total_qtime_violation'] +
            W_UNFINISHED * decoded['unfinished_job_count'] +
            W_BREAKDOWN_DISCARD * decoded['breakdown_discard_count']
        )
        ind.fitness = fitness
        ind.decoded = decoded
        return fitness

    def tournament_selection(self, population: List[Individual]) -> Individual:
        """토너먼트 선택"""
        sampled = random.sample(population, TOURNAMENT_K)
        sampled.sort(key=lambda x: x.fitness)
        return sampled[0]

    def crossover_sequence(self, p1: List[str], p2: List[str]) -> List[str]:
        """작업 순서 유전자 교차"""
        size = len(p1)
        mask = [random.randint(0, 1) for _ in range(size)]
        child = [None] * size

        required_counts = {job_id: self.num_ops_per_job[job_id] for job_id in self.job_ids}
        used_counts = {job_id: 0 for job_id in self.job_ids}

        for i in range(size):
            if mask[i] == 1:
                job_id = p1[i]
                if used_counts[job_id] < required_counts[job_id]:
                    child[i] = job_id
                    used_counts[job_id] += 1

        p2_fill: List[str] = []
        temp_counts = used_counts.copy()
        for job_id in p2:
            if temp_counts[job_id] < required_counts[job_id]:
                p2_fill.append(job_id)
                temp_counts[job_id] += 1

        idx = 0
        for i in range(size):
            if child[i] is None:
                child[i] = p2_fill[idx]
                idx += 1

        return child

    def crossover_machine_gene(self, g1: Dict[str, str], g2: Dict[str, str]) -> Dict[str, str]:
        """장비 선택 유전자 교차"""
        child = {}
        for op_id in g1.keys():
            chosen = g1[op_id] if random.random() < 0.5 else g2[op_id]
            if chosen not in self.feasible_machines[op_id]:
                chosen = random.choice(self.feasible_machines[op_id])
            child[op_id] = chosen
        return child

    def crossover(self, p1: Individual, p2: Individual) -> Tuple[Individual, Individual]:
        """두 부모로부터 두 자식 생성"""
        if random.random() > CROSSOVER_RATE:
            c1 = Individual(seq_gene=p1.seq_gene[:], machine_gene=p1.machine_gene.copy())
            c2 = Individual(seq_gene=p2.seq_gene[:], machine_gene=p2.machine_gene.copy())
            return c1, c2

        c1_seq = self.crossover_sequence(p1.seq_gene, p2.seq_gene)
        c2_seq = self.crossover_sequence(p2.seq_gene, p1.seq_gene)
        c1_mac = self.crossover_machine_gene(p1.machine_gene, p2.machine_gene)
        c2_mac = self.crossover_machine_gene(p2.machine_gene, p1.machine_gene)
        return Individual(c1_seq, c1_mac), Individual(c2_seq, c2_mac)

    def mutate(self, ind: Individual):
        """돌연변이 수행"""
        if random.random() < MUTATION_RATE_SEQ:
            i, j = random.sample(range(len(ind.seq_gene)), 2)
            ind.seq_gene[i], ind.seq_gene[j] = ind.seq_gene[j], ind.seq_gene[i]

        if random.random() < MUTATION_RATE_MAC:
            num_changes = random.randint(1, max(1, self.total_num_ops // 10))
            target_ops = random.sample(self.operations['op_id'].tolist(), num_changes)
            for op_id in target_ops:
                ind.machine_gene[op_id] = random.choice(self.feasible_machines[op_id])

    def run(self) -> Tuple[Individual, pd.DataFrame]:
        """GA 반복 실행"""
        population = [self.create_random_individual() for _ in range(POP_SIZE)]
        history = []
        best = min(population, key=lambda x: x.fitness)

        for gen in range(GENERATIONS):
            population.sort(key=lambda x: x.fitness)
            new_population = population[:ELITE_SIZE]

            while len(new_population) < POP_SIZE:
                p1 = self.tournament_selection(population)
                p2 = self.tournament_selection(population)
                c1, c2 = self.crossover(p1, p2)
                self.mutate(c1)
                self.mutate(c2)
                self.evaluate(c1)
                self.evaluate(c2)
                new_population.append(c1)
                if len(new_population) < POP_SIZE:
                    new_population.append(c2)

            population = new_population
            current_best = min(population, key=lambda x: x.fitness)
            if current_best.fitness < best.fitness:
                best = current_best

            history.append({
                'generation': gen + 1,
                'best_fitness': best.fitness,
                'current_best_fitness': current_best.fitness,
                'best_makespan': best.decoded['makespan'],
                'best_total_qtime_violation': best.decoded['total_qtime_violation'],
                'best_qtime_violation_count': best.decoded['qtime_violation_count'],
                'best_unfinished_job_count': best.decoded['unfinished_job_count'],
                'best_breakdown_discard_count': best.decoded['breakdown_discard_count'],
            })

            if (gen + 1) % 10 == 0:
                print(
                    f"Gen {gen + 1:3d} | "
                    f"Best Fitness={best.fitness:.2f} | "
                    f"Makespan={best.decoded['makespan']:.2f} | "
                    f"Q-viol={best.decoded['total_qtime_violation']:.2f} | "
                    f"Unfinished={best.decoded['unfinished_job_count']}"
                )

        history_df = pd.DataFrame(history)
        return best, history_df


def main():
    # 로컬 실행 기준:
    # 1) 현재 파이썬 파일과 같은 폴더에 CSV가 있거나
    # 2) schema 폴더 아래에 CSV가 있으면 자동으로 탐색한다.
    scheduler = GAScheduler(
        jobs_path='jobs.csv',
        machines_path='machines.csv',
        operations_path='operations.csv',
        op_machine_map_path='operation_machine_map.csv',
        qtime_path='qtime_constraints.csv',
        setup_path='setup_times.csv',
        machine_failure_path='machine_failure.csv',
    )

    best, history_df = scheduler.run()

    print('\n========== 최종 결과 ==========' )
    print(f"최적 적합도              : {best.fitness:.2f}")
    print(f"Makespan                 : {best.decoded['makespan']:.2f}")
    print(f"총 Q-time 위반량         : {best.decoded['total_qtime_violation']:.2f}")
    print(f"Q-time 위반 횟수         : {best.decoded['qtime_violation_count']}")
    print(f"미완료 Job 수            : {best.decoded['unfinished_job_count']}")
    print(f"고장 중 폐기 Job 수      : {best.decoded['breakdown_discard_count']}")

    history_df.to_csv('ga_history.csv', index=False, encoding='utf-8-sig')
    if not best.decoded['schedule_df'].empty:
        best.decoded['schedule_df'].to_csv('best_schedule.csv', index=False, encoding='utf-8-sig')
    if not best.decoded['job_summary_df'].empty:
        best.decoded['job_summary_df'].to_csv('best_job_summary.csv', index=False, encoding='utf-8-sig')

    print('\n결과 파일 저장 완료')
    print('- ga_history.csv')
    print('- best_schedule.csv')
    print('- best_job_summary.csv')


if __name__ == '__main__':
    main()
