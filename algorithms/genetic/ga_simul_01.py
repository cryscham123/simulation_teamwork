import os
import random
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import simpy

try:
    from data_loader import DataLoader
    from machine import Machine
except ImportError:
    # 패키지 내부에서 실행할 때를 위한 예외 처리
    from .data_loader import DataLoader
    from .machine import Machine


# 유전알고리즘 파라미터
POP_SIZE = 200
GENERATIONS = 200
TOURNAMENT_K = 5
CROSSOVER_RATE = 0.9
MUTATION_RATE_SEQ = 0.20
MUTATION_RATE_MAC = 0.45
ELITE_SIZE = 3
RANDOM_SEED = 42

# 목적함수 가중치
W_MAKESPAN = 1.0
W_QTIME = 1000.0
W_NOT_FINISHED = 100000.0
W_DISCARDED = 50000.0

# 시뮬레이션 시간 제한
SIM_TIME_LIMIT = 10**7


@dataclass
class Individual:
    # 염색체 하나가 스케줄 후보 하나를 의미한다.
    seq_gene: List[Any]
    machine_gene: Dict[Any, Any]
    fitness: float = float("inf")
    decoded: Optional[dict] = None


class GAControlledScheduler:
    """
    GA가 만든 염색체를 실제 SimPy 환경에서 반영하기 위한 스케줄러

    핵심 아이디어
    1) seq_gene는 작업 투입 우선순서를 결정한다.
    2) machine_gene는 각 operation이 선호하는 machine을 결정한다.
    3) Job 프로세스는 이 스케줄러에 machine을 요청하고,
       스케줄러는 염색체 정보를 이용해 실제 machine을 할당한다.
    """

    def __init__(self,
                 env: simpy.Environment,
                 machine_df: pd.DataFrame,
                 operations_df: pd.DataFrame,
                 machine_failure_df: pd.DataFrame,
                 setup_times_df: pd.DataFrame,
                 op_machine_df: pd.DataFrame,
                 dispatch_priority: Dict[Tuple[Any, int], int],
                 preferred_machine: Dict[Tuple[Any, int], Any]):
        self.env = env
        self.dispatch_priority = dispatch_priority
        self.preferred_machine = preferred_machine

        # 상대 코드의 scheduler.py가 하던 머신 초기화 로직을
        # GA 파일 안으로 가져와서 그대로 재사용한다.
        # 외부 파일은 수정하지 않기 위해 여기에서 동일한 구조를 다시 만든다.
        self.machine_store = {
            group: simpy.FilterStore(env, capacity=float('inf'))
            for group in machine_df['machine_group'].unique()
        }

        self.machine_by_id: Dict[Any, Machine] = {}
        for machine_id, row in machine_df.set_index('machine_id').iterrows():
            machine_group = row['machine_group']

            failure_info = machine_failure_df[
                machine_failure_df['machine_id'] == machine_id
            ].iloc[0].to_dict()

            setup_time_info = setup_times_df[
                setup_times_df['machine_group'] == machine_group
            ]

            process_time_info = op_machine_df[
                op_machine_df['machine_id'] == machine_id
            ]

            machine = Machine(
                env=env,
                id=machine_id,
                group=machine_group,
                failure_info=failure_info,
                setup_time_info=setup_time_info,
                process_time_info=process_time_info
            )

            self.machine_by_id[machine_id] = machine
            self.machine_store[machine_group].put(machine)

        self.op_table = operations_df.sort_values(
            ['job_id', 'op_seq']
        ).set_index(['job_id', 'op_seq'])

        self.feasible_machine_map: Dict[Any, List[Any]] = {}
        for op_id, group_df in op_machine_df.groupby('op_id'):
            self.feasible_machine_map[op_id] = group_df['machine_id'].tolist()

    def get_matched_machine(self, job_id: Any, op_seq: int):
        """
        염색체가 지정한 machine_gene를 우선 사용한다.
        해당 machine이 현재 사용 불가이면 같은 그룹의 다른 feasible machine을 대체 사용한다.
        """
        op_row = self.op_table.loc[(job_id, op_seq)]
        op_group = op_row['op_group']
        op_id = op_row['op_id']

        # 염색체가 지정한 선호 machine
        preferred = self.preferred_machine.get((job_id, op_seq))
        feasible_machines = set(self.feasible_machine_map.get(op_id, []))

        # 먼저 염색체가 지정한 machine을 직접 노려본다.
        if preferred in feasible_machines:
            machine = yield self.machine_store[op_group].get(
                lambda x: x.id == preferred and x.is_idle()
            )
            return machine

        # 지정한 machine이 없거나 잘못된 경우 feasible machine 중 하나를 받는다.
        machine = yield self.machine_store[op_group].get(
            lambda x: x.id in feasible_machines and x.is_idle()
        )
        return machine

    def put_back_machine(self, machine: Machine):
        self.machine_store[machine.group].put(machine)

    def get_dispatch_rank(self, job_id: Any, op_seq: int) -> int:
        # 숫자가 작을수록 먼저 배치되도록 한다.
        return self.dispatch_priority.get((job_id, op_seq), 10**9)


class GASimJob:
    """
    기존 job.py를 직접 수정할 수 없으므로,
    GA 파일 안에서 염색체 해석이 가능한 Job 프로세스를 새로 정의한다.

    외부 코드와 연결되는 지점
    - Machine.setup()
    - Machine.work()
    - Scheduler 방식의 machine 반환
    위 세 부분은 상대 코드의 인터페이스를 그대로 사용한다.
    """

    def __init__(self,
                 env: simpy.Environment,
                 job_info: Dict[str, Any],
                 op_info: pd.DataFrame,
                 scheduler: GAControlledScheduler,
                 monitor: Dict[str, list]):
        self.env = env
        self.job_id = job_info['job_id']
        self.job_type = job_info['job_type']
        self.release_time = float(job_info['release_time'])
        self.due_date = float(job_info['due_date'])
        self.priority = float(job_info['priority'])
        self.scheduler = scheduler
        self.monitor = monitor

        self.op_df = op_info.sort_values('op_seq').reset_index(drop=True)
        self.qtime_values = self.op_df['qtime'].tolist() if 'qtime' in self.op_df.columns else [float('inf')] * len(self.op_df)

        self.job_process = env.process(self.run())
        self.sub_process: Optional[simpy.Process] = None
        self.cur_machine: Optional[Machine] = None
        self.current_stage: Optional[str] = None

        self.completed = False
        self.discarded = False
        self.discard_reason = None
        self.completed_op_count = 0
        self.last_op_end_time: Optional[float] = None
        self.qtime_violation_count = 0
        self.qtime_violation_total = 0.0

    def _record_event(self,
                      event_type: str,
                      op_id: Optional[Any] = None,
                      op_seq: Optional[int] = None,
                      machine_id: Optional[Any] = None,
                      start_time: Optional[float] = None,
                      end_time: Optional[float] = None,
                      extra: Optional[dict] = None):
        row = {
            'time': round(self.env.now, 6),
            'event_type': event_type,
            'job_id': self.job_id,
            'op_id': op_id,
            'op_seq': op_seq,
            'machine_id': machine_id,
            'start_time': start_time,
            'end_time': end_time,
        }
        if extra:
            row.update(extra)
        self.monitor['event_log'].append(row)

    def _record_operation(self,
                          op_id: Any,
                          op_seq: int,
                          machine_id: Any,
                          setup_start: float,
                          process_start: float,
                          end_time: float,
                          q_wait: float,
                          q_limit: Optional[float],
                          q_violation: float):
        self.monitor['schedule_log'].append({
            'job_id': self.job_id,
            'op_id': op_id,
            'op_seq': op_seq,
            'machine_id': machine_id,
            'setup_start': setup_start,
            'process_start': process_start,
            'end_time': end_time,
            'q_wait': q_wait,
            'q_limit': q_limit,
            'q_violation': q_violation,
        })

    def _check_qtime(self, allowed_wait: float):
        try:
            yield self.env.timeout(allowed_wait)
            self.job_process.interrupt(('qtime_timeout', allowed_wait))
        except simpy.Interrupt:
            pass

    def run(self):
        yield self.env.timeout(self.release_time)

        try:
            for _, row in self.op_df.iterrows():
                op_id = row['op_id']
                op_seq = int(row['op_seq'])
                q_limit = None
                if op_seq >= 2:
                    raw_q = self.qtime_values[op_seq - 1]
                    if pd.notna(raw_q):
                        q_limit = float(raw_q)

                # 현재 operation 순서를 기록한다.
                self._record_event(
                    event_type='op_ready',
                    op_id=op_id,
                    op_seq=op_seq,
                    extra={'dispatch_rank': self.scheduler.get_dispatch_rank(self.job_id, op_seq)}
                )

                qtime_process = None
                if q_limit is not None:
                    qtime_process = self.env.process(self._check_qtime(q_limit))

                operation_completed = False
                while not operation_completed:
                    try:
                        self.cur_machine = yield self.env.process(
                            self.scheduler.get_matched_machine(self.job_id, op_seq)
                        )

                        if qtime_process is not None:
                            try:
                                if qtime_process.is_alive:
                                    qtime_process.interrupt()
                            except Exception:
                                pass

                        with self.cur_machine.resource.request(
                            priority=self.scheduler.get_dispatch_rank(self.job_id, op_seq),
                            preempt=False
                        ) as req:
                            yield req

                            setup_start = self.env.now
                            self.current_stage = 'setup'
                            self._record_event(
                                event_type='setup_start',
                                op_id=op_id,
                                op_seq=op_seq,
                                machine_id=self.cur_machine.id,
                                start_time=setup_start
                            )
                            self.sub_process = self.env.process(self.cur_machine.setup(self.job_type))
                            yield self.sub_process

                            process_start = self.env.now
                            q_wait = 0.0
                            q_violation = 0.0
                            if self.last_op_end_time is not None:
                                q_wait = process_start - self.last_op_end_time
                                if q_limit is not None:
                                    q_violation = max(0.0, q_wait - q_limit)

                            self.current_stage = 'work'
                            self._record_event(
                                event_type='process_start',
                                op_id=op_id,
                                op_seq=op_seq,
                                machine_id=self.cur_machine.id,
                                start_time=process_start,
                                extra={
                                    'q_wait': q_wait,
                                    'q_limit': q_limit,
                                    'q_violation': q_violation,
                                }
                            )
                            self.sub_process = self.env.process(self.cur_machine.work(op_id))
                            yield self.sub_process

                            end_time = self.env.now
                            self._record_event(
                                event_type='process_end',
                                op_id=op_id,
                                op_seq=op_seq,
                                machine_id=self.cur_machine.id,
                                end_time=end_time
                            )
                            self._record_operation(
                                op_id=op_id,
                                op_seq=op_seq,
                                machine_id=self.cur_machine.id,
                                setup_start=setup_start,
                                process_start=process_start,
                                end_time=end_time,
                                q_wait=q_wait,
                                q_limit=q_limit,
                                q_violation=q_violation
                            )

                            self.completed_op_count += 1
                            self.last_op_end_time = end_time
                            self.qtime_violation_total += q_violation
                            if q_violation > 0:
                                self.qtime_violation_count += 1

                            operation_completed = True

                    except simpy.Interrupt:
                        if self.current_stage == 'setup':
                            self._record_event(
                                event_type='breakdown_during_setup',
                                op_id=op_id,
                                op_seq=op_seq,
                                machine_id=self.cur_machine.id if self.cur_machine else None
                            )
                            self.sub_process = None
                            if self.cur_machine is not None:
                                self.scheduler.put_back_machine(self.cur_machine)
                                self.cur_machine = None
                        elif self.current_stage == 'work':
                            self._record_event(
                                event_type='discarded_by_breakdown',
                                op_id=op_id,
                                op_seq=op_seq,
                                machine_id=self.cur_machine.id if self.cur_machine else None
                            )
                            self.sub_process = None
                            if self.cur_machine is not None:
                                self.scheduler.put_back_machine(self.cur_machine)
                                self.cur_machine = None
                            self.discarded = True
                            self.discard_reason = 'machine_breakdown_during_work'
                            return

                if self.cur_machine is not None:
                    self.scheduler.put_back_machine(self.cur_machine)
                    self.cur_machine = None
                self.sub_process = None
                self.current_stage = None

            self.completed = True
            self._record_event(event_type='job_completed', end_time=self.env.now)

        except simpy.Interrupt as interrupt_info:
            reason = interrupt_info.cause
            self.discarded = True
            self.discard_reason = 'qtime_violation'
            self._record_event(
                event_type='discarded_by_qtime',
                extra={'reason': reason}
            )
            if self.sub_process is not None:
                try:
                    self.sub_process.interrupt()
                except Exception:
                    pass
            if self.cur_machine is not None:
                self.scheduler.put_back_machine(self.cur_machine)
                self.cur_machine = None


class GAScheduler:
    def __init__(self,
                 base_data_path='schema',
                 jobs_path=None,
                 machines_path=None,
                 operations_path=None,
                 op_machine_map_path=None,
                 qtime_path='schema/qtime_constraints.csv',
                 setup_path=None,
                 machine_failure_path=None):
        random.seed(RANDOM_SEED)

        self.base_data_path = base_data_path

        # 상대방 data_loader.py를 우선 사용한다.
        # 외부 코드가 이미 schema 구조를 정리해두었으므로,
        # 가능한 한 그 구조를 그대로 가져오는 방식으로 연결한다.
        loader = DataLoader(base_data_path=base_data_path)
        data = loader.load_all_data()

        self.jobs = data['jobs'].copy()
        self.machines = data['machines'].copy()
        self.operations = data['operations'].copy()
        self.op_machine_map = data['operation_machine_map'].copy()
        self.machine_failure = data['machine_failure'].copy()
        self.setup = data['setup_times'].copy()

        # qtime 정보는 두 가지 경우를 모두 지원한다.
        # 1) operations.csv 안에 qtime 컬럼이 이미 있는 경우
        # 2) 기존 GA처럼 별도 qtime_constraints.csv를 사용하는 경우
        self.qtime_df = None
        if qtime_path is not None and os.path.exists(qtime_path):
            self.qtime_df = pd.read_csv(qtime_path)

        self._build_lookup()
        self._inject_qtime_to_operations()

    def _build_lookup(self):
        self.job_ids = self.jobs['job_id'].tolist()
        self.job_info = self.jobs.set_index('job_id').to_dict('index')
        self.job_type_of = {r['job_id']: r['job_type'] for _, r in self.jobs.iterrows()}
        self.release_of = {r['job_id']: float(r['release_time']) for _, r in self.jobs.iterrows()}
        self.due_of = {r['job_id']: float(r['due_date']) for _, r in self.jobs.iterrows()}
        self.priority_of = {r['job_id']: float(r['priority']) for _, r in self.jobs.iterrows()}

        self.operations = self.operations.sort_values(['job_id', 'op_seq']).reset_index(drop=True)
        self.ops_by_job: Dict[Any, List[dict]] = {}
        self.op_seq_of: Dict[Any, int] = {}
        self.op_group_of: Dict[Any, Any] = {}
        self.op_job_of: Dict[Any, Any] = {}
        self.op_id_by_job_seq: Dict[Tuple[Any, int], Any] = {}

        for _, row in self.operations.iterrows():
            row_dict = row.to_dict()
            self.ops_by_job.setdefault(row['job_id'], []).append(row_dict)
            self.op_seq_of[row['op_id']] = int(row['op_seq'])
            self.op_group_of[row['op_id']] = row['op_group']
            self.op_job_of[row['op_id']] = row['job_id']
            self.op_id_by_job_seq[(row['job_id'], int(row['op_seq']))] = row['op_id']

        self.total_num_ops = len(self.operations)
        self.num_ops_per_job = {job_id: len(rows) for job_id, rows in self.ops_by_job.items()}

        self.machine_ids = self.machines['machine_id'].tolist()
        self.machine_group_of = {
            row['machine_id']: row['machine_group']
            for _, row in self.machines.iterrows()
        }

        self.feasible_machines: Dict[Any, List[Any]] = {}
        self.proc_time: Dict[Tuple[Any, Any], float] = {}
        for _, row in self.op_machine_map.iterrows():
            op_id = row['op_id']
            machine_id = row['machine_id']
            process_time = float(row['process_time'])
            self.feasible_machines.setdefault(op_id, []).append(machine_id)
            self.proc_time[(op_id, machine_id)] = process_time

    def _inject_qtime_to_operations(self):
        if 'qtime' not in self.operations.columns:
            self.operations['qtime'] = pd.NA

        # operations 내부 qtime이 비어 있을 때만 외부 qtime 파일 정보를 반영한다.
        if self.qtime_df is None:
            return

        if {'job_id', 'from_op_seq', 'to_op_seq', 'max_qtime'}.issubset(self.qtime_df.columns):
            for _, row in self.qtime_df.iterrows():
                job_id = row['job_id']
                to_op_seq = int(row['to_op_seq'])
                max_qtime = row['max_qtime']
                mask = (self.operations['job_id'] == job_id) & (self.operations['op_seq'] == to_op_seq)
                self.operations.loc[mask, 'qtime'] = max_qtime

        self.operations = self.operations.sort_values(['job_id', 'op_seq']).reset_index(drop=True)
        self.ops_by_job = {}
        for _, row in self.operations.iterrows():
            self.ops_by_job.setdefault(row['job_id'], []).append(row.to_dict())

    def create_random_individual(self) -> Individual:
        seq_gene = []
        for job_id in self.job_ids:
            seq_gene.extend([job_id] * self.num_ops_per_job[job_id])
        random.shuffle(seq_gene)

        machine_gene = {}
        for op_id in self.operations['op_id'].tolist():
            machine_gene[op_id] = random.choice(self.feasible_machines[op_id])

        ind = Individual(seq_gene=seq_gene, machine_gene=machine_gene)
        self.evaluate(ind)
        return ind

    def decode(self, ind: Individual) -> dict:
        """
        여기서 시간표를 만드는 것이 아니라,
        염색체를 SimPy가 바로 사용할 수 있는 형태로 해석한다.

        seq_gene 해석 방식
        - 같은 job_id가 seq_gene에서 몇 번째로 등장했는지에 따라
          해당 job의 op_seq가 결정된다.

        machine_gene 해석 방식
        - 각 op_id가 어느 machine을 우선 사용할지 지정한다.
        """
        next_op_index = {job_id: 0 for job_id in self.job_ids}
        dispatch_priority: Dict[Tuple[Any, int], int] = {}
        preferred_machine: Dict[Tuple[Any, int], Any] = {}
        decoded_rows = []

        rank = 0
        for job_id in ind.seq_gene:
            current_index = next_op_index[job_id]
            if current_index >= self.num_ops_per_job[job_id]:
                continue

            op = self.ops_by_job[job_id][current_index]
            op_id = op['op_id']
            op_seq = int(op['op_seq'])
            machine_id = ind.machine_gene[op_id]

            if machine_id not in self.feasible_machines[op_id]:
                machine_id = random.choice(self.feasible_machines[op_id])
                ind.machine_gene[op_id] = machine_id

            dispatch_priority[(job_id, op_seq)] = rank
            preferred_machine[(job_id, op_seq)] = machine_id
            decoded_rows.append({
                'dispatch_rank': rank,
                'job_id': job_id,
                'op_id': op_id,
                'op_seq': op_seq,
                'preferred_machine': machine_id,
                'op_group': self.op_group_of[op_id],
            })

            next_op_index[job_id] += 1
            rank += 1

        return {
            'dispatch_df': pd.DataFrame(decoded_rows).sort_values('dispatch_rank').reset_index(drop=True),
            'dispatch_priority': dispatch_priority,
            'preferred_machine': preferred_machine,
        }

    def run_simulation(self, ind: Individual) -> dict:
        decoded = self.decode(ind)
        env = simpy.Environment()

        # 여기부터가 상대방 시뮬레이션 코드와 만나는 핵심 지점이다.
        # 외부 scheduler.py를 직접 고치지 않고,
        # 동일한 machine 초기화 구조를 사용하는 GA 전용 scheduler를 생성한다.
        scheduler = GAControlledScheduler(
            env=env,
            machine_df=self.machines,
            operations_df=self.operations,
            machine_failure_df=self.machine_failure,
            setup_times_df=self.setup,
            op_machine_df=self.op_machine_map,
            dispatch_priority=decoded['dispatch_priority'],
            preferred_machine=decoded['preferred_machine'],
        )

        monitor = {
            'event_log': [],
            'schedule_log': [],
            'job_log': [],
        }

        sim_jobs: List[GASimJob] = []
        for _, job_row in self.jobs.iterrows():
            job_id = job_row['job_id']
            job_ops = self.operations[self.operations['job_id'] == job_id].copy()

            # 여기서 기존 job.py 대신 GASimJob을 사용한다.
            # 이유는 기존 job.py는 염색체 기반 machine 선택 로직을 받을 수 없기 때문이다.
            sim_job = GASimJob(
                env=env,
                job_info=job_row.to_dict(),
                op_info=job_ops,
                scheduler=scheduler,
                monitor=monitor,
            )
            sim_jobs.append(sim_job)

        env.run(until=SIM_TIME_LIMIT)

        job_summary_rows = []
        completed_end_times = []
        qtime_violation_total = 0.0
        qtime_violation_count = 0
        discarded_count = 0
        unfinished_count = 0

        for sim_job in sim_jobs:
            if sim_job.completed and sim_job.last_op_end_time is not None:
                completed_end_times.append(sim_job.last_op_end_time)
            if sim_job.discarded:
                discarded_count += 1
            if (not sim_job.completed) and (not sim_job.discarded):
                unfinished_count += 1

            qtime_violation_total += sim_job.qtime_violation_total
            qtime_violation_count += sim_job.qtime_violation_count

            job_summary_rows.append({
                'job_id': sim_job.job_id,
                'completed': sim_job.completed,
                'discarded': sim_job.discarded,
                'discard_reason': sim_job.discard_reason,
                'completed_op_count': sim_job.completed_op_count,
                'total_op_count': len(self.ops_by_job[sim_job.job_id]),
                'last_op_end_time': sim_job.last_op_end_time,
                'qtime_violation_count': sim_job.qtime_violation_count,
                'qtime_violation_total': sim_job.qtime_violation_total,
            })

        schedule_df = pd.DataFrame(monitor['schedule_log'])
        event_df = pd.DataFrame(monitor['event_log'])
        job_summary_df = pd.DataFrame(job_summary_rows)

        if completed_end_times:
            makespan = max(completed_end_times)
        else:
            makespan = float(SIM_TIME_LIMIT)

        return {
            'dispatch_df': decoded['dispatch_df'],
            'schedule_df': schedule_df,
            'event_df': event_df,
            'job_summary_df': job_summary_df,
            'makespan': makespan,
            'total_qtime_violation': qtime_violation_total,
            'qtime_violation_count': qtime_violation_count,
            'discarded_count': discarded_count,
            'unfinished_count': unfinished_count,
        }

    def evaluate(self, ind: Individual) -> float:
        simulated = self.run_simulation(ind)
        fitness = (
            W_MAKESPAN * simulated['makespan']
            + W_QTIME * simulated['total_qtime_violation']
            + W_NOT_FINISHED * simulated['unfinished_count']
            + W_DISCARDED * simulated['discarded_count']
        )
        ind.fitness = fitness
        ind.decoded = simulated
        return fitness

    def tournament_selection(self, population: List[Individual]) -> Individual:
        sampled = random.sample(population, TOURNAMENT_K)
        sampled.sort(key=lambda x: x.fitness)
        return sampled[0]

    def crossover_sequence(self, p1: List[Any], p2: List[Any]) -> List[Any]:
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

        p2_fill = []
        temp_counts = used_counts.copy()
        for job_id in p2:
            if temp_counts[job_id] < required_counts[job_id]:
                p2_fill.append(job_id)
                temp_counts[job_id] += 1

        fill_idx = 0
        for i in range(size):
            if child[i] is None:
                child[i] = p2_fill[fill_idx]
                fill_idx += 1

        return child

    def crossover_machine_gene(self, g1: Dict[Any, Any], g2: Dict[Any, Any]) -> Dict[Any, Any]:
        child = {}
        for op_id in g1.keys():
            chosen = g1[op_id] if random.random() < 0.5 else g2[op_id]
            if chosen not in self.feasible_machines[op_id]:
                chosen = random.choice(self.feasible_machines[op_id])
            child[op_id] = chosen
        return child

    def crossover(self, p1: Individual, p2: Individual) -> Tuple[Individual, Individual]:
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
        if random.random() < MUTATION_RATE_SEQ:
            i, j = random.sample(range(len(ind.seq_gene)), 2)
            ind.seq_gene[i], ind.seq_gene[j] = ind.seq_gene[j], ind.seq_gene[i]

        if random.random() < MUTATION_RATE_MAC:
            num_changes = random.randint(1, max(1, self.total_num_ops // 10))
            op_ids = random.sample(self.operations['op_id'].tolist(), num_changes)
            for op_id in op_ids:
                ind.machine_gene[op_id] = random.choice(self.feasible_machines[op_id])

    def run(self):
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
                'best_discarded_count': best.decoded['discarded_count'],
                'best_unfinished_count': best.decoded['unfinished_count'],
            })

            if (gen + 1) % 10 == 0:
                print(
                    f"Gen {gen + 1:3d} | "
                    f"Best Fitness={best.fitness:.2f} | "
                    f"Makespan={best.decoded['makespan']:.2f} | "
                    f"Q-viol={best.decoded['total_qtime_violation']:.2f} | "
                    f"Discarded={best.decoded['discarded_count']} | "
                    f"Unfinished={best.decoded['unfinished_count']}"
                )

        history_df = pd.DataFrame(history)
        return best, history_df


def main():
    scheduler = GAScheduler(
        base_data_path='schema',
        qtime_path='schema/qtime_constraints.csv'
    )

    best, history_df = scheduler.run()

    print('\n========== FINAL RESULT ==========')
    print(f"Best fitness                : {best.fitness:.2f}")
    print(f"Makespan                    : {best.decoded['makespan']:.2f}")
    print(f"Total qtime violation       : {best.decoded['total_qtime_violation']:.2f}")
    print(f"Qtime violation count       : {best.decoded['qtime_violation_count']}")
    print(f"Discarded jobs              : {best.decoded['discarded_count']}")
    print(f"Unfinished jobs             : {best.decoded['unfinished_count']}")