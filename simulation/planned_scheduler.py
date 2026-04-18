import simpy
import pandas as pd
from typing import Dict, Tuple, Any, List

from .machine import Machine
from utils import EventLogger


class PlannedScheduler:
    """
    Gurobi Stage 1 결과(planned_schedule_df)를 replay하는 scheduler.

    특징
    ----
    1. optimizer가 정한 machine assignment를 그대로 따름
    2. machine별 planned sequence를 그대로 따름
    3. setup을 미리 시작하도록 machine을 넘겨줌
       -> simulation의 working start가 Gurobi start_time과 맞도록 설계
    4. enable_failure=False이면 breakdown/repair 비활성화
    """

    def __init__(
        self,
        env: simpy.Environment,
        machine_df: pd.DataFrame,
        operations_df: pd.DataFrame,
        machine_failure_df: pd.DataFrame,
        setup_times_df: pd.DataFrame,
        op_machine_df: pd.DataFrame,
        planned_schedule_df: pd.DataFrame,
        event_logger: EventLogger,
        enable_failure: bool = False,
    ):
        self.__env = env
        self.__machine_store = simpy.FilterStore(env, capacity=float("inf"))
        self.__broken_chk_events = []
        self.__enable_failure = enable_failure
        self.__event_logger = event_logger

        required_cols = {
            "job_id", "op_id", "op_seq", "machine_id", "start_time", "end_time"
        }
        missing = required_cols - set(planned_schedule_df.columns)
        if missing:
            raise ValueError(
                f"planned_schedule_df is missing required columns: {sorted(missing)}"
            )

        # ---- base tables ----
        self.__ops = operations_df.sort_values(["job_id", "op_seq"]).copy()
        self.__jobs = None  # jobs_df는 직접 받지 않으므로 operations/other tables에서 못 얻음
        self.__machine_df = machine_df.copy()
        self.__setup_df = setup_times_df.copy()

        # op_id -> meta
        self.__op_meta = {
            row["op_id"]: row.to_dict()
            for _, row in self.__ops.iterrows()
        }

        # job_id -> ordered operations
        self.__job_ops = (
            self.__ops.groupby("job_id")["op_id"]
            .apply(list)
            .to_dict()
        )

        # machine_id -> machine_group
        self.__machine_group = dict(
            zip(machine_df["machine_id"], machine_df["machine_group"])
        )

        # job_id -> job_type
        # operations_df에는 보통 job_type이 없으므로, planned_schedule_df에 없으면
        # machine_df, setup_df만으로는 못 만들기 때문에 operations_df에 job_id만 있어도
        # 아래에서 job_type map이 꼭 필요함.
        # 현재 프로젝트에서는 jobs.csv를 별도로 이미 로드하고 있으므로,
        # notebook에서 아래 한 줄 추가 추천:
        # planned_scheduler.attach_job_types(data["jobs"])
        self.__job_type = {}

        # setup lookup: (machine_group, from_type, to_type) -> setup_time
        self.__setup_lookup = {
            (row["machine_group"], row["from_job_type"], row["to_job_type"]): float(row["setup_time"])
            for _, row in setup_times_df.iterrows()
        }

        # planned schedule raw lookup
        self.__planned_schedule: Dict[Tuple[Any, int], Dict[str, Any]] = {}
        for _, row in planned_schedule_df.iterrows():
            key = (row["job_id"], int(row["op_seq"]))
            self.__planned_schedule[key] = {
                "job_id": row["job_id"],
                "op_id": row["op_id"],
                "op_seq": int(row["op_seq"]),
                "machine_id": row["machine_id"],
                "start_time": float(row["start_time"]),   # processing start
                "end_time": float(row["end_time"]),
            }

        # machine별 planned sequence
        self.__machine_sequence: Dict[Any, List[Tuple[Any, int]]] = {}
        self.__machine_seq_index: Dict[Any, int] = {}

        sorted_planned = planned_schedule_df.sort_values(
            ["machine_id", "start_time", "job_id", "op_seq"]
        )
        for machine_id, grp in sorted_planned.groupby("machine_id"):
            seq = [(row["job_id"], int(row["op_seq"])) for _, row in grp.iterrows()]
            self.__machine_sequence[machine_id] = seq
            self.__machine_seq_index[machine_id] = 0

        # machine별 planned predecessor 기반 setup-before-processing 계산용
        self.__planned_setup_before: Dict[Tuple[Any, int], float] = {}

        # 기존 Scheduler와 비슷한 op table 유지
        self.__op_table = self.__ops.set_index(["job_id", "op_seq"])

        # Machine 객체 생성
        for machine_id, row in machine_df.set_index("machine_id").iterrows():
            machine_group = row["machine_group"]

            failure_info = machine_failure_df[
                machine_failure_df["machine_id"] == machine_id
            ].iloc[0].to_dict()

            setup_time_info = setup_times_df[
                setup_times_df["machine_group"] == machine_group
            ]

            process_time_info = op_machine_df[
                op_machine_df["machine_id"] == machine_id
            ]

            machine = Machine(
                env=env,
                id=machine_id,
                group=machine_group,
                failure_info=failure_info,
                setup_time_info=setup_time_info,
                process_time_info=process_time_info,
                event_logger=event_logger,
            )

            self.__machine_store.put(machine)

            if self.__enable_failure:
                self.__broken_chk_events.append(env.process(machine.down()))

        if self.__enable_failure:
            env.process(self.__chk_machine_broken())

    # ------------------------------------------------------------------
    # Optional hook: attach jobs_df after construction
    # ------------------------------------------------------------------
    def attach_job_types(self, jobs_df: pd.DataFrame):
        """
        jobs_df의 job_type 정보를 나중에 주입.
        notebook 수정 최소화를 위해 선택적으로 분리.
        """
        self.__job_type = dict(zip(jobs_df["job_id"], jobs_df["job_type"]))
        self.__build_planned_setup_before()

    # ------------------------------------------------------------------
    # Planned schedule helpers
    # ------------------------------------------------------------------
    def __build_planned_setup_before(self):
        """
        machine별 planned sequence를 바탕으로,
        각 (job_id, op_seq)의 processing start 전에 필요한 setup time 계산.
        """
        if not self.__job_type:
            raise ValueError(
                "job_type 정보가 없습니다. scheduler.attach_job_types(data['jobs'])를 먼저 호출하세요."
            )

        self.__planned_setup_before.clear()

        for machine_id, seq in self.__machine_sequence.items():
            grp = self.__machine_group[machine_id]
            prev_job_type = None

            for job_id, op_seq in seq:
                cur_job_type = self.__job_type[job_id]

                if prev_job_type is None or prev_job_type == cur_job_type:
                    s = 0.0
                else:
                    s = self.__setup_lookup.get((grp, prev_job_type, cur_job_type), 0.0)

                self.__planned_setup_before[(job_id, op_seq)] = float(s)
                prev_job_type = cur_job_type

    def get_planned_info(self, job_id: Any, op_seq: int) -> Dict[str, Any]:
        key = (job_id, int(op_seq))
        if key not in self.__planned_schedule:
            raise KeyError(f"No planned schedule found for key={key}")

        planned = dict(self.__planned_schedule[key])

        # attach setup_before if available
        planned["setup_before"] = self.__planned_setup_before.get(key, 0.0)
        planned["setup_start_time"] = planned["start_time"] - planned["setup_before"]
        return planned

    def _is_my_turn(self, job_id: Any, op_seq: int) -> bool:
        planned = self.get_planned_info(job_id, op_seq)
        machine_id = planned["machine_id"]

        seq = self.__machine_sequence[machine_id]
        idx = self.__machine_seq_index[machine_id]

        if idx >= len(seq):
            return False

        return seq[idx] == (job_id, int(op_seq))

    def _advance_machine_pointer(self, machine_id: Any):
        self.__machine_seq_index[machine_id] += 1

    # ------------------------------------------------------------------
    # Failure / repair logic
    # ------------------------------------------------------------------
    def __chk_machine_broken(self):
        while True:
            broken_machines = yield self.__env.any_of(self.__broken_chk_events)
            for event in broken_machines:
                machine, is_broken = event.value
                self.__broken_chk_events.remove(event)

                if is_broken:
                    self.__env.process(self.__machine_repair(machine))

                self.__broken_chk_events.append(self.__env.process(machine.down()))

    def __machine_repair(self, machine: Machine):
        with machine.resource.request(priority=-1, preempt=True) as req:
            yield req
            yield self.__machine_store.get(lambda x: x.id == machine.id)
            yield self.__env.process(machine.repair())
        self.__machine_store.put(machine)

    # ------------------------------------------------------------------
    # Main scheduler interface
    # ------------------------------------------------------------------
    def get_matched_machine(self, job_id: Any, op_seq: int):
        """
        planned machine / planned order / planned setup-start를 고려해
        machine을 반환한다.

        핵심:
        - scheduler는 'processing start'가 아니라
          'setup start' 시점에 machine을 넘겨준다.
        """
        planned = self.get_planned_info(job_id, op_seq)
        target_machine_id = planned["machine_id"]
        setup_start = max(0.0, planned["setup_start_time"])

        while True:
            # 아직 setup 시작 전이면 기다림
            if self.__env.now < setup_start:
                yield self.__env.timeout(setup_start - self.__env.now)

            # machine sequence에서 내 차례가 아니면 잠깐 대기
            if not self._is_my_turn(job_id, op_seq):
                yield self.__env.timeout(0.01)
                continue

            # 해당 machine이 idle이면 꺼내서 반환
            target = yield self.__machine_store.get(
                lambda x: x.id == target_machine_id and x.is_idle()
            )

            # machine 넘기는 순간 sequence pointer 전진
            self._advance_machine_pointer(target_machine_id)
            return target

    def put_back_machine(self, machine: Machine):
        self.__machine_store.put(machine)