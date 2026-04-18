import simpy
import pandas as pd
from typing import Optional, Dict, Tuple, Any

from .machine import Machine
from utils import EventLogger


class PlannedScheduler:
    """
    Gurobi Stage 1 결과(planned_schedule_df)를 그대로 따라가는 scheduler.

    기존 Scheduler와 최대한 비슷한 인터페이스를 유지:
    - get_matched_machine(job_id, op_seq)
    - put_back_machine(machine)

    추가 기능:
    - get_planned_info(job_id, op_seq)
    - enable_failure=False 이면 breakdown/repair 프로세스를 시작하지 않음
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

        # (job_id, op_seq) -> planned info
        self.__planned_schedule: Dict[Tuple[Any, int], Dict[str, Any]] = {}

        required_cols = {
            "job_id", "op_id", "op_seq", "machine_id", "start_time", "end_time"
        }
        missing = required_cols - set(planned_schedule_df.columns)
        if missing:
            raise ValueError(
                f"planned_schedule_df is missing required columns: {sorted(missing)}"
            )

        for _, row in planned_schedule_df.iterrows():
            key = (row["job_id"], int(row["op_seq"]))
            self.__planned_schedule[key] = {
                "job_id": row["job_id"],
                "op_id": row["op_id"],
                "op_seq": int(row["op_seq"]),
                "machine_id": row["machine_id"],
                "start_time": float(row["start_time"]),
                "end_time": float(row["end_time"]),
            }

        # 기존 Scheduler와 비슷하게 op table도 유지
        self.__op_table = operations_df.sort_values(
            ["job_id", "op_seq"]
        ).set_index(["job_id", "op_seq"])

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

            # 고장 기능을 켤 때만 down 프로세스 시작
            if self.__enable_failure:
                self.__broken_chk_events.append(env.process(machine.down()))

        if self.__enable_failure:
            env.process(self.__chk_machine_broken())

    # ------------------------------------------------------------------
    # Planned schedule helpers
    # ------------------------------------------------------------------
    def get_planned_info(self, job_id: Any, op_seq: int) -> Dict[str, Any]:
        """
        (job_id, op_seq)에 해당하는 planned schedule 정보 반환
        """
        key = (job_id, int(op_seq))
        if key not in self.__planned_schedule:
            raise KeyError(f"No planned schedule found for key={key}")
        return self.__planned_schedule[key]

    # ------------------------------------------------------------------
    # Failure / repair logic
    # ------------------------------------------------------------------
    def __chk_machine_broken(self):
        """
        기존 Scheduler와 동일한 고장 감시 프로세스
        """
        while True:
            broken_machines = yield self.__env.any_of(self.__broken_chk_events)
            for event in broken_machines:
                machine, is_broken = event.value
                self.__broken_chk_events.remove(event)

                if is_broken:
                    self.__env.process(self.__machine_repair(machine))

                self.__broken_chk_events.append(self.__env.process(machine.down()))

    def __machine_repair(self, machine: Machine):
        """
        머신 수리 프로세스
        """
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
        Gurobi planned schedule에 명시된 machine만 반환한다.
        기존 Scheduler와 같은 이름/시그니처를 유지한다.
        """
        planned = self.get_planned_info(job_id, op_seq)
        target_machine_id = planned["machine_id"]

        target = yield self.__machine_store.get(
            lambda x: x.id == target_machine_id and x.is_idle()
        )
        return target

    def put_back_machine(self, machine: Machine):
        """
        machine을 다시 store에 반환
        """
        self.__machine_store.put(machine)