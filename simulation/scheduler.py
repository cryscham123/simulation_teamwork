import simpy
import pandas as pd
from .machine import Machine


class Scheduler:
    """시뮬레이션 환경의 스케줄러 클래스"""

    def __init__(self, env: simpy.Environment, machine_df: pd.DataFrame,
                 operations_df: pd.DataFrame, machine_failure_df: pd.DataFrame,
                 setup_times_df: pd.DataFrame, op_machine_df: pd.DataFrame,
                 preferred_machines: dict = None):
        """
        Scheduler 초기화

        Args:
            env: SimPy 환경
            machine_df: 머신 정보 DataFrame
            operations_df: 작업 정보 DataFrame
            machine_failure_df: 머신 고장 정보 DataFrame
            setup_times_df: 셋업 시간 정보 DataFrame
            op_machine_df: 작업-머신 매핑 정보 DataFrame
            preferred_machines: GA 머신 선택 염색체 {(job_id, op_seq): machine_id}
        """
        # 머신 그룹별로 FilterStore 생성
        self.__machine_store = {
            group: simpy.FilterStore(env, capacity=float('inf'))
            for group in machine_df['machine_group'].unique()
        }

        # 머신 인스턴스 생성 및 스토어에 추가
        for machine_id, row in machine_df.set_index('machine_id').iterrows():
            machine_group = row['machine_group']

            # 해당 머신의 고장 정보 가져오기
            failure_info = machine_failure_df[
                machine_failure_df['machine_id'] == machine_id
            ].iloc[0].to_dict()

            # 해당 머신 그룹의 셋업 시간 정보 가져오기
            setup_time_info = setup_times_df[
                setup_times_df['machine_group'] == machine_group
            ]

            # 해당 머신의 처리 시간 정보 가져오기
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

            self.__machine_store[machine_group].put(machine)

        self.__preferred_machines = preferred_machines or {}

        # 작업 테이블 설정
        self.__op_table = operations_df.sort_values(
            ['job_id', 'op_seq']
        ).set_index(['job_id', 'op_seq'])

    def get_matched_machine(self, job_id: int, op_seq: int):
        """
        주어진 작업에 매칭되는 유휴 머신 반환
        preferred_machines에 선호 머신이 지정된 경우 해당 머신이 idle이면 우선 선택,
        선호 머신이 없거나 busy/고장 중이면 그룹 내 임의 idle 머신으로 fallback

        Args:
            job_id: 작업 ID
            op_seq: 작업 시퀀스

        Returns:
            Machine: 할당된 머신
        """
        op_group = self.__op_table.loc[(job_id, op_seq), 'op_group']
        store = self.__machine_store[op_group]
        preferred_id = self.__preferred_machines.get((job_id, op_seq))

        if preferred_id is not None:
            pref_idle = any(x.id == preferred_id and x.is_idle() for x in store.items)
            if pref_idle:
                target = yield store.get(
                    lambda x, pid=preferred_id: x.id == pid and x.is_idle()
                )
                return target

        target = yield store.get(lambda x: x.is_idle())
        return target

    def put_back_machine(self, machine: Machine):
        """
        머신을 다시 스토어에 반환

        Args:
            machine: 반환할 머신
        """
        self.__machine_store[machine.group].put(machine)

