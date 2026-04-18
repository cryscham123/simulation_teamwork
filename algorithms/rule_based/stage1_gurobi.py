"""
FJSP MILP Solver (Gurobi)

목적함수: min  Cmax + w * Σ δ[j,k]
          Cmax  = makespan
          δ[j,k] = Q-Time 위반량 (op_seq k 완료 후 op_seq k+1 시작까지의 초과 대기 시간)

제약조건:
  1. 각 오퍼레이션은 해당 그룹의 머신 중 정확히 하나에 배정
  2. 같은 Job 내 오퍼레이션 순서 준수 (Sequence constraint)
  3. 동일 머신 위에서 두 오퍼레이션은 겹칠 수 없음 (No-overlap with setup times)
  4. Release time 반영
  5. Makespan ≥ 모든 오퍼레이션 완료 시각
  6. Q-Time violation 계산 (소프트 제약)
"""

from __future__ import annotations

import itertools
from typing import Dict, List, Tuple

import pandas as pd

from .base import BaseScheduler


class FJSPGurobiScheduler(BaseScheduler):
    """
    Gurobi MILP 기반 FJSP 스케줄러

    Parameters
    ----------
    jobs_df : DataFrame
        columns: job_id, job_type, release_time, due_date, priority
    operations_df : DataFrame
        columns: job_id, op_id, op_seq, op_group
    machines_df : DataFrame
        columns: machine_id, machine_group
    op_machine_df : DataFrame
        columns: op_id, machine_id, process_time
    qtime_constraints_df : DataFrame
        columns: job_id, from_op_seq, to_op_seq, max_qtime
    setup_times_df : DataFrame
        columns: machine_group, from_job_type, to_job_type, setup_time
    qtime_weight : float
        목적함수에서 Q-Time violation에 부여하는 가중치
    time_limit : float
        Gurobi 최적화 시간 제한 (초)
    """

    def __init__(
        self,
        jobs_df: pd.DataFrame,
        operations_df: pd.DataFrame,
        machines_df: pd.DataFrame,
        op_machine_df: pd.DataFrame,
        qtime_constraints_df: pd.DataFrame,
        setup_times_df: pd.DataFrame,
        qtime_weight: float = 1.0,
        time_limit: float = 300.0,
    ):
        self._jobs = jobs_df.copy()
        self._ops = operations_df.copy()
        self._machines = machines_df.copy()
        self._op_machine = op_machine_df.copy()
        self._qtime = qtime_constraints_df.copy()
        self._setup = setup_times_df.copy()
        self._w = qtime_weight
        self._time_limit = time_limit

    # ------------------------------------------------------------------
    # 전처리 헬퍼
    # ------------------------------------------------------------------

    def _build_indices(self):
        """모델 구성에 필요한 인덱스·파라미터 사전을 빌드한다."""
        ops = self._ops.sort_values(['job_id', 'op_seq'])

        # op_id → (job_id, op_seq, op_group)
        op_meta: Dict[str, dict] = {
            row['op_id']: row.to_dict()
            for _, row in ops.iterrows()
        }

        # job_id → job_type
        job_type: Dict[str, str] = dict(
            zip(self._jobs['job_id'], self._jobs['job_type'])
        )

        # job_id → release_time
        release: Dict[str, float] = dict(
            zip(self._jobs['job_id'], self._jobs['release_time'].astype(float))
        )

        # op_id → list of eligible machine_ids
        eligible: Dict[str, List[str]] = (
            self._op_machine.groupby('op_id')['machine_id']
            .apply(list)
            .to_dict()
        )

        # (op_id, machine_id) → process_time
        proc: Dict[Tuple[str, str], float] = {
            (row['op_id'], row['machine_id']): float(row['process_time'])
            for _, row in self._op_machine.iterrows()
        }

        # (machine_group, from_job_type, to_job_type) → setup_time
        setup: Dict[Tuple[str, str, str], float] = {
            (row['machine_group'], row['from_job_type'], row['to_job_type']): float(row['setup_time'])
            for _, row in self._setup.iterrows()
        }

        # machine_id → machine_group
        mach_group: Dict[str, str] = dict(
            zip(self._machines['machine_id'], self._machines['machine_group'])
        )

        # (job_id, from_op_seq, to_op_seq) → max_qtime
        qtime: Dict[Tuple[str, int, int], float] = {
            (row['job_id'], int(row['from_op_seq']), int(row['to_op_seq'])): float(row['max_qtime'])
            for _, row in self._qtime.iterrows()
        }

        # job_id → ordered list of op_ids
        job_ops: Dict[str, List[str]] = (
            ops.groupby('job_id')['op_id']
            .apply(list)
            .to_dict()
        )

        # BigM: 넉넉한 상한 (모든 처리 시간 + 셋업 시간 합계)
        big_m = (
            self._op_machine['process_time'].sum()
            + self._setup['setup_time'].sum()
            + self._jobs['release_time'].max()
        )

        return op_meta, job_type, release, eligible, proc, setup, mach_group, qtime, job_ops, big_m

    # ------------------------------------------------------------------
    # 공개 인터페이스
    # ------------------------------------------------------------------

    def solve(self) -> pd.DataFrame:
        """
        Gurobi MILP를 실행하고 최적(또는 최선) 스케줄을 반환한다.

        Returns
        -------
        DataFrame with columns:
            job_id, op_id, op_seq, machine_id, start_time, end_time
        """
        try:
            import gurobipy as gp
            from gurobipy import GRB
        except ImportError as e:
            raise ImportError(
                "gurobipy가 설치되어 있지 않습니다. "
                "`pip install gurobipy` 후 유효한 라이선스를 설정하세요."
            ) from e

        (
            op_meta, job_type, release, eligible,
            proc, setup, mach_group, qtime, job_ops, big_m
        ) = self._build_indices()

        all_ops = list(op_meta.keys())
        all_machines = list(self._machines['machine_id'])

        model = gp.Model("FJSP")
        model.Params.TimeLimit = self._time_limit
        model.Params.LogToConsole = 1

        # ── 변수 ─────────────────────────────────────────────────────────

        # x[i, m] = 1  오퍼레이션 i 를 머신 m 에 배정
        x = {}
        for op_id in all_ops:
            for m in eligible[op_id]:
                x[op_id, m] = model.addVar(vtype=GRB.BINARY, name=f"x_{op_id}_{m}")

        # S[i] : 오퍼레이션 i 의 처리 시작 시각 (setup 완료 후)
        S = {
            op_id: model.addVar(lb=0.0, vtype=GRB.CONTINUOUS, name=f"S_{op_id}")
            for op_id in all_ops
        }

        # C[i] : 오퍼레이션 i 의 처리 완료 시각
        C = {
            op_id: model.addVar(lb=0.0, vtype=GRB.CONTINUOUS, name=f"C_{op_id}")
            for op_id in all_ops
        }

        # y[i1, i2, m] = 1  머신 m 위에서 i1 이 i2 보다 먼저 처리됨
        # 동일 그룹 내 오퍼레이션 쌍에만 생성
        y = {}
        for m in all_machines:
            grp = mach_group[m]
            ops_on_m = [op for op in all_ops if m in eligible[op]]
            for i1, i2 in itertools.combinations(ops_on_m, 2):
                y[i1, i2, m] = model.addVar(vtype=GRB.BINARY, name=f"y_{i1}_{i2}_{m}")
                y[i2, i1, m] = model.addVar(vtype=GRB.BINARY, name=f"y_{i2}_{i1}_{m}")

        # delta[j, from_seq, to_seq] : Q-Time 위반량 (≥ 0)
        delta = {}
        for _, row in self._qtime.iterrows():
            key = (row['job_id'], int(row['from_op_seq']), int(row['to_op_seq']))
            delta[key] = model.addVar(lb=0.0, vtype=GRB.CONTINUOUS, name=f"delta_{key}")

        # Cmax : makespan
        Cmax = model.addVar(lb=0.0, vtype=GRB.CONTINUOUS, name="Cmax")

        model.update()

        # ── 제약 ─────────────────────────────────────────────────────────

        # 1. 각 오퍼레이션은 정확히 하나의 머신에 배정
        for op_id in all_ops:
            model.addConstr(
                gp.quicksum(x[op_id, m] for m in eligible[op_id]) == 1,
                name=f"assign_{op_id}"
            )

        # 2. 완료 시각 = 시작 시각 + 처리 시간 (배정 머신에 따라 결정)
        for op_id in all_ops:
            model.addConstr(
                C[op_id] == S[op_id] + gp.quicksum(
                    x[op_id, m] * proc[op_id, m] for m in eligible[op_id]
                ),
                name=f"completion_{op_id}"
            )

        # 3. Job 내 오퍼레이션 순서 (Sequence constraint)
        for job_id, op_list in job_ops.items():
            for k in range(len(op_list) - 1):
                pred, succ = op_list[k], op_list[k + 1]
                model.addConstr(
                    S[succ] >= C[pred],
                    name=f"seq_{pred}_{succ}"
                )

        # 4. Release time
        for job_id, op_list in job_ops.items():
            first_op = op_list[0]
            model.addConstr(
                S[first_op] >= float(release[job_id]),
                name=f"release_{first_op}"
            )

        # 5. 동일 머신 위 No-overlap (sequence-dependent setup 포함)
        for m in all_machines:
            grp = mach_group[m]
            ops_on_m = [op for op in all_ops if m in eligible[op]]
            for i1, i2 in itertools.combinations(ops_on_m, 2):
                j1 = op_meta[i1]['job_id']
                j2 = op_meta[i2]['job_id']
                t1, t2 = job_type[j1], job_type[j2]
                s12 = setup.get((grp, t1, t2), 0.0)
                s21 = setup.get((grp, t2, t1), 0.0)

                # i1 → i2 순서일 때: S[i2] ≥ S[i1] + p[i1,m] + setup(t1→t2)
                model.addConstr(
                    S[i2] >= S[i1] + proc[i1, m] + s12
                    - big_m * (1 - y[i1, i2, m])
                    - big_m * (1 - x[i1, m])
                    - big_m * (1 - x[i2, m]),
                    name=f"nooverlap_{i1}_{i2}_{m}_fwd"
                )
                # i2 → i1 순서일 때
                model.addConstr(
                    S[i1] >= S[i2] + proc[i2, m] + s21
                    - big_m * y[i1, i2, m]
                    - big_m * (1 - x[i1, m])
                    - big_m * (1 - x[i2, m]),
                    name=f"nooverlap_{i1}_{i2}_{m}_bwd"
                )
                # 두 op이 같은 머신에 배정됐을 때 반드시 하나의 순서를 선택
                model.addConstr(
                    y[i1, i2, m] + y[i2, i1, m] >= x[i1, m] + x[i2, m] - 1,
                    name=f"order_lb_{i1}_{i2}_{m}"
                )
                model.addConstr(
                    y[i1, i2, m] + y[i2, i1, m] <= x[i1, m],
                    name=f"order_ub1_{i1}_{i2}_{m}"
                )
                model.addConstr(
                    y[i1, i2, m] + y[i2, i1, m] <= x[i2, m],
                    name=f"order_ub2_{i1}_{i2}_{m}"
                )

        # 6. Makespan ≥ 모든 오퍼레이션 완료 시각
        for op_id in all_ops:
            model.addConstr(Cmax >= C[op_id], name=f"makespan_{op_id}")

        # 7. Q-Time violation (소프트 제약)
        #    delta[j,k,k+1] ≥ S[op_{k+1}] - C[op_k] - max_qtime
        for (job_id, from_seq, to_seq), max_qt in qtime.items():
            op_list = job_ops[job_id]
            pred_op = op_list[from_seq - 1]
            succ_op = op_list[to_seq - 1]
            key = (job_id, from_seq, to_seq)
            model.addConstr(
                delta[key] >= S[succ_op] - C[pred_op] - max_qt,
                name=f"qtime_{job_id}_{from_seq}_{to_seq}"
            )

        # ── 목적함수 ──────────────────────────────────────────────────────
        qtime_violation = gp.quicksum(delta.values()) if delta else 0
        model.setObjective(Cmax + self._w * qtime_violation, GRB.MINIMIZE)

        # ── 최적화 ────────────────────────────────────────────────────────
        model.optimize()

        if model.Status not in (GRB.OPTIMAL, GRB.TIME_LIMIT, GRB.SUBOPTIMAL):
            raise RuntimeError(
                f"Gurobi가 실행 가능한 해를 찾지 못했습니다. Status={model.Status}"
            )

        # ── 결과 추출 ─────────────────────────────────────────────────────
        records = []
        for op_id in all_ops:
            assigned_m = next(
                m for m in eligible[op_id]
                if x[op_id, m].X > 0.5
            )
            pt = proc[op_id, assigned_m]
            st = S[op_id].X
            records.append({
                'job_id':     op_meta[op_id]['job_id'],
                'op_id':      op_id,
                'op_seq':     op_meta[op_id]['op_seq'],
                'machine_id': assigned_m,
                'start_time': round(st, 4),
                'end_time':   round(st + pt, 4),
            })

        result_df = pd.DataFrame(records).sort_values(['job_id', 'op_seq']).reset_index(drop=True)

        print(f"\n[FJSP 결과]  Makespan={round(Cmax.X, 4)}  "
              f"Q-Time Violation={round(sum(d.X for d in delta.values()), 4)}")

        return result_df
