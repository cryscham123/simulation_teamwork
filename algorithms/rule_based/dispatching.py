from __future__ import annotations

import math
import random
from typing import Dict, List
from numpy import inf
import simpy

from algorithms.base import Algorithm
from simulation import Job, Machine


# simulation/ 패키지의 private 데이터에 접근하는 helper.
def _job_cur_seq(job: Job) -> int:
    return job._Job__cur_seq  # type: ignore[attr-defined]


def _job_op_id(job: Job):
    cs = _job_cur_seq(job)
    if cs == 0:
        return None
    return job._Job__op_seq[cs - 1][0]  # type: ignore[attr-defined]


def _job_op_group(job: Job):
    cs = _job_cur_seq(job)
    if cs == 0:
        return None
    return job._Job__op_group[cs - 1][0]  # type: ignore[attr-defined]


def _job_type(job: Job):
    return job._Job__type  # type: ignore[attr-defined]


def _job_remaining_qtime(job: Job) -> float:
    """
    Q-time 위반까지 남은 시간.
      - 첫 operation 이전(cur_seq==0) → inf
      - Q-time 한계가 inf → inf
      - 이미 위반 중 → 음수(= 위반 경과 시간)
      - 그 외 → 현 구간의 Q-time 한계값(보수적 상한)
    """
    cs = _job_cur_seq(job)
    if cs == 0:
        return float('inf')
    qlim = float(job._Job__qtime[cs - 1])  # type: ignore[attr-defined]
    if qlim == float('inf'):
        return float('inf')
    if job._Job__is_over_qtime:  # type: ignore[attr-defined]
        return -(job._Job__env.now - job._Job__qtime_over_time_start)  # type: ignore[attr-defined]
    return qlim


def _machine_hazard_params(machine: Machine):
    h0 = machine._Machine__base_hazard  # type: ignore[attr-defined]
    hr = machine._Machine__hazard_increase_rate  # type: ignore[attr-defined]
    return h0, hr


class RuleBasedDispatch(Algorithm):
    def __init__(
        self,
        env: simpy.Environment,
        qtime_urgency_factor: float = 1.5,
        pm_hazard_threshold: float = 0.1
    ):
        """
        Args:
            env: SimPy 환경 (PM 절대 시각 계산에 사용).
            qtime_urgency_factor: Remaining Q-time 이 후보 평균 processing time
                * factor 미만이면 Q-time urgent 로 판단.
            pm_hazard_threshold: 누적 고장률 PM 임계치. 기본 0.15 (= 15%).
        """
        self._env = env
        self.qtime_urgency_factor = qtime_urgency_factor
        self.pm_hazard_threshold = pm_hazard_threshold
        # machine_id → PM 이 예정된 절대 시뮬레이션 시각
        # calculate_PM_time 이 호출될 때마다 갱신된다.
        self._pm_due_at: Dict[int, float] = {}


    # Rule 1 : Job → Machine dispatching
    def match_job_machine(self, job: Job, machine_list: List[Machine]) -> Machine:
        now = job._Job__env.now  # type: ignore[attr-defined]
        op_group = _job_op_group(job)
        op_id = _job_op_id(job)
        job_type = _job_type(job)

        idle_candidates = [
            m for m in machine_list
            if m.group == op_group
            and m.is_idle()
            # PM 타임이 지났거나 이미 PM/REPAIRING 상태인 머신은 제외
            and m.cur_state == Machine.State.IDLE
            and self._pm_due_at.get(m.id, inf) > now
        ]
        candidates = idle_candidates if idle_candidates else [
            m for m in machine_list if m.group == op_group
        ]
        if not candidates:
            raise RuntimeError(
                f"No machine in group '{op_group}' for job {job.id} "
                f"(op_id={op_id})"
            )

        def ect(m: Machine) -> float:
            return m.get_setup_time(job_type) + m.get_process_time(op_id)

        avg_proc = sum(m.get_process_time(op_id) for m in candidates) / len(candidates)
        urgency_threshold = avg_proc * self.qtime_urgency_factor
        _is_urgent = _job_remaining_qtime(job) < urgency_threshold  # noqa: F841

        return min(candidates, key=ect)

    # Rule 2 : Failure time — inverse-CDF 샘플링
    def calculate_down_time(self, machine: Machine) -> float:
        h0, hr = _machine_hazard_params(machine)
        u = random.random()
        if hr > 0:
            return (-h0 + math.sqrt(h0 ** 2 - 2.0 * hr * math.log(u))) / hr
        if h0 > 0:
            return -math.log(u) / h0
        return inf

    # Rule 3 : PM timing — 누적 고장률이 threshold 에 도달하는 t*
    #          + 절대 PM 예정 시각을 _pm_due_at 에 기록
    def calculate_PM_time(self, machine: Machine) -> float:
        h0, hr = _machine_hazard_params(machine)
        thr = self.pm_hazard_threshold
        if hr > 0:
            t_star = (-h0 + math.sqrt(h0 * h0 + 2.0 * hr * thr)) / hr
        elif h0 > 0:
            t_star = thr / h0
        else:
            t_star = inf

        # PM / 수리 완료 시점(= env.now)에서 t_star 후가 다음 PM 예정 시각
        self._pm_due_at[machine.id] = self._env.now + t_star
        return t_star
