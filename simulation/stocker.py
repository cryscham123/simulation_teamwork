from .job import Job
import simpy
import os
import random

class Stocker():
    def __init__(self, env, signal, op_machine=None, job_priority=None):
        self.__resource = simpy.FilterStore(env, capacity=float('inf'))
        self.machine_end_signal = signal
        self.__op_machine = op_machine
        # job_id → 순위 인덱스. 낮을수록 우선. lookup O(1).
        self.__job_priority = (
            {jid: i for i, jid in enumerate(job_priority)} if job_priority is not None else None
        )
        env.process(self.wait_until_machine_ready())

    def run(self, job:Job):
        yield self.__resource.put(job)
        job.prev_not_completed = True

    def __select_job(self, candidates, machine, rule):
        """
        JOB_RULE에 따라 stocker의 candidates 중 하나의 job을 선택
        """
        if rule == 'random':
            return random.choice(candidates)
        if rule == 'FIFO':
            return candidates[0]
        if rule == 'SPT':
            return min(
                candidates,
                key=lambda j: machine.get_process_time(j.get_current_operation())
            )
        if rule == 'LPT':
            return max(
                candidates,
                key=lambda j: machine.get_process_time(j.get_current_operation())
            )
        if rule == 'MIN_QTIME':
            return min(candidates, key=lambda j: j.get_remain_qtime())
        if rule == 'SPTSSU':
            return min(
                candidates,
                key=lambda j: machine.get_setup_time(j.job_type)
                + machine.get_process_time(j.get_current_operation())
            )
        raise ValueError(f"알 수 없는 JOB_RULE 값: {rule}")

    def wait_until_machine_ready(self):
        """
        machine이 idle 신호를 보내면 JOB_RULE에 따라 stocker에서 job 한 개를 선택해 dispatch
        """
        while True:
            machine = yield self.machine_end_signal.get()
            if self.__op_machine is not None:
                # GA 모드: GA가 이 머신에 배정한 job들만 후보
                candidates = [
                    x for x in self.__resource.items
                    if self.__op_machine[x.get_current_operation()] == machine.id
                ]
            else:
                # 룰 기반: 같은 group이면 후보
                candidates = [
                    x for x in self.__resource.items
                    if x.get_op_group() == machine.group
                ]
            if len(candidates) == 0:
                continue
            if self.__job_priority is not None:
                # GA 모드: 우선순위 인덱스가 낮은 job 선택
                best = min(candidates, key=lambda j: self.__job_priority[j.id])
            else:
                rule = os.getenv('JOB_RULE', 'random')
                best = self.__select_job(candidates, machine, rule)
            job = yield self.__resource.get(lambda x: x is best)
            # job에 맞는 machine이 idle이 되면 다시 dispatch
            job.operation_end_signal.put(False)
