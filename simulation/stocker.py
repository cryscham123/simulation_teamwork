from .job import Job
import simpy
import os
import random

class AllwaysTrueGroup():
    def __eq__(self, other):
        return True

class Stocker():
    def __init__(self, env, signal):
        self.__env = env
        self.__id = 'stocker'
        self.group = AllwaysTrueGroup()
        # 시스템 큐임
        self.__resource = simpy.FilterStore(env, capacity=float('inf'))
        self.machine_end_signal = signal
        env.process(self.wait_until_machine_ready())

    @property
    def id(self):
        return self.__id

    def program_done(self):
        pass

    def get_process_time(self, op_id: int):
        return 0

    def get_setup_time(self, job_type: str):
        return 0

    def is_idle(self):
        return True

    def set_busy(self, status):
        pass

    def run(self, job:Job):
        yield self.__resource.put(job)
        job.prev_stocker = True

    def __select_job(self, candidates, machine, rule):
        """
        JOB_RULE에 따라 stocker의 candidates 중 하나의 job을 선택
        """
        if rule == 'random':
            return random.choice(candidates)
        if rule == 'SPT':
            return min(
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
            candidates = [
                x for x in self.__resource.items
                if x.get_op_group() == machine.group
            ]
            if not candidates:
                continue
            rule = os.getenv('JOB_RULE', 'random')
            best = self.__select_job(candidates, machine, rule)
            job = yield self.__resource.get(lambda x: x is best)
            # job에 맞는 machine이 idle이 되면 다시 dispatch
            job.operation_end_signal.put(False)
