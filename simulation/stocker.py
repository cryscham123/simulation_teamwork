from .job import Job
import simpy
import os

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

    def wait_until_machine_ready(self):
        """
        machine이 가용 가능한 상태가 되면 stocker에서 할당이 가능한 작업 한 개만 다시 machine을 할당 받을 수 있는 상태로 만들어줌
        """
        while True:
            machine = yield self.machine_end_signal.get()
            # stocker에 작업이 있으면 꺼내고, 아니면 그냥 패스
            candidates = [x for x in self.__resource.items if x.get_op_group() == machine.group]
            if len(candidates) == 0:
                continue

            choice_method = os.getenv('MACHINE_CHOICE', 'random')
            if choice_method == 'QSPT':
                # QSPT: process_time + urgency_factor * remain_qtime 이 가장 작은 job 우선
                # remain_qtime이 작을수록(초과에 가까울수록) 점수가 낮아져 먼저 선택됨
                urgency_factor = float(os.getenv('QTIME_URGENCY_FACTOR', '1.0'))
                best_job = min(
                    candidates,
                    key=lambda j: machine.get_process_time(j.get_current_operation())
                                  + urgency_factor * j.get_remain_qtime()
                )
                job = yield self.__resource.get(lambda x: x is best_job)
            else:
                job = yield self.__resource.get(lambda x: x.get_op_group() == machine.group)

            # job에 맞는 machine이 idle이 되면 다시 dispatch
            job.operation_end_signal.put(False)
