from typing import Optional
import simpy

class EventLogger:
    def __init__(self, env: simpy.Environment):
        self.__logs = []
        self.__env = env

    @property
    def logs(self):
        return self.__logs

    def log_event_start(self, id: int, event: str, description: Optional[str] = None):
        self.__logs.append({
            'id': id,
            'event': event,
            'description': description,
            'start': self.__env.now
        })
        return len(self.__logs) - 1

    def log_event_finish(self, index: int):
        if index < 0:
            return
        self.__logs[index]['finish'] = self.__env.now

    def log_point_event(self, id: int, event: str, description: Optional[str] = None):
        self.__logs.append({
            'id': id,
            'event': event,
            'description': description,
            'start': self.__env.now,
            'finish': self.__env.now + 1
        })

