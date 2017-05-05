from datetime import datetime
from dateutil.parser import parse as date_parse
from enum import Enum
import json
from uuid import uuid4


class Status(Enum):
    READY = 'READY'
    DISPATCHED = 'DISPATCHED'


class Job(object):
    _status = None

    def __init__(self,
                 time_to_process: datetime, status: str,
                 command: str, args: str='{}', schedule: str=None,
                 on_success: str=None, on_failure: str=None,
                 id=None):

        if id is None:
            id = str(uuid4())
        self.id = id
        if type(time_to_process) == str:
            time_to_process = date_parse(time_to_process)
        self.time_to_process = time_to_process
        self.command = command
        self.args = json.loads(args)
        self.status = status
        if schedule is not None:
            schedule = json.loads(schedule)
        self.schedule = schedule
        if on_success is not None:
            on_success = json.loads(on_success)
        self.on_success = on_success
        if on_failure is not None:
            on_failure = json.loads(on_failure)
        self.on_failure = on_failure

    @property
    def status(self):
        return self._status.value

    @status.setter
    def status(self, status):
        self._status = Status(status)

    def __repr__(self):
        return str(self.to_dict())

    def __gt__(self, other):
        return self.time_to_process > other.time_to_process

    def __eq__(self, other):
        me_dict = self.to_dict()
        other_dict = other.to_dict()
        return me_dict == other_dict

    def to_dict(self):
        schedule = self.schedule
        if schedule is not None:
            schedule = json.dumps(schedule)
        on_success = self.on_success
        if on_success is not None:
            on_success = json.dumps(on_success)
        on_failure = self.on_failure
        if on_failure is not None:
            on_failure = json.dumps(on_failure)

        return {
            'id': self.id,
            'time_to_process': str(self.time_to_process),
            'schedule': schedule,
            'status': self.status,
            'command': self.command,
            'args': json.dumps(self.args),
            'on_success': on_success,
            'on_failure': on_failure
        }


class CompletedJob(Job):
    def __init__(self, success, result, processed_time, **kwargs):
        super().__init__(**kwargs)
        self.success = success
        self.result = result
        self.processed_time = processed_time

    def to_dict(self):
        output = super().to_dict()
        output.update({
            'success': self.success,
            'result': self.result,
            'processed_time': self.processed_time
        })
        return output
