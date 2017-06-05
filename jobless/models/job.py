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
    _on_success = None
    _on_failure = None

    def __init__(self,
                 time_to_process: datetime, command: str, status: str = Status.READY,
                 args: dict=None, schedule: dict=None, on_success=None, on_failure=None,
                 id=None):

        if id is None:
            id = str(uuid4())
        self.id = id

        # allow python or string datetime
        if type(time_to_process) == str:
            time_to_process = date_parse(time_to_process)
        self.time_to_process = time_to_process
        self.command = command
        self.args = args
        self.status = status
        self.schedule = schedule
        self.on_success = on_success
        self.on_failure = on_failure

    @property
    def status(self):
        return self._status.value

    @status.setter
    def status(self, status):
        self._status = Status(status)

    @property
    def on_success(self):
        if self._on_success is not None:
            return self._on_success

    @on_success.setter
    def on_success(self, on_success):
        if type(on_success) == dict:
            on_success = Job(**on_success)
        elif not (type(on_success) == Job or on_success is None):
            raise ValueError("on_success must be of type Job not: {0}"
                             .format(type(on_success)))
        self._on_success = on_success

    @property
    def on_failure(self):
        if self._on_failure is not None:
            return self._on_failure

    @on_failure.setter
    def on_failure(self, on_failure):
        if type(on_failure) == dict:
            on_failure = Job(**on_failure)
        elif not (type(on_failure) == Job or on_failure is None):
            raise ValueError("on_success must be of type Job not: {0}"
                             .format(type(on_failure)))
        self._on_failure = on_failure

    def __repr__(self):
        return str(self.to_dict())

    def __gt__(self, other):
        return self.time_to_process > other.time_to_process

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        for attr in self.__dict__.keys():
            if attr.startswith("_") or attr == 'time_to_process':
                continue

            self_attr_value = getattr(self, attr)
            other_attr_value = getattr(other, attr)

            if self_attr_value != other_attr_value:
                return False

        return (self.status == other.status and
                self.on_success == other.on_success and
                self.on_failure == other.on_failure)

    def to_dict(self):
        on_success = self.on_success
        if on_success is not None:
            on_success = on_success.to_dict()
        on_failure = self.on_failure
        if on_failure is not None:
            on_failure = on_failure.to_dict()

        return {
            'id': self.id,
            'time_to_process': str(self.time_to_process),
            'schedule': self.schedule,
            'status': self.status,
            'command': self.command,
            'args': self.args,
            'on_success': on_success,
            'on_failure': on_failure
        }


class CompletedJob(Job):
    def __init__(self, job_id: str, success: bool, result: str,
                 processed_time: datetime=None, **kwargs):
        super().__init__(**kwargs)
        self.job_id = job_id
        self.success = success
        self.result = result
        if type(processed_time) == str:
            processed_time = date_parse(processed_time)
        if processed_time is None:
            processed_time = datetime.now()
        self.processed_time = processed_time

    def to_dict(self):
        output = super().to_dict()
        output.update({
            'job_id': self.job_id,
            'success': self.success,
            'result': self.result,
            'processed_time': str(self.processed_time)
        })
        return output
