from abc import ABCMeta, abstractmethod
from typing import List

from jobless.models.job import CompletedJob


class JobsLog:
    __metaclass__ = ABCMeta

    @abstractmethod
    def save(self, session, completed_job: CompletedJob):
        pass

    @abstractmethod
    def get_completed_jobs(self, session, job_id, limit, offset) -> List[CompletedJob]:
        pass

    @abstractmethod
    def session_scope(self):
        pass