from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import List

from jobless.models.job import Job


class JobsRepo:
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_job(self, session, job_id: str):
        """Retrieves a single job."""
        pass

    @abstractmethod
    def get_window(self, session, window: timedelta) -> List[Job]:
        """Retrieves a list of jobs within a time window."""
        pass

    @abstractmethod
    def insert(self, session, job: Job) -> None:
        """Inserts a job into the schedule."""
        pass

    @abstractmethod
    def update(self, session, job: Job) -> None:
        """Updates a job in the schedule."""

    @abstractmethod
    def delete(self, session, job_id: str) -> None:
        """Deletes a job from the schedule."""
        pass

    @contextmanager
    @abstractmethod
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        pass

    @staticmethod
    def adjust_time_to_process(job: Job):
        """Adjusts ttp in the past to now."""
        if job.time_to_process < datetime.now():
            job.time_to_process = datetime.now()
