from abc import ABCMeta, abstractmethod

from jobless.models.job import CompletedJob


class JobsLog:
    __metaclass__ = ABCMeta

    @abstractmethod
    def save(self, session, completed_job: CompletedJob):
        pass

    @abstractmethod
    def session_scope(self):
        pass