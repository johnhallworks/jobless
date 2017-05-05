from abc import ABCMeta, abstractmethod
from contextlib import contextmanager


class Lock:
    __metaclass__ = ABCMeta

    @abstractmethod
    def lock(self) -> None:
        pass

    @abstractmethod
    def unlock(self, lock) -> None:
        pass

    @contextmanager
    @abstractmethod
    def lock_scope(self, block=True):
        pass


class LockException(Exception):
    pass
