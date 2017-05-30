from contextlib import contextmanager
from time import sleep

from redlock import Redlock

from jobless.jobs_service.locks.base import Lock, LockException


class RedlockJobsLock(Lock):
    LOCK_NAME = "jobs_lock"

    def __init__(self, hosts, validity_time_seconds=10):
        self.dlm = self._redlock(hosts)
        self.validity_time = validity_time_seconds*1000

    @staticmethod
    def _redlock(hosts):
        return Redlock(hosts)

    def _attempt_lock(self):
        return self.dlm.lock(self.LOCK_NAME, self.validity_time)

    def lock(self, block=True):
        """Keep trying to obtain a lock every second for twice the validity time."""
        max_attempts = (self.validity_time / 1000) * 2
        attempts = 1
        lock = self._attempt_lock()

        while lock is False and block:
            lock = self._attempt_lock()
            if lock is False:
                if attempts == max_attempts:
                    raise LockException("Lock was not released in time")
                else:
                    sleep(1)
                attempts += 1
        return lock

    def unlock(self, lock):
        return self.dlm.unlock(lock)

    @contextmanager
    def lock_scope(self, block=True):
        lock = self.lock(block=block)
        try:
            yield lock
        finally:
            self.unlock(lock)
