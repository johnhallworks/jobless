# -*- coding: utf-8; -*-

"""Test suite for redlock jobs lock."""
from unittest import mock

import pytest

from jobless.jobs_service.locks.redlock import RedlockJobsLock
from jobless.jobs_service.locks.base import LockException


@mock.patch('jobless.jobs_service.locks.redlock.RedlockJobsLock._redlock')
@pytest.fixture(name='redlock_jobs_lock')
def redlock_lock_service_fixture(redlock):
    redlock.return_value = mock.MagicMock()
    return RedlockJobsLock([])


@mock.patch('jobless.jobs_service.locks.redlock.RedlockJobsLock._redlock')
@pytest.fixture(name='redlock_jobs_lock_blocked')
def redlock_lock_service_blocked_fixture(redlock):
    redlock.return_value = False
    jl = RedlockJobsLock([])
    jl._attempt_lock = mock.MagicMock(return_value=False)
    return jl


def test_lock_scope(redlock_jobs_lock):
    """Tests the lock context manager."""
    with redlock_jobs_lock.lock_scope() as lock:
        assert len(redlock_jobs_lock.dlm.mock_calls) == 1
        fn_name, args, kwargs = redlock_jobs_lock.dlm.mock_calls[0]
        assert fn_name == 'lock'
        assert args == ('jobs_lock', redlock_jobs_lock.validity_time)
        assert len(kwargs) == 0

    assert len(redlock_jobs_lock.dlm.mock_calls) == 2
    fn_name, args, kwargs = redlock_jobs_lock.dlm.mock_calls[1]
    assert fn_name == 'unlock'
    assert args == (lock,)
    assert len(kwargs) == 0


def test_lock(redlock_jobs_lock_blocked):
    redlock_jobs_lock_blocked.validity_time=1000
    with pytest.raises(LockException):
        with redlock_jobs_lock_blocked.lock_scope() as lock:
            print(lock)
