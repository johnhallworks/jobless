# -*- coding: utf-8; -*-

"""Test suite for Scheduler."""
import json
import heapq
from copy import deepcopy
from unittest import mock

from jobless.models.job import Job


def test_get_jobs_heap(scheduler):
    """Tests that the function returns aheap of jobs in the correct order."""
    jobs_heap = scheduler.get_jobs_heap()
    previous_job = None
    assert len(jobs_heap) > 0
    while len(jobs_heap) is not 0:
        this_job = heapq.heappop(jobs_heap)
        if previous_job is not None:
            assert this_job > previous_job
            assert isinstance(this_job, Job)
        previous_job = this_job


def test_process_jobs(scheduler):
    """Tests that process_jobs processes all jobs in the heap."""
    jobs_heap = scheduler.get_jobs_heap()
    scheduler._process_job = mock.MagicMock()
    scheduler.process_jobs(deepcopy(jobs_heap))
    assert len(jobs_heap) > 0
    while len(jobs_heap) > 0:
        this_job = heapq.heappop(jobs_heap)
        name, args, kwargs = scheduler._process_job.mock_calls.pop(0)
        assert this_job == args[0]
