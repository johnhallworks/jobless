# -*- coding: utf-8; -*-

"""Test suite for Scheduler."""
import json
import heapq
from copy import deepcopy
from unittest import mock

from jobless.models.job import Job
from jobless.scheduler import Scheduler

from tests import base


class TestScheduler(base.TestCase):

    @mock.patch('jobless.jobs_service.jobs_service.JobsService')
    def setUp(self, jobs_service):
        with self.fixture('jobs.json') as fixture:
            jobs_arr = [Job(**job_json) for job_json in json.loads(fixture.read())]
            jobs_service.fetch_jobs = mock.MagicMock(return_value=jobs_arr)
            self.scheduler = Scheduler(jobs_service=jobs_service)

    def test_get_jobs_heap(self):
        """Tests that the function returns aheap of jobs in the correct order."""
        jobs_heap = self.scheduler.get_jobs_heap()
        previous_job = None
        assert len(jobs_heap) > 0
        while len(jobs_heap) is not 0:
            this_job = heapq.heappop(jobs_heap)
            if previous_job is not None:
                assert this_job > previous_job
                assert isinstance(this_job, Job)
            previous_job = this_job

    def test_process_jobs(self):
        """Tests that process_jobs processes all jobs in the heap."""
        jobs_heap = self.scheduler.get_jobs_heap()
        self.scheduler._process_job = mock.MagicMock()
        self.scheduler.process_jobs(deepcopy(jobs_heap))
        assert len(jobs_heap) > 0
        while len(jobs_heap) > 0:
            this_job = heapq.heappop(jobs_heap)
            name, args, kwargs = self.scheduler._process_job.mock_calls.pop(0)
            assert this_job == args[0]
