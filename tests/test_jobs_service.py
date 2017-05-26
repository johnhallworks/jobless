# -*- coding: utf-8; -*-

"""Test suite for JobsService."""
import json
from datetime import timedelta
from unittest import mock

from jobless.models.job import Job, Status
from jobless.jobs_service.jobs_service import JobsService

from tests import base


class TestJobsService(base.TestCase):

    @mock.patch('jobless.jobs_service.locks.base.Lock')
    @mock.patch('jobless.jobs_service.jobs_repos.base.JobsRepo')
    def setUp(self, jobs_repo, redlock):
        with self.fixture('jobs.json') as fixture:
            self.jobs_arr = [Job(**job_json) for job_json in json.loads(fixture.read())]
            self.jobs_service = JobsService(jobs_repo=jobs_repo, lock_service=redlock)
            self.jobs_service.jobs_repo.get_window = mock.MagicMock(return_value=self.jobs_arr)

    def test_fetch_jobs(self):
        """Tests fetching jobs from the repository."""
        jobs = self.jobs_service.fetch_jobs()

        get_args = [args for name, args, kwargs in
                    self.jobs_service.jobs_repo.get_window.mock_calls]
        for args in get_args:
            assert args[1] == self.jobs_service.window

        assert len(jobs) > 0
        for job in jobs:
            assert isinstance(job, Job)
            assert job.status == 'DISPATCHED'

        assert self.jobs_service.lock_service.lock_scope.called

    def test_schedule(self):
        """Tests inserting jobs into the schedule."""
        for job in self.jobs_arr:
            self.jobs_service.schedule(job)
            assert self.jobs_service.jobs_repo.insert.called
            self.jobs_service.jobs_repo.insert.called = False

        inserted_jobs = [args[1] for name, args, kwargs in
                         self.jobs_service.jobs_repo.insert.mock_calls]
        for job in self.jobs_arr:
            assert job in inserted_jobs

    def test_reschedule_one_time_jobs(self):
        """Tests rescheduling one time jobs."""
        for job in self.jobs_arr:
            job.schedule = None
            self.jobs_service.reschedule(job)
            assert self.jobs_service.jobs_repo.delete.called
            assert self.jobs_service.jobs_repo.update.called is False
            self.jobs_service.jobs_repo.delete.called = False

        deleted_job_ids = [args[1] for name, args, kwargs in
                        self.jobs_service.jobs_repo.delete.mock_calls]
        for job in self.jobs_arr:
            assert job.id in deleted_job_ids

    def test_reschedule_reoccurring_jobs(self):
        """Tests rescheduling jobs that have a schedule."""
        for job in self.jobs_arr:
            assert job.schedule
            job.status = Status.DISPATCHED.value
            self.jobs_service.reschedule(job)

            assert self.jobs_service.jobs_repo.update.called
            assert self.jobs_service.jobs_repo.delete.called is False
            self.jobs_service.jobs_repo.update.called = False

        updated_jobs = [args[1] for name, args, kwargs in
                        self.jobs_service.jobs_repo.update.mock_calls]
        for job in self.jobs_arr:
            job.time_to_process = job.time_to_process + timedelta(**job.schedule)
            job.status = Status.READY.value
            assert job in updated_jobs
