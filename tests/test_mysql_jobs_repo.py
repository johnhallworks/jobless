# -*- coding: utf-8; -*-

"""Test suite for JobsService."""
import json
from datetime import timedelta
from unittest import mock

from jobless.models.job import Job, Status
from jobless.jobs_service.jobs_service import JobsService
from jobless.jobs_service.jobs_repos.mysql import MysqlJobsRepo, orm_to_job

from tests import base


class TestMysqlJobsRepo(base.TestCase):

    @mock.patch('jobless.jobs_service.jobs_repos.mysql.MysqlJobsRepo._create_session_maker')
    def setUp(self, session_maker):
        with self.fixture('jobs.json') as fixture:
            self.jobs_arr = [Job(**job_json) for job_json in json.loads(fixture.read())]
        session_maker.return_value = mock.MagicMock()
        self.jobs_repo = MysqlJobsRepo('foobar')

    def test_insert(self):
        for job in self.jobs_arr:
            with self.jobs_repo.session_scope() as session:
                self.jobs_repo.insert(session, job)
                assert session.add.called
                session.add.called = False

                inserted_jobs = [orm_to_job(args[0]) for name, args, kwargs in
                                 session.add.mock_calls]
                assert job in inserted_jobs
