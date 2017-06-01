# -*- coding: utf-8; -*-

"""Test suite for MysqlJobsLog."""
import os
from datetime import datetime

import pytest

from jobless.completed_jobs_logs.mysql import Base, MysqlJobsLog
from jobless.models.job import CompletedJob
from jobless.utils import create_tables


@pytest.fixture(name='test_completed_jobs')
def test_completed_jobs_fixture(test_jobs):
    success = True
    completed_jobs = []
    for job in test_jobs:
        job_dict = job.to_dict()
        job_dict['job_id'] = job_dict['id']
        del job_dict['id']
        job_dict['success'] = success
        job_dict['processed_time'] = datetime.now()
        job_dict['result'] = 'foobar'

        completed_jobs.append(CompletedJob(**job_dict))
        success = not success

    return completed_jobs


@pytest.fixture(name='jobs_log')
def jobs_log_fixture():
    db_filename = "completed_jobs_repo.db"
    db_url_format = "sqlite:///{0}"
    create_tables(Base, db_url_format.format(db_filename))
    yield MysqlJobsLog(db_url_format.format(db_filename))
    os.remove(db_filename)


def test_save(jobs_log, test_completed_jobs):
    with jobs_log.session_scope() as session:
        for completed_job in test_completed_jobs:
            jobs_log.save(session, completed_job)
