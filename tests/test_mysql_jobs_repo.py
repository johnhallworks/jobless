# -*- coding: utf-8; -*-

"""Test suite for MysqlJobsRepo."""
import math
from datetime import datetime, timedelta

import pytest

from jobless.jobs_service.jobs_repos.exceptions import JobNotFoundException
from jobless.models.job import Job, Status


def test_insert_get_by_id(test_jobs, jobs_repo):
    """Tests inserting jobs and getting each by id."""
    for job in test_jobs:
        with jobs_repo.session_scope() as session:
            jobs_repo.insert(session, job)
            job_retrieved = jobs_repo.get_job(session, job.id)
            assert job_retrieved == job


def test_insert_get_window(test_jobs, jobs_repo):
    """Tests inserting jobs and getting jobs by a window."""
    days = 1
    for job in test_jobs:
        with jobs_repo.session_scope() as session:
            job.time_to_process = datetime.now() + timedelta(days=days)
            jobs_repo.insert(session, job)
            days += 1
    with jobs_repo.session_scope() as session:
        jobs = jobs_repo.get_window(session, timedelta(days=days/2))
        assert len(jobs) == math.ceil(len(test_jobs) / 2.0)


def test_insert_update(test_jobs, jobs_repo):
    """Tests inserting jobs and updating them."""
    for job in test_jobs:
        with jobs_repo.session_scope() as session:
            jobs_repo.insert(session, job)
            jobP = Job(**job.to_dict())
            jobP.command = 'foobarbaz'
            jobP.time_to_process = job.time_to_process + timedelta(minutes=1)
            jobP.status = Status.DISPATCHED
            jobs_repo.update(session, jobP)
            job_retrieved = jobs_repo.get_job(session, jobP.id)
            assert job_retrieved == jobP
            assert job_retrieved != job


def test_insert_delete(test_jobs, jobs_repo):
    """Tests inserting a job and deleting it."""
    for job in test_jobs:
        with jobs_repo.session_scope() as session:
            jobs_repo.insert(session, job)
            jobs_repo.delete(session, job.id)
            with pytest.raises(JobNotFoundException):
                jobs_repo.get_job(session, job.id)
