# -*- coding: utf-8; -*-

import os
import json
from pathlib import Path
from typing import List
from unittest import mock

import pytest

from jobless.jobs_service.jobs_service import JobsService
from jobless.jobs_service.jobs_repos.mysql import MysqlJobsRepo, Base
from jobless.jobs_service.locks.base import Lock
from jobless.models.job import Job
from jobless.scheduler import Scheduler
from jobless.utils import create_tables


CURRENT_FOLDER = Path(__file__).parent.absolute()


def load_fixture(fixture_filename, mode="rt"):
    """Reads a fixture from a file from fixtures/
    Args:
       - mode (str): a mode for opening a file, see help(open),

    Returns a file object
    """

    return CURRENT_FOLDER.joinpath(
        "fixtures", fixture_filename).open(mode=mode)


@pytest.fixture(name='test_jobs')
def test_jobs_fixture() -> List[Job]:
    with load_fixture('jobs.json') as fixture:
        return [Job(**job_json) for job_json in json.loads(fixture.read())]


@pytest.fixture(name='jobs_repo')
def jobs_repo_fixture() -> MysqlJobsRepo:
    db_filename = "jobs_repo.db"
    db_url_format = "sqlite:///{0}"
    create_tables(Base, db_url_format.format(db_filename))
    yield MysqlJobsRepo(db_url_format.format(db_filename))
    os.remove(db_filename)


@mock.patch('jobless.jobs_service.locks.base.Lock')
@pytest.fixture(name='lock_service')
def lock_service_fixture(lock) -> Lock:
    return lock


@pytest.fixture(name='jobs_service')
def jobs_service_fixture(jobs_repo, lock_service):
    return JobsService(jobs_repo=jobs_repo, lock_service=lock_service)


@pytest.fixture(name='jobs_service_with_jobs')
def jobs_service_with_jobs_fixture(jobs_service, test_jobs):
    with jobs_service.jobs_repo.session_scope() as session:
        for job in test_jobs:
            jobs_service.jobs_repo.insert(session, job)
    return jobs_service


@pytest.fixture(name='scheduler')
def scheduler_fixture(jobs_service_with_jobs):
    return Scheduler(jobs_service=jobs_service_with_jobs)
