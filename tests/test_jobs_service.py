# -*- coding: utf-8; -*-

"""Test suite for JobsService."""
from datetime import timedelta

from jobless.models.job import Status


def test_fetch_jobs(jobs_service_with_jobs, test_jobs):
    """Tests fetching jobs from the repository."""
    jobs = jobs_service_with_jobs.fetch_jobs()

    assert len(jobs) == len(test_jobs)
    for job in jobs:
        assert job.status == Status.DISPATCHED.value
        job.status = Status.READY.value
        assert job in test_jobs


def test_schedule(jobs_service, test_jobs):
    """Tests inserting jobs into the schedule."""
    for job in test_jobs:
        jobs_service.schedule(job)

    inserted_jobs = jobs_service.fetch_jobs()
    assert len(test_jobs) == len(inserted_jobs)
    for job in inserted_jobs:
        assert job.status == Status.DISPATCHED.value
        job.status = Status.READY.value
        assert job in test_jobs


def test_reschedule_one_time_jobs(jobs_service_with_jobs, test_jobs):
    """Tests rescheduling one time jobs."""
    jobs_service_with_jobs.window = timedelta(days=100)
    for job in test_jobs:
        job.schedule = None
        jobs_service_with_jobs.reschedule(job)
        jobs = jobs_service_with_jobs.fetch_jobs()
        assert job not in jobs


def test_reschedule_reoccurring_jobs(jobs_service_with_jobs, test_jobs):
    """Tests rescheduling jobs that have a schedule."""
    jobs_service_with_jobs.window = timedelta(days=100)
    for job in test_jobs:
        assert job.schedule
        job.status = Status.DISPATCHED.value
        jobs_service_with_jobs.reschedule(job)

    updated_jobs = jobs_service_with_jobs.fetch_jobs()
    assert len(updated_jobs) == len(test_jobs)
    for job in test_jobs:
        job.time_to_process = job.time_to_process + timedelta(**job.schedule)
        job.status = Status.DISPATCHED.value
        assert job in updated_jobs
