from datetime import (datetime,
                      timedelta)

from jobless.models.job import Job, Status
from jobless.jobs_service.jobs_repos.base import JobsRepo
from jobless.jobs_service.jobs_repos.exceptions import JobNotFoundException
from jobless.jobs_service.locks.base import Lock


class JobsService(object):
    """A service to control the jobs repo and locking service.

    Handles the business logic around the the jobs
    """
    def __init__(self, jobs_repo: JobsRepo, lock_service: Lock, window: timedelta=None):
        """Initializes dependencies and window."""
        if window is None:
            window = timedelta(minutes=1)
        self.window = window
        self.jobs_repo = jobs_repo
        self.lock_service = lock_service

    def fetch_jobs(self):
        """Returns a list of jobs to process."""
        with self.lock_service.lock_scope():
            with self.jobs_repo.session_scope() as session:
                deleted_jobs = []
                jobs = self.jobs_repo.get_window(session, self.window)
                for job in jobs:
                    try:
                        self._set_dispatched(session, job)
                    except JobNotFoundException:
                        print("Job was deleted between getting the window and declaring it as DISPATCHED")
                        deleted_jobs.append(job)
        return [job for job in jobs if job not in deleted_jobs]

    def schedule(self, job: Job):
        """Schedules a new job."""
        with self.jobs_repo.session_scope() as session:
            self.jobs_repo.insert(session, job)

    def reschedule(self, job: Job):
        """Reschedules a job if it has a schedule."""
        with self.jobs_repo.session_scope() as session:
            try:
                if job.schedule:
                    job = self.jobs_repo.get_job(session, job.id)
                    self._apply_schedule(job)
                    self.jobs_repo.update(session, job)
                else:
                    self.jobs_repo.delete(session, job.id)
            except JobNotFoundException:
                print("Job deleted while the job was dispatched.")

    def _set_dispatched(self, session, job: Job):
        """Updates a job status to DISPATCHED."""
        job.status = Status.DISPATCHED.value
        self.jobs_repo.update(session, job)

    @staticmethod
    def _apply_schedule(job: Job):
        """Applies the timedelta from a job schedule."""
        job.status = Status.READY.value
        job.time_to_process = job.time_to_process + timedelta(**job.schedule)
