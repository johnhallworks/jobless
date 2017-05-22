from datetime import timedelta

from jobless.models.job import Job, Status
from jobless.jobs_service.jobs_repos.base import JobsRepo
from jobless.jobs_service.locks.base import Lock


class JobsService(object):
    """A service to control the jobs repo and locking service."""
    def __init__(self, jobs_repo: JobsRepo, lock_service: Lock, window=None):
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
                jobs = self.jobs_repo.get(session, self.window)
                for job in jobs:
                    self._set_dispatched(session, job)
        return jobs

    def schedule(self, job: Job):
        """Schedules a new job."""
        with self.jobs_repo.session_scope() as session:
            self.jobs_repo.insert(session, job)

    def reschedule(self, job: Job):
        """Reschedules a job if it has a schedule."""
        new_job = Job(**job.to_dict())
        with self.jobs_repo.session_scope() as session:
            if job.schedule:
                self._apply_schedule(new_job)
                self.jobs_repo.update(session, new_job)
            else:
                self.jobs_repo.delete(session, new_job)

    def _set_dispatched(self, session, job: Job):
        """Updates a job status to DISPATCHED."""
        job.status = Status.DISPATCHED.value
        self.jobs_repo.update(session, job)

    @staticmethod
    def _apply_schedule(job: Job):
        """Applies the timedelta from a job schedule."""
        job.status = Status.READY.value
        job.time_to_process = job.time_to_process + timedelta(**job.schedule)
