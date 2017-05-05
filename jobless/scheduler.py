from contextlib import contextmanager
from datetime import datetime
import heapq
import time

from jobless.models.job import Job, CompletedJob
from jobless.jobs_service.jobs_service import JobsService
from jobless.completed_jobs_logs.base import JobsLog


def print_task(job: Job):
    print(job)
    return True, 'Task dispatched {0} after scheduled'.format(datetime.now() - job.time_to_process)


class JobLog(JobsLog):
    def __init__(self):
        self.completed_jobs = []

    def save(self, session, completed_job: CompletedJob):
        self.completed_jobs.append(completed_job)

    @contextmanager
    def session_scope(self):
        yield None


class Scheduler:
    """Distributed Scheduler."""
    _started = False
    tasks = {
        'print': print_task,
    }

    def __init__(self, jobs_service: JobsService, completed_jobs_log: JobsLog):
        self.jobs_service = jobs_service
        self.completed_jobs_log = completed_jobs_log
        self.sleep_time_seconds = jobs_service.window.total_seconds()

    def start(self):
        """Starts job processing."""
        self._started = True
        while self._started:
            jobs_heap = self.get_jobs_heap()
            if len(jobs_heap) > 0:
                self.process_jobs(jobs_heap)
            else:
                time.sleep(self.sleep_time_seconds)

    def stop(self):
        """Stops job processing."""
        self._started = False

    def get_jobs_heap(self):
        """Retrieves a window of jobs organized in a heap."""
        jobs_heap = self.jobs_service.fetch_jobs()
        heapq.heapify(jobs_heap)
        return jobs_heap

    def process_jobs(self, jobs_heap):
        """Processes all jobs in heap."""
        while len(jobs_heap) > 0:
            job = heapq.heappop(jobs_heap)
            seconds_until_due = (job.time_to_process - datetime.now()).total_seconds()

            if seconds_until_due <= 0:
                self.process_job(job)
            else:
                heapq.heappush(jobs_heap, job)
                time.sleep(seconds_until_due)

    def process_job(self, job: Job):
        """processes a ready job."""
        completed_job = self.execute_job(job)
        self.log_completed_job(completed_job)
        self.handle_side_effects(completed_job)
        self.jobs_service.reschedule(job)

    def execute_job(self, job):
        success, result = self.tasks[job.command](job)
        return CompletedJob(success, result, datetime.now(), **job.to_dict())

    def log_completed_job(self, completed_job: CompletedJob):
        """Logs a completed job using the completed log service."""
        with self.completed_jobs_log.session_scope() as session:
            self.completed_jobs_log.save(session, completed_job)

    def handle_side_effects(self, completed_job: CompletedJob):
        """Handles scheduling on_success and on_failure jobs."""
        side_effect_job = None
        if completed_job.success:
            if completed_job.on_success is not None:
                side_effect_job = Job(**completed_job.on_success)
        elif completed_job.on_failure:
            side_effect_job = Job(**completed_job.on_failure)

        if side_effect_job is not None:
            side_effect_job.args.update({'success': completed_job.success, 'result': completed_job.result})
            self.jobs_service.schedule(side_effect_job)
