from datetime import datetime
import heapq
import signal
import time
from typing import List

from celery import chain, group

from jobless.celery.worker import (execute_job_command,
                                   log_completed_job,
                                   handle_side_effects)
from jobless.models.job import Job
from jobless.jobs_service.jobs_service import JobsService


class Scheduler:
    """Distributed Scheduler."""
    _started = False

    def __init__(self, jobs_service: JobsService):
        self.jobs_service = jobs_service
        self.original_sigint_handler = signal.getsignal(signal.SIGINT)

        signal.signal(signal.SIGINT, self.stop_sig_handler)

    def start(self):
        """Starts job processing."""
        self._started = True
        while self._started:
            jobs_heap = self.get_jobs_heap()
            if len(jobs_heap) > 0:
                self.process_jobs(jobs_heap)
            else:
                time.sleep(self.jobs_service.window.total_seconds())

    def stop(self):
        """Stops job processing."""
        self._started = False

    def stop_sig_handler(self, *args):
        """Signal handler to gracefully stop scheduler."""
        self.stop()
        print("Attempting to stop scheduler gracefully")
        print("To forcefully shut down try sending another SIGINT signal")
        signal.signal(signal.SIGINT, self.original_sigint_handler)

    def get_jobs_heap(self) -> List[Job]:
        """Retrieves a window of jobs organized in a heap."""
        jobs_heap = self.jobs_service.fetch_jobs()
        heapq.heapify(jobs_heap)
        return jobs_heap

    def process_jobs(self, jobs_heap: List[Job]) -> None:
        """Processes all jobs in heap."""
        while len(jobs_heap) > 0:
            job = heapq.heappop(jobs_heap)
            seconds_until_due = (job.time_to_process - datetime.now()).total_seconds()

            if seconds_until_due <= 0:
                self.process_job(job)
            else:
                heapq.heappush(jobs_heap, job)
                time.sleep(seconds_until_due)

    def process_job(self, job: Job) -> None:
        """processes a ready job."""
        print("PROCESSING JOB")
        chain(
            execute_job_command.s(job),
            group(
                [log_completed_job.s(),
                 handle_side_effects.s()]
            )

        ).apply_async()

        self.jobs_service.reschedule(job)
