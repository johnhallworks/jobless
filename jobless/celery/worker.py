from datetime import datetime

from celery import Celery, Task

from jobless.brokers import (load_completed_jobs_log,
                             load_jobs_service)
from jobless.commands import command_registry
from jobless.models.job import Job, CompletedJob

app = Celery()
app.config_from_object('jobless.celery.celeryconfig')


@app.task
def execute_job_command(job: Job) -> CompletedJob:
    """Executes the command of a job."""
    success, result = command_registry[job.command](**job.args)
    return CompletedJob(success, result, datetime.now(), **job.to_dict())


class LogCompletedJobBase(Task):
    """Base class to set properties required for jobs log."""
    abstract = True
    _completed_jobs_log = None

    @property
    def completed_jobs_log(self):
        if self._completed_jobs_log is None:
            self._completed_jobs_log = load_completed_jobs_log()
        return self._completed_jobs_log


@app.task(bind=True, base=LogCompletedJobBase)
def log_completed_job(self, completed_job: CompletedJob) -> None:
    """Logs a completed job using the completed log service."""
    with self.completed_jobs_log.session_scope() as session:
        self.completed_jobs_log.save(session, completed_job)


class HandleSideEffectsBase(Task):
    """Base class to set properties required for handling job side effects."""
    abstract = True
    _jobs_service = None

    @property
    def jobs_service(self):
        if self._jobs_service is None:
            self._jobs_service = load_jobs_service()
        return self._jobs_service


@app.task(bind=True, base=HandleSideEffectsBase)
def handle_side_effects(self, completed_job: CompletedJob) -> None:
    """Handles scheduling on_success and on_failure jobs."""
    side_effect_job = None
    if completed_job.success:
        if completed_job.on_success is not None:
            side_effect_job = completed_job.on_success
    elif completed_job.on_failure:
        side_effect_job = completed_job.on_failure

    if side_effect_job is not None:
        side_effect_job.args.update({'success': completed_job.success, 'result': completed_job.result})
        self.jobs_service.schedule(side_effect_job)
