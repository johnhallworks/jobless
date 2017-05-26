from datetime import timedelta

from jobless.completed_jobs_logs.base import JobsLog
from jobless.completed_jobs_logs.mysql import MysqlJobsLog
from jobless.conf import Config
from jobless.jobs_service.jobs_repos.base import JobsRepo
from jobless.jobs_service.jobs_repos.mysql import MysqlJobsRepo
from jobless.jobs_service.jobs_service import JobsService
from jobless.jobs_service.locks.base import Lock
from jobless.jobs_service.locks.redlock import RedlockJobsLock


def load_jobs_repo() -> JobsRepo:
    return MysqlJobsRepo(Config.mysql_uri, Config.max_fetch_size)


def load_lock_service() -> Lock:
    return RedlockJobsLock(Config.lock_hosts)


def load_completed_jobs_log() -> JobsLog:
    return MysqlJobsLog(uri=Config.mysql_uri)


def load_jobs_service() -> JobsService:
    return JobsService(jobs_repo=load_jobs_repo(), lock_service=load_lock_service(),
                       window=timedelta(seconds=10))
