import time

from sqlalchemy.exc import OperationalError

from jobless.conf import Config
from jobless.completed_jobs_logs.mysql import Base as CompletedJobsBase
from jobless.jobs_service.jobs_repos.mysql import Base as JobsBase
from jobless.utils import create_tables

MAX_ATTEMPTS = 30
tables_created = False

# mysql not available immediately with docker-compose. Keep trying until available or max attempts.
attempts = 0
while (not tables_created) and (attempts < MAX_ATTEMPTS):
    try:
        create_tables(JobsBase, Config.mysql_uri)
        create_tables(CompletedJobsBase, Config.mysql_uri)
        tables_created = True
    except OperationalError:
        attempts += 1
        print("MYSQL CONNECTION REFUSED. SLEEPING 1 SECOND")
        time.sleep(1)

if not tables_created:
    raise Exception("Mysql connection still refused after {0} attempts/seconds".format(attempts))
