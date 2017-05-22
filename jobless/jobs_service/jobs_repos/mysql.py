from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import List

from sqlalchemy import (Column,
                        Boolean,
                        Integer,
                        String,
                        DateTime,
                        Text,
                        create_engine)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from jobless.models.job import Job, Status
from jobless.jobs_service.jobs_repos.base import JobsRepo


Base = declarative_base()


class JobOrm(Base):
    __tablename__ = 'jobs'
    id = Column(String(100), primary_key=True)
    time_to_process = Column(DateTime)
    schedule = Column(String(100))
    status = Column(String(100))
    command = Column(String(100))
    args = Column(Text)
    on_success = Column(Text)
    on_failure = Column(Text)

    def __repr__(self):
        return str(self.to_dict())

    def to_dict(self):
        return {
            'id': self.id,
            'time_to_process': self.time_to_process,
            'schedule': self.schedule,
            'status': self.status,
            'command': self.command,
            'args': self.args,
            'on_success': self.on_success,
            'on_failure': self.on_failure
        }


class MysqlJobsRepo(JobsRepo):
    def __init__(self, uri, max_fetch_size=100):
        self.session_maker = self._create_session_maker(uri)
        self.max_fetch_size = max_fetch_size

    def get(self, session, window: timedelta) -> List[Job]:
        jobs_due_before = datetime.now() + window
        jobs_orm = session.query(JobOrm)\
            .filter(JobOrm.time_to_process <= jobs_due_before)\
            .filter(JobOrm.status == Status.READY.value) \
            .order_by(JobOrm.time_to_process.asc()) \
            .limit(self.max_fetch_size).all()
        return [Job(**job.to_dict()) for job in jobs_orm]

    def insert(self, session, job: Job) -> None:
        job_orm = JobOrm(**job.to_dict())
        session.add(job_orm)

    def update(self, session, job: Job) -> None:
        job_orm = session.query(JobOrm).filter(JobOrm.id == job.id).first()
        for prop, val in job.to_dict().items():
            setattr(job_orm, prop, val)

    def delete(self, session, job: Job) -> None:
        job_orm = session.query(JobOrm).filter(JobOrm.id == job.id).first()
        session.delete(job_orm)

    @staticmethod
    def _create_session_maker(uri):
        return sessionmaker(bind=create_engine(uri))

    @contextmanager
    def session_scope(self):
        session = self.session_maker()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
