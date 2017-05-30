import json
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import List

from dateutil.parser import parse as date_parse
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
from jobless.jobs_service.jobs_repos.exceptions import JobNotFoundException


Base = declarative_base()


def _job_to_orm(job):
    orm_dict = job.to_dict()
    orm_dict['time_to_process'] = job.time_to_process
    if orm_dict['schedule'] is not None:
        orm_dict['schedule'] = json.dumps(orm_dict['schedule'])
    if orm_dict['args'] is not None:
        orm_dict['args'] = json.dumps(orm_dict['args'])
    if orm_dict['on_success'] is not None:
        orm_dict['on_success'] = json.dumps(orm_dict['on_success'])
    if orm_dict['on_failure'] is not None:
        orm_dict['on_failure'] = json.dumps(orm_dict['on_failure'])

    return JobOrm(**orm_dict)


def _orm_to_job(job_orm):
    orm_dict = job_orm.to_dict()
    # convert text-json fields to dictionaries
    if orm_dict['schedule'] is not None:
        orm_dict['schedule'] = json.loads(orm_dict['schedule'])
    if orm_dict['args'] is not None:
        orm_dict['args'] = json.loads(orm_dict['args'])
    if orm_dict['on_success'] is not None:
        orm_dict['on_success'] = json.loads(orm_dict['on_success'])
    if orm_dict['on_failure'] is not None:
        orm_dict['on_failure'] = json.loads(orm_dict['on_failure'])

    return Job(**orm_dict)


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

    def get_job(self, session, job_id: str) -> Job:
        job_orm = session.query(JobOrm).filter(JobOrm.id == job_id).first()
        if job_orm is None:
            raise JobNotFoundException("Could not find job with id: {0}"
                                       .format(job_id))
        return _orm_to_job(job_orm)

    def get_window(self, session, window: timedelta) -> List[Job]:
        jobs_due_before = datetime.now() + window
        jobs_orm = session.query(JobOrm)\
            .filter(JobOrm.time_to_process <= jobs_due_before)\
            .filter(JobOrm.status == Status.READY.value) \
            .order_by(JobOrm.time_to_process.asc()) \
            .limit(self.max_fetch_size).all()
        return [_orm_to_job(job_orm) for job_orm in jobs_orm]

    def insert(self, session, job: Job) -> None:
        job_orm = _job_to_orm(job)
        session.add(job_orm)

    def update(self, session, job: Job) -> None:
        job_orm = session.query(JobOrm).filter(JobOrm.id == job.id).first()
        updated_job_dict = _job_to_orm(job).to_dict()
        for prop, val in updated_job_dict.items():
            setattr(job_orm, prop, val)

    def delete(self, session, job_id: str) -> None:
        job_orm = session.query(JobOrm).filter(JobOrm.id == job_id).first()
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
