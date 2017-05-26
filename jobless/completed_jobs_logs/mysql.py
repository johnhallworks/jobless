import json
from contextlib import contextmanager

from sqlalchemy import (Column,
                        Boolean,
                        Integer,
                        String,
                        DateTime,
                        Text,
                        create_engine)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from jobless.models.job import CompletedJob
from jobless.completed_jobs_logs.base import JobsLog


Base = declarative_base()


def completed_job_to_orm(completed_job):
    orm_dict = completed_job.to_dict()
    if orm_dict['schedule'] is not None:
        orm_dict['schedule'] = json.dumps(orm_dict['schedule'])
    if orm_dict['args'] is not None:
        orm_dict['args'] = json.dumps(orm_dict['args'])
    if orm_dict['on_success'] is not None:
        orm_dict['on_success'] = json.dumps(orm_dict['on_success'])
    if orm_dict['on_failure'] is not None:
        orm_dict['on_failure'] = json.dumps(orm_dict['on_failure'])

    return CompletedJobOrm(**orm_dict)


def orm_to_completed_job(completed_job_orm):
    orm_dict = completed_job_orm.to_dict()
    # convert text-json fields to dictionaries
    if orm_dict['schedule'] is not None:
        orm_dict['schedule'] = json.loads(orm_dict['schedule'])
    if orm_dict['args'] is not None:
        orm_dict['args'] = json.loads(orm_dict['args'])
    if orm_dict['on_success'] is not None:
        orm_dict['on_success'] = json.loads(orm_dict['on_success'])
    if orm_dict['on_failure'] is not None:
        orm_dict['on_failure'] = json.loads(orm_dict['on_failure'])

    return CompletedJob(**orm_dict)


class CompletedJobOrm(Base):
    __tablename__ = 'completed_jobs'
    id = Column(String(100), primary_key=True)
    time_to_process = Column(DateTime)
    schedule = Column(String(100))
    status = Column(String(100))
    command = Column(String(100))
    args = Column(Text)
    on_success = Column(Text)
    on_failure = Column(Text)

    success = Column(Boolean)
    result = Column(Text)
    processed_time = Column(DateTime)

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
            'on_failure': self.on_failure,
            'processed_time': self.processed_time,
            'result': self.result,
            'success': self.success
        }


class MysqlJobsLog(JobsLog):
    def __init__(self, uri, max_fetch_size=100):
        self.session_maker = self._create_session_maker(uri)
        self.max_fetch_size = max_fetch_size

    def save(self, session, completed_job: CompletedJob):
        session.add(completed_job_to_orm(completed_job))

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
