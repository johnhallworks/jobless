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
        completed_job_orm = CompletedJobOrm(**completed_job.to_dict())
        session.add(completed_job_orm)

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
