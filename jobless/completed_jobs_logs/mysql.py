from contextlib import contextmanager

from sqlalchemy import (Column,
                        Boolean,
                        Integer,
                        String,
                        DateTime,
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
    args = Column(String(100))
    on_success = Column(String(100))
    on_failure = Column(String(100))
    success = Column(Boolean)
    result = Column(String(1000))
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
        self.engine = create_engine(uri)
        self.Session = sessionmaker(bind=self.engine)
        self.max_fetch_size = max_fetch_size

    @contextmanager
    def session_scope(self):
        session = self.Session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()