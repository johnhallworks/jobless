#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='Jobless',
    version='1.0',
    description='Distributed Job Scheduler',
    author='Michael Nelson',
    author_email='michaeldnelson.mdn@gmail.com',
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    install_requires=['cassandra-driver', 'Flask', 'celery[redis]', 'requests',
                      'redlock-py', 'sqlalchemy', 'pymysql', 'python-dateutil'],
    entry_points={
        'celery.beat_schedulers': [
            'run_once = jobless.schedulers:RunOnceScheduler',
        ],
    }
)
