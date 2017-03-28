from celery import Celery
from celery.schedules import crontab, BaseSchedule
from jobless.schedules import RunOnceSchedule
import datetime

app = Celery('test')

app.conf.broker_url = 'redis://redis/0'
app.conf.task_always_eager = True


# class RunOnceSchedule(BaseSchedule):
#     def __init__(self, nowfun=None, app=None, time_to_run=None):
#         self.time_to_run = time_to_run
#         super().__init__(nowfun=nowfun, app=app)
#
#     def is_due(self, las_run_at):
#         print(self.time_to_run - self.now())
#         """last run is irrelevant everything is run once"""
#         remaining = self.time_to_run - self.now()
#         print(remaining.total_seconds())
#         return (self.time_to_run - self.now()).microseconds <= 0, False

# @app.on_after_configure.connect
# def setup_periodic_tasks(sender, **kwargs):
#     # Calls test('hello') every 10 seconds.
#     sender.add_periodic_task(0.5, test.s('test'), name='add every 10')
#
# print(app._conf.beat_schedule)
# print(app.configured)

@app.task
def test(arg):
    print(arg)

# app.add_periodic_task(0.5, test.s('test'), name='add every 10')
t = test.s('wooorkkked')


nowd = datetime.datetime.now()
nowp = nowd + datetime.timedelta(seconds=10)
print(nowp)

app._conf.beat_schedule['run_once_thing'] = {
    'schedule': RunOnceSchedule(time_to_run=nowp),
    'task': t.name,
    'args': t.args,
}