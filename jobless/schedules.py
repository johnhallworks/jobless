from celery.schedules import BaseSchedule


class RunOnceSchedule(BaseSchedule):
    def __init__(self, nowfun=None, app=None, time_to_run=None):
        self.time_to_run = time_to_run
        super().__init__(nowfun=nowfun, app=app)

    def is_due(self, las_run_at):
        print(self.time_to_run - self.now())
        """last run is irrelevant everything is run once"""
        remaining = (self.time_to_run - self.now()).total_seconds()
        return remaining <= 0, remaining
