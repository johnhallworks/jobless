from kombu.utils.functional import reprcall, maybe_evaluate
from collections import namedtuple
from .schedules import RunOnceSchedule
import celery

import heapq
import time

DEFAULT_MAX_INTERVAL = 300  # 5 minutes
logger = celery.utils.log.get_logger(__name__)
debug, info, error, warning = (logger.debug, logger.info,
                               logger.error, logger.warning)

event_t = namedtuple('event_t', ('time', 'priority', 'entry'))


# class RunOnceSchedule(celery.schedules.BaseSchedule):
#     def __init__(self, nowfun=None, app=None, time_to_run=None):
#         self.time_to_run = time_to_run
#         super().__init__(nowfun=nowfun, app=app)
#
#     def is_due(self, las_run_at):
#         """last run is irrelevant everything is run once"""
#         return (self.now() - self.time_to_run).microseconds > 0, False


# class ScheduleEntry(object):
#     """An entry in the scheduler.
#
#     Arguments:
#         name (str): see :attr:`name`.
#         schedule (~celery.schedules.schedule): see :attr:`schedule`.
#         args (Tuple): see :attr:`args`.
#         kwargs (Dict): see :attr:`kwargs`.
#         options (Dict): see :attr:`options`.
#         last_run_at (~datetime.datetime): see :attr:`last_run_at`.
#         total_run_count (int): see :attr:`total_run_count`.
#         relative (bool): Is the time relative to when the server starts?
#     """
#
#     #: The task name
#     name = None
#
#     #: The schedule (:class:`~celery.schedules.schedule`)
#     schedule = None
#
#     #: Positional arguments to apply.
#     args = None
#
#     #: Keyword arguments to apply.
#     kwargs = None
#
#     #: Task execution options.
#     options = None
#
#     def __init__(self, name=None, task=None, schedule=None, args=(), kwargs={},
#                  options={}, relative=False, app=None):
#         self.app = app
#         self.name = name
#         self.task = task
#         self.args = args
#         self.kwargs = kwargs
#         self.options = options
#         self.schedule = schedule
#
#     def _default_now(self):
#         return self.schedule.now() if self.schedule else self.app.now()
#
#     def is_due(self):
#         """See :meth:`~celery.schedule.schedule.is_due`."""
#         return self.schedule.is_due(self.last_run_at)
#
#     def __repr__(self):
#         return '<{name}: {0.name} {call} {0.schedule}'.format(
#             self,
#             call=reprcall(self.task, self.args or (), self.kwargs or {}),
#             name=type(self).__name__,
#         )
#
#     def __lt__(self, other):
#         if isinstance(other, ScheduleEntry):
#             # How the object is ordered doesn't really matter, as
#             # in the scheduler heap, the order is decided by the
#             # preceding members of the tuple ``(time, priority, entry)``.
#             #
#             # If all that's left to order on is the entry then it can
#             # just as well be random.
#             return id(self) < id(other)
#         return NotImplemented


class RunOnceScheduler(celery.beat.PersistentScheduler):

    # pylint disable=redefined-outer-name
    def tick(self, event_t=event_t, min=min,
             heappop=heapq.heappop, heappush=heapq.heappush,
             heapify=heapq.heapify, mktime=time.mktime):
        """Run a tick - one iteration of the scheduler.

        Executes one due task per call.

        Returns:
            float: preferred delay in seconds for next call.
        """
        def _when(entry, next_time_to_run):
            return (mktime(entry.schedule.now().timetuple()) +
                    (adjust(next_time_to_run) or 0))

        adjust = self.adjust
        max_interval = self.max_interval
        H = self._heap
        if H is None:
            """
            Lazily builds a heap of named tuples, with priority always 5
            I don't understand why tuples are used, or how the comparison of named tuples works

            """
            H = self._heap = [event_t(_when(e, e.is_due()[1]) or 0, 5, e)
                              for e in self.schedule.values()]
            heapify(H)
        if not H:
            return max_interval

        event = H[0]
        entry = event[2]
        is_due, next_time_to_run = self.is_due(entry)
        if is_due:
            verify = heappop(H)
            if verify is event:
                if not isinstance(entry.schedule, RunOnceSchedule):
                    next_entry = self.reserve(entry)
                    self.apply_entry(entry, producer=self.producer)
                    heappush(H, event_t(_when(next_entry, next_time_to_run),
                                        event[1], next_entry))
                else:
                    self.apply_entry(entry, producer=self.producer)
                return 0
            else:
                """
                Not sure this case is possible, need to log it
                """
                heappush(H, verify)
                return min(verify[0], max_interval)
        return min(adjust(next_time_to_run) or max_interval, max_interval)