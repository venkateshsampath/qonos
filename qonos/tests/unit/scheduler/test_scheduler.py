import time

from qonos.scheduler import scheduler
from qonos.tests import utils as test_utils


class TestScheduler(test_utils.BaseTestCase):

    def setUp(self):
        super(TestScheduler, self).setUp()
        self.scheduler = scheduler.Scheduler()

    def tearDown(self):
        super(TestScheduler, self).tearDown()

    def test_run_loop(self):
        self.config(job_schedule_interval=5)
        called = {'enqueue_jobs': False}

        def fake(*args, **kwargs):
            called['enqueue_jobs'] = True

        self.stubs.Set(self.scheduler, 'enqueue_jobs', fake)
        fake_sleep = lambda x: None
        self.stubs.Set(time, 'sleep', fake_sleep)

        self.scheduler.run(run_once=True)
        self.assertTrue(called['enqueue_jobs'])

    def test_run_loop_take_too_long(self):
        self.config(job_schedule_interval=-1)
        called = {'enqueue_jobs': False,
                  'log_warn': False}

        def fake(*args, **kwargs):
            called['enqueue_jobs'] = True

        self.stubs.Set(self.scheduler, 'enqueue_jobs', fake)
        fake_sleep = lambda x: None
        self.stubs.Set(time, 'sleep', fake_sleep)

        def fake_warn(msg, *args, **kwargs):
            called['log_warn'] = True

        self.stubs.Set(scheduler.LOG, 'warn', fake_warn)

        self.scheduler.run(run_once=True)
        self.assertTrue(called['enqueue_jobs'])
        self.assertTrue(called['log_warn'])
