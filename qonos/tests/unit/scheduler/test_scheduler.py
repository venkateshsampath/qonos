import datetime
import mox
import time

from qonos.openstack.common import timeutils
from qonos.scheduler import scheduler
from qonos.tests import utils as test_utils
from qonos.tests.unit import utils as unit_utils


class TestScheduler(test_utils.BaseTestCase):

    def setUp(self):
        super(TestScheduler, self).setUp()
        self.mox = mox.Mox()
        self.client = self.mox.CreateMockAnything()

        def client_factory(*args, **kwargs):
            return self.client

        self.scheduler = scheduler.Scheduler(client_factory)

    def tearDown(self):
        super(TestScheduler, self).tearDown()

    def test_run_loop(self):
        self.config(job_schedule_interval=5, group='scheduler')
        called = {'enqueue_jobs': False}

        def fake(*args, **kwargs):
            called['enqueue_jobs'] = True

        self.stubs.Set(self.scheduler, 'enqueue_jobs', fake)
        fake_sleep = lambda x: None
        self.stubs.Set(time, 'sleep', fake_sleep)

        self.scheduler.run(run_once=True)
        self.assertTrue(called['enqueue_jobs'])

    def test_run_loop_take_too_long(self):
        self.config(job_schedule_interval=-1, group='scheduler')
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

    def test_enqueue_jobs(self):
        called = {'get_schedules': False}

        def fake(*args, **kwargs):
            called['get_schedules'] = True
            return [{'id': unit_utils.SCHEDULE_UUID1}]

        self.stubs.Set(self.scheduler, 'get_schedules', fake)
        self.client.create_job(mox.IgnoreArg())
        self.mox.ReplayAll()
        self.scheduler.enqueue_jobs()
        self.mox.VerifyAll()

    def test_get_schedules(self):
        timeutils.set_time_override()
        previous_run = timeutils.isotime()
        timeutils.advance_time_seconds(30)
        current_run = timeutils.isotime()

        filter_args = {'next_run_after': previous_run,
                       'next_run_before': current_run}
        self.client.list_schedules(filter_args=filter_args).AndReturn([])
        self.mox.ReplayAll()
        self.scheduler.get_schedules(previous_run, current_run)
        self.mox.VerifyAll()

    def test_get_schedules_no_previous_run(self):
        current_run = timeutils.isotime()

        epoch = timeutils.isotime(datetime.datetime(1970, 1, 1))
        filter_args = {'next_run_after': epoch,
                       'next_run_before': current_run}
        self.client.list_schedules(filter_args=filter_args).AndReturn([])
        self.mox.ReplayAll()
        self.scheduler.get_schedules(current_run=current_run)
        self.mox.VerifyAll()
