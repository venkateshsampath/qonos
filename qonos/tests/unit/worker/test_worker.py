import datetime
import fakes
import mox
import time

from qonos.openstack.common import timeutils
from qonos.worker import worker
from qonos.tests import utils as test_utils
from qonos.tests.unit import utils as unit_utils


class TestWorker(test_utils.BaseTestCase):
    def setUp(self):
        super(TestWorker, self).setUp()
        self.mox = mox.Mox()
        self.client = self.mox.CreateMockAnything()

        def client_factory(*args, **kwargs):
            return self.client

        self.worker = TestableWorker(client_factory)

    def tearDown(self):
        self.mox.UnsetStubs()
        super(TestWorker, self).tearDown()

    def prepare_client_mock(self, job=None, empty_jobs=0):
        self.client.create_worker(mox.IsA(str), mox.IsA(str)).\
            AndReturn(fakes.WORKER)
        # Argh! Mox why you no have "Times(x)" function?!?!
        for i in range(empty_jobs):
            print "Adding empty return call..."
            self.client.get_next_job(str(fakes.WORKER_ID), mox.IsA(str)).\
                AndReturn(None)

        self.client.get_next_job(str(fakes.WORKER_ID), mox.IsA(str)).\
            AndReturn(job)
        self.client.delete_worker(str(fakes.WORKER_ID))

    def test_run_loop_no_jobs(self):
        self.prepare_client_mock()
        self.mox.ReplayAll()

        self.config(job_poll_interval=5, group='worker')
        self.config(worker_name=fakes.WORKER_NAME, group='worker')
        self.config(action_type='snapshot', group='worker')

        fake_sleep = lambda x: None
        self.stubs.Set(time, 'sleep', fake_sleep)

        self.worker.run(run_once=True, poll_once=True)
        self.assertTrue(self.worker.was_init_worker_called(1))
        self.assertTrue(self.worker.was_process_job_called(0))
        self.assertTrue(self.worker.was_cleanup_worker_called(1))

        self.mox.VerifyAll()

    def test_run_loop_with_job(self):
        self.prepare_client_mock(job=fakes.JOB)
        self.mox.ReplayAll()

        self.config(job_poll_interval=5, group='worker')
        self.config(worker_name=fakes.WORKER_NAME, group='worker')
        self.config(action_type='snapshot', group='worker')

        fake_sleep = lambda x: None
        self.stubs.Set(time, 'sleep', fake_sleep)

        self.worker.run(run_once=True, poll_once=True)
        self.assertTrue(self.worker.was_init_worker_called(1))
        self.assertTrue(self.worker.was_process_job_called(1))
        self.assertTrue(self.worker.was_cleanup_worker_called(1))

        self.mox.VerifyAll()

    def test_run_loop_wait_for_job(self):
        self.prepare_client_mock(job=fakes.JOB, empty_jobs=3)
        self.mox.ReplayAll()

        self.config(job_poll_interval=5, group='worker')
        self.config(worker_name=fakes.WORKER_NAME, group='worker')
        self.config(action_type='snapshot', group='worker')

        fake_sleep = lambda x: None
        self.stubs.Set(time, 'sleep', fake_sleep)

        self.worker.run(run_once=True, poll_once=False)
        self.assertTrue(self.worker.was_init_worker_called(1))
        self.assertTrue(self.worker.was_process_job_called(1))
        self.assertTrue(self.worker.was_cleanup_worker_called(1))

        self.mox.VerifyAll()


class TestableWorker(worker.Worker):

    def __init__(self, client_factory):
        self.init_worker_called = 0
        self.process_job_called = 0
        self.cleanup_worker_called = 0
        super(TestableWorker, self).__init__(client_factory)

    def init_worker(self):
        self.init_worker_called += 1

    def process_job(self, job):
        self.process_job_called += 1

    def cleanup_worker(self):
        self.cleanup_worker_called += 1

    def was_init_worker_called(self, times):
        return self.init_worker_called == times

    def was_process_job_called(self, times):
        return self.process_job_called == times

    def was_cleanup_worker_called(self, times):
        return self.cleanup_worker_called == times
