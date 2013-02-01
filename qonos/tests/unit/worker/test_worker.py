import fakes
import mox
import time

from qonos.tests import utils as test_utils
from qonos.worker import worker


class TestWorker(test_utils.BaseTestCase):
    def setUp(self):
        super(TestWorker, self).setUp()
        self.mox = mox.Mox()
        self.client = self.mox.CreateMockAnything()

        def client_factory(*args, **kwargs):
            return self.client

        self.processor = FakeProcessor()
        self.worker = worker.Worker(client_factory, self.processor)

    def tearDown(self):
        self.mox.UnsetStubs()
        super(TestWorker, self).tearDown()

    def prepare_client_mock(self, job=fakes.JOB_NONE, empty_jobs=0):
        self.client.create_worker(mox.IsA(str), mox.IsA(str)).\
            AndReturn(fakes.WORKER)
        # Argh! Mox why you no have "Times(x)" function?!?!
        for i in range(empty_jobs):
            print "Adding empty return call..."
            self.client.get_next_job(str(fakes.WORKER_ID), mox.IsA(str)).\
                AndReturn(fakes.JOB_NONE)

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
        self.assertTrue(self.processor.was_init_processor_called(1))
        self.assertTrue(self.processor.was_process_job_called(0))
        self.assertTrue(self.processor.was_cleanup_processor_called(1))

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
        self.assertTrue(self.processor.was_init_processor_called(1))
        self.assertTrue(self.processor.was_process_job_called(1))
        self.assertTrue(self.processor.was_cleanup_processor_called(1))

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
        self.assertTrue(self.processor.was_init_processor_called(1))
        self.assertTrue(self.processor.was_process_job_called(1))
        self.assertTrue(self.processor.was_cleanup_processor_called(1))

        self.mox.VerifyAll()


class FakeProcessor(worker.JobProcessor):

    def __init__(self):
        self.init_processor_called = 0
        self.process_job_called = 0
        self.cleanup_processor_called = 0
        super(FakeProcessor, self).__init__()

    def init_processor(self, worker):
        super(FakeProcessor, self).init_processor(worker)
        self.init_processor_called += 1

    def process_job(self, job):
        self.process_job_called += 1

    def cleanup_processor(self):
        self.cleanup_processor_called += 1

    def was_init_processor_called(self, times):
        return self.init_processor_called == times

    def was_process_job_called(self, times):
        return self.process_job_called == times

    def was_cleanup_processor_called(self, times):
        return self.cleanup_processor_called == times
