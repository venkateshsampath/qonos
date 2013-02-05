import copy
import datetime
import mox

from qonos.openstack.common import timeutils
from qonos.tests.unit.worker import fakes
from qonos.tests import utils as test_utils
from qonos.worker.snapshot import snapshot


IMAGE_ID = '66666666-6666-6666-6666-66666666'


class TestSnapshotProcessor(test_utils.BaseTestCase):

    def setUp(self):
        super(TestSnapshotProcessor, self).setUp()
        self.mox = mox.Mox()

        self.nova_client = MockNovaClient()
        self.nova_client.servers = self.mox.CreateMockAnything()
        self.nova_client.images = self.mox.CreateMockAnything()
        self.worker = self.mox.CreateMockAnything()

    def tearDown(self):
        self.mox.UnsetStubs()
        super(TestSnapshotProcessor, self).tearDown()

    def test_process_job_should_succeed_immediately(self):
        timeutils.set_time_override()
        self.nova_client.servers.create_image(mox.IsA(str),
            mox.IsA(str)).AndReturn(IMAGE_ID)
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('ACTIVE'))
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', None)
        self.worker.update_job(fakes.JOB_ID, 'DONE', None)
        self.mox.ReplayAll()

        processor = TestableSnapshotProcessor(self.nova_client)
        processor.init_processor(self.worker)

        processor.process_job(fakes.JOB['job'])

        self.mox.VerifyAll()

    def test_process_job_should_succeed_after_multiple_tries(self):
        timeutils.set_time_override()
        self.nova_client.servers.create_image(mox.IsA(str),
            mox.IsA(str)).AndReturn(IMAGE_ID)
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('QUEUED'))
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('SAVING'))
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('SAVING'))
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('ACTIVE'))
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', None)
        self.worker.update_job(fakes.JOB_ID, 'DONE', None)
        self.mox.ReplayAll()

        processor = TestableSnapshotProcessor(self.nova_client)
        processor.init_processor(self.worker)

        processor.process_job(fakes.JOB['job'])

        self.mox.VerifyAll()

    def test_process_job_should_update_status_only(self):
        base_time = timeutils.utcnow()
        time_seq = [
            base_time,
            base_time,
            base_time + datetime.timedelta(seconds=305),
            base_time + datetime.timedelta(seconds=605),
            ]
        timeutils.set_time_override_seq(time_seq)

        job = copy.deepcopy(fakes.JOB['job'])
        job['timeout'] = base_time + datetime.timedelta(minutes=60)

        self.nova_client.servers.create_image(mox.IsA(str),
            mox.IsA(str)).AndReturn(IMAGE_ID)
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('QUEUED'))
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('SAVING'))
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('SAVING'))
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('ACTIVE'))
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', None)
        self.worker.update_job(fakes.JOB_ID, 'DONE', None)
        self.mox.ReplayAll()

        processor = TestableSnapshotProcessor(self.nova_client)
        processor.init_processor(self.worker)

        processor.process_job(job)

        self.mox.VerifyAll()

    def test_process_job_should_update_status_and_timestamp(self):
        base_time = timeutils.utcnow()
        time_seq = [
            base_time,
            base_time,
            base_time + datetime.timedelta(seconds=305),
            base_time + datetime.timedelta(minutes=60, seconds=5),
            base_time + datetime.timedelta(minutes=60, seconds=305),
            ]
        timeutils.set_time_override_seq(time_seq)

        job = copy.deepcopy(fakes.JOB['job'])
        job['timeout'] = base_time + datetime.timedelta(minutes=60)

        self.nova_client.servers.create_image(mox.IsA(str),
            mox.IsA(str)).AndReturn(IMAGE_ID)
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('QUEUED'))
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('SAVING'))
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('SAVING'))
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('ACTIVE'))
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING',
                               mox.IsA(datetime.datetime))
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', None)
        self.worker.update_job(fakes.JOB_ID, 'DONE', None)
        self.mox.ReplayAll()

        processor = TestableSnapshotProcessor(self.nova_client)
        processor.init_processor(self.worker)

        processor.process_job(job)

        self.mox.VerifyAll()

    def test_process_job_should_update_status_and_timestamp(self):
        base_time = timeutils.utcnow()
        time_seq = [
            base_time,
            base_time,
            base_time + datetime.timedelta(seconds=305),
            base_time + datetime.timedelta(minutes=60, seconds=5),
            base_time + datetime.timedelta(minutes=120, seconds=5),
            base_time + datetime.timedelta(minutes=180, seconds=5),
            base_time + datetime.timedelta(minutes=240, seconds=5),
            ]
        timeutils.set_time_override_seq(time_seq)

        job = copy.deepcopy(fakes.JOB['job'])
        job['timeout'] = base_time + datetime.timedelta(minutes=60)

        self.nova_client.servers.create_image(mox.IsA(str),
            mox.IsA(str)).AndReturn(IMAGE_ID)
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('QUEUED'))
        self.nova_client.images.get(IMAGE_ID).MultipleTimes().AndReturn(
            MockImageStatus('SAVING'))

        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING',
                               mox.IsA(datetime.datetime))
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING',
                               mox.IsA(datetime.datetime))
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING',
                               mox.IsA(datetime.datetime))
        self.worker.update_job(fakes.JOB_ID, 'TIMED_OUT', None)

        self.mox.ReplayAll()

        processor = TestableSnapshotProcessor(self.nova_client)
        processor.init_processor(self.worker)

        processor.process_job(job)

        self.mox.VerifyAll()

    def test_process_job_should_update_image_error(self):
        base_time = timeutils.utcnow()
        time_seq = [
            base_time,
            base_time,
            base_time + datetime.timedelta(seconds=305),
            base_time + datetime.timedelta(seconds=605),
            base_time + datetime.timedelta(seconds=905),
            base_time + datetime.timedelta(seconds=1205),
            base_time + datetime.timedelta(seconds=1505),
            ]
        timeutils.set_time_override_seq(time_seq)

        job = copy.deepcopy(fakes.JOB['job'])
        job['timeout'] = base_time + datetime.timedelta(minutes=60)

        self.nova_client.servers.create_image(mox.IsA(str),
            mox.IsA(str)).AndReturn(IMAGE_ID)
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('QUEUED'))
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('SAVING'))
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('SAVING'))
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('SAVING'))
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('ERROR'))

        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', None)
        self.worker.update_job(fakes.JOB_ID, 'ERROR', None)

        self.mox.ReplayAll()

        processor = TestableSnapshotProcessor(self.nova_client)
        processor.init_processor(self.worker)

        processor.process_job(job)

        self.mox.VerifyAll()


class MockNovaClient(object):
    def __init__(self):
        self.servers = None
        self.images = None


class MockImageStatus(object):
    def __init__(self, status):
        self.status = status


class TestableSnapshotProcessor(snapshot.SnapshotProcessor):
    def __init__(self, nova_client):
        super(TestableSnapshotProcessor, self).__init__()
        self.nova_client = nova_client

    def _get_nova_client(self):
        return self.nova_client
