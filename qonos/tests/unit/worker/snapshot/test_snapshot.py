# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright 2013 Rackspace
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import copy
import datetime
import mox

from qonos.common import timeutils
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
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', timeout=None,
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'DONE', timeout=None,
                               error_message=None)
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
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', timeout=None,
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'DONE', timeout=None,
                               error_message=None)
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
            base_time + datetime.timedelta(seconds=905),
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
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', timeout=None,
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', timeout=None,
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', timeout=None,
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', timeout=None,
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'DONE', timeout=None,
                               error_message=None)
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
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', timeout=None,
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', timeout=None,
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING',
                               timeout=mox.IsA(datetime.datetime),
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', timeout=None,
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'DONE', timeout=None,
                               error_message=None)
        self.mox.ReplayAll()

        processor = TestableSnapshotProcessor(self.nova_client)
        processor.init_processor(self.worker)

        processor.process_job(job)

        self.mox.VerifyAll()

    def test_process_job_should_update_status_timestamp_no_retries(self):
        base_time = timeutils.utcnow()
        time_seq = [
            base_time,
            base_time,
            base_time + datetime.timedelta(minutes=5, seconds=5),
            base_time + datetime.timedelta(minutes=60, seconds=5),
            base_time + datetime.timedelta(minutes=120, seconds=5),
            base_time + datetime.timedelta(minutes=180, seconds=5),
            base_time + datetime.timedelta(minutes=240, seconds=5),
            ]
        print "Time_seq: %s" % str(time_seq)
        timeutils.set_time_override_seq(time_seq)

        job = copy.deepcopy(fakes.JOB['job'])
        job['timeout'] = base_time + datetime.timedelta(minutes=60)

        self.nova_client.servers.create_image(mox.IsA(str),
            mox.IsA(str)).AndReturn(IMAGE_ID)
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('QUEUED'))
        self.nova_client.images.get(IMAGE_ID).MultipleTimes().AndReturn(
            MockImageStatus('SAVING'))

        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', timeout=None,
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', timeout=None,
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING',
                               timeout=mox.IsA(datetime.datetime),
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING',
                               timeout=mox.IsA(datetime.datetime),
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING',
                               timeout=mox.IsA(datetime.datetime),
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'TIMED_OUT', timeout=None,
                               error_message=None)

        self.mox.ReplayAll()

        processor = TestableSnapshotProcessor(self.nova_client)
        processor.init_processor(self.worker)

        processor.process_job(job)

        self.mox.VerifyAll()

    def _do_test_process_job_should_update_image_error(self, error_status):
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
            error_status)

        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', timeout=None,
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', timeout=None,
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', timeout=None,
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', timeout=None,
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', timeout=None,
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'ERROR', timeout=None,
                               error_message=mox.IsA(str))

        self.mox.ReplayAll()

        processor = TestableSnapshotProcessor(self.nova_client)
        processor.init_processor(self.worker)

        processor.process_job(job)

        self.mox.VerifyAll()

    def test_process_job_should_update_image_error(self):
        status = MockImageStatus('ERROR')
        self._do_test_process_job_should_update_image_error(status)

    def test_process_job_should_update_image_killed(self):
        status = MockImageStatus('KILLED')
        self._do_test_process_job_should_update_image_error(status)

    def test_process_job_should_update_image_deleted(self):
        status = MockImageStatus('DELETED')
        self._do_test_process_job_should_update_image_error(status)

    def test_process_job_should_update_image_pending_delete(self):
        status = MockImageStatus('PENDING_DELETE')
        self._do_test_process_job_should_update_image_error(status)

    def test_process_job_should_update_image_none(self):
        self._do_test_process_job_should_update_image_error(None)


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

    def _get_utcnow(self):
        now = timeutils.utcnow()
        print "Returning NOW: %s" % str(now)
        return now
