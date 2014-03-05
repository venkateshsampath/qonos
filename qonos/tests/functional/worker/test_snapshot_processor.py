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
import datetime

import mock

from oslo.config import cfg
from qonos.common import timeutils

from qonos.tests import utils as utils
from qonos.worker.snapshot.snapshot import SnapshotProcessor


CONF = cfg.CONF


class TestSnapshotProcessor(utils.BaseTestCase):


    def setUp(self):
        super(TestSnapshotProcessor, self).setUp()

    def tearDown(self):
        super(TestSnapshotProcessor, self).tearDown()

    def job_fixture(self):
        now = timeutils.utcnow()
        timeout = now + datetime.timedelta(hours=1)
        hard_timeout = now + datetime.timedelta(hours=4)

        fixture = {
            'id': 'JOB_1',
            'schedule_id': 'SCH_1',
            'tenant': 'TENANT1',
            'worker_id': 'WORKER_1',
            'action': 'snapshot',
            'status': 'QUEUED',
            'timeout': timeout,
            'hard_timeout': hard_timeout,
            'retry_count': 0,
            'metadata':
                {
                    'instance_id': 'INSTANCE_ID',
                    'value': 'my_instance',
                },
        }
        return fixture

    def mock_worker(self):
        worker = mock.Mock()

        def update_job(job_id, status, timeout=None, error_message=None):
            return {'job_id': job_id, 'status': status,
                    'timeout': timeout, 'error_message': error_message}

        def update_job_metadata(job_id, metadata):
            return metadata

        worker.update_job = update_job
        worker.update_job_metadata = update_job_metadata

        return worker

    def mock_nova_client(self, server, image):
        nova_client = mock.Mock()
        nova_client.servers = mock.Mock()
        nova_client.servers.create_image = mock.Mock(mock.ANY,
                                                     return_value=image.id)
        nova_client.servers.get = mock.Mock(mock.ANY, return_value=server)
        nova_client.images.get = mock.Mock(mock.ANY, return_value=image)
        return nova_client

    def mock_nova_client_factory(self, job, nova_client):
        nova_client_factory = mock.Mock()
        nova_client_factory.get_nova_client = mock.Mock(job,
                                                        return_value=nova_client)
        return nova_client_factory

    def image_fixture(self, image_id, status):
        image = mock.Mock()
        image.id = image_id
        image.status = status
        return image

    def server_instance_fixture(self, name):
        server = mock.Mock()
        server.name = name
        return server

    def test_successfully_processed_job(self):
        job = self.job_fixture()
        worker = self.mock_worker()

        server = self.server_instance_fixture("test")
        image = self.image_fixture('IMAGE_ID', 'ACTIVE')
        nova_client = self.mock_nova_client(server, image)

        nova_client_factory = self.mock_nova_client_factory(job, nova_client)

        processor = SnapshotProcessor()
        processor.init_processor(worker, nova_client_factory=nova_client_factory)
        processor.process_job(job)
        self.assertEqual('DONE', job['status'])