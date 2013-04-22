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

import uuid
import webob.exc

from qonos.api.v1 import job_metadata
from qonos.db.simple import api as db_api
from qonos.tests.unit import utils as unit_utils
from qonos.tests import utils as test_utils


class TestJobMetadataApi(test_utils.BaseTestCase):

    def setUp(self):
        super(TestJobMetadataApi, self).setUp()
        self.controller = job_metadata.\
            JobMetadataController(db_api=db_api)
        self._create_jobs_meta()

    def tearDown(self):
        super(TestJobMetadataApi, self).tearDown()
        db_api.reset()

    def _create_jobs_meta(self):
        fixture = {
            'id': unit_utils.JOB_UUID1,
            'tenant': unit_utils.TENANT1,
            'action': 'snapshot',
        }
        self.job_1 = db_api.job_create(fixture)
        meta_fixture1 = {'key': 'key1', 'value': 'value1'}
        self.meta_1 = db_api.job_meta_create(self.job_1['id'], meta_fixture1)
        meta_fixture2 = {'key': 'key2', 'value': 'value2'}
        self.meta_2 = db_api.job_meta_create(self.job_1['id'], meta_fixture2)

    def test_list_meta(self):
        request = unit_utils.get_fake_request(method='GET')
        metadata = self.controller.list(request, self.job_1['id'])
        self.assertEqual(2, len(metadata['metadata']))
        self.assertMetaInList(metadata['metadata'],
                              {self.meta_1['key']: self.meta_1['value']})
        self.assertMetaInList(metadata['metadata'],
                              {self.meta_2['key']: self.meta_2['value']})

    def test_list_meta_job_not_found(self):
        request = unit_utils.get_fake_request(method='GET')
        job_id = uuid.uuid4()
        self.assertRaises(webob.exc.HTTPNotFound, self.controller.list,
                          request, job_id)

    def test_update_metadata(self):
        request = unit_utils.get_fake_request(method='PUT')
        expected = {'metadata': {'key1': 'value1'}}
        actual = self.controller.update(request, self.job_1['id'],
                                        expected)

        self.assertEqual(expected, actual)

    def test_update_metadata_empty(self):
        request = unit_utils.get_fake_request(method='PUT')
        expected = {'metadata': {}}
        actual = self.controller.update(request, self.job_1['id'],
                                        expected)

        self.assertEqual(expected, actual)

    def test_update_meta_job_not_found(self):
        request = unit_utils.get_fake_request(method='PUT')
        job_id = uuid.uuid4()
        fixture = {'metadata': {'key1': 'value1'}}
        self.assertRaises(webob.exc.HTTPNotFound, self.controller.update,
                          request, job_id, fixture)
