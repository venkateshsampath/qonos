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

from qonos.api.v1 import schedule_metadata
from qonos.db.simple import api as db_api
from qonos.tests.unit import utils as unit_utils
from qonos.tests import utils as test_utils


class TestScheduleMetadataApi(test_utils.BaseTestCase):

    def setUp(self):
        super(TestScheduleMetadataApi, self).setUp()
        self.controller = schedule_metadata.\
            ScheduleMetadataController(db_api=db_api)
        self._create_schedules()

    def tearDown(self):
        super(TestScheduleMetadataApi, self).tearDown()
        db_api.reset()

    def _create_schedules(self):
        fixture = {
            'id': unit_utils.SCHEDULE_UUID1,
            'tenant': unit_utils.TENANT1,
            'action': 'snapshot',
            'minute': '30',
            'hour': '2',
            'schedule_metadata': [{'key': 'key1', 'value': 'value1'},
                                  {'key': 'key2', 'value': 'value2'}]
        }
        self.schedule_1 = db_api.schedule_create(fixture)
        fixture = {
            'id': unit_utils.SCHEDULE_UUID2,
            'tenant': unit_utils.TENANT2,
            'action': 'snapshot',
            'minute': '30',
            'hour': '2',
        }
        self.schedule_2 = db_api.schedule_create(fixture)

    def test_list_meta(self):
        request = unit_utils.get_fake_request(method='POST')
        fixture = {'metadata': {'key1': 'value1', 'key2': 'value2'}}
        actual = self.controller.list(request, self.schedule_1['id'])
        self.assertEqual(actual, fixture)

    def test_list_meta_schedule_not_found(self):
        request = unit_utils.get_fake_request(method='POST')
        schedule_id = uuid.uuid4()
        self.assertRaises(webob.exc.HTTPNotFound, self.controller.list,
                          request, schedule_id)

    def test_update_metadata(self):
        request = unit_utils.get_fake_request(method='PUT')
        expected = {'metadata': {'key1': 'value1'}}
        actual = self.controller.update(request, self.schedule_1['id'],
                                        expected)

        self.assertEqual(expected, actual)

    def test_update_metadata_empty(self):
        request = unit_utils.get_fake_request(method='PUT')
        expected = {'metadata': {}}
        actual = self.controller.update(request, self.schedule_1['id'],
                                        expected)

        self.assertEqual(expected, actual)

    def test_update_metadata_empty_key(self):
        request = unit_utils.get_fake_request(method='PUT')
        schedule_id = self.schedule_1['id']
        fixture = {'metadata': {'': 'value'}}

        self.assertRaises(webob.exc.HTTPBadRequest, self.controller.update,
                          request, schedule_id, fixture)

    def test_update_metadata_whitespace_key(self):
        request = unit_utils.get_fake_request(method='PUT')
        schedule_id = self.schedule_1['id']
        fixture = {'metadata': {'   ': 'value'}}

        self.assertRaises(webob.exc.HTTPBadRequest, self.controller.update,
                          request, schedule_id, fixture)

    def test_update_metadata_empty_and_nonempty_keys(self):
        request = unit_utils.get_fake_request(method='PUT')
        schedule_id = self.schedule_1['id']
        fixture = {'metadata': {'key': 'value', '': 'value2'}}

        self.assertRaises(webob.exc.HTTPBadRequest, self.controller.update,
                          request, schedule_id, fixture)

    def test_update_meta_schedule_not_found(self):
        request = unit_utils.get_fake_request(method='PUT')
        schedule_id = uuid.uuid4()
        fixture = {'metadata': {'key1': 'value1'}}
        self.assertRaises(webob.exc.HTTPNotFound, self.controller.update,
                          request, schedule_id, fixture)
