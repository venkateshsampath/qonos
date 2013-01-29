import uuid
import webob.exc

from qonos.api.v1 import schedule_metadata
from qonos.db.simple import api as db_api
from qonos.common import exception
from qonos.tests import utils as test_utils
from qonos.tests.unit import utils as unit_utils


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
            'id':  unit_utils.SCHEDULE_UUID1,
            'tenant_id': unit_utils.TENANT1,
            'action': 'snapshot',
            'minute': '30',
            'hour': '2',
        }
        self.schedule_1 = db_api.schedule_create(fixture)
        fixture = {
            'id':  unit_utils.SCHEDULE_UUID2,
            'tenant_id': unit_utils.TENANT2,
            'action': 'snapshot',
            'minute': '30',
            'hour': '2',
        }
        self.schedule_2 = db_api.schedule_create(fixture)

    def test_create_meta(self):
        request = unit_utils.get_fake_request(method='POST')
        fixture = {'meta': {'key': 'key1', 'value': 'value1'}}
        meta = self.controller.create(request, self.schedule_1['id'], fixture)
        self.assertEqual(meta['meta']['key'], fixture['meta']['key'])
        self.assertEqual(meta['meta']['value'], fixture['meta']['value'])

    def test_create_meta_duplicate(self):
        request = unit_utils.get_fake_request(method='POST')
        fixture = {'meta': {'key': 'key1', 'value': 'value1'}}
        meta = self.controller.create(request, self.schedule_1['id'], fixture)
        # Same schedule ID and key conflict
        fixture = {'meta': {'key': 'key1', 'value': 'value2'}}
        self.assertRaises(webob.exc.HTTPConflict, self.controller.create,
                          request, self.schedule_1['id'], fixture)

    def test_list_meta(self):
        request = unit_utils.get_fake_request(method='POST')
        fixture = {'meta': {'key': 'key1', 'value': 'value1'}}
        self.controller.create(request, self.schedule_1['id'], fixture)
        fixture2 = {'meta': {'key': 'key2', 'value': 'value2'}}
        self.controller.create(request, self.schedule_1['id'], fixture2)
        request = unit_utils.get_fake_request(method='GET')
        metadata = self.controller.list(request, self.schedule_1['id'])
        self.assertEqual(2, len(metadata['metadata']))
        self.assertMetadataInList(metadata['metadata'], fixture['meta'])
        self.assertMetadataInList(metadata['metadata'], fixture2['meta'])

    def test_get_meta(self):
        request = unit_utils.get_fake_request(method='POST')
        fixture = {'meta': {'key': 'key1', 'value': 'value1'}}
        self.controller.create(request, self.schedule_1['id'], fixture)
        request = unit_utils.get_fake_request(method='GET')
        meta = self.controller.get(request, self.schedule_1['id'], 'key1')
        self.assertEqual(meta['meta']['key'], fixture['meta']['key'])
        self.assertEqual(meta['meta']['value'], fixture['meta']['value'])

    def test_get_meta_schedule_not_found(self):
        request = unit_utils.get_fake_request(method='GET')
        schedule_id = uuid.uuid4()
        self.assertRaises(webob.exc.HTTPNotFound, self.controller.get,
                          request, schedule_id, 'key1')

    def test_get_meta_key_not_found(self):
        request = unit_utils.get_fake_request(method='GET')
        self.assertRaises(webob.exc.HTTPNotFound, self.controller.get,
                          request, self.schedule_1['id'], 'key1')

    def test_delete_meta(self):
        request = unit_utils.get_fake_request(method='POST')
        fixture = {'meta': {'key': 'key1', 'value': 'value1'}}
        self.controller.create(request, self.schedule_1['id'], fixture)
        request = unit_utils.get_fake_request(method='DELETE')
        self.controller.delete(request, self.schedule_1['id'], 'key1')
        request = unit_utils.get_fake_request(method='GET')
        self.assertRaises(webob.exc.HTTPNotFound, self.controller.get,
                          request, self.schedule_1['id'], 'key1')

    def test_delete_meta_schedule_not_found(self):
        request = unit_utils.get_fake_request(method='DELETE')
        schedule_id = uuid.uuid4()
        self.assertRaises(webob.exc.HTTPNotFound, self.controller.delete,
                          request, schedule_id, 'key1')

    def test_delete_meta_key_not_found(self):
        request = unit_utils.get_fake_request(method='DELETE')
        self.assertRaises(webob.exc.HTTPNotFound, self.controller.delete,
                          request, self.schedule_1['id'], 'key1')

    def test_update_meta(self):
        request = unit_utils.get_fake_request(method='POST')
        fixture = {'meta': {'key': 'key1', 'value': 'value1'}}
        self.controller.create(request, self.schedule_1['id'], fixture)
        request = unit_utils.get_fake_request(method='PUT')
        update_fixture = {'meta': {'key': 'key1', 'value': 'value2'}}

        self.controller.update(request, self.schedule_1['id'], 'key1',
                               update_fixture)
        meta = self.controller.get(request, self.schedule_1['id'], 'key1')

        self.assertEqual(meta['meta']['key'],
                         fixture['meta']['key'])
        self.assertEqual(meta['meta']['key'],
                         update_fixture['meta']['key'])
        self.assertEqual(meta['meta']['value'],
                         update_fixture['meta']['value'])
        self.assertNotEqual(meta['meta']['value'],
                            fixture['meta']['value'])

    def test_update_meta_schedule_not_found(self):
        request = unit_utils.get_fake_request(method='PUT')
        schedule_id = uuid.uuid4()
        fixture = {'meta': {'key1': 'value1'}}
        self.assertRaises(webob.exc.HTTPNotFound, self.controller.update,
                          request, schedule_id, 'key1', fixture)

    def test_update_meta_bad_request(self):
        request = unit_utils.get_fake_request(method='PUT')
        fixture = {'meta': {'key1': 'value1'}}
        self.assertRaises(webob.exc.HTTPNotFound, self.controller.update,
                          request, self.schedule_1['id'], 'key11',
                          fixture)
