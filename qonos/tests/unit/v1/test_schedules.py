import uuid
import webob.exc

from qonos.api.v1 import schedules
from qonos.db.simple import api as db_api
from qonos.common import exception
from qonos.tests import utils as test_utils
from qonos.tests.unit import utils as unit_utils


class TestSchedulesApi(test_utils.BaseTestCase):

    def setUp(self):
        super(TestSchedulesApi, self).setUp()
        self.controller = schedules.SchedulesController()
        self._create_schedules()

    def tearDown(self):
        super(TestSchedulesApi, self).tearDown()
        db_api.reset()

    def _create_schedules(self):
        fixture = {
            'tenant_id': unit_utils.TENANT1,
            'action': 'snapshot',
            'minute': '30',
            'hour': '2',
        }
        self.schedule_1 = db_api.schedule_create(fixture)
        fixture = {
            'tenant_id': unit_utils.TENANT2,
            'action': 'snapshot',
            'minute': '30',
            'hour': '2',
        }
        self.schedule_2 = db_api.schedule_create(fixture)

    def _create_test_fixture(self):
        fixture = {
            'schedule': {
                'tenant_id': str(uuid.uuid4()),
                'action': 'snapshot',
                'minute': '30',
                'hour': '2',
            }
        }
        return fixture

    def test_list(self):
        request = unit_utils.get_fake_request(method='GET')
        schedules = self.controller.list(request).get('schedules')
        self.assertEqual(len(schedules), 2)
        self.assertEqual(schedules[0]['tenant_id'],
                         self.schedule_1['tenant_id'])
        self.assertEqual(schedules[0]['action'],
                         self.schedule_1['action'])
        self.assertEqual(schedules[0]['minute'],
                         self.schedule_1['minute'])
        self.assertEqual(schedules[0]['hour'],
                         self.schedule_1['hour'])
        self.assertEqual(schedules[0]['id'],
                         self.schedule_1['id'])
        self.assertEqual(schedules[0]['created_at'],
                         self.schedule_1['created_at'])
        self.assertEqual(schedules[0]['updated_at'],
                         self.schedule_1['updated_at'])

    def test_get(self):
        request = unit_utils.get_fake_request(method='GET')
        actual = self.controller.get(request,
                                     self.schedule_1['id']).get('schedule')
        self.assertEqual(actual['tenant_id'],
                         self.schedule_1['tenant_id'])
        self.assertEqual(actual['action'],
                         self.schedule_1['action'])
        self.assertEqual(actual['minute'],
                         self.schedule_1['minute'])
        self.assertEqual(actual['hour'],
                         self.schedule_1['hour'])
        self.assertEqual(actual['id'],
                         self.schedule_1['id'])
        self.assertEqual(actual['created_at'],
                         self.schedule_1['created_at'])
        self.assertEqual(actual['updated_at'],
                         self.schedule_1['updated_at'])

    def test_get_not_found(self):
        request = unit_utils.get_fake_request(method='GET')
        schedule_id = str(uuid.uuid4())
        self.assertRaises(webob.exc.HTTPNotFound,
                          self.controller.get, request, schedule_id)

    def test_create(self):
        fixture = self._create_test_fixture()
        expected = fixture['schedule']
        request = unit_utils.get_fake_request(method='POST')

        actual = self.controller.create(request, fixture)['schedule']

        self.assertIsNotNone(actual.get('id'))
        self.assertIsNotNone(actual.get('created_at'))
        self.assertIsNotNone(actual.get('updated_at'))
        self.assertEqual(expected['tenant_id'], actual['tenant_id'])
        self.assertEqual(expected['action'], actual['action'])
        self.assertEqual(expected['minute'], actual['minute'])
        self.assertEqual(expected['hour'], actual['hour'])

    def test_create_no_body_bad_request(self):
        request = unit_utils.get_fake_request(method='POST')
        schedule_id = str(uuid.uuid4())
        self.assertRaises(webob.exc.HTTPBadRequest, self.controller.create,
                          request, None)

    def test_create_no_schedule_bad_request(self):
        request = unit_utils.get_fake_request(method='POST')
        schedule_id = str(uuid.uuid4())
        self.assertRaises(webob.exc.HTTPBadRequest, self.controller.create,
                          request, {'minute': '5'})

    def test_delete(self):
        request = unit_utils.get_fake_request(method='GET')
        request = unit_utils.get_fake_request(method='DELETE')
        self.controller.delete(request, self.schedule_1['id'])
        self.assertRaises(exception.NotFound, db_api.worker_get_by_id,
                          self.schedule_1['id'])

    def test_delete_not_found(self):
        request = unit_utils.get_fake_request(method='DELETE')
        schedule_id = str(uuid.uuid4())
        self.assertRaises(webob.exc.HTTPNotFound,
                          self.controller.delete, request, schedule_id)

    def test_update(self):
        request = unit_utils.get_fake_request(method='PUT')
        update_fixture = {'schedule': {'hour': '5'}}

        updated = self.controller.update(request, self.schedule_1['id'],
                                         update_fixture)['schedule']

        self.assertIsNotNone(updated.get('created_at'))
        self.assertIsNotNone(updated.get('updated_at'))
        self.assertEqual(self.schedule_1['tenant_id'], updated['tenant_id'])
        self.assertEqual(self.schedule_1['action'], updated['action'])
        self.assertEqual(self.schedule_1['minute'], updated['minute'])
        self.assertEqual(update_fixture['schedule']['hour'],
                         updated['hour'])

    def test_update_not_found(self):
        request = unit_utils.get_fake_request(method='PUT')
        schedule_id = str(uuid.uuid4())
        self.assertRaises(webob.exc.HTTPNotFound, self.controller.update,
                          request, schedule_id, {'schedule': {}})

    def test_update_no_body_bad_request(self):
        request = unit_utils.get_fake_request(method='PUT')
        schedule_id = str(uuid.uuid4())
        self.assertRaises(webob.exc.HTTPBadRequest, self.controller.update,
                          request, schedule_id, None)

    def test_update_no_schedule_bad_request(self):
        request = unit_utils.get_fake_request(method='PUT')
        schedule_id = str(uuid.uuid4())
        self.assertRaises(webob.exc.HTTPBadRequest, self.controller.update,
                          request, schedule_id, {'minute': '5'})
