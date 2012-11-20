import uuid
import webob.exc

from qonos.api.v1 import schedules
from qonos.db.simple import api as db_api
from qonos.common import exception
from qonos.tests import utils as test_utils
from qonos.tests.unit import utils as unit_test_utils
from qonos.tests.unit.utils import SCHEDULE_UUID1


class TestSchedulesApi(test_utils.BaseTestCase):

    def setUp(self):
        super(TestSchedulesApi, self).setUp()
        self.controller = schedules.SchedulesController()

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
        fixture = self._create_test_fixture()
        request = unit_test_utils.get_fake_request(method='GET')
        schedule = db_api.schedule_create(fixture['schedule'])
        schedules = self.controller.list(request).get('schedules')
        self.assertTrue(schedule in schedules)

    def test_get(self):
        fixture = self._create_test_fixture()
        request = unit_test_utils.get_fake_request(method='GET')
        expected = db_api.schedule_create(fixture['schedule'])
        actual = self.controller.get(request, expected['id']).get('schedule')
        self.assertEqual(actual, expected)

    def test_get_not_found(self):
        request = unit_test_utils.get_fake_request(method='GET')
        schedule_id = str(uuid.uuid4())
        self.assertRaises(webob.exc.HTTPNotFound,
                          self.controller.get, request, schedule_id)

    def test_create(self):
        fixture = self._create_test_fixture()
        expected = fixture['schedule']
        request = unit_test_utils.get_fake_request(method='POST')

        actual = self.controller.create(request, fixture)['schedule']

        self.assertIsNotNone(actual.get('id'))
        self.assertIsNotNone(actual.get('created_at'))
        self.assertIsNotNone(actual.get('updated_at'))
        self.assertEqual(expected['tenant_id'], actual['tenant_id'])
        self.assertEqual(expected['action'], actual['action'])
        self.assertEqual(expected['minute'], actual['minute'])
        self.assertEqual(expected['hour'], actual['hour'])

    def test_create_no_body_bad_request(self):
        request = unit_test_utils.get_fake_request(method='POST')
        schedule_id = str(uuid.uuid4())
        self.assertRaises(webob.exc.HTTPBadRequest, self.controller.create,
                          request, None)

    def test_create_no_schedule_bad_request(self):
        request = unit_test_utils.get_fake_request(method='POST')
        schedule_id = str(uuid.uuid4())
        self.assertRaises(webob.exc.HTTPBadRequest, self.controller.create,
                          request, {'minute': '5'})

    def test_delete(self):
        request = unit_test_utils.get_fake_request(method='GET')
        fixture = self._create_test_fixture()
        schedule = db_api.schedule_create(fixture['schedule'])
        request = unit_test_utils.get_fake_request(method='DELETE')
        self.controller.delete(request, schedule['id'])
        self.assertRaises(exception.NotFound, db_api.worker_get_by_id,
                          schedule['id'])

    def test_delete_not_found(self):
        request = unit_test_utils.get_fake_request(method='DELETE')
        schedule_id = str(uuid.uuid4())
        self.assertRaises(webob.exc.HTTPNotFound,
                          self.controller.delete, request, schedule_id)

    def test_update(self):
        request = unit_test_utils.get_fake_request(method='PUT')
        fixture = self._create_test_fixture()
        schedule = db_api.schedule_create(fixture['schedule'])
        update_fixture = {'schedule': {'hour': '5'}}

        updated = self.controller.update(request, schedule['id'],
                                         update_fixture)['schedule']

        self.assertIsNotNone(updated.get('created_at'))
        self.assertIsNotNone(updated.get('updated_at'))
        self.assertEqual(schedule['tenant_id'], updated['tenant_id'])
        self.assertEqual(schedule['action'], updated['action'])
        self.assertEqual(schedule['minute'], updated['minute'])
        self.assertEqual(update_fixture['schedule']['hour'],
                         updated['hour'])

    def test_update_not_found(self):
        request = unit_test_utils.get_fake_request(method='PUT')
        schedule_id = str(uuid.uuid4())
        self.assertRaises(webob.exc.HTTPNotFound, self.controller.update,
                          request, schedule_id, {'schedule': {}})

    def test_update_no_body_bad_request(self):
        request = unit_test_utils.get_fake_request(method='PUT')
        schedule_id = str(uuid.uuid4())
        self.assertRaises(webob.exc.HTTPBadRequest, self.controller.update,
                          request, schedule_id, None)

    def test_update_no_schedule_bad_request(self):
        request = unit_test_utils.get_fake_request(method='PUT')
        schedule_id = str(uuid.uuid4())
        self.assertRaises(webob.exc.HTTPBadRequest, self.controller.update,
                          request, schedule_id, {'minute': '5'})
