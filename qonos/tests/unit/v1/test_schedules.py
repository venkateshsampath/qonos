import uuid
import webob.exc

from qonos.api.v1 import schedules
from qonos.db.simple import api as db_api
from qonos.common import exception
from qonos.common import utils as qonos_utils
from qonos.openstack.common import cfg
from qonos.tests import utils as test_utils
from qonos.tests.unit import utils as unit_utils


SCHEDULE_ATTRS = ['id', 'tenant_id', 'action',
                  'minute', 'hour']


CONF = cfg.CONF


class TestSchedulesApi(test_utils.BaseTestCase):

    def setUp(self):
        super(TestSchedulesApi, self).setUp()
        self.controller = schedules.SchedulesController(db_api=db_api)
        self._create_schedules()

    def tearDown(self):
        super(TestSchedulesApi, self).tearDown()
        db_api.reset()

    def _create_schedules(self):
        fixture = {
            'id': unit_utils.SCHEDULE_UUID1,
            'tenant_id': unit_utils.TENANT1,
            'action': 'snapshot',
            'minute': '30',
            'hour': '2',
            'next_run': qonos_utils.cron_string_to_next_datetime(30, 2)
        }
        self.schedule_1 = db_api.schedule_create(fixture)
        fixture = {
            'id': unit_utils.SCHEDULE_UUID2,
            'tenant_id': unit_utils.TENANT2,
            'action': 'snapshot',
            'minute': '30',
            'hour': '3',
            'next_run': qonos_utils.cron_string_to_next_datetime(30, 3)
        }
        self.schedule_2 = db_api.schedule_create(fixture)
        fixture = {
            'id': unit_utils.SCHEDULE_UUID3,
            'tenant_id': unit_utils.TENANT3,
            'action': 'snapshot',
            'minute': '30',
            'hour': '4',
            'next_run': qonos_utils.cron_string_to_next_datetime(30, 4)
        }
        self.schedule_3 = db_api.schedule_create(fixture)
        fixture = {
            'id': unit_utils.SCHEDULE_UUID4,
            'tenant_id': unit_utils.TENANT4,
            'action': 'snapshot',
            'minute': '30',
            'hour': '5',
            'next_run': qonos_utils.cron_string_to_next_datetime(30, 5)
        }
        self.schedule_4 = db_api.schedule_create(fixture)

    def test_list(self):
        request = unit_utils.get_fake_request(method='GET')
        schedules = self.controller.list(request).get('schedules')
        self.assertEqual(len(schedules), 4)
        for k in SCHEDULE_ATTRS:
            self.assertEqual(set([s[k] for s in schedules]),
                             set([self.schedule_1[k], self.schedule_2[k],
                                  self.schedule_3[k], self.schedule_4[k]]))

    def test_list_next_run_filtered(self):
        next_run = self.schedule_1['next_run']
        path = '?next_run_after=%s&next_run_before=%s'
        path = path % (next_run, next_run)
        request = unit_utils.get_fake_request(path=path, method='GET')
        schedules = self.controller.list(request).get('schedules')
        self.assertEqual(len(schedules), 1)

    def test_list_limit(self):
        path = '?limit=2'
        request = unit_utils.get_fake_request(path=path, method='GET')
        schedules = self.controller.list(request).get('schedules')
        self.assertEqual(len(schedules), 2)

    def test_list_limit_invalid_format(self):
        path = '?limit=a'
        request = unit_utils.get_fake_request(path=path, method='GET')
        self.assertRaises(webob.exc.HTTPBadRequest,
                          self.controller.list, request)

    def test_list_zero_limit(self):
        path = '?limit=0'
        request = unit_utils.get_fake_request(path=path, method='GET')
        self.assertRaises(webob.exc.HTTPBadRequest,
                          self.controller.list, request)

    def test_list_negative_limit(self):
        path = '?limit=-1'
        request = unit_utils.get_fake_request(path=path, method='GET')
        self.assertRaises(webob.exc.HTTPBadRequest,
                          self.controller.list, request)

    def test_list_fraction_limit(self):
        path = '?limit=1.1'
        request = unit_utils.get_fake_request(path=path, method='GET')
        self.assertRaises(webob.exc.HTTPBadRequest,
                          self.controller.list, request)

    def test_list_limit_max(self):
        self.config(api_limit_max=3)
        path = '?limit=4'
        request = unit_utils.get_fake_request(path=path, method='GET')
        schedules = self.controller.list(request).get('schedules')
        self.assertEqual(len(schedules), 3)

    def test_list_default_limit(self):
        self.config(limit_param_default=2)
        request = unit_utils.get_fake_request(method='GET')
        schedules = self.controller.list(request).get('schedules')
        self.assertEqual(len(schedules), 2)

    def test_list_with_marker(self):
        self.config(limit_param_default=2, api_limit_max=4)
        path = '?marker=%s' % unit_utils.SCHEDULE_UUID1
        request = unit_utils.get_fake_request(path=path, method='GET')
        schedules = self.controller.list(request).get('schedules')
        self.assertEqual(len(schedules), 2)
        for k in SCHEDULE_ATTRS:
            self.assertEqual(set([s[k] for s in schedules]),
                             set([self.schedule_2[k], self.schedule_3[k]]))

    def test_list_marker_not_specified(self):
        self.config(limit_param_default=2, api_limit_max=4)
        path = '?marker=%s' % ''
        request = unit_utils.get_fake_request(path=path, method='GET')
        self.assertRaises(webob.exc.HTTPNotFound,
                          self.controller.list, request)

    def test_list_marker_not_found(self):
        self.config(limit_param_default=2, api_limit_max=4)
        path = '?marker=%s' % '3c5817e2-76cb-41fe-b012-2935e406db87'
        request = unit_utils.get_fake_request(path=path, method='GET')
        self.assertRaises(webob.exc.HTTPNotFound,
                          self.controller.list, request)

    def test_list_invalid_marker(self):
        self.config(limit_param_default=2, api_limit_max=4)
        path = '?marker=%s' % '3c5817e2-76cb'
        request = unit_utils.get_fake_request(path=path, method='GET')
        self.assertRaises(webob.exc.HTTPNotFound,
                          self.controller.list, request)

    def test_list_with_limit_and_marker(self):
        self.config(limit_param_default=2, api_limit_max=4)
        path = '?marker=%s&limit=1' % unit_utils.SCHEDULE_UUID1
        request = unit_utils.get_fake_request(path=path, method='GET')
        schedules = self.controller.list(request).get('schedules')
        self.assertEqual(len(schedules), 1)
        for k in SCHEDULE_ATTRS:
            self.assertEqual(set([s[k] for s in schedules]),
                             set([self.schedule_2[k]]))

    def test_get(self):
        request = unit_utils.get_fake_request(method='GET')
        actual = self.controller.get(request,
                                     self.schedule_1['id']).get('schedule')
        for k in SCHEDULE_ATTRS:
            self.assertEqual(actual[k], self.schedule_1[k])

    def test_get_not_found(self):
        request = unit_utils.get_fake_request(method='GET')
        schedule_id = str(uuid.uuid4())
        self.assertRaises(webob.exc.HTTPNotFound,
                          self.controller.get, request, schedule_id)

    def test_create(self):
        fixture = {'schedule': {
            'id': unit_utils.SCHEDULE_UUID5,
            'tenant_id': unit_utils.TENANT1,
            'action': 'snapshot',
            'minute': '30',
            'hour': '2',
        }}
        expected = fixture['schedule']
        request = unit_utils.get_fake_request(method='POST')

        actual = self.controller.create(request, fixture)['schedule']

        self.assertIsNotNone(actual.get('id'))
        self.assertIsNotNone(actual.get('created_at'))
        self.assertIsNotNone(actual.get('updated_at'))
        self.assertIsNotNone(actual.get('next_run'))
        self.assertEqual(expected['tenant_id'], actual['tenant_id'])
        self.assertEqual(expected['action'], actual['action'])
        self.assertEqual(expected['minute'], actual['minute'])
        self.assertEqual(expected['hour'], actual['hour'])

    def test_create_no_body_bad_request(self):
        request = unit_utils.get_fake_request(method='POST')
        schedule_id = str(uuid.uuid4())
        self.assertRaises(webob.exc.HTTPBadRequest, self.controller.create,
                          request, None)

    def test_create_malformed_body_bad_request(self):
        request = unit_utils.get_fake_request(method='POST')
        schedule_id = str(uuid.uuid4())
        self.assertRaises(webob.exc.HTTPBadRequest, self.controller.create,
                          request, 'fake-body')

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
        self.assertNotEqual(updated['next_run'], self.schedule_1['next_run'])

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
