import webob.exc

from qonos.api.v1 import schedules
from qonos.tests import utils as test_utils
from qonos.tests.unit import utils as unit_test_utils
from qonos.tests.unit.utils import SCHEDULE_UUID1


class TestSchedulesApi(test_utils.BaseTestCase):

    def setUp(self):
        super(TestSchedulesApi, self).setUp()
        self.controller = schedules.SchedulesController()

    def test_list_schedules_unimplemented(self):
        request = unit_test_utils.get_fake_request(method='GET')
        self.assertRaises(webob.exc.HTTPNotImplemented,
                          self.controller.list, request)

    def test_get_schedule_unimplemented(self):
        request = unit_test_utils.get_fake_request(method='GET')
        self.assertRaises(webob.exc.HTTPNotImplemented,
                          self.controller.get, request, SCHEDULE_UUID1)

    def test_create_schedule_unimplemented(self):
        request = unit_test_utils.get_fake_request(method='POST')
        self.assertRaises(webob.exc.HTTPNotImplemented,
                          self.controller.create, request,
                          unit_test_utils.get_schedule())

    def test_update_schedule_unimplemented(self):
        request = unit_test_utils.get_fake_request(method='PUT')
        self.assertRaises(webob.exc.HTTPNotImplemented,
                          self.controller.update, request, SCHEDULE_UUID1,
                          unit_test_utils.get_schedule())

    def test_delete_schedule_unimplemented(self):
        request = unit_test_utils.get_fake_request(method='DELETE')
        self.assertRaises(webob.exc.HTTPNotImplemented,
                          self.controller.delete, request, SCHEDULE_UUID1)
