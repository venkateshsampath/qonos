import webob.exc

import chronos.api.v1.tasks
from chronos.tests import utils as test_utils
from chronos.tests.unit import utils as unit_test_utils
from chronos.tests.unit.utils import TASK_UUID1


class TestTaskApi(test_utils.BaseTestCase):

    def setUp(self):
        super(TestTaskApi, self).setUp()
        self.controller = chronos.api.v1.tasks.TasksController()

    def test_get_task_unimplemented(self):
        request = unit_test_utils.get_fake_request(method='GET')
        self.assertRaises(webob.exc.HTTPNotImplemented,
                          self.controller.get_task, request, TASK_UUID1)

    def test_create_task_unimplemented(self):
        request = unit_test_utils.get_fake_request(method='POST')
        self.assertRaises(webob.exc.HTTPNotImplemented,
                          self.controller.create_task, request,
                          unit_test_utils.get_task())

    def test_list_tasks_unimplemented(self):
        request = unit_test_utils.get_fake_request(method='GET')
        self.assertRaises(webob.exc.HTTPNotImplemented,
                          self.controller.list_tasks, request)

    def test_update_task_unimplemented(self):
        request = unit_test_utils.get_fake_request(method='PUT')
        self.assertRaises(webob.exc.HTTPNotImplemented,
                          self.controller.update_task, request, TASK_UUID1,
                          unit_test_utils.get_task())

    def test_delete_task_unimplemented(self):
        request = unit_test_utils.get_fake_request(method='DELETE')
        self.assertRaises(webob.exc.HTTPNotImplemented,
                          self.controller.delete_task, request, TASK_UUID1)
