import webob.exc

from qonos.api.v1 import workers
from qonos.tests import utils as test_utils
from qonos.tests.unit import utils as unit_test_utils
from qonos.tests.unit.utils import WORKER_UUID1


class TestWorkersApi(test_utils.BaseTestCase):

    def setUp(self):
        super(TestWorkersApi, self).setUp()
        self.controller = workers.WorkersController()

    def test_list_workers_unimplemented(self):
        request = unit_test_utils.get_fake_request(method='GET')
        self.assertRaises(webob.exc.HTTPNotImplemented,
                          self.controller.list, request)

    def test_get_worker_unimplemented(self):
        request = unit_test_utils.get_fake_request(method='GET')
        self.assertRaises(webob.exc.HTTPNotImplemented,
                          self.controller.get, request, WORKER_UUID1)

    def test_create_worker_unimplemented(self):
        request = unit_test_utils.get_fake_request(method='POST')
        self.assertRaises(webob.exc.HTTPNotImplemented,
                          self.controller.create, request)

    def test_delete_worker_unimplemented(self):
        request = unit_test_utils.get_fake_request(method='DELETE')
        self.assertRaises(webob.exc.HTTPNotImplemented,
                          self.controller.delete, request, WORKER_UUID1)

    def test_get_next_job_unimplemented(self):
        request = unit_test_utils.get_fake_request(method='PUT')
        self.assertRaises(webob.exc.HTTPNotImplemented,
                          self.controller.get_next_job, request,
                          WORKER_UUID1)
