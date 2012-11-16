import webob.exc

from qonos.api.v1 import jobs
from qonos.tests import utils as test_utils
from qonos.tests.unit import utils as unit_test_utils
from qonos.tests.unit.utils import JOB_UUID1


class TestJobsApi(test_utils.BaseTestCase):

    def setUp(self):
        super(TestJobsApi, self).setUp()
        self.controller = jobs.JobsController()

    def test_list_jobs_unimplemented(self):
        request = unit_test_utils.get_fake_request(method='GET')
        self.assertRaises(webob.exc.HTTPNotImplemented,
                          self.controller.list, request)

    def test_get_job_unimplemented(self):
        request = unit_test_utils.get_fake_request(method='GET')
        self.assertRaises(webob.exc.HTTPNotImplemented,
                          self.controller.get, request, JOB_UUID1)

    def test_delete_job_unimplemented(self):
        request = unit_test_utils.get_fake_request(method='DELETE')
        self.assertRaises(webob.exc.HTTPNotImplemented,
                          self.controller.delete, request, JOB_UUID1)

    def test_get_heartbeat_unimplemented(self):
        request = unit_test_utils.get_fake_request(method='GET')
        self.assertRaises(webob.exc.HTTPNotImplemented,
                          self.controller.get_heartbeat, request, JOB_UUID1)

    def test_update_heartbeat_unimplemented(self):
        request = unit_test_utils.get_fake_request(method='PUT')
        self.assertRaises(webob.exc.HTTPNotImplemented,
                          self.controller.update_heartbeat, request,
                          JOB_UUID1, {'heartbeat': '2012-11-16T18:41:43Z'})

    def test_get_status_unimplemented(self):
        request = unit_test_utils.get_fake_request(method='GET')
        self.assertRaises(webob.exc.HTTPNotImplemented,
                          self.controller.get_status, request, JOB_UUID1)

    def test_update_status_unimplemented(self):
        request = unit_test_utils.get_fake_request(method='PUT')
        self.assertRaises(webob.exc.HTTPNotImplemented,
                          self.controller.update_status, request,
                          JOB_UUID1, {'status': 'completed'})
