import datetime
import uuid
import webob.exc

from qonos.api.v1 import jobs
from qonos.common import exception
import qonos.db.simple.api as db_api
from qonos.openstack.common import timeutils
from qonos.tests import utils as test_utils
from qonos.tests.unit import utils as unit_test_utils
from qonos.tests.unit import utils as unit_utils


class TestJobsApi(test_utils.BaseTestCase):

    def setUp(self):
        super(TestJobsApi, self).setUp()
        self.controller = jobs.JobsController()
        self._create_jobs()

    def tearDown(self):
        super(TestJobsApi, self).tearDown()
        db_api.reset()

    def _create_jobs(self):
        fixture = {
            'schedule_id': unit_utils.SCHEDULE_UUID1,
            'worker_id': unit_utils.WORKER_UUID1,
            'status': 'queued',
            'retry_count': 0,
        }
        self.job_1 = db_api.job_create(fixture)
        fixture = {
            'schedule_id': unit_utils.SCHEDULE_UUID2,
            'worker_id': unit_utils.WORKER_UUID2,
            'status': 'error',
            'retry_count': 0,
        }
        self.job_2 = db_api.job_create(fixture)

    def test_list(self):
        request = unit_test_utils.get_fake_request(method='GET')
        jobs = self.controller.list(request).get('jobs')
        self.assertEqual(len(jobs), 2)

    def test_get(self):
        request = unit_test_utils.get_fake_request(method='GET')
        job = self.controller.get(request, self.job_1['id']).get('job')
        self.assertEqual(job['status'], 'queued')
        self.assertEqual(job['schedule_id'], unit_utils.SCHEDULE_UUID1)
        self.assertEqual(job['worker_id'], unit_utils.WORKER_UUID1)
        self.assertEqual(job['retry_count'], 0)
        self.assertNotEqual(job['updated_at'], None)
        self.assertNotEqual(job['created_at'], None)

    def test_delete(self):
        request = unit_test_utils.get_fake_request(method='DELETE')
        self.controller.delete(request, self.job_1['id'])
        self.assertRaises(exception.NotFound, db_api.job_get_by_id,
                          self.job_1['id'])

    def test_delete_not_found(self):
        request = unit_test_utils.get_fake_request(method='DELETE')
        job_id = str(uuid.uuid4())
        self.assertRaises(webob.exc.HTTPNotFound,
                          self.controller.delete, request, job_id)

    def test_get_heartbeat(self):
        request = unit_test_utils.get_fake_request(method='GET')
        response = self.controller.get_heartbeat(request, self.job_1['id'])
        heartbeat = response.get('heartbeat')
        self.assertEqual(heartbeat,
                         timeutils.isotime(self.job_1['updated_at']))

    def test_get_heartbeat_not_found(self):
        request = unit_test_utils.get_fake_request(method='GET')
        job_id = str(uuid.uuid4())
        self.assertRaises(webob.exc.HTTPNotFound,
                          self.controller.get_heartbeat, request, job_id)

    def test_update_heartbeat(self):
        request = unit_test_utils.get_fake_request(method='PUT')
        body = {'heartbeat': '2012-11-16T18:41:43Z'}
        self.controller.update_heartbeat(request, self.job_1['id'], body)
        expected = timeutils.parse_isotime(body['heartbeat'])
        actual = db_api.job_get_by_id(self.job_1['id'])['updated_at']
        self.assertEqual(actual, expected)

    def test_update_heartbeat_empty_body(self):
        request = unit_test_utils.get_fake_request(method='PUT')
        body = {}
        self.assertRaises(webob.exc.HTTPBadRequest,
                          self.controller.update_heartbeat,
                          request, unit_utils.JOB_UUID1, body)

    def test_update_heartbeat_bad_time_format(self):
        request = unit_test_utils.get_fake_request(method='PUT')
        body = {'heartbeat': 'blah'}
        self.assertRaises(webob.exc.HTTPBadRequest,
                          self.controller.update_heartbeat,
                          request, unit_utils.JOB_UUID1, body)

    def test_update_heartbeat_not_found(self):
        request = unit_test_utils.get_fake_request(method='PUT')
        job_id = str(uuid.uuid4())
        body = {'heartbeat': '2012-11-16T18:41:43Z'}
        self.assertRaises(webob.exc.HTTPNotFound,
                          self.controller.update_heartbeat,
                          request, job_id, body)

    def test_get_status(self):
        request = unit_test_utils.get_fake_request(method='GET')
        response = self.controller.get_status(request, self.job_1['id'])
        status = response.get('status')
        self.assertEqual(status, self.job_1['status'])

    def test_get_status_not_found(self):
        request = unit_test_utils.get_fake_request(method='GET')
        job_id = str(uuid.uuid4())
        self.assertRaises(webob.exc.HTTPNotFound,
                          self.controller.get_status, request, job_id)

    def test_update_status(self):
        request = unit_test_utils.get_fake_request(method='PUT')
        body = {'status': 'error'}
        self.controller.update_status(request, self.job_1['id'], body)
        actual = db_api.job_get_by_id(self.job_1['id'])['status']
        self.assertEqual(actual, body['status'])

    def test_update_status_empty_body(self):
        request = unit_test_utils.get_fake_request(method='PUT')
        body = {}
        self.assertRaises(webob.exc.HTTPBadRequest,
                          self.controller.update_status,
                          request, unit_utils.JOB_UUID1, body)

    def test_update_status_not_found(self):
        request = unit_test_utils.get_fake_request(method='PUT')
        job_id = str(uuid.uuid4())
        body = {'status': 'queued'}
        self.assertRaises(webob.exc.HTTPNotFound,
                          self.controller.update_status,
                          request, job_id, body)
