import uuid
import webob.exc

from qonos.api.v1 import workers
from qonos.common import exception
import qonos.db.simple.api as db_api
from qonos.tests import utils as test_utils
from qonos.tests.unit import utils as unit_test_utils
from qonos.tests.unit.utils import WORKER_UUID1


WORKER_ATTRS = ['id', 'host']


class TestWorkersApi(test_utils.BaseTestCase):

    def setUp(self):
        super(TestWorkersApi, self).setUp()
        self.controller = workers.WorkersController(db_api=db_api)
        self._create_workers()

    def tearDown(self):
        super(TestWorkersApi, self).tearDown()
        db_api.reset()

    def _create_workers(self):
        fixture = {'host': 'ameade.cow'}
        self.worker_1 = db_api.worker_create(fixture)
        fixture = {'host': 'foo.bar'}
        self.worker_2 = db_api.worker_create(fixture)

    def test_list(self):
        request = unit_test_utils.get_fake_request(method='GET')
        workers = self.controller.list(request).get('workers')
        self.assertEqual(len(workers), 2)
        for k in WORKER_ATTRS:
            self.assertEqual(set([s[k] for s in workers]),
                             set([self.worker_1[k], self.worker_2[k]]))

    def test_get(self):
        request = unit_test_utils.get_fake_request(method='GET')
        actual = self.controller.get(request,
                                     self.worker_1['id']).get('worker')
        for k in WORKER_ATTRS:
            self.assertEqual(actual[k], self.worker_1[k])

    def test_get_not_found(self):
        request = unit_test_utils.get_fake_request(method='GET')
        worker_id = str(uuid.uuid4())
        self.assertRaises(webob.exc.HTTPNotFound,
                          self.controller.get, request, worker_id)

    def test_create(self):
        request = unit_test_utils.get_fake_request(method='POST')
        host = 'blah'
        fixture = {'worker': {'host': host}}
        actual = self.controller.create(request, fixture)['worker']
        self.assertEqual(host, actual['host'])

    def test_delete(self):
        request = unit_test_utils.get_fake_request(method='GET')
        request = unit_test_utils.get_fake_request(method='DELETE')
        self.controller.delete(request, self.worker_1['id'])
        self.assertRaises(exception.NotFound, db_api.worker_get_by_id,
                          self.worker_1['id'])

    def test_delete_not_found(self):
        request = unit_test_utils.get_fake_request(method='DELETE')
        worker_id = str(uuid.uuid4())
        self.assertRaises(webob.exc.HTTPNotFound,
                          self.controller.delete, request, worker_id)

    def test_get_next_job_unimplemented(self):
        request = unit_test_utils.get_fake_request(method='PUT')
        self.assertRaises(webob.exc.HTTPNotImplemented,
                          self.controller.get_next_job, request,
                          WORKER_UUID1)
