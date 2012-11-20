import uuid

from qonos.common import exception
import qonos.db.simple.api as db_api
from qonos.openstack.common import uuidutils
from qonos.tests import utils as utils
from qonos.tests.unit import utils as unit_utils


class TestSimpleDBApi(utils.BaseTestCase):

    def setUp(self):
        super(TestSimpleDBApi, self).setUp()

    def test_reset(self):
        db_api.DATA['foo'] = 'bar'
        self.assertEqual(db_api.DATA.get('foo'), 'bar')
        db_api.reset()
        self.assertNotEqual(db_api.DATA.get('foo'), 'bar')

    def test_worker_get_all(self):
        fixture = {'host': ''}
        worker = db_api.worker_create(fixture)
        worker2 = db_api.worker_create(fixture)
        self.assertTrue(worker in db_api.worker_get_all())
        self.assertTrue(worker2 in db_api.worker_get_all())
        self.assertNotEqual(worker, worker2)

    def test_worker_get_by_id(self):
        worker = db_api.worker_create({'host': 'mydomain'})
        self.assertEquals(db_api.worker_get_by_id(worker['id']), worker)

    def test_worker_get_by_id_not_found(self):
        worker_id = str(uuid.uuid4())
        self.assertRaises(exception.NotFound,
                          db_api.worker_get_by_id, worker_id)

    def test_worker_create(self):
        fixture = {'host': 'i.am.cowman'}
        worker = db_api.worker_create(fixture)
        self.assertTrue(uuidutils.is_uuid_like(worker['id']))
        self.assertEqual(worker['host'], fixture['host'])
        self.assertNotEqual(worker.get('created_at'), None)
        self.assertNotEqual(worker.get('updated_at'), None)

    def test_worker_delete(self):
        fixture = {'host': ''}
        worker = db_api.worker_create(fixture)
        self.assertTrue(worker in db_api.worker_get_all())
        db_api.worker_delete(worker['id'])
        self.assertFalse(worker in db_api.worker_get_all())

    def test_worker_delete_not_found(self):
        worker_id = str(uuid.uuid4())
        self.assertRaises(exception.NotFound, db_api.worker_delete, worker_id)


class TestJobs(utils.BaseTestCase):

    def setUp(self):
        super(TestJobs, self).setUp()
        self._create_jobs()

    def tearDown(self):
        super(TestJobs, self).tearDown()
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


    def test_job_create(self):
        fixture = {
            'schedule_id': unit_utils.SCHEDULE_UUID2,
            'worker_id': unit_utils.WORKER_UUID2,
            'status': 'queued',
            'retry_count': 0,
        }
        job = db_api.job_create(fixture)
        self.assertTrue(uuidutils.is_uuid_like(job['id']))
        self.assertNotEqual(job.get('created_at'), None)
        self.assertNotEqual(job.get('updated_at'), None)
        self.assertEqual(job['schedule_id'], fixture['schedule_id'])
        self.assertEqual(job['worker_id'], fixture['worker_id'])
        self.assertEqual(job['status'], fixture['status'])
        self.assertEqual(job['retry_count'], fixture['retry_count'])

    def test_job_get_all(self):
        workers = db_api.job_get_all()
        self.assertEqual(len(workers), 2)

    def test_job_get_by_id(self):
        expected = self.job_1
        actual = db_api.job_get_by_id(self.job_1['id'])
        self.assertEqual(actual['schedule_id'], expected['schedule_id'])
        self.assertEqual(actual['worker_id'], expected['worker_id'])
        self.assertEqual(actual['status'], expected['status'])
        self.assertEqual(actual['retry_count'], expected['retry_count'])


    def test_job_update(self):
        fixture = {
            'status': 'error',
            'retry_count': 2,
        }
        old = db_api.job_get_by_id(self.job_1['id'])
        db_api.job_update(self.job_1['id'], fixture)
        updated = db_api.job_get_by_id(self.job_1['id'])

        self.assertEqual(old['schedule_id'], updated['schedule_id'])
        self.assertEqual(old['worker_id'], updated['worker_id'])
        self.assertNotEqual(old['status'], updated['status'])
        self.assertNotEqual(old['retry_count'], updated['retry_count'])

        self.assertEqual(updated['status'], 'error')
        self.assertEqual(updated['retry_count'], 2)

    def test_job_delete(self):
        self.assertEqual(len(db_api.job_get_all()), 2)
        db_api.job_delete(self.job_1['id'])
        self.assertEqual(len(db_api.job_get_all()), 1)

    def test_job_delete_not_found(self):
        self.assertRaises(exception.NotFound,
                         db_api.job_delete, str(uuid.uuid4))
