import qonos.db.simple.api as db_api
from qonos.openstack.common import uuidutils
from qonos.tests import utils as utils


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
