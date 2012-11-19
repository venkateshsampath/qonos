import qonos.db.simple.api as db_api
from qonos.tests import utils as test_utils


class TestSimpleDBApi(test_utils.BaseTestCase):

    def setUp(self):
        super(TestSimpleDBApi, self).setUp()


    def test_reset(self):
        db_api.DATA['foo'] = 'bar'
        self.assertEqual(db_api.DATA.get('foo'), 'bar')
        db_api.reset()
        self.assertNotEqual(db_api.DATA.get('foo'), 'bar')
