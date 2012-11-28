from qonos.openstack.common import cfg
from qonos.tests.functional.v1 import base

CONF = cfg.CONF


class TestApi_Simple_DB(base.TestApi):

    def setUp(self):
        CONF.db_api = 'qonos.db.simple.api'
        super(TestApi_Simple_DB, self).setUp()

    def tearDown(self):
        super(TestApi_Simple_DB, self).tearDown()
