from qonos.openstack.common import cfg
from qonos.tests import utils as utils
from qonos.tests.functional.v1 import base

CONF = cfg.CONF


class TestApi_Sqlalchemy_DB(base.TestApi):

    def setUp(self):
        CONF.db_api = 'qonos.db.sqlalchemy.api'
        super(TestApi_Sqlalchemy_DB, self).setUp()

    def tearDown(self):
        super(TestApi_Sqlalchemy_DB, self).tearDown()
