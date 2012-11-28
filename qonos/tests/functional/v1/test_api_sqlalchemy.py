import sys

from qonos.openstack.common import cfg
from qonos.tests import utils as utils
from qonos.tests.functional.v1 import base

CONF = cfg.CONF


def setUpModule():
    CONF.db_api = 'qonos.db.sqlalchemy.api'


def tearDownModule():
    CONF.db_api = None


module = sys.modules[__name__]
utils.import_test_cases(module, base, suffix="_Sqlalchemy_DB")
