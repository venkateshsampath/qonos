import sys

import qonos.db.sqlalchemy.api
from qonos.tests.functional.db import base
from qonos.tests import utils


def setUpModule():
    """Stub in get_db and reset_db for testing the simple db api."""
    base.db_api = qonos.db.sqlalchemy.api
    base.db_api.configure_db()


def tearDownModule():
    """Reset get_db and reset_db for cleanliness."""
    base.db_api = None


#NOTE(ameade): Pull in cross driver db tests
thismodule = sys.modules[__name__]
utils.import_test_cases(thismodule, base, suffix="_Sqlalchemy_DB")
