import inspect
import unittest

from qonos.common import config


class BaseTestCase(unittest.TestCase):

    def setUp(self):
        super(BaseTestCase, self).setUp()
        #NOTE(ameade): we need config options to be registered
        config.parse_args(args=[])

    def tearDown(self):
        super(BaseTestCase, self).tearDown()


def import_test_cases(target_module, test_module, suffix=""):
    """Adds test cases to target module.

    Adds all testcase classes in test_module to target_module and appends an
    optional suffix.

    :param target_module: module which has an attribute set for each test case
    :param test_module: module containing test cases to copy
    :param suffix: an optional suffix to be added to each test case class name

    """
    for name, obj in inspect.getmembers(test_module):
        if inspect.isclass(obj) and issubclass(obj, BaseTestCase):
            setattr(target_module, name + suffix,
                    type(name + suffix, (obj,), {}))
