import inspect
import stubout
import unittest

from qonos.common import config
from qonos.openstack.common import cfg


CONF = cfg.CONF


class BaseTestCase(unittest.TestCase):

    def setUp(self):
        super(BaseTestCase, self).setUp()
        #NOTE(ameade): we need config options to be registered
        config.parse_args(args=[])
        self.stubs = stubout.StubOutForTesting()

    def tearDown(self):
        super(BaseTestCase, self).tearDown()
        CONF.reset()
        self.stubs.UnsetAll()

    def config(self, **kw):
        """
        Override some configuration values.

        The keyword arguments are the names of configuration options to
        override and their values.

        If a group argument is supplied, the overrides are applied to
        the specified configuration option group.

        All overrides are automatically cleared at the end of the current
        test by the tearDown() method.
        """
        group = kw.pop('group', None)
        for k, v in kw.iteritems():
            CONF.set_override(k, v, group)

    def assertMetadataInList(self, metadata, meta):
        found = False
        for element in metadata:
            if element['key'] == meta['key']:
                found = True
                self.assertEqual(element['value'], meta['value'])
        self.assertTrue(found)


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
