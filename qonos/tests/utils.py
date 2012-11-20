import unittest

from qonos.common import config


class BaseTestCase(unittest.TestCase):

    def setUp(self):
        super(BaseTestCase, self).setUp()
        #NOTE(ameade): we need config options to be registered
        config.parse_args(args=[])

    def tearDown(self):
        super(BaseTestCase, self).tearDown()
