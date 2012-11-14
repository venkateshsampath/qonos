import unittest


class BaseTestCase(unittest.TestCase):

    def setUp(self):
        super(BaseTestCase, self).setUp()

    def tearDown(self):
        super(BaseTestCase, self).tearDown()
