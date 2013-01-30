from qonos.common import exception as exc
from qonos.common import utils
from qonos.tests import utils as test_utils


class TestLimitParam(test_utils.BaseTestCase):

    def test_list_limit(self):
        limit = utils.get_pagination_limit({'limit': '1'})
        one = 1
        self.assertEqual({'limit': one}, limit)

    def test_list_limit_invalid_format(self):
        self.assertRaises(exc.Invalid, utils.get_pagination_limit,
                          {'limit': 'a'})

    def test_list_zero_limit(self):
        self.assertRaises(exc.Invalid, utils.get_pagination_limit,
                          {'limit': '0'})

    def test_list_negative_limit(self):
        self.assertRaises(exc.Invalid, utils.get_pagination_limit,
                          {'limit': '-1'})

    def test_list_fraction_limit(self):
        self.assertRaises(exc.Invalid, utils.get_pagination_limit,
                          {'limit': '1.1'})
