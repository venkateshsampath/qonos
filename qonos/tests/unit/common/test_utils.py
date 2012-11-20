import datetime

from qonos.common import utils
from qonos.tests import utils as test_utils


class TestUtils(test_utils.BaseTestCase):

    def setUp(self):
        super(TestUtils, self).setUp()

    def test_serialize_datetimes(self):
        date_1 = datetime.datetime(2012, 5, 16, 15, 27, 36, 325355)
        date_2 = datetime.datetime(2013, 5, 16, 15, 27, 36, 325355)
        date_1_str = '2012-05-16T15:27:36Z'
        date_2_str = '2013-05-16T15:27:36Z'
        data = {'foo': date_1, 'bar': date_2}
        expected = {'foo': date_1_str, 'bar': date_2_str}
        utils.serialize_datetimes(data)
        self.assertEqual(data, expected)
