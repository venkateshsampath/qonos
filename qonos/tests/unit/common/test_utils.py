import datetime

from qonos.common import utils
from qonos.openstack.common import timeutils
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

    def test_serialize_datetimes_list(self):
        data = {
            'data': [
                {'foo': datetime.datetime(2012, 5, 16, 15, 27, 36, 325355)},
                {'foo': datetime.datetime(2013, 5, 16, 15, 27, 36, 325355)},
            ]
        }
        date_1_str = '2012-05-16T15:27:36Z'
        date_2_str = '2013-05-16T15:27:36Z'
        utils.serialize_datetimes(data)
        self.assertEqual(data['data'][0]['foo'], date_1_str)
        self.assertEqual(data['data'][1]['foo'], date_2_str)

    def test_serialize_datetimes_nested_dict(self):
        data = {
            'data': {
                'foo': {
                    'bar': datetime.datetime(2012, 5, 16, 15, 27, 36, 325355)
                }
            }
        }
        date_1_str = '2012-05-16T15:27:36Z'
        utils.serialize_datetimes(data)
        self.assertEqual(data['data']['foo']['bar'], date_1_str)

    def test_cron_string_to_datetime(self):
        minute = timeutils.utcnow().minute
        if minute == 0:
            minute = 59
        else:
            minute -= 1

        hour = timeutils.utcnow().hour
        if hour == 0:
            hour = 23
        else:
            hour -= 1
        next_run = utils.cron_string_to_next_datetime(minute=minute,
                                                      hour=hour)

        self.assertTrue(next_run > timeutils.utcnow())
