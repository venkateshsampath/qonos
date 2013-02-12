# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright 2013 Rackspace
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import datetime
import logging as pylog

from qonos.common import utils
from qonos.openstack.common import timeutils
from qonos.tests import utils as test_utils


class TestUtils(test_utils.BaseTestCase):

    def setUp(self):
        super(TestUtils, self).setUp()

    def tearDown(self):
        super(TestUtils, self).tearDown()
        self.stubs.UnsetAll()

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

    def test_cron_string_to_datetime_from_time(self):
        start_time = datetime.datetime(1900, 5, 16, 0, 0, 0, 0)
        next_run = utils.cron_string_to_next_datetime(minute=30,
                                                      hour=5,
                                                      start_time=start_time)
        expected = datetime.datetime(1900, 5, 16, 5, 30, 0, 0)

        self.assertTrue(next_run == expected)

    def test_get_qonos_open_file_log_handlers(self):

        class FakeStream(object):
            pass

        fake_stream = FakeStream()
        fake_stream.fileno = ''

        class FakeHandler(object):
            pass

        fake_handler = FakeHandler()
        fake_handler.stream = fake_stream

        class FakeLogger(object):
            pass

        fake_logger = FakeLogger()
        fake_logger.handlers = [None, fake_handler, FakeHandler()]

        def fake_get_logger(name):
            self.assertEqual(name, 'qonos')
            return fake_logger

        self.stubs.Set(pylog, 'getLogger', fake_get_logger)
        open_files = utils.get_qonos_open_file_log_handlers()
        self.assertEqual(open_files, [fake_stream])
