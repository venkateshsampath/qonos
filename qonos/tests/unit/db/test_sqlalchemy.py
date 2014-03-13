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

from qonos.common import timeutils
import qonos.db.sqlalchemy.api as db_api
import qonos.db.sqlalchemy.models as models
from qonos.tests import utils as utils


class TestSqlalchemySpecificApi(utils.BaseTestCase):

    def test_force_dict(self):
        @db_api.force_dict
        def return_object():
            return (('foo', 'bar'), ('_sa_instance_state', 'blah'))

        value = return_object()
        self.assertTrue(isinstance(value, dict))
        self.assertFalse('_sa_instance_state' in value)

    def test_force_dict_bad_data(self):
        @db_api.force_dict
        def return_object():
            return "I can't be a dict"

        self.assertRaises(ValueError, return_object)

    def test_force_dict_bad_list(self):
        @db_api.force_dict
        def return_object():
            return ["I can't be a dict"]

        self.assertRaises(ValueError, return_object)

    def test_force_dict_list(self):
        @db_api.force_dict
        def return_object():
            return [(('foo', 'bar'),), (('_sa_instance_state', 'blah'),)]

        value = return_object()
        self.assertFalse(isinstance(value, dict))
        self.assertTrue(isinstance(value[0], dict))
        self.assertEqual(value[0].get('foo'), 'bar')
        self.assertFalse('_sa_instance_state' in value[1])


class TestSqlalchemyTypeDecorator(utils.BaseTestCase):

    def test_no_timezone_datetime_for_db_storing(self):
        datetime_with_tz = timeutils.parse_isotime('2014-03-14T02:30:00Z')

        self.assertIsNotNone(datetime_with_tz.tzinfo)
        no_tz_datetime = models.NoTZDateTime()
        result_datetime = no_tz_datetime.process_bind_param(datetime_with_tz,
                                                            None)
        self.assertIsNone(result_datetime.tzinfo)

    def test_no_timezone_datetime_for_db_retrieval(self):
        datetime_value = timeutils.utcnow()

        self.assertIsNone(datetime_value.tzinfo)
        no_tz_datetime = models.NoTZDateTime()
        result_datetime = no_tz_datetime.process_result_value(datetime_value,
                                                              None)
        self.assertEqual(datetime_value, result_datetime)
