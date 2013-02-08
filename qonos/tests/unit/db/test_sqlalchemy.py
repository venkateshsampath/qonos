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

import qonos.db.sqlalchemy.api as db_api
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
