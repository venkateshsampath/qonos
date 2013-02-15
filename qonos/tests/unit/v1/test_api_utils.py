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

from qonos.api.v1 import api_utils
from qonos.common import exception as exc
from qonos.tests import utils as test_utils


class TestAPIUtils(test_utils.BaseTestCase):

    def test_check_read_only_properties(self):
        values = {'name': 'instance-1'}
        new_values = api_utils.check_read_only_properties(values)
        self.assertEquals(values, new_values)

    def test_check_read_only_properties_exception(self):
        values = {'name': '1', 'updated_at': '2013-02-02 10:10:23'}
        self.assertRaises(exc.Forbidden,
            api_utils.check_read_only_properties, values)
