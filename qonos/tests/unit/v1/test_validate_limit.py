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
