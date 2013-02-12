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

from croniter.croniter import croniter

from qonos.common import exception as exc
from qonos.openstack.common import cfg
from qonos.openstack.common.gettextutils import _
from qonos.openstack.common import timeutils


CONF = cfg.CONF


def serialize_datetimes(data):
    """Serializes datetimes to strings in the top level values of a dict."""
    for (k, v) in data.iteritems():
        if isinstance(v, datetime.datetime):
            data[k] = timeutils.isotime(v)
        elif isinstance(v, list):
            for item in v:
                serialize_datetimes(item)
        elif isinstance(v, dict):
            serialize_datetimes(v)


def cron_string_to_next_datetime(minute="*", hour="*", day_of_month="*",
                                 month="*", day_of_week="*", start_time=None):
    start_time = start_time or timeutils.utcnow()
    cron_string = ("%s %s %s %s %s" %
                  (minute or '*',
                   hour or '*',
                   day_of_month or '*',
                   month or '*',
                   day_of_week or '*'))
    iter = croniter(cron_string, start_time)
    return iter.get_next(datetime.datetime)


def _validate_limit(limit):
    try:
        limit = int(limit)
    except ValueError:
        msg = _("limit param must be an integer")
        raise exc.Invalid(message=msg)
    if limit <= 0:
        msg = _("limit param must be positive")
        raise exc.Invalid(message=msg)
    return limit


def get_pagination_limit(params):
        limit = params.get('limit') or CONF.limit_param_default
        limit = _validate_limit(limit)
        limit = min(CONF.api_limit_max, limit)
        params['limit'] = limit
        return params
