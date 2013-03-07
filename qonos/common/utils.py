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

import contextlib
import datetime
import logging as pylog

from croniter.croniter import croniter
from oslo.config import cfg

from qonos.common import exception as exc
from qonos.common import timeutils
from qonos.openstack.common.gettextutils import _
import qonos.openstack.common.log as logging
from qonos.openstack.common.notifier import api as notifier_api

LOG = logging.getLogger(__name__)


CONF = cfg.CONF
CONF.import_opt('host', 'qonos.netconf')


def generate_notification(context, event_type, payload, level='DEBUG'):
    publisher_id = notifier_api.publisher_id('qonos', CONF.host)
    notifier_api.notify(context, publisher_id, event_type, level, payload)


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


def get_qonos_open_file_log_handlers():
    """Returns a list of all open file log handlers."""
    open_files = []
    for handler in pylog.getLogger('qonos').handlers:
        if (hasattr(handler, 'stream') and
                hasattr(handler.stream, 'fileno')):
            open_files.append(handler.stream)
    return open_files


@contextlib.contextmanager
def log_warning_and_dismiss_exception():
    try:
        yield
    except Exception as ex:
        msg = '%(name)s: %(message)s'
        LOG.warn(msg % {'name': unicode(type(ex).__name__),
                        'message': unicode(ex)})
