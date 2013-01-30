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
                                 month="*", day_of_week="*"):
    cron_string = ("%s %s %s %s %s" %
                  (minute or '*',
                   hour or '*',
                   day_of_month or '*',
                   month or '*',
                   day_of_week or '*'))
    iter = croniter(cron_string, timeutils.utcnow())
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
