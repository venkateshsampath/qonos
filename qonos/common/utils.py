import datetime

from croniter.croniter import croniter

from qonos.openstack.common import timeutils


def serialize_datetimes(data):
    """Serializes datetimes to strings in the top level values of a dict."""
    for (k, v) in data.iteritems():
        if isinstance(v, datetime.datetime):
            data[k] = timeutils.isotime(v)


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
