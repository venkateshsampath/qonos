import datetime

from qonos.openstack.common import timeutils


def serialize_datetimes(data):
    """Serializes datetimes to strings in the top level values of a dict."""
    for (k, v) in data.iteritems():
        if isinstance(v, datetime.datetime):
            data[k] = timeutils.isotime(v)
