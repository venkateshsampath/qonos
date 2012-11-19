from qonos.openstack.common.gettextutils import _


class QonosException(Exception):
    message = _('An unknown exception occurred')

    def __init__(self, message=None, *args, **kwargs):
        if not message:
            message = self.message
        try:
            message = message % kwargs
        except Exception:
            # at least get the core message out if something happened
            pass
        super(QonosException, self).__init__(message)


class NotFound(QonosException):
    message = _('An object with the specified identifier could not be found.')
