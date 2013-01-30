import webob
import webob.exc

from qonos.openstack.common.gettextutils import _
from qonos.openstack.common import log as logging
from qonos.openstack.common import wsgi as wsgi

LOG = logging.getLogger(__name__)


class FaultWrapper(wsgi.Middleware):
    """Calls down the middleware stack, making exceptions into faults."""

    _status_to_type = {}

    def _error(self, inner, req):
        LOG.exception(_("Caught error: %s"), unicode(inner))

        safe = getattr(inner, 'safe', False)
        headers = getattr(inner, 'headers', None)
        status = getattr(inner, 'code', 500)
        if status is None:
            status = 500

        msg_dict = dict(url=req.url, status=status)
        LOG.info(_("%(url)s returned with HTTP %(status)d") % msg_dict)

        return webob.exc.HTTPInternalServerError()

    @webob.dec.wsgify(RequestClass=wsgi.Request)
    def __call__(self, req):
        try:
            return req.get_response(self.application)
        except Exception as ex:
            return self._error(ex, req)

    @classmethod
    def factory(cls, global_conf, **local_conf):
        def filter(app):
            return cls(app)
        return filter
