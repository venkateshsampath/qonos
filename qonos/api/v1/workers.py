import webob.exc

from qonos.common import exception
import qonos.db.simple.api as db_api
from qonos.openstack.common import wsgi
from qonos.openstack.common.gettextutils import _


class WorkersController(object):

    def list(self, request):
        return {'workers': db_api.worker_get_all()}

    def create(self, request, body):
        return {'worker': db_api.worker_create(body.get('worker'))}

    def get(self, request, worker_id):
        try:
            worker = db_api.worker_get_by_id(worker_id)
        except exception.NotFound:
            msg = _('Worker %s could not be found.') % worker_id
            raise webob.exc.HTTPNotFound(explanation=msg)
        return {'worker': worker}

    def delete(self, request, worker_id):
        try:
            db_api.worker_delete(worker_id)
        except exception.NotFound:
            msg = _('Worker %s could not be found.') % worker_id
            raise webob.exc.HTTPNotFound(explanation=msg)

    def get_next_job(self, request, worker_id):
        raise webob.exc.HTTPNotImplemented


def create_resource():
    """QonoS resource factory method"""
    return wsgi.Resource(WorkersController())
