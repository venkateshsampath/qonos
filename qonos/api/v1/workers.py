import webob.exc

from qonos.common import exception
from qonos.common import utils
import qonos.db.simple.api as db_api
from qonos.openstack.common import wsgi
from qonos.openstack.common.gettextutils import _


class WorkersController(object):

    def list(self, request):
        workers = db_api.worker_get_all()
        [utils.serialize_datetimes(worker) for worker in workers]
        return {'workers': workers}

    def create(self, request, body):
        worker = db_api.worker_create(body.get('worker'))
        utils.serialize_datetimes(worker)
        return {'worker': worker}

    def get(self, request, worker_id):
        try:
            worker = db_api.worker_get_by_id(worker_id)
        except exception.NotFound:
            msg = _('Worker %s could not be found.') % worker_id
            raise webob.exc.HTTPNotFound(explanation=msg)
        utils.serialize_datetimes(worker)
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
