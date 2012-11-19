import webob.exc

import qonos.db.simple.api as db_api
from qonos.openstack.common import wsgi


class WorkersController(object):

    def list(self, request):
        return {'workers': db_api.worker_get_all()}

    def create(self, request, body):
        return {'worker': db_api.worker_create(body.get('worker'))}

    def get(self, request, id):
        return {'worker': db_api.worker_get_by_id(id)}

    def delete(self, request, id):
        db_api.worker_delete(id)

    def get_next_job(self, request, id):
        raise webob.exc.HTTPNotImplemented


def create_resource():
    """QonoS resource factory method"""
    return wsgi.Resource(WorkersController())
