import webob.exc

from qonos.openstack.common import wsgi


class WorkersController(object):

    def list(self, request):
        raise webob.exc.HTTPNotImplemented

    def create(self, request):
        raise webob.exc.HTTPNotImplemented

    def get(self, request, id):
        raise webob.exc.HTTPNotImplemented

    def delete(self, request, id):
        raise webob.exc.HTTPNotImplemented

    def get_next_job(self, request, id):
        raise webob.exc.HTTPNotImplemented


def create_resource():
    """QonoS resource factory method"""
    return wsgi.Resource(WorkersController())
