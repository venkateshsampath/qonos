import webob.exc

from qonos.openstack.common import wsgi


class JobsController(object):

    def list(self, request):
        raise webob.exc.HTTPNotImplemented

    def get(self, request, id):
        raise webob.exc.HTTPNotImplemented

    def delete(self, request, id):
        raise webob.exc.HTTPNotImplemented

    def get_heartbeat(self, request, id):
        raise webob.exc.HTTPNotImplemented

    def update_heartbeat(self, request, id, body):
        raise webob.exc.HTTPNotImplemented

    def get_status(self, request, id):
        raise webob.exc.HTTPNotImplemented

    def update_status(self, request, id, body):
        raise webob.exc.HTTPNotImplemented


def create_resource():
    """QonoS resource factory method"""
    return wsgi.Resource(JobsController())
