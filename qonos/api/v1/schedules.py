import webob.exc

from qonos.openstack.common import wsgi


class SchedulesController(object):

    def list(self, request):
        raise webob.exc.HTTPNotImplemented

    def create(self, request, body):
        raise webob.exc.HTTPNotImplemented

    def get(self, request, id):
        raise webob.exc.HTTPNotImplemented

    def delete(self, request, id):
        raise webob.exc.HTTPNotImplemented

    def update(self, request, id, body):
        raise webob.exc.HTTPNotImplemented


def create_resource():
    """QonoS resource factory method"""
    return wsgi.Resource(SchedulesController())
