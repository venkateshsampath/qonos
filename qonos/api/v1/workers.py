from webob.exc import HTTPNotImplemented

from qonos.openstack.common import wsgi


class WorkersController(object):

    def list(self, request):
        raise HTTPNotImplemented

    def create(self, request):
        raise HTTPNotImplemented

    def get(self, request, id):
        raise HTTPNotImplemented

    def delete(self, request, id):
        raise HTTPNotImplemented

    def get_next_job(self, request, id):
        raise HTTPNotImplemented


def create_resource():
    """QonoS resource factory method"""
    return wsgi.Resource(WorkersController())
