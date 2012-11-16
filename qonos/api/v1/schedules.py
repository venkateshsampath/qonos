from webob.exc import HTTPNotImplemented

from qonos.openstack.common import wsgi


class SchedulesController(object):

    def list(self, request):
        raise HTTPNotImplemented

    def create(self, request, body):
        raise HTTPNotImplemented

    def get(self, request, id):
        raise HTTPNotImplemented

    def delete(self, request, id):
        raise HTTPNotImplemented

    def update(self, request, id, body):
        raise HTTPNotImplemented


def create_resource():
    """QonoS resource factory method"""
    return wsgi.Resource(SchedulesController())
