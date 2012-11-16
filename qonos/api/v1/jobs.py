from webob.exc import HTTPNotImplemented

from qonos.openstack.common import wsgi


class JobsController(object):

    def list(self, request):
        raise HTTPNotImplemented

    def get(self, request, id):
        raise HTTPNotImplemented

    def delete(self, request, id):
        raise HTTPNotImplemented

    def get_heartbeat(self, request, id):
        raise HTTPNotImplemented

    def update_heartbeat(self, request, id, body):
        raise HTTPNotImplemented

    def get_status(self, request, id):
        raise HTTPNotImplemented

    def update_status(self, request, id, body):
        raise HTTPNotImplemented


def create_resource():
    """QonoS resource factory method"""
    return wsgi.Resource(JobsController())
