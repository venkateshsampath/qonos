from webob.exc import (HTTPNotImplemented)

from qonos.openstack.common import wsgi


class TasksController(object):

    def list_tasks(self, req):
        raise HTTPNotImplemented

    def create_task(self, req, body=None):
        raise HTTPNotImplemented

    def get_task(self, req, id):
        raise HTTPNotImplemented

    def update_task(self, req, id, body=None):
        raise HTTPNotImplemented

    def delete_task(self, req, id):
        raise HTTPNotImplemented


def create_resource():
    """QonoS resource factory method"""
    return wsgi.Resource(TasksController())
