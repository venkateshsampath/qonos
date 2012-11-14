from webob.exc import (HTTPNotImplemented)

from chronos.openstack.common import wsgi


class TasksController(object):

    def list_tasks(self, req):
        raise HTTPNotImplemented

    def create_task(self, req):
        raise HTTPNotImplemented

    def get_task(self, req, id):
        raise HTTPNotImplemented

    def update_task(self, req, id):
        raise HTTPNotImplemented

    def delete_task(self, req, id):
        raise HTTPNotImplemented


def create_resource():
    """Chronos resource factory method"""
    return wsgi.Resource(TasksController())
