from webob.exc import (HTTPNotImplemented)

import qonos.openstack.common.log as logging
from qonos.openstack.common import wsgi

LOG = logging.getLogger(__name__)


class TasksController(object):

    def list_tasks(self, req):
        raise HTTPNotImplemented

    def create_task(self, req, body=None):
        raise HTTPNotImplemented

    def get_task(self, req, id):
        LOG.error("test")
        raise HTTPNotImplemented

    def update_task(self, req, id, body=None):
        raise HTTPNotImplemented

    def delete_task(self, req, id):
        raise HTTPNotImplemented


def create_resource():
    """QonoS resource factory method"""
    return wsgi.Resource(TasksController())
