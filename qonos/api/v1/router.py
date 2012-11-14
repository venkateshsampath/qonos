import routes

from qonos.api.v1 import tasks
from qonos.openstack.common import wsgi


class API(wsgi.Router):

    def __init__(self, mapper):
        qonos_resource = tasks.create_resource()

        mapper.connect('/tasks',
                       controller=qonos_resource,
                       action='list_tasks',
                       conditions=dict(method=['GET']))

        mapper.connect('/tasks',
                       controller=qonos_resource,
                       action='create_task',
                       conditions=dict(method=['POST']))

        mapper.connect('/tasks/{id}',
                       controller=qonos_resource,
                       action='get_task',
                       conditions=dict(method=['GET']))

        mapper.connect('/tasks/{id}',
                       controller=qonos_resource,
                       action='update_task',
                       conditions=dict(method=['PUT']))

        mapper.connect('/tasks/{id}',
                       controller=qonos_resource,
                       action='delete_task',
                       conditions=dict(method=['DELETE']))

        super(API, self).__init__(mapper)

    @classmethod
    def factory(cls, global_conf, **local_conf):
        return cls(routes.Mapper())
