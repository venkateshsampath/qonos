import routes

from chronos.api.v1 import tasks
from chronos.openstack.common import wsgi


class API(wsgi.Router):

    def __init__(self, mapper):
        chronos_resource = tasks.create_resource()

        mapper.connect('/tasks',
                       controller=chronos_resource,
                       action='list_tasks',
                       conditions=dict(method=['GET']))

        mapper.connect('/tasks',
                       controller=chronos_resource,
                       action='create_task',
                       conditions=dict(method=['POST']))

        mapper.connect('/tasks/{id}',
                       controller=chronos_resource,
                       action='get_task',
                       conditions=dict(method=['GET']))

        mapper.connect('/tasks/{id}',
                       controller=chronos_resource,
                       action='update_task',
                       conditions=dict(method=['PUT']))

        mapper.connect('/tasks/{id}',
                       controller=chronos_resource,
                       action='delete_task',
                       conditions=dict(method=['DELETE']))

        super(API, self).__init__(mapper)

    @classmethod
    def factory(cls, global_conf, **local_conf):
        return cls(routes.Mapper())
