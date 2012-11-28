import routes

from qonos.api.v1 import jobs
from qonos.api.v1 import schedules
from qonos.api.v1 import schedule_metadata
from qonos.api.v1 import workers
from qonos.openstack.common import wsgi


class API(wsgi.Router):

    def __init__(self, mapper):
        schedules_resource = schedules.create_resource()

        mapper.connect('/schedules',
                       controller=schedules_resource,
                       action='list',
                       conditions=dict(method=['GET']))

        mapper.connect('/schedules',
                       controller=schedules_resource,
                       action='create',
                       conditions=dict(method=['POST']))

        mapper.connect('/schedules/{schedule_id}',
                       controller=schedules_resource,
                       action='get',
                       conditions=dict(method=['GET']))

        mapper.connect('/schedules/{schedule_id}',
                       controller=schedules_resource,
                       action='update',
                       conditions=dict(method=['PUT']))

        mapper.connect('/schedules/{schedule_id}',
                       controller=schedules_resource,
                       action='delete',
                       conditions=dict(method=['DELETE']))

        schedule_meta_resource = schedule_metadata.create_resource()

        mapper.connect('/schedules/{schedule_id}/meta',
                       controller=schedule_meta_resource,
                       action='list',
                       conditions=dict(method=['GET']))

        mapper.connect('/schedules/{schedule_id}/meta',
                       controller=schedule_meta_resource,
                       action='create',
                       conditions=dict(method=['POST']))

        mapper.connect('/schedules/{schedule_id}/meta/{key}',
                       controller=schedule_meta_resource,
                       action='get',
                       conditions=dict(method=['GET']))

        mapper.connect('/schedules/{schedule_id}/meta/{key}',
                       controller=schedule_meta_resource,
                       action='update',
                       conditions=dict(method=['PUT']))

        mapper.connect('/schedules/{schedule_id}/meta/{key}',
                       controller=schedule_meta_resource,
                       action='delete',
                       conditions=dict(method=['DELETE']))

        jobs_resource = jobs.create_resource()

        mapper.connect('/jobs',
                       controller=jobs_resource,
                       action='list',
                       conditions=dict(method=['GET']))

        mapper.connect('/jobs',
                       controller=jobs_resource,
                       action='create',
                       conditions=dict(method=['POST']))

        mapper.connect('/jobs/{job_id}',
                       controller=jobs_resource,
                       action='get',
                       conditions=dict(method=['GET']))

        mapper.connect('/jobs/{job_id}',
                       controller=jobs_resource,
                       action='delete',
                       conditions=dict(method=['DELETE']))

        mapper.connect('/jobs/{job_id}/heartbeat',
                       controller=jobs_resource,
                       action='get_heartbeat',
                       conditions=dict(method=['GET']))

        mapper.connect('/jobs/{job_id}/heartbeat',
                       controller=jobs_resource,
                       action='update_heartbeat',
                       conditions=dict(method=['PUT']))

        mapper.connect('/jobs/{job_id}/status',
                       controller=jobs_resource,
                       action='get_status',
                       conditions=dict(method=['GET']))

        mapper.connect('/jobs/{job_id}/status',
                       controller=jobs_resource,
                       action='update_status',
                       conditions=dict(method=['PUT']))

        workers_resource = workers.create_resource()

        mapper.connect('/workers',
                       controller=workers_resource,
                       action='list',
                       conditions=dict(method=['GET']))

        mapper.connect('/workers',
                       controller=workers_resource,
                       action='create',
                       conditions=dict(method=['POST']))

        mapper.connect('/workers/{worker_id}',
                       controller=workers_resource,
                       action='get',
                       conditions=dict(method=['GET']))

        mapper.connect('/workers/{worker_id}',
                       controller=workers_resource,
                       action='delete',
                       conditions=dict(method=['DELETE']))

        mapper.connect('/workers/{worker_id}/jobs/next',
                       controller=workers_resource,
                       action='get_next_job',
                       conditions=dict(method=['PUT']))

        super(API, self).__init__(mapper)

    @classmethod
    def factory(cls, global_conf, **local_conf):
        return cls(routes.Mapper())
