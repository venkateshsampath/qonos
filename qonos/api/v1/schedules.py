import webob.exc
from qonos.common import exception

import qonos.db.simple.api as db_api
from qonos.openstack.common import wsgi
from qonos.openstack.common.gettextutils import _


class SchedulesController(object):

    def list(self, request):
        return {'schedules': db_api.schedule_get_all()}

    def create(self, request, body):
        return {'schedule': db_api.schedule_create(body['schedule'])}

    def get(self, request, schedule_id):
        try:
            schedule = db_api.schedule_get_by_id(schedule_id)
        except exception.NotFound:
            msg = _('Schedule %s could not be found.') % schedule_id
            raise webob.exc.HTTPNotFound(explanation=msg)
        return {'schedule': schedule}

    def delete(self, request, schedule_id):
        try:
            db_api.schedule_delete(schedule_id)
        except exception.NotFound:
            msg = _('Schedule %s could not be found.') % schedule_id
            raise webob.exc.HTTPNotFound(explanation=msg)

    def update(self, request, schedule_id, body):
        try:
            schedule = db_api.schedule_update(schedule_id, body['schedule'])
        except exception.NotFound:
            msg = _('Schedule %s could not be found.') % schedule_id
            raise webob.exc.HTTPNotFound(explanation=msg)
        return {'schedule': schedule}


def create_resource():
    """QonoS resource factory method"""
    return wsgi.Resource(SchedulesController())
