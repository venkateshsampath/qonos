import webob.exc

from qonos.common import exception
from qonos.common import utils
import qonos.db
from qonos.openstack.common import wsgi
from qonos.openstack.common.gettextutils import _


class SchedulesController(object):

    def __init__(self, db_api=None):
        self.db_api = db_api or qonos.db.get_api()

    def list(self, request):
        schedules = self.db_api.schedule_get_all()
        [utils.serialize_datetimes(sched) for sched in schedules]
        return {'schedules': schedules}

    def create(self, request, body):
        if body is None or body.get('schedule') is None:
            raise webob.exc.HTTPBadRequest()

        schedule = self.db_api.schedule_create(body['schedule'])
        utils.serialize_datetimes(schedule)
        return {'schedule': schedule}

    def get(self, request, schedule_id):
        try:
            schedule = self.db_api.schedule_get_by_id(schedule_id)
            utils.serialize_datetimes(schedule)
        except exception.NotFound:
            msg = _('Schedule %s could not be found.') % schedule_id
            raise webob.exc.HTTPNotFound(explanation=msg)
        return {'schedule': schedule}

    def delete(self, request, schedule_id):
        try:
            self.db_api.schedule_delete(schedule_id)
        except exception.NotFound:
            msg = _('Schedule %s could not be found.') % schedule_id
            raise webob.exc.HTTPNotFound(explanation=msg)

    def update(self, request, schedule_id, body):
        if body is None or body.get('schedule') is None:
            raise webob.exc.HTTPBadRequest()

        try:
            schedule = self.db_api.schedule_update(schedule_id,
                                                   body['schedule'])
            utils.serialize_datetimes(schedule)
        except exception.NotFound:
            msg = _('Schedule %s could not be found.') % schedule_id
            raise webob.exc.HTTPNotFound(explanation=msg)
        return {'schedule': schedule}


def create_resource():
    """QonoS resource factory method"""
    return wsgi.Resource(SchedulesController())
