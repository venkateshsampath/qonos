import webob.exc

from qonos.common import exception
from qonos.common import utils
import qonos.db
from qonos.openstack.common import timeutils
from qonos.openstack.common import cfg
from qonos.openstack.common import wsgi
from qonos.openstack.common.gettextutils import _


CONF = cfg.CONF


class SchedulesController(object):

    def __init__(self, db_api=None):
        self.db_api = db_api or qonos.db.get_api()

    def _validate_limit(self, limit):
        try:
            limit = int(limit)
        except ValueError:
            msg = _("limit param must be an integer")
            raise webob.exc.HTTPBadRequest(explanation=msg)

        if limit <= 0:
            msg = _("limit param must be positive")
            raise webob.exc.HTTPBadRequest(explanation=msg)

        return limit

    def _get_request_params(self, request):
        filter_args = {}
        if request.params.get('next_run_after') is not None:
            next_run_after = request.params['next_run_after']
            next_run_after = timeutils.parse_isotime(next_run_after)
            next_run_after = timeutils.normalize_time(next_run_after)
            filter_args['next_run_after'] = next_run_after

        if request.params.get('next_run_before') is not None:
            next_run_before = request.params['next_run_before']
            next_run_before = timeutils.parse_isotime(next_run_before)
            next_run_before = timeutils.normalize_time(next_run_before)
            filter_args['next_run_before'] = next_run_before

        if request.params.get('tenant_id') is not None:
            filter_args['tenant_id'] = request.params['tenant_id']

        if request.params.get('instance_id') is not None:
            filter_args['instance_id'] = request.params['instance_id']

        if request.params.get('limit') is not None:
            limit = request.params['limit']
            filter_args['limit'] = limit

        if request.params.get('marker') is not None:
            marker = request.params['marker']
            filter_args['marker'] = marker

        return filter_args

    def list(self, request):
        filter_args = self._get_request_params(request)
        limit = filter_args.get('limit') or CONF.limit_param_default
        limit = self._validate_limit(limit)
        limit = min(CONF.api_limit_max, limit)
        filter_args['limit'] = limit
        try:
            schedules = self.db_api.schedule_get_all(filter_args=filter_args)
        except exception.NotFound:
            msg = _('The specified marker could not be found')
            raise webob.exc.HTTPNotFound(explanation=msg)
        [utils.serialize_datetimes(sched) for sched in schedules]
        return {'schedules': schedules}

    def _schedule_to_next_run(self, schedule):
        minute = schedule.get('minute', '*')
        hour = schedule.get('hour', '*')
        day_of_month = schedule.get('day_of_month', '*')
        month = schedule.get('month', '*')
        day_of_week = schedule.get('day_of_week', '*')
        return utils.cron_string_to_next_datetime(minute, hour, day_of_month,
                                                  month, day_of_week)

    def create(self, request, body=None):
        if (body is None or type(body).__name__ != 'dict' or
            body.get('schedule') is None):
            raise webob.exc.HTTPBadRequest()

        values = {}
        values.update(body['schedule'])
        values['next_run'] = self._schedule_to_next_run(body['schedule'])
        schedule = self.db_api.schedule_create(values)

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
            values = {}
            values.update(body['schedule'])
            values['next_run'] = self._schedule_to_next_run(body['schedule'])
            schedule = self.db_api.schedule_update(schedule_id,
                                                   values)
            utils.serialize_datetimes(schedule)
        except exception.NotFound:
            msg = _('Schedule %s could not be found.') % schedule_id
            raise webob.exc.HTTPNotFound(explanation=msg)
        return {'schedule': schedule}


def create_resource():
    """QonoS resource factory method"""
    return wsgi.Resource(SchedulesController())
