import webob.exc

from qonos.common import exception
from qonos.common import utils
import qonos.db
from qonos.openstack.common import timeutils
from qonos.openstack.common import wsgi
from qonos.openstack.common.gettextutils import _


class JobsController(object):

    def __init__(self, db_api=None):
        self.db_api = db_api or qonos.db.get_api()

    def list(self, request):
        jobs = self.db_api.job_get_all()
        [utils.serialize_datetimes(job) for job in jobs]
        return {'jobs': jobs}

    def create(self, request, body):
        if (body is None or body.get('job') is None or
                body['job'].get('schedule_id') is None):
            raise webob.exc.HTTPBadRequest()
        job = body['job']

        try:
            schedule = self.db_api.schedule_get_by_id(job['schedule_id'])
        except exception.NotFound:
            raise webob.exc.HTTPNotFound()

        values = {}
        values.update(job)
        values['tenant_id'] = schedule['tenant_id']
        values['action'] = schedule['action']
        values['status'] = 'queued'

        job = self.db_api.job_create(values)
        utils.serialize_datetimes(job)

        return {'job': job}

    def get(self, request, job_id):
        try:
            job = self.db_api.job_get_by_id(job_id)
        except exception.NotFound:
            raise webob.exc.HTTPNotFound
        utils.serialize_datetimes(job)
        return {'job': job}

    def delete(self, request, job_id):
        try:
            self.db_api.job_delete(job_id)
        except exception.NotFound:
            msg = _('Job %s could not be found.') % job_id
            raise webob.exc.HTTPNotFound(explanation=msg)

    def get_heartbeat(self, request, job_id):
        try:
            updated_at = self.db_api.job_updated_at_get_by_id(job_id)
        except exception.NotFound:
            msg = _('Job %s could not be found.') % job_id
            raise webob.exc.HTTPNotFound(explanation=msg)

        heartbeat = {'heartbeat': updated_at}
        utils.serialize_datetimes(heartbeat)
        return heartbeat

    def update_heartbeat(self, request, job_id, body):
        updated_at = body.get('heartbeat')
        if not updated_at:
            raise webob.exc.HTTPBadRequest()

        try:
            updated_at = timeutils.parse_isotime(updated_at)
        except ValueError:
            msg = _('Must supply a timestamp in valid format.')
            raise webob.exc.HTTPBadRequest(explanation=msg)

        try:
            self.db_api.job_update(job_id, {'updated_at': updated_at})
        except exception.NotFound:
            msg = _('Job %s could not be found.') % job_id
            raise webob.exc.HTTPNotFound(explanation=msg)

    def get_status(self, request, job_id):
        try:
            status = self.db_api.job_status_get_by_id(job_id)
        except exception.NotFound:
            msg = _('Job %s could not be found.') % job_id
            raise webob.exc.HTTPNotFound(explanation=msg)

        return {'status': status}

    def update_status(self, request, job_id, body):
        status = body.get('status')
        if not status:
            raise webob.exc.HTTPBadRequest()

        try:
            self.db_api.job_update(job_id, {'status': status})
        except exception.NotFound:
            msg = _('Job %s could not be found.') % job_id
            raise webob.exc.HTTPNotFound(explanation=msg)


def create_resource():
    """QonoS resource factory method"""
    return wsgi.Resource(JobsController())
