import webob.exc

from qonos.api.v1 import api_utils
from qonos.common import exception
from qonos.common import utils
import qonos.db
from qonos.openstack.common.gettextutils import _
from qonos.openstack.common import wsgi


class WorkersController(object):

    def __init__(self, db_api=None):
        self.db_api = db_api or qonos.db.get_api()

    def _get_request_params(self, request):
        params = {}
        params['limit'] = request.params.get('limit')
        params['marker'] = request.params.get('marker')
        return params

    def list(self, request):
        params = self._get_request_params(request)
        try:
            params = utils.get_pagination_limit(params)
        except exception.Invalid as e:
            raise webob.exc.HTTPBadRequest(explanation=str(e))
        try:
            workers = self.db_api.worker_get_all(params=params)
        except exception.NotFound:
            raise webob.exc.HTTPNotFound()
        [utils.serialize_datetimes(worker) for worker in workers]
        return {'workers': workers}

    def create(self, request, body):
        worker = self.db_api.worker_create(body.get('worker'))
        utils.serialize_datetimes(worker)
        return {'worker': worker}

    def get(self, request, worker_id):
        try:
            worker = self.db_api.worker_get_by_id(worker_id)
        except exception.NotFound:
            msg = _('Worker %s could not be found.') % worker_id
            raise webob.exc.HTTPNotFound(explanation=msg)
        utils.serialize_datetimes(worker)
        return {'worker': worker}

    def delete(self, request, worker_id):
        try:
            self.db_api.worker_delete(worker_id)
        except exception.NotFound:
            msg = _('Worker %s could not be found.') % worker_id
            raise webob.exc.HTTPNotFound(explanation=msg)

    def get_next_job(self, request, worker_id, body):
        action = body.get('action')
        try:
            # Check that worker exists
            self.db_api.worker_get_by_id(worker_id)
        except exception.NotFound as e:
            msg = _('Worker %s could not be found.') % worker_id
            raise webob.exc.HTTPNotFound(explanation=msg)

        job = self.db_api.job_get_and_assign_next_by_action(action,
                                                            worker_id)
        if job:
            utils.serialize_datetimes(job)
            api_utils.serialize_job_metadata(job)
        return {'job': job}


def create_resource():
    """QonoS resource factory method."""
    return wsgi.Resource(WorkersController())
