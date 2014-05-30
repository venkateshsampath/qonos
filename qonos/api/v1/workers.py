# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright 2013 Rackspace
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import webob.exc

from qonos.api import api
from qonos.api.v1 import api_utils
from qonos.common import exception
from qonos.common import utils
import qonos.db
from qonos.openstack.common.gettextutils import _
from qonos.openstack.common import wsgi


CONF = api.CONF


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

        max_retry = api.job_get_max_retry(action)
        new_timeout = api_utils.get_new_timeout_by_action(action)

        job = self.db_api.job_get_and_assign_next_by_action(
            action, worker_id, max_retry, new_timeout)
        if job:
            utils.serialize_datetimes(job)
            api_utils.serialize_job_metadata(job)
        return {'job': job}


def create_resource():
    """QonoS resource factory method."""
    return wsgi.Resource(WorkersController())
