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

import httplib

from qonos.common import utils
from qonos.openstack.common import log as logging

try:
    import json
except ImportError:
    import simplejson as json

from qonos.qonosclient import exception

LOG = logging.getLogger(__name__)


class Client(object):

    def __init__(self, endpoint, port):
        self.endpoint = endpoint
        self.port = port

    def _do_request(self, method, url, body=None):
        conn = httplib.HTTPConnection(self.endpoint, self.port)
        if body and isinstance(body, dict):
            body = json.dumps(body)
        conn.request(method, url, body=body,
                     headers={'Content-Type': 'application/json'})
        response = conn.getresponse()
        if response.status == 400:
            raise exception.BadRequest('Bad Request Received')

        if response.status == 404:
            raise exception.NotFound('Resource Not Found')

        if response.status == 409:
            raise exception.Duplicate('Resource Exists')

        if method != 'DELETE':
            body = response.read()
            if body != '':
                return json.loads(body)

    ######## workers

    def list_workers(self, params={}):
        path = '/v1/workers%s'
        query = '?'
        for param in params:
            query += ('%s=%s&' % (param, params[param]))
        return self._do_request('GET', path % query)['workers']

    def create_worker(self, host):
        body = {'worker': {'host': host}}
        return self._do_request('POST', '/v1/workers', body)['worker']

    def get_worker(self, worker_id):
        return self._do_request('GET', '/v1/workers/%s' % worker_id)['worker']

    def delete_worker(self, worker_id):
        self._do_request('DELETE', '/v1/workers/%s' % worker_id)

    def get_next_job(self, worker_id, action):
        body = {'action': action}
        return self._do_request('POST', '/v1/workers/%s/jobs' % worker_id,
                                body)

    ######## schedules

    def list_schedules(self, filter_args={}):
        path = '/v1/schedules%s'
        query = '?'
        for key in filter_args:
            query += ('%s=%s&' % (key, filter_args[key]))
        response = self._do_request('GET', path % query)
        schedules = response.get('schedules')
        return schedules

    def create_schedule(self, schedule):
        return self._do_request('POST', '/v1/schedules', schedule)['schedule']

    def get_schedule(self, schedule_id):
        path = '/v1/schedules/%s' % schedule_id
        return self._do_request('GET', path)['schedule']

    def update_schedule(self, schedule_id, schedule):
        path = '/v1/schedules/%s' % schedule_id
        return self._do_request('PUT', path, schedule)['schedule']

    def delete_schedule(self, schedule_id):
        self._do_request('DELETE', '/v1/schedules/%s' % schedule_id)

    ######## schedule metadata

    def list_schedule_metadata(self, schedule_id):
        path = '/v1/schedules/%s/metadata' % schedule_id
        return self._do_request('GET', path)['metadata']

    def update_schedule_metadata(self, schedule_id, values):
        meta = {'metadata': values}
        path = '/v1/schedules/%s/metadata' % schedule_id
        return self._do_request('PUT', path, meta)['metadata']

    ######## jobs

    def list_jobs(self, params={}):
        path = '/v1/jobs%s'
        query = '?'
        for key in params:
            query += ('%s=%s&' % (key, params[key]))
        return self._do_request('GET', path % query)['jobs']

    def create_job(self, schedule_id, next_run=None):
        job = {'job': {'schedule_id': schedule_id}}
        if next_run:
            job['job']['next_run'] = next_run
        return self._do_request('POST', 'v1/jobs', job)['job']

    def get_job(self, job_id):
        path = '/v1/jobs/%s' % job_id
        return self._do_request('GET', path)['job']

    def update_job_status(self, job_id, status, timeout=None,
                          error_message=None):
        body = {'status': {'status': status}}

        if status.upper() == 'ERROR' and error_message:
            body['status']['error_message'] = error_message
        if timeout:
            body['status']['timeout'] = timeout
            utils.serialize_datetimes(body)

        path = '/v1/jobs/%s/status' % job_id
        return self._do_request('PUT', path, body)['status']

    def delete_job(self, job_id):
        path = '/v1/jobs/%s' % job_id
        return self._do_request('DELETE', path)

    def list_job_metadata(self, job_id):
        path = '/v1/jobs/%s/metadata' % job_id
        return self._do_request('GET', path)['metadata']

    def update_job_metadata(self, job_id, values):
        meta = {'metadata': values}
        path = '/v1/jobs/%s/metadata' % job_id
        return self._do_request('PUT', path, meta)['metadata']


def create_client(endpoint, port):
    return Client(endpoint, port)
