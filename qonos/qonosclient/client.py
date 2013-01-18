import httplib

from qonos.openstack.common import timeutils

try:
    import json
except ImportError:
    import simplejson as json

from qonos.qonosclient import exception


class Client(object):

    def __init__(self, endpoint, port):
        self.endpoint = endpoint
        self.port = port

    def _do_request(self, method, url, body=None):
        conn = httplib.HTTPConnection(self.endpoint, self.port)
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

    def create_worker(self, host, worker_name):
        body = {'worker': {'host': host, 'worker_name': worker_name}}
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
        #return self._do_request('GET', path % query)['schedules']
        res = self._do_request('GET', path % query)
        return [res['schedules'], res['next_page']]

    def create_schedule(self, schedule):
        return self._do_request('POST', '/v1/schedules', schedule)['schedule']

    def get_schedule(self, schedule_id):
        path = '/v1/schedules/%s' % schedule_id
        return self._do_request('GET', path)['schedule']
        #return self._do_request('GET', path)

    def update_schedule(self, schedule_id, schedule):
        path = '/v1/schedules/%s' % schedule_id
        return self._do_request('PUT', path, schedule)['schedule']

    def delete_schedule(self, schedule_id):
        self._do_request('DELETE', '/v1/schedules/%s' % schedule_id)

    ######## schedule metadata

    def list_schedule_meta(self, schedule_id):
        path = '/v1/schedules/%s/meta' % schedule_id
        return self._do_request('GET', path)['metadata']

    def create_schedule_meta(self, schedule_id, key, value):
        meta = {'meta': {'key': key, 'value': value}}
        path = '/v1/schedules/%s/meta' % schedule_id
        return self._do_request('POST', path, meta)['meta']

    def get_schedule_meta(self, schedule_id, key):
        path = '/v1/schedules/%s/meta/%s' % (schedule_id, key)
        return self._do_request('GET', path)['meta']['value']

    def update_schedule_meta(self, schedule_id, key, value):
        meta = {'meta': {'key': key, 'value': value}}
        path = '/v1/schedules/%s/meta/%s' % (schedule_id, key)
        return self._do_request('PUT', path, meta)['meta']['value']

    def delete_schedule_meta(self, schedule_id, key):
        path = '/v1/schedules/%s/meta/%s' % (schedule_id, key)
        return self._do_request('DELETE', path)

    ######## jobs

    def list_jobs(self, params={}):
        path = '/v1/jobs%s'
        query = '?'
        for key in params:
            query += ('%s=%s&' % (key, params[key]))
        return self._do_request('GET', path % query)['jobs']

    def create_job(self, schedule_id):
        job = {'job': {'schedule_id': schedule_id}}
        return self._do_request('POST', 'v1/jobs', job)['job']

    def get_job(self, job_id):
        path = '/v1/jobs/%s' % job_id
        return self._do_request('GET', path)['job']

    def get_job_heartbeat(self, job_id):
        path = '/v1/jobs/%s/heartbeat' % job_id
        return self._do_request('GET', path)

    def job_heartbeat(self, job_id):
        body = {'heartbeat': timeutils.isotime()}
        path = '/v1/jobs/%s/heartbeat' % job_id
        return self._do_request('PUT', path, body)

    def get_job_status(self, job_id):
        path = '/v1/jobs/%s/status' % job_id
        return self._do_request('GET', path)

    def update_job_status(self, job_id, status, timeout=None):
        body = {'status': {'status': status}}
        if timeout:
            body['status']['timeout'] = timeout

        path = '/v1/jobs/%s/status' % job_id
        return self._do_request('PUT', path, body)

    def delete_job(self, job_id):
        path = '/v1/jobs/%s' % job_id
        return self._do_request('DELETE', path)

    def list_job_metadata(self, job_id):
        path = '/v1/jobs/%s/meta' % job_id
        return self._do_request('GET', path)['metadata']

    def get_job_metadata(self, job_id, key):
        path = '/v1/jobs/%s/meta/%s' % (job_id, key)
        return self._do_request('GET', path)['meta']['value']


def create_client(endpoint, port):
    return Client(endpoint, port)
