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
        if response.status == 404:
            raise exception.NotFound('Resource Not Found')

        if method != 'DELETE':
            body = response.read()
            if body != '':
                return json.loads(body)

    ######## workers

    def list_workers(self):
        return self._do_request('GET', '/v1/workers')

    def create_worker(self, host):
        body = {'worker': {'host': host}}
        return self._do_request('POST', '/v1/workers', body)

    def get_worker(self, worker_id):
        return self._do_request('GET', '/v1/workers/%s' % worker_id)

    def delete_worker(self, worker_id):
        self._do_request('DELETE', '/v1/workers/%s' % worker_id)

    ######## schedules

    def list_schedules(self, filter_args={}):
        path = '/v1/schedules%s'
        query = '?'
        for key in filter_args:
            query += ('%s=%s&' % (key, filter_args[key]))
        return self._do_request('GET', path % query)

    def create_schedule(self, schedule):
        return self._do_request('POST', '/v1/schedules', schedule)

    def get_schedule(self, schedule_id):
        return self._do_request('GET', '/v1/schedules/%s' % schedule_id)

    def update_schedule(self, schedule_id, schedule):
        path = '/v1/schedules/%s' % schedule_id
        return self._do_request('PUT', path, schedule)

    def delete_schedule(self, schedule_id):
        self._do_request('DELETE', '/v1/schedules/%s' % schedule_id)

    ######## schedule metadata

    def list_schedule_meta(self, schedule_id):
        return self._do_request('GET', '/v1/schedules/%s/meta' % schedule_id)

    def create_schedule_meta(self, schedule_id, key, value):
        meta = {'meta': {'key': key, 'value': value}}
        path = '/v1/schedules/%s/meta' % schedule_id
        return self._do_request('POST', path, meta)

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

    def list_jobs(self):
        return self._do_request('GET', '/v1/jobs')

    def create_job(self, job):
        return self._do_request('POST', 'v1/jobs', job)

    def get_job(self, job_id):
        path = '/v1/jobs/%s' % job_id
        return self._do_request('GET', path)

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

    def update_job_status(self, job_id, status):
        body = {'status': status}
        path = '/v1/jobs/%s/status' % job_id
        return self._do_request('PUT', path, body)

    def delete_job(self, job_id):
        path = '/v1/jobs/%s' % job_id
        return self._do_request('DELETE', path)
