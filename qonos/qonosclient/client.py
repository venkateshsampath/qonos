import httplib
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

            return json.loads(body)

    # workers

    def list_workers(self):
        return self._do_request('GET', '/v1/workers')

    def create_worker(self, host):
        body = {'worker': {'host': host}}
        return self._do_request('POST', '/v1/workers', body)

    def get_worker(self, worker_id):
        return self._do_request('GET', '/v1/workers/%s' % worker_id)

    def delete_worker(self, worker_id):
        self._do_request('DELETE', '/v1/workers/%s' % worker_id)

    # schedules

    def list_schedules(self):
        return self._do_request('GET', '/v1/schedules')

    def create_schedule(self, schedule):
        return self._do_request('POST', '/v1/schedules', schedule)

    def get_schedule(self, schedule_id):
        return self._do_request('GET', '/v1/schedules/%s' % schedule_id)

    def update_schedule(self, schedule_id, schedule):
        path = '/v1/schedules/%s' % schedule_id
        return self._do_request('PUT', path, schedule)

    def delete_schedule(self, schedule_id):
        self._do_request('DELETE', '/v1/schedules/%s' % schedule_id)

    # schedule metadata

    def list_schedule_meta(self, schedule_id):
        return self._do_request('GET', '/v1/schedules/%s/meta' % schedule_id)

    def create_schedule_meta(self, schedule_id, key, value):
        meta = {'meta': {key: value}}
        path = '/v1/schedules/%s/meta' % schedule_id
        return self._do_request('POST', path, meta)

    def get_schedule_meta(self, schedule_id, key):
        path = '/v1/schedules/%s/meta/%s' % (schedule_id, key)
        return self._do_request('GET', path)['meta'][key]

    def update_schedule_meta(self, schedule_id, key, value):
        meta = {'meta': {key: value}}
        path = '/v1/schedules/%s/meta/%s' % (schedule_id, key)
        return self._do_request('PUT', path, meta)['meta'][key]

    def delete_schedule_meta(self, schedule_id, key):
        path = '/v1/schedules/%s/meta/%s' % (schedule_id, key)
        return self._do_request('DELETE', path,)
