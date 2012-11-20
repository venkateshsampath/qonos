import webob.exc

from qonos.common import exception
from qonos.common import utils
import qonos.db
from qonos.openstack.common import wsgi
from qonos.openstack.common.gettextutils import _


class ScheduleMetadataController(object):

    def __init__(self, db_api=None):
        self.db_api = db_api or qonos.db.get_api()

    def list(self, request, schedule_id):
        db_metadata = self.db_api.schedule_meta_get_all(schedule_id)
        metadata = [self._db_to_api_meta(db_meta) for db_meta in db_metadata]
        return {'metadata': metadata}

    def _validate_meta(self, body):
        if (body is None or body.get('meta') is None or
                len(body['meta'].keys()) != 1):
            raise webob.exc.HTTPBadRequest()

    def _api_to_db_meta(self, meta):
        key = meta['meta'].keys()[0]
        value = meta['meta'][key]
        db_meta = {'key': key, 'value': value}
        return db_meta

    def _db_to_api_meta(self, db_meta):
        meta = {}
        meta[db_meta['key']] = db_meta['value']
        return meta

    def create(self, request, schedule_id, body):
        self._validate_meta(body)
        db_meta = self._api_to_db_meta(body)
        db_meta = self.db_api.schedule_meta_create(schedule_id, db_meta)
        return {'meta': self._db_to_api_meta(db_meta)}

    def get(self, request, schedule_id, key):
        try:
            db_meta = self.db_api.schedule_meta_get(schedule_id, key)
        except exception.NotFound, e:
            raise webob.exc.HTTPNotFound(explanation=e)

        meta = self._db_to_api_meta(db_meta)
        return {'meta': meta}

    def delete(self, request, schedule_id, key):
        try:
            self.db_api.schedule_meta_delete(schedule_id, key)
        except exception.NotFound, e:
            raise webob.exc.HTTPNotFound(explanation=e)

    def update(self, request, schedule_id, key, body):
        self._validate_meta(body)
        db_meta = self._api_to_db_meta(body)
        try:
            updated_db_meta = self.db_api.schedule_meta_update(schedule_id,
                                                               key,
                                                               db_meta)
        except exception.NotFound, e:
            raise webob.exc.HTTPNotFound(explanation=e)

        updated_meta = self._db_to_api_meta(updated_db_meta)
        return {'meta': updated_meta}


def create_resource():
    """QonoS resource factory method"""
    return wsgi.Resource(ScheduleMetadataController())
