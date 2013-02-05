import webob.exc

import qonos.api.v1.api_utils as api_utils
from qonos.common import exception
import qonos.db
from qonos.openstack.common import wsgi


class ScheduleMetadataController(object):

    def __init__(self, db_api=None):
        self.db_api = db_api or qonos.db.get_api()

    def list(self, request, schedule_id):
        metadata = self.db_api.schedule_meta_get_all(schedule_id)
        return {'metadata': api_utils.serialize_metadata(metadata)}

    def create(self, request, schedule_id, body):
        meta = body['meta']
        new_meta = api_utils.deserialize_meta(meta)
        try:
            meta = self.db_api.schedule_meta_create(
                schedule_id, new_meta)
        except exception.Duplicate, e:
            raise webob.exc.HTTPConflict(explanation=e)
        return {'meta': api_utils.serialize_meta(meta)}

    def get(self, request, schedule_id, key):
        try:
            meta = self.db_api.schedule_meta_get(schedule_id, key)
        except exception.NotFound, e:
            raise webob.exc.HTTPNotFound(explanation=e)

        return {'meta': api_utils.serialize_meta(meta)}

    def delete(self, request, schedule_id, key):
        try:
            self.db_api.schedule_meta_delete(schedule_id, key)
        except exception.NotFound, e:
            raise webob.exc.HTTPNotFound(explanation=e)

    def update(self, request, schedule_id, key, body):
        meta = body['meta']
        new_meta = api_utils.deserialize_meta(meta)
        try:
            updated_meta = self.db_api.schedule_meta_update(schedule_id,
                                                            key, new_meta)
        except exception.NotFound, e:
            raise webob.exc.HTTPNotFound(explanation=e)
        return {'meta': api_utils.serialize_meta(updated_meta)}


def create_resource():
    """QonoS resource factory method."""
    return wsgi.Resource(ScheduleMetadataController())
