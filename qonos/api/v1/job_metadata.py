import webob.exc

import qonos.api.v1.api_utils as api_utils
from qonos.common import exception
import qonos.db
from qonos.openstack.common import wsgi


class JobMetadataController(object):

    def __init__(self, db_api=None):
        self.db_api = db_api or qonos.db.get_api()

    def list(self, request, job_id):
        metadata = self.db_api.job_meta_get_all_by_job_id(job_id)
        return {'metadata': api_utils.serialize_metadata(metadata)}

    def get(self, request, job_id, key):
        try:
            meta = self.db_api.job_meta_get(job_id, key)
        except exception.NotFound, e:
            raise webob.exc.HTTPNotFound(explanation=e)

        return {'meta': api_utils.serialize_meta(meta)}


def create_resource():
    """QonoS resource factory method."""
    return wsgi.Resource(JobMetadataController())
