from qonos.worker import worker
from novaclient.v1_1 import client

from qonos.openstack.common import cfg
from qonos.openstack.common.gettextutils import _
import qonos.openstack.common.log as logging
from qonos.common import config
import datetime

LOG = logging.getLogger(__name__)

snapshot_worker_opts = [
    cfg.StrOpt('auth_url', default="http://127.0.0.100:5000/v2.0/"),
    cfg.StrOpt('nova_admin_user', default='admin'),
    cfg.StrOpt('nova_admin_password', default='admin'),
    cfg.BoolOpt('http_log_debug', default=True),
]

CONF = cfg.CONF
CONF.register_opts(snapshot_worker_opts, group='snapshot_worker')


class SnapshotProcessor(worker.JobProcessor):
    def __init__(self):
        super(SnapshotProcessor, self).__init__()

    def init_processor(self, worker):
        super(SnapshotProcessor, self).init_processor(worker)

    def process_job(self, job):
        LOG.debug("Process job: %s" % str(job))
        auth_url = CONF.snapshot_worker.auth_url
        user = CONF.snapshot_worker.nova_admin_user
        password = CONF.snapshot_worker.nova_admin_password
        debug = CONF.snapshot_worker.http_log_debug

        tenant_id = job['tenant_id']

        c = client.Client(user,
                          password,
                          project_id=tenant_id,
                          auth_url=auth_url,
                          insecure=False,
                          http_log_debug=debug)
        instance_id = self._get_instance_id(job)
        image_id = c.servers.create_image(instance_id,
                                          ('Daily-' +
                                           str(datetime.datetime.utcnow())))
        LOG.debug("Created image: %s" % image_id)
        not_active = True
        while not_active:
            image_status = c.images.get(image_id).status
            LOG.debug("Image status: %s" % image_status)
            not_active = image_status != 'ACTIVE'
        LOG.debug("Snapshot complete")

    def cleanup_processor(self):
        """
        Override to perform processor-specific setup.

        Called AFTER the worker is unregistered from QonoS.
        """
        pass

    def _get_instance_id(self, job):
        metadata = job['job_metadata']
        for meta in metadata:
            if meta['key'] == 'instance_id':
                return meta['value']
        return None
