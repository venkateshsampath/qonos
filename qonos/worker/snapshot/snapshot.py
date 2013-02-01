import datetime

from novaclient.v1_1 import client
from qonos.openstack.common import cfg
from qonos.openstack.common.gettextutils import _
import qonos.openstack.common.log as logging
import qonos.openstack.common.timeutils as timeutils
from qonos.worker import worker

LOG = logging.getLogger(__name__)

snapshot_worker_opts = [
    cfg.StrOpt('auth_url', default="http://127.0.0.100:5000/v2.0/"),
    cfg.StrOpt('nova_admin_user', default='admin'),
    cfg.StrOpt('nova_admin_password', default='admin'),
    cfg.BoolOpt('http_log_debug', default=True),
    cfg.IntOpt('job_update_interval_sec', default=300,
               help=_('How often to update the job status, in seconds')),
    cfg.IntOpt('job_timeout_update_interval_min', default=60,
               help=_('How often to update the job timeout, in minutes')),
    cfg.IntOpt('job_timeout_update_increment_min', default=60,
               help=_('How much to increment the timeout, in minutes')),
    cfg.IntOpt('job_timeout_max_updates', default=3,
               help=_('How many times to update the timeout before '
                      'considering the job to be failed')),
]

CONF = cfg.CONF
CONF.register_opts(snapshot_worker_opts, group='snapshot_worker')


class SnapshotProcessor(worker.JobProcessor):
    def __init__(self):
        super(SnapshotProcessor, self).__init__()

    def init_processor(self, worker):
        super(SnapshotProcessor, self).init_processor(worker)
        self.timeout_count = 0
        self.timeout_max_updates = CONF.snapshot_worker.job_timeout_max_updates
        self.next_timeout = None
        self.update_interval = datetime.timedelta(
            seconds=CONF.snapshot_worker.job_update_interval_sec)
        self.timeout_increment = datetime.timedelta(
            minutes=CONF.snapshot_worker.job_timeout_update_increment_min)

    def process_job(self, job):
        LOG.debug(_("Process job: %s") % str(job))

        job_id = job['id']
        self.update_job(job_id, 'PROCESSING')
        self.next_timeout = job['timeout']
        self.next_update = timeutils.utcnow() + self.update_interval

        c = self._get_nova_client()

        instance_id = self._get_instance_id(job)
        image_id = c.servers.create_image(instance_id,
                                          ('Daily-' +
                                           str(datetime.datetime.utcnow())))
        LOG.debug("Created image: %s" % image_id)

        image_status = None
        not_active = True
        retry = True

        while not_active and retry:
            image_status = c.images.get(image_id).status
            LOG.debug("Image status: %s" % image_status)
            if image_status == 'ERROR':
                break
            not_active = image_status != 'ACTIVE'
            if not_active:
                retry = self._try_update(job_id, 'PROCESSING')
            else:
                self._job_succeeded(job_id)

        if not_active and not retry:
            self._job_timed_out(job_id)

        if image_status == 'ERROR':
            self._job_failed(job_id)

        LOG.debug("Snapshot complete")

    def cleanup_processor(self):
        """
        Override to perform processor-specific setup.

        Called AFTER the worker is unregistered from QonoS.
        """
        pass

    def _get_nova_client(self):
        auth_url = CONF.snapshot_worker.auth_url
        user = CONF.snapshot_worker.nova_admin_user
        password = CONF.snapshot_worker.nova_admin_password
        debug = CONF.snapshot_worker.http_log_debug

        tenant_id = job['tenant_id']

        nova_client = client.Client(user,
                                    password,
                                    project_id=tenant_id,
                                    auth_url=auth_url,
                                    insecure=False,
                                    http_log_debug=debug)
        return nova_client

    def _job_succeeded(self, job_id):
        self.update_job(job_id, 'DONE')

    def _job_timed_out(self, job_id):
        self.update_job(job_id, 'TIMED_OUT')

    def _job_failed(self, job_id):
        self.update_job(job_id, 'ERROR')

    def _try_update(self, job_id, status):
        now = timeutils.utcnow()
        # Time for a timeout update?
        if now >= self.next_timeout:
            # Out of timeouts?
            if self.timeout_count >= self.timeout_max_updates:
                return False
            self.next_timeout = self.next_timeout + self.timeout_increment
            self.timeout_count += 1
            self.update_job(job_id, status, self.next_timeout)
            return True

        # Time for a status-only update?
        if now >= self.next_update:
            self.next_update = now + self.update_interval
            self.update_job(job_id, status)

        return True

    def _get_instance_id(self, job):
        metadata = job['job_metadata']
        for meta in metadata:
            if meta['key'] == 'instance_id':
                return meta['value']
        return None
