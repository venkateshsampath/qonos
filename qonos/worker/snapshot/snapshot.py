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

import datetime
from operator import itemgetter
import time

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
    cfg.IntOpt('image_poll_interval_sec', default=0,
               help=_('How often to poll Nova for the image status')),
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
        self.status_map = {
            "QUEUED": "PROCESSING",
            "SAVING": "PROCESSING",
            "ACTIVE": "DONE",
            "KILLED": "ERROR",
            "DELETED": "ERROR",
            "PENDING_DELETE": "ERROR",
            "ERROR": "ERROR"
        }

    def init_processor(self, worker):
        super(SnapshotProcessor, self).init_processor(worker)
        self.current_job = None
        self.timeout_count = 0
        self.timeout_max_updates = CONF.snapshot_worker.job_timeout_max_updates
        self.next_timeout = None
        self.update_interval = datetime.timedelta(
            seconds=CONF.snapshot_worker.job_update_interval_sec)
        self.timeout_increment = datetime.timedelta(
            minutes=CONF.snapshot_worker.job_timeout_update_increment_min)
        self.image_poll_interval = CONF.snapshot_worker.image_poll_interval_sec

    def process_job(self, job):
        LOG.debug(_("Process job: %s") % str(job))
        self.current_job = job

        job_id = job['id']
        self.update_job(job_id, 'PROCESSING')
        self.next_timeout = job['timeout']
        self.next_update = self._get_utcnow() + self.update_interval

        nova_client = self._get_nova_client()

        instance_id = self._get_instance_id(job)
        metadata = {
            "org.openstack__1__created-by": "scheduled_images_service"
            }
        image_id = nova_client.servers.create_image(
            instance_id, ('Daily-' + str(self._get_utcnow())), metadata)
        LOG.debug("Created image: %s" % image_id)

        image_status = None
        active = False
        retry = True

        status = None
        while retry and not active:
            status = self._get_image_status(nova_client, image_id)
            LOG.debug("Status: %s" % status)
            if self._is_error_status(status):
                break

            active = self._is_active_status(status)
            if active:
                self._job_succeeded(job_id)
            else:
                retry = self._try_update(job_id, status['job_status'])

            time.sleep(self.image_poll_interval)

        if (not active) and (not retry):
            self._job_timed_out(job_id)

        if active:
            self._process_retention(nova_client, instance_id)

        LOG.debug("Snapshot complete")

    def cleanup_processor(self):
        """
        Override to perform processor-specific setup.

        Called AFTER the worker is unregistered from QonoS.
        """
        pass

    def _process_retention(self, nova_client, instance_id):
        retention = self._get_retention(nova_client, instance_id)

        if retention > 0:
            scheduled_images = self._find_scheduled_images_for_server(
                nova_client, instance_id)

            if len(scheduled_images) > retention:
                to_delete = scheduled_images[retention:]
                LOG.warn(_('Removing %(remove)d images for a retention '
                           'of %(retention)d') % {'remove': len(to_delete),
                                                 'retention': retention})
                for image in to_delete:
                    image_id = image.get('id')
                    nova_client.images.delete(image_id)
                    LOG.warn(_('Removed image %s') % image_id)

    def _get_retention(self, nova_client, instance_id):
        server = nova_client.servers.get(instance_id)
        ret_str = server.metadata.get("org.openstack__1__retention")
        retention = int(ret_str or 0)

        return retention

    def _find_scheduled_images_for_server(self, nova_client, instance_id):
        images = nova_client.images.list(detailed=True)
        scheduled_images = []
        for image in images:
            metadata = image.get('metadata')
            if (metadata.get("org.openstack__1__created_by")
                == "scheduled_images_service" and
                metadata.get("instance_uuid") == instance_id):
                scheduled_images.append(image)
                scheduled_images = sorted(scheduled_images,
                                          key=itemgetter('created'),
                                          reverse=True)
        return scheduled_images

    def _is_error_status(self, status):
        job_status = status['job_status']
        if job_status == 'ERROR':
            instance_id = self._get_instance_id(self.current_job)
            msg = (('Error occurred while taking snapshot: '
                    'Instance: %(instance_id)s, image: %(image_id)s, '
                     'status: %(image_status)s') %
                   {'instance_id': instance_id,
                    'image_id': status['image_id'],
                    'image_status': status['image_status']})
            LOG.warn(msg)
            self._job_failed(self.current_job['id'], msg)
            return True
        return False

    def _is_active_status(self, status):
        job_id = self.current_job['id']
        job_status = status['job_status']
        image_status = status['image_status']
        active = image_status == 'ACTIVE'
        return active

    def _get_image_status(self, nova_client, image_id):
        image = nova_client.images.get(image_id)
        if image:
            image_status = image.status
        else:
            image_status = 'KILLED'

        return {'image_id': image_id,
                'image_status': image_status,
                'job_status': self.status_map[image_status]}

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

    def _job_failed(self, job_id, error_message):
        self.update_job(job_id, 'ERROR', error_message=error_message)

    def _try_update(self, job_id, status):
        now = self._get_utcnow()
        LOG.debug("Now: %s  Timeout: %s" % (str(now), str(self.next_timeout)))
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
        metadata = job['metadata']
        return metadata.get('instance_id')

    # Seam for testing
    def _get_utcnow(self):
        return timeutils.utcnow()
