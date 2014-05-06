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

import calendar
import datetime
from operator import attrgetter
import sys
import time
import traceback as tb

from novaclient import exceptions
from oslo.config import cfg

from qonos.common import exception as exc
from qonos.common import timeutils
from qonos.openstack.common.gettextutils import _
from qonos.openstack.common import importutils
import qonos.openstack.common.log as logging
import qonos.qonosclient.exception as qonos_ex
from qonos.worker import worker


LOG = logging.getLogger(__name__)

snapshot_worker_opts = [
    cfg.StrOpt('nova_client_factory_class',
               default='qonos.worker.snapshot.simple_nova_client_factory.'
                       'NovaClientFactory'),
    cfg.IntOpt('image_poll_interval_sec', default=30,
               help=_('How often to poll Nova for the image status')),
    cfg.IntOpt('job_update_interval_sec', default=300,
               help=_('How often to update the job status, in seconds')),
    cfg.IntOpt('job_timeout_update_interval_min', default=60,
               help=_('How often to update the job timeout, in minutes')),
    cfg.IntOpt('job_timeout_update_increment_min', default=60,
               help=_('How much to increment the timeout, in minutes')),
    cfg.IntOpt('job_timeout_max_updates', default=3,
               help=_('How many times to update the timeout before '
                      'considering the job to be failed')),
    cfg.IntOpt('job_timeout_backoff_factor', default=2,
               help=_('Timeout multiplier to use when an error occurs')),
]

CONF = cfg.CONF
CONF.register_opts(snapshot_worker_opts, group='snapshot_worker')

_FAILED_IMAGE_STATUSES = ['KILLED', 'DELETED', 'PENDING_DELETE', 'ERROR']


class SnapshotProcessor(worker.JobProcessor):
    def __init__(self):
        super(SnapshotProcessor, self).__init__()

    def init_processor(self, worker, nova_client_factory=None):
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
        self.job_timeout_backoff_factor = (CONF.snapshot_worker
                                           .job_timeout_backoff_factor)

        if not nova_client_factory:
            nova_client_factory = importutils.import_object(
                CONF.snapshot_worker.nova_client_factory_class)
        self.nova_client_factory = nova_client_factory

    def process_job(self, job):
        LOG.info(_("Worker %(worker_id)s Processing job: %(job)s") %
                 {'worker_id': self.worker.worker_id,
                     'job': job['id']})
        LOG.debug(_("Worker %(worker_id)s Processing job: %(job)s") %
                  {'worker_id': self.worker.worker_id,
                   'job': str(job)})

        payload = {'job': job}
        if job['status'] == 'QUEUED':
            self.send_notification_start(payload)
        else:
            self.send_notification_retry(payload)

        job_id = job['id']

        if not self._check_schedule_exists(job):
            msg = ('Schedule %(schedule_id)s deleted for job %(job_id)s' %
                   {'schedule_id': job['schedule_id'], 'job_id': job_id})
            self._job_cancelled(job, msg)

            LOG.info(_('Worker %(worker_id)s Job cancelled: %(msg)s') %
                     {'worker_id': self.worker.worker_id,
                      'msg': msg})
            return

        self.current_job = job

        now = self._get_utcnow()
        self.next_timeout = now + self.timeout_increment
        self._job_processing(job, self.next_timeout)
        self.next_update = self._get_utcnow() + self.update_interval

        instance_id = self._get_instance_id(job)
        if not instance_id:
            msg = ('Job %s does not specify an instance_id in its metadata.'
                   % job_id)
            self._job_cancelled(job, msg)
            return

        image_id = self._get_image_id(job)
        if image_id is None:
            image_id = self._create_image(job, instance_id,
                                          job['schedule_id'])
            if image_id is None:
                return
        else:
            LOG.info(_("Worker %(worker_id)s Resuming image: %(image_id)s")
                     % {'worker_id': self.worker.worker_id,
                        'image_id': image_id})

        active = False
        retry = True

        while retry and not active:
            image_status = self._poll_image_status(job, image_id)

            active = image_status == 'ACTIVE'
            if not active:
                retry = self._try_update(job_id, "PROCESSING")
                time.sleep(self.image_poll_interval)

        if (not active) and (not retry):
            self._job_timed_out(job)

        if active:
            self._process_retention(instance_id, job['schedule_id'])
            self._job_succeeded(job)
        LOG.debug("Snapshot complete")

    def cleanup_processor(self):
        """
        Override to perform processor-specific setup.

        Called AFTER the worker is unregistered from QonoS.
        """
        pass

    def _get_image_id(self, job):
        image_id = None
        if 'image_id' in job['metadata']:
            image_id = job['metadata']['image_id']
            try:
                image_status = self._get_image_status(image_id)
                if image_status in _FAILED_IMAGE_STATUSES:
                    return None
            except Exception:
                exc_type, exc_value, exc_tb = sys.exc_info()
                org_err_msg = tb.format_exception_only(exc_type, exc_value)
                err_val = {"job_id": job['id'],
                           "image_id": image_id,
                           "org_err_msg": org_err_msg}
                err_msg = _("ERROR get_image_id():"
                            " job_id: %(job_id)s, image_id: %(image_id)s"
                            " err:%(org_err_msg)s") % err_val
                LOG.exception("Error getting snapshot image details. %s",
                              err_msg)
                self._update_job_error(job, error_msg=err_msg)
                raise exc.PollingException(err_msg)
        return image_id

    def _create_image(self, job, instance_id, schedule_id):
        metadata = {
            "org.openstack__1__created_by": "scheduled_images_service"}

        image_id = None
        try:
            instance_name_msg = ("Attempting to get the instance name for "
                               "instance_id %s" % instance_id)
            LOG.info(instance_name_msg)
            server_name = self._get_nova_client().servers.\
                get(instance_id).name
            msg = ("Attempting to create an image for instance "
                   "%s" % server_name)
            LOG.info(msg)
            image_id = self._get_nova_client().servers.create_image(
                instance_id,
                self.generate_image_name(server_name),
                metadata)
        except exceptions.NotFound:
            msg = ('Instance %(instance_id)s specified by job %(job_id)s '
                   'was not found.' %
                   {'instance_id': instance_id, 'job_id': job['id']})

            timeout = self._get_updated_job_timeout(job['id'])
            self.update_job(job['id'], 'ERROR', timeout=timeout,
                            error_message=msg)
            return None

        LOG.info(_("Worker %(worker_id)s Started create image: "
                   " %(image_id)s") % {'worker_id': self.worker.worker_id,
                                       'image_id': image_id})

        self._add_job_metadata(image_id=image_id)
        return image_id

    def generate_image_name(self, server_name):
        """
        Creates a string based on the specified server name and current time.

        The string is of the format:
        "Daily-<truncated-server-name>-<unix-timestamp>"
        """
        prefix = 'Daily-'
        max_name_length = 255
        now = str(calendar.timegm(self._get_utcnow().utctimetuple()))

        #NOTE(ameade): Truncate the server name so the image name is within
        # 255 characters total
        server_name_len = max_name_length - len(now) - len(prefix) - len('-')
        server_name = server_name[:server_name_len]

        return (prefix + server_name + '-' + str(now))

    def _add_job_metadata(self, **to_add):
        metadata = self.current_job['metadata']
        for key in to_add:
            metadata[key] = to_add[key]

        self.current_job['metadata'] = self.update_job_metadata(
            self.current_job['id'], metadata)

    def _poll_image_status(self, job, image_id):
        try:
            image_status = self._get_image_status(image_id)
        except Exception:
            exc_type, exc_value, exc_tb = sys.exc_info()
            org_err_msg = tb.format_exception_only(exc_type, exc_value)
            err_val = {'job_id': job['id'],
                       'image_id': image_id,
                       'org_err_msg': org_err_msg}
            err_msg = (
                _("PollingExc: image: %(image_id)s, err: %(org_err_msg)s") %
                err_val)
            LOG.exception(err_msg)
            self._update_job_error(job, error_msg=err_msg)
            raise exc.PollingException(err_msg)

        if image_status is None or image_status in _FAILED_IMAGE_STATUSES:
            err_val = {'image_id': image_id,
                       "image_status": image_status,
                       "job_id": job['id']}
            err_msg = (
                _("PollingErr: Got failed image status. Details:"
                  " image_id: %(image_id)s, 'image_status': %(image_status)s"
                  " job_id: %(job_id)s") % err_val)
            self._update_job_error(job, error_msg=err_msg)
            raise exc.PollingException(err_msg)
        return image_status

    def _process_retention(self, instance_id, schedule_id):
        LOG.debug(_("Processing retention."))
        retention = self._get_retention(instance_id)

        if retention > 0:
            scheduled_images = self._find_scheduled_images_for_server(
                instance_id)

            if len(scheduled_images) > retention:
                to_delete = scheduled_images[retention:]
                LOG.info(_('Worker %(worker_id)s '
                           'Removing %(remove)d images for a retention '
                           'of %(retention)d')
                          % {'worker_id': self.worker.worker_id,
                             'remove': len(to_delete),
                             'retention': retention})
                for image in to_delete:
                    image_id = image.id
                    self._get_nova_client().images.delete(image_id)
                    LOG.info(_('Worker %(worker_id)s Removed image '
                               '%(image_id)s')
                              % {'worker_id': self.worker.worker_id,
                                 'image_id': image_id})
        else:
            msg = ("Retention %(retention)s is found for for schedule "
                   "%(sched)s for %(instance)s" % {'retention': retention,
                                                   'sched': schedule_id,
                                                   'instance': instance_id})
            LOG.info(msg)

    def _get_retention(self, instance_id):
        ret_str = None
        retention = 0
        try:
            result = self._get_nova_client().\
                rax_scheduled_images_python_novaclient_ext.get(instance_id)
            ret_str = result.retention
            retention = int(ret_str or 0)
        except exceptions.NotFound, e:
            msg = _('Could not retrieve retention for server %s: either the'
                    ' server was deleted or scheduled images for'
                    ' the server was disabled.') % instance_id

            LOG.warn(msg)
        except Exception, e:
            msg = _('Error getting retention for server %s: ')
            LOG.exception(msg % instance_id)

        return retention

    def _find_scheduled_images_for_server(self, instance_id):
        images = self._get_nova_client().images.list(detailed=True)
        scheduled_images = []
        for image in images:
            metadata = image.metadata
            # Note(Hemanth): In the following condition,
            # 'image.status.upper() == "ACTIVE"' is a temporary hack to
            # incorporate rm2400. Ideally, this filtering should be performed
            # by passing an appropriate filter to the novaclient.
            if (metadata.get("org.openstack__1__created_by")
                == "scheduled_images_service" and
                metadata.get("instance_uuid") == instance_id and
                image.status.upper() == "ACTIVE"):
                scheduled_images.append(image)

        scheduled_images = sorted(scheduled_images,
                                  key=attrgetter('created'),
                                  reverse=True)

        return scheduled_images

    def _is_active_status(self, status):
        job_id = self.current_job['id']
        job_status = status['job_status']
        image_status = status['image_status']
        active = image_status == 'ACTIVE'
        return active

    def _get_image_status(self, image_id):
        """
        Get image status with novaclient
        """
        image_status = None
        image = self._get_nova_client().images.get(image_id)

        if image is not None:
            image_status = image.status

        return image_status

    def _get_nova_client(self):
        nova_client = self.nova_client_factory.get_nova_client(
            self.current_job)
        return nova_client

    def _job_succeeded(self, job):
        response = self.update_job(job['id'], 'DONE')
        if response:
            self._update_job_with_response(job, response)
        self.send_notification_end({'job': job})

    def _job_processing(self, job, timeout):
        response = self.update_job(job['id'], 'PROCESSING',
                                   timeout=self.next_timeout)
        if response:
            self._update_job_with_response(job, response)
        self.send_notification_job_update({'job': job})

    def _job_timed_out(self, job):
        response = self.update_job(job['id'], 'TIMED_OUT')
        if response:
            self._update_job_with_response(job, response)
        self.send_notification_job_update({'job': job})

    def _get_updated_job_timeout(self, job_id):
        backoff_factor = (self.job_timeout_backoff_factor
                          ** int(self.current_job['retry_count']))
        timeout_increment = self.timeout_increment * backoff_factor

        now = self._get_utcnow()
        timeout = now + timeout_increment
        return timeout

    def _job_cancelled(self, job, message):
        response = self.update_job(job['id'], 'CANCELLED',
                                   error_message=message)
        if response:
            self._update_job_with_response(job, response)
        self.send_notification_job_update({'job': job})

    def _update_job_error(self, job, error_msg=None):
        timeout = self._get_updated_job_timeout(self.current_job['id'])
        self.update_job(job['id'], 'ERROR',
                        timeout=timeout, error_message=error_msg)

    def _update_job_with_response(self, job, resp):
        job['status'] = resp.get('status')
        job['timeout'] = resp.get('timeout')

    def _try_update(self, job_id, status):
        now = self._get_utcnow()
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

    def _get_username(self, job):
        metadata = job['metadata']
        return metadata.get('user_name')

    def _check_schedule_exists(self, job):
        qonosclient = self.get_qonos_client()
        try:
            qonosclient.get_schedule(job['schedule_id'])
            return True
        except qonos_ex.NotFound, ex:
            return False

    # Seam for testing
    def _get_utcnow(self):
        return timeutils.utcnow()
