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

import signal
import socket
import time

from qonos.common import utils
from qonos.openstack.common import cfg
from qonos.openstack.common.gettextutils import _
from qonos.openstack.common import importutils
import qonos.openstack.common.log as logging

LOG = logging.getLogger(__name__)

# TODO(WORKER) action_type should be queried from the job processor
worker_opts = [
    cfg.IntOpt('job_poll_interval', default=5,
               help=_('Interval to poll api for ready jobs in seconds')),
    cfg.StrOpt('api_endpoint', default='localhost',
               help=_('Address of the QonoS API server')),
    cfg.IntOpt('api_port', default=8080,
               help=_('Port on which to contact QonoS API server')),
    cfg.BoolOpt('daemonized', default=False,
                help=_('True to run the worker as a daemon')),
    cfg.StrOpt('action_type', default='None',
               help=_('A string identifying the type of action this '
                      'worker handles')),
    cfg.StrOpt('processor_class', default=None,
               help=_('The fully qualified class name of the processor '
                      'to use in this worker')),
]

CONF = cfg.CONF
CONF.register_opts(worker_opts, group='worker')


class Worker(object):
    def __init__(self, client_factory, processor=None):
        self.client = client_factory(CONF.worker.api_endpoint,
                                     CONF.worker.api_port)
        if not processor:
            processor = importutils.import_object(CONF.worker.processor_class)

        self.processor = processor
        self.worker_id = None
        self.host = socket.gethostname()
        self.running = False

    def run(self, run_once=False, poll_once=False):
        LOG.info(_('Starting qonos worker service'))

        if CONF.worker.daemonized:
            LOG.debug(_('Entering daemon mode'))
            import daemon
            #NOTE(ameade): We need to preserve all open files for logging
            open_files = utils.get_qonos_open_file_log_handlers()
            signal_map = self._signal_map()
            with daemon.DaemonContext(files_preserve=open_files,
                                      signal_map=signal_map):
                self._run_loop(run_once, poll_once)
        else:
            self._run_loop(run_once, poll_once)

    def _signal_map(self):
        return {
            signal.SIGTERM: self._terminate,
            signal.SIGHUP: self._terminate,
        }

    def _run_loop(self, run_once=False, poll_once=False):
        self.running = True
        self.processor.init_processor(self)
        self.worker_id = self._register_worker()

        while self.running:
            job = self._poll_for_next_job(poll_once)
            if job:
                LOG.debug(_('Processing job: %s') % job)
                self.processor.process_job(job)

            if run_once:
                self.running = False

        LOG.info(_("Worker is terminating"))
        self._unregister_worker()
        self.processor.cleanup_processor()

    def _register_worker(self):
        LOG.info(_('Registering worker.'))
        while self.running:
            try:
                worker = self.client.create_worker(self.host)
                msg = _('Worker has been registered with ID: %s')
                LOG.info(msg % worker['id'])
                return worker['id']
            except Exception, ex:
                LOG.warn(_('Error occurred registering worker with Qonos. '
                           'Is the Qonos API running? Will retry...'))
                LOG.debug(_('Exception: %s') % str(ex))
            time.sleep(CONF.worker.job_poll_interval)

    def _unregister_worker(self):
        LOG.info(_('Unregistering worker. ID: %s') % self.worker_id)
        try:
            self.client.delete_worker(self.worker_id)
        except Exception, ex:
            LOG.warn(_('Error occurred unregistering worker from Qonos. '
                       'Is the Qonos API running? Will NOT retry...'))
            LOG.debug(_('Exception: %s') % str(ex))

    def _terminate(self, signum, frame):
        self.running = False

    def _poll_for_next_job(self, poll_once=False):
        job = None

        while job is None and self.running:
            LOG.debug(_("Attempting to get next job from API"))
            time.sleep(CONF.worker.job_poll_interval)
            try:
                job = self.client.get_next_job(self.worker_id,
                                               CONF.worker.action_type)['job']
            except Exception, ex:
                LOG.warn(_('Error occurred fetching next job from qonos. '
                           'Is the Qonos API running? Will retry...'))
                LOG.debug(_('Exception: %s') % str(ex))
                job = None

            if poll_once:
                break

        return job

    def update_job(self, job_id, status, timeout=None, error_message=None):
        msg = (_("Worker: [%(worker_id)s] updating "
               "job [%(job_id)s] Status: %(status)s") %
                {'worker_id': self.worker_id,
                 'job_id': job_id,
                 'status': status})

        if timeout:
            msg += _("Timeout: %s") % str(timeout)

        if error_message:
            msg += _("Error message: %s") % error_message

        LOG.debug(msg)
        self.client.update_job_status(job_id, status, timeout, error_message)

    def update_job_metadata(self, job_id, metadata):
        return self.client.update_job_metadata(job_id, metadata)


class JobProcessor(object):
    def __init__(self):
        self.worker = None

    def update_job(self, job_id, status, timeout=None, error_message=None):
        self.worker.update_job(job_id, status, timeout=timeout,
                               error_message=error_message)

    def update_job_metadata(self, job_id, metadata):
        return self.worker.update_job_metadata(job_id, metadata)

    def init_processor(self, worker):
        """
        Override to perform processor-specific setup.
        Implementations should call the superclass implementation
        to insure the worker attribute is initialized.

        Called BEFORE the worker is registered with QonoS.
        """
        self.worker = worker

    def process_job(self, job):
        """
        Override to perform actual job processing.

        Called each time a new job is fetched.
        """
        pass

    def cleanup_processor(self):
        """
        Override to perform processor-specific setup.

        Called AFTER the worker is unregistered from QonoS.
        """
        pass
