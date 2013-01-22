import logging as pylog
import time
import signal
import sys

from qonos.openstack.common import cfg
from qonos.openstack.common.gettextutils import _
import qonos.openstack.common.log as logging
import socket

LOG = logging.getLogger(__name__)

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
    cfg.StrOpt('worker_name', default=__name__,
               help=_('A string to uniquely identify the instance of the '
                      'worker on the host')),
]

worker_cli_opts = [
    cfg.StrOpt('worker_name', default=None,
               help=_('A string to uniquely identify the instance of the '
                      'worker on the host')),
]

CONF = cfg.CONF
CONF.register_opts(worker_opts, group='worker')
CONF.register_cli_opts(worker_cli_opts)


class Worker(object):
    def __init__(self, client_factory):
        self.client = client_factory(CONF.worker.api_endpoint,
                                     CONF.worker.api_port)
        self.worker_id = None
        self.host = socket.gethostname()
        self.worker_name = CONF.worker.worker_name
        if CONF.worker_name:
            self.worker_name = CONF.worker_name

    def run(self, run_once=False, poll_once=False):
        LOG.debug(_('Starting qonos worker service'))

        self.init_worker()
        self.worker_id = self._register_worker()

        if CONF.worker.daemonized:
            import daemon
            #NOTE(ameade): We need to preserve all open files for logging
            open_files = []
            for handler in pylog.getLogger().handlers:
                if (hasattr(handler, 'stream') and
                        hasattr(handler.stream, 'fileno')):
                    open_files.append(handler.stream)
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
        while self.running:
            job = self._poll_for_next_job(poll_once)
            LOG.debug(_('Processing job: %s' % job))
            if not job is None:
                self.process_job(job)

            if run_once:
                self.running = False

        self._unregister_worker()
        self.cleanup_worker()

    def _register_worker(self):
        worker_name = self.worker_name
        LOG.debug(_('Registering worker. Name: %s' % worker_name))
        worker = self.client.create_worker(self.host, worker_name)
        return worker['id']

    def _unregister_worker(self):
        worker_name = self.worker_name
        LOG.debug(_('Unregistering worker. Name: %s, ID: %s' %
                    (worker_name, self.worker_id)))

        self.client.delete_worker(self.worker_id)

    def _terminate(self, signum, frame):
        self.running = False

    def _poll_for_next_job(self, poll_once=False):
        LOG.debug(_("Attempting to get next job from API"))
        job = None
        while job is None:
            time.sleep(CONF.worker.job_poll_interval)
            job = self.client.get_next_job(self.worker_id,
                                           CONF.worker.action_type)
            if poll_once:
                break

        return job

    def update_job(self, job_id, status, timeout=None):
        pass

    def init_worker(self):
        """
        Override to perform worker-specific setup.

        Called BEFORE the worker is registered with QonoS.
        """
        pass

    def process_job(self, job):
        """
        Override to perform worker-specific job processing.

        Called each time a new job is fetched.
        """
        pass

    def cleanup_worker(self):
        """
        Override to perform worker-specific setup.

        Called BEFORE the worker is registered with QonoS.
        """
        pass
