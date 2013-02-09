import logging as pylog
import signal
import socket
import time

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
    cfg.StrOpt('worker_name', default=__name__,
               help=_('A string to uniquely identify the instance of the '
                      'worker on the host')),
    cfg.StrOpt('processor_class', default=None,
               help=_('The fully qualified class name of the processor '
                      'to use in this worker')),
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
    def __init__(self, client_factory, product_name='qonos', processor=None):
        self.client = client_factory(CONF.worker.api_endpoint,
                                     CONF.worker.api_port)
        if not processor:
            processor = importutils.import_object(CONF.worker.processor_class)

        self.processor = processor
        self.worker_id = None
        self.host = socket.gethostname()
        self.product_name = product_name
        self.worker_name = CONF.worker.worker_name
        if CONF.worker_name:
            self.worker_name = CONF.worker_name

    def run(self, run_once=False, poll_once=False):
        LOG.debug(_('Starting qonos worker service'))

        self.processor.init_processor(self)
        self.worker_id = self._register_worker()

        if CONF.worker.daemonized:
            import daemon
            #NOTE(ameade): We need to preserve all open files for logging
            open_files = []
            for handler in pylog.getLogger(self.product_name).handlers:
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
            if job:
                LOG.debug(_('Processing job: %s') % job)
                self.processor.process_job(job)

            if run_once:
                self.running = False

        self._unregister_worker()
        self.processor.cleanup_processor()

    def _register_worker(self):
        worker_name = self.worker_name
        LOG.debug(_('Registering worker. Name: %s') % worker_name)
        worker = self.client.create_worker(self.host, worker_name)
        return worker['id']

    def _unregister_worker(self):
        worker_name = self.worker_name
        LOG.debug(_('Unregistering worker. Name: %(name)s, ID: %(id)s') %
                    {'name': worker_name, 'id': self.worker_id})

        self.client.delete_worker(self.worker_id)

    def _terminate(self, signum, frame):
        self.running = False

    def _poll_for_next_job(self, poll_once=False):
        LOG.debug(_("Attempting to get next job from API"))
        job = None
        while job is None:
            time.sleep(CONF.worker.job_poll_interval)
            job = self.client.get_next_job(self.worker_id,
                                           CONF.worker.action_type)['job']
            if poll_once:
                break

        return job

    def update_job(self, job_id, status, timeout=None):
        LOG.debug(_("Worker: %(name)s [%(worker_id)d] updating "
                    "job [%(job_id)d] Status: %(status)s Timeout: %(timeout)s")
                    % {'worker_id': self.worker_name,
                       'job_id': job_id,
                       'status': status,
                       'timeout': str(timeout)})
        self.client.update_job_status(job_id, status, timeout)


class JobProcessor(object):
    def __init__(self):
        self.worker = None

    def update_job(self, job_id, status, timeout=None):
        self.worker.update_job(job_id, status, timeout)

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
