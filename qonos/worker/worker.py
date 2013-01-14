import logging as pylog
import time

from qonos.openstack.common import cfg
from qonos.openstack.common.gettextutils import _
import qonos.openstack.common.log as logging
import socket

LOG = logging.getLogger(__name__)

worker_opts = [
    cfg.IntOpt('job_poll_interval', default=5,
               help=_('Interval to poll api for ready jobs in seconds')),
    cfg.StrOpt('api_endpoint', default='localhost'),
    cfg.IntOpt('api_port', default=8080),
    cfg.BoolOpt('daemonized', default=False),
    cfg.StrOpt('worker_name', default=__name__)
    cfg.StrOpt('action_type', default='None')
]

CONF = cfg.CONF
CONF.register_opts(worker_opts, group='worker')


class Worker(object):
    def __init__(self, client_factory):
        self.client = client_factory(CONF.worker.api_endpoint,
                                     CONF.worker.api_port)
        self.worker_id = None
        self.host = socket.gethostname()

    def run(self, run_once=False):
        LOG.debug(_('Starting qonos worker service'))

        self.worker_id = _register_worker()

        if CONF.worker.daemonized:
            import daemon
            #NOTE(ameade): We need to preserve all open files for logging
            open_files = []
            for handler in pylog.getLogger().handlers:
                if (hasattr(handler, 'stream') and
                        hasattr(handler.stream, 'fileno')):
                    open_files.append(handler.stream)
            with daemon.DaemonContext(files_preserve=open_files):
                self._run_loop(run_once)
        else:
            self._run_loop(run_once)

    def _run_loop(self, run_once=False):
        while True:
            self._poll_for_next_job(run_once)

            # do work

            if run_once:
                break

    def _register_worker(self):
        worker_name = CONF.worker.worker_name
        LOG.debug(_('Registering worker. Name: %s' % worker_name))
        return self.client.create_worker(self.host, worker_name)

    def _unregister_worker(self):
        worker_name = CONF.worker.worker_name
        LOG.debug(_('Unregistering worker. Name: %s, ID: %s'.format(
                    CONF.worker.worker_name, self.worker_id)))

        self.client.delete_worker(self.worker_id)

    def _poll_for_next_job(self, run_once=False):
        LOG.debug(_("Attempting to get next job from API"))
        job = None
        while job is None:
            time.sleep(CONF.worker.job_poll_interval)

            job = self.client.get_next_job(self.worker_id,
                                           CONF.worker.action_type)
            if run_once:
                break

        return job
