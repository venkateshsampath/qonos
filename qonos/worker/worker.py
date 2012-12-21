<<<<<<< HEAD
=======
import logging as pylog
import time

>>>>>>> 53ebcb5b558800d83501979dd7a1000ba5a83cef
from qonos.openstack.common import cfg
from qonos.openstack.common.gettextutils import _
import qonos.openstack.common.log as logging

LOG = logging.getLogger(__name__)

worker_opts = [
    cfg.IntOpt('job_poll_interval', default=5,
               help=_('Interval to poll api for ready jobs in seconds')),
    cfg.StrOpt('api_endpoint', default='localhost'),
    cfg.IntOpt('api_port', default=8080),
    cfg.BoolOpt('daemonized', default=False),
]

CONF = cfg.CONF
CONF.register_opts(worker_opts, group='worker')


class Worker(object):
    def __init__(self, client_factory):
        self.client = client_factory(CONF.worker.api_endpoint,
                                     CONF.worker.api_port)

    def run(self, run_once=False):
        LOG.debug(_('Starting qonos worker service'))

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

    def _poll_for_next_job(self, run_once=False):
        LOG.debug(_("Attempting to get next job from API"))
        while True:
            time.sleep(CONF.worker.job_poll_interval)

            if run_once:
                break
