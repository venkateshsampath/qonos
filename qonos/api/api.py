import logging as pylog

from qonos.openstack.common import cfg
from qonos.openstack.common.gettextutils import _
import qonos.openstack.common.log as logging
from qonos.openstack.common.wsgi import run_server

LOG = logging.getLogger(__name__)

api_opts = [
    cfg.BoolOpt('daemonized', default=False),
    cfg.IntOpt('port', default=8080),
]

CONF = cfg.CONF
CONF.register_opts(api_opts, group='api')


class API(object):
    def __init__(self, app):
        self.app = app

    def run(self, run_once=False):
        LOG.debug(_('Starting qonos-api service'))

        if CONF.api.daemonized:
            import daemon
            #NOTE(ameade): We need to preserve all open files for logging
            open_files = []
            for handler in pylog.getLogger().handlers:
                if (hasattr(handler, 'stream') and
                        hasattr(handler.stream, 'fileno')):
                    open_files.append(handler.stream)
            with daemon.DaemonContext(files_preserve=open_files):
                run_server(self.app, CONF.api.port)
        else:
            run_server(self.app, CONF.api.port)
