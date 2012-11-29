import datetime
import time

from qonos.openstack.common import cfg
from qonos.openstack.common.gettextutils import _
from qonos.openstack.common import timeutils
import qonos.openstack.common.log as logging

LOG = logging.getLogger(__name__)

scheduler_opts = [
    cfg.IntOpt('job_schedule_interval', default=5,
               help=_('Interval to poll api for ready jobs in seconds')),
]

CONF = cfg.CONF
CONF.register_opts(scheduler_opts)


class Scheduler(object):
    def __init__(self):
        pass

    def run(self, run_once=False):
        LOG.debug(_('Starting qonos scheduler service'))
        next_run = None

        while True:
            next_run = time.time() + CONF.job_schedule_interval

            # do work
            self.enqueue_jobs()

            # do nothing until next run
            seconds = next_run - time.time()
            if seconds > 0:
                time.sleep(seconds)
            else:
                LOG.warn(_('Scheduling of jobs took longer than expected.'))

            if run_once:
                break

    # TODO
    def enqueue_jobs(self):
        LOG.debug(_('Creating new jobs'))
        pass

    # TODO
    def get_schedules(self):
        pass
