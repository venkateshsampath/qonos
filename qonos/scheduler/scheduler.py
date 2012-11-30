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
    cfg.StrOpt('api_endpoint', default='localhost'),
    cfg.IntOpt('api_port', default=8080),
]

CONF = cfg.CONF
CONF.register_opts(scheduler_opts)


class Scheduler(object):
    def __init__(self, client_factory):
        self.client = client_factory(CONF.api_endpoint, CONF.api_port)

    def run(self, run_once=False):
        LOG.debug(_('Starting qonos scheduler service'))
        next_run = None
        current_run = None

        while True:
            prev_run = current_run
            current_run = timeutils.isotime()
            next_run = time.time() + CONF.job_schedule_interval

            # do work
            self.enqueue_jobs(prev_run, current_run)

            # do nothing until next run
            seconds = next_run - time.time()
            if seconds > 0:
                time.sleep(seconds)
            else:
                LOG.warn(_('Scheduling of jobs took longer than expected.'))

            if run_once:
                break

    def enqueue_jobs(self, previous_run=None, current_run=None):
        LOG.debug(_('Creating new jobs'))
        schedules = self.get_schedules(previous_run, current_run)
        for schedule in schedules:
            self.client.create_job(schedule['id'])

    def get_schedules(self, previous_run=None, current_run=None):
        filter_args = {'next_run_before': current_run}

        # TODO(ameade): change api to not require both query params
        year_one = timeutils.isotime(datetime.datetime(1970, 1, 1))
        filter_args['next_run_after'] = previous_run or year_one

        return self.client.list_schedules(filter_args=filter_args)
