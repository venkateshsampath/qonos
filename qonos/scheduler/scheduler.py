import datetime
import logging as pylog
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
    cfg.BoolOpt('daemonized', default=False),
]

CONF = cfg.CONF
CONF.register_opts(scheduler_opts, group='scheduler')


class Scheduler(object):
    def __init__(self, client_factory):
        self.client = client_factory(CONF.scheduler.api_endpoint,
                                     CONF.scheduler.api_port)

    def run(self, run_once=False):
        LOG.debug(_('Starting qonos scheduler service'))

        if CONF.scheduler.daemonized:
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
        next_run = None
        current_run = None

        while True:
            prev_run = current_run
            current_run = timeutils.isotime()
            next_run = time.time() + CONF.scheduler.job_schedule_interval

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

        schedules = self.client.list_schedules(filter_args=filter_args)
        response = schedules
        while response:
            filter_args['marker'] = response[-1]
            response = self.client.list_schedules(filter_args=filter_args)
            schedules = schedules.append(response)

        return schedules
