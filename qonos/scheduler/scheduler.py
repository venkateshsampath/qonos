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

import time

from qonos.common import utils
from qonos.openstack.common import cfg
from qonos.openstack.common.gettextutils import _
import qonos.openstack.common.log as logging
from qonos.openstack.common import timeutils

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
            open_files = utils.get_qonos_open_file_log_handlers()
            with daemon.DaemonContext(files_preserve=open_files):
                self._run_loop(run_once)
        else:
            self._run_loop(run_once)

    def _run_loop(self, run_once=False):
        next_run = None
        current_run = None

        while True:
            current_run = timeutils.isotime()
            next_run = time.time() + CONF.scheduler.job_schedule_interval

            # do work
            self.enqueue_jobs(end_time=current_run)

            # do nothing until next run
            seconds = next_run - time.time()
            if seconds > 0:
                time.sleep(seconds)
            else:
                LOG.warn(_('Scheduling of jobs took longer than expected.'))

            if run_once:
                break

    def enqueue_jobs(self, start_time=None, end_time=None):
        LOG.debug(_('Fetching schedules to process'))
        schedules = self.get_schedules(start_time, end_time)
        if schedules:
            LOG.debug(_('Creating %d jobs') % len(schedules))
            for schedule in schedules:
                self.client.create_job(schedule['id'])

    def get_schedules(self, start_time=None, end_time=None):
        filter_args = {'next_run_before': end_time}

        if start_time:
            filter_args['next_run_after'] = start_time

        try:
            schedules = self.client.list_schedules(filter_args=filter_args)

            response = schedules
            while response:
                filter_args['marker'] = response[-1]['id']
                response = self.client.list_schedules(filter_args=filter_args)
                schedules.extend(response)

            return schedules
        except Exception, ex:
            LOG.warn(_('Error occurred fetching jobs from qonos. '
                       'Is the Qonos API running? Will retry...'))
            LOG.debug(_('Exception: %s') % str(ex))
            return None
