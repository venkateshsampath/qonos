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

import datetime

from qonos.api import api
from qonos.common import exception
from qonos.common import timeutils
from qonos.common import utils


CONF = api.CONF


def serialize_metadata(metadata):
    to_return = {}
    for item in metadata:
        to_return[item['key']] = item['value']
    return to_return


def deserialize_metadata(metadata):
    return [{'key': key, 'value': value}
            for key, value in metadata.iteritems()]


def check_read_only_properties(values):
    _read_only_properties = ['created_at', 'updated_at']
    for key in values.keys():
        if key in _read_only_properties:
            msg = "%s is a read only attribute" % key
            raise exception.Forbidden(explanation=unicode(msg))

    return values


def serialize_schedule_metadata(schedule):
    metadata = schedule.pop('schedule_metadata')
    schedule['metadata'] = serialize_metadata(metadata)


def deserialize_schedule_metadata(schedule):
    if 'metadata' in schedule:
        metadata = schedule.pop('metadata')
        schedule['schedule_metadata'] = deserialize_metadata(metadata)


def serialize_job_metadata(job):
    metadata = job.pop('job_metadata')
    job['metadata'] = serialize_metadata(metadata)


def deserialize_job_metadata(job):
    if 'metadata' in job:
        metadata = job.pop('metadata')
        job['job_metadata'] = deserialize_metadata(metadata)


def schedule_to_next_run(schedule, start_time=None):
    start_time = start_time or timeutils.utcnow()
    minute = schedule.get('minute', '*')
    hour = schedule.get('hour', '*')
    day_of_month = schedule.get('day_of_month', '*')
    month = schedule.get('month', '*')
    day_of_week = schedule.get('day_of_week', '*')
    return utils.cron_string_to_next_datetime(minute, hour, day_of_month,
                                              month, day_of_week,
                                              start_time)


def get_new_timeout_by_action(action):
    now = timeutils.utcnow()

    group = 'action_' + action
    if group not in CONF:
        group = 'action_default'
    job_timeout_seconds = CONF.get(group).timeout_seconds
    return now + datetime.timedelta(seconds=job_timeout_seconds)
