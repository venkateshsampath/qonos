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


def serialize_metadata(metadata):
    return {meta['key']: meta['value'] for meta in metadata}


def deserialize_metadata(metadata):
    return [{'key': key, 'value': value}
            for key, value in metadata.iteritems()]


def serialize_meta(meta):
    return {meta['key']: meta['value']}


def deserialize_meta(meta):
    key = meta.keys()[0]
    value = meta[key]
    return {'key': key, 'value': value}


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
