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
