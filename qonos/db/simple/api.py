import functools
import uuid
import copy

from qonos.common import exception
from qonos.openstack.common import timeutils
from qonos.openstack.common.gettextutils import _


DATA = {
    'schedules': {},
    'schedule_metadata': {},
    'jobs': {},
    'job_metadata': {},
    'workers': {},
    'job_faults': {},
}


def configure_db():
    pass


def reset():
    global DATA
    for k in DATA:
        DATA[k] = {}


def _gen_base_attributes():
    values = {}
    values['id'] = str(uuid.uuid4())
    values['created_at'] = timeutils.utcnow()
    values['updated_at'] = timeutils.utcnow()
    return copy.deepcopy(values)


def _schedule_create(values):
    global DATA
    DATA['schedules'][values['id']] = values
    _schedule_meta_init(values['id'])
    return copy.deepcopy(values)


def schedule_get_all(filter_args={}):
    to_return = []
    schedules = copy.deepcopy(DATA['schedules'].values())

    if not filter_args:
        to_return = schedules
        for schedule in schedules:
            schedule['schedule_metadata'] =\
                copy.deepcopy(schedule_meta_get_all(schedule['id']))

    if (filter_args.get('next_run_after') is not None and
            filter_args.get('next_run_before') is not None):
        for schedule in schedules:
            if ((schedule['next_run'] > filter_args['next_run_after'] and
                    schedule['next_run'] < filter_args['next_run_before']) or
                    schedule['next_run'] == filter_args['next_run_after']):
                schedule['schedule_metadata'] =\
                    copy.deepcopy(schedule_meta_get_all(schedule['id']))
                to_return.append(schedule)
    return to_return


def schedule_get_by_id(schedule_id):
    if schedule_id not in DATA['schedules']:
        raise exception.NotFound()
    schedule = copy.deepcopy(DATA['schedules'][schedule_id])
    schedule['schedule_metadata'] = \
        copy.deepcopy(schedule_meta_get_all(schedule_id))
    return schedule


def schedule_create(schedule_values):
    values = copy.deepcopy(schedule_values)
    schedule = {}

    metadata = []
    if 'schedule_metadata' in values:
        metadata = values['schedule_metadata']
        del values['schedule_metadata']

    schedule.update(values)
    schedule.update(_gen_base_attributes())
    schedule = _schedule_create(schedule)

    for metadatum in metadata:
        schedule_meta_create(schedule['id'], metadatum)

    return schedule_get_by_id(schedule['id'])


def schedule_update(schedule_id, schedule_values):
    global DATA
    values = schedule_values.copy()

    if schedule_id not in DATA['schedules']:
        raise exception.NotFound()

    metadata = []
    if 'schedule_metadata' in values:
        metadata = values['schedule_metadata']
        del values['schedule_metadata']

    if len(values) > 0:
        schedule = DATA['schedules'][schedule_id]
        schedule['updated_at'] = timeutils.utcnow()
        schedule.update(values)

    if len(metadata) > 0:
        DATA['schedule_metadata'][schedule_id] = {}
        for metadatum in metadata:
            schedule_meta_create(schedule_id, metadatum)

    return schedule_get_by_id(schedule_id)


def schedule_delete(schedule_id):
    global DATA
    if schedule_id not in DATA['schedules']:
        raise exception.NotFound()
    del DATA['schedules'][schedule_id]


def _schedule_meta_init(schedule_id):
    if DATA['schedule_metadata'].get(schedule_id) is None:
        DATA['schedule_metadata'][schedule_id] = {}


def schedule_meta_create(schedule_id, values):
    global DATA
    if DATA['schedules'].get(schedule_id) is None:
        msg = _('Schedule %s could not be found') % schedule_id
        raise exception.NotFound(message=msg)

    _schedule_meta_init(schedule_id)

    try:
        _check_meta_exists(schedule_id, values['key'])
    except exception.NotFound:
        pass
    else:
        raise exception.Duplicate()

    meta = {}
    values['schedule_id'] = schedule_id
    meta.update(values)
    meta.update(_gen_base_attributes())
    DATA['schedule_metadata'][schedule_id][values['key']] = meta
    return copy.deepcopy(meta)


def _check_schedule_exists(schedule_id):
    if DATA['schedules'].get(schedule_id) is None:
        msg = _('Schedule %s could not be found') % schedule_id
        raise exception.NotFound(message=msg)


def _check_meta_exists(schedule_id, key):
    _check_schedule_exists(schedule_id)

    if (DATA['schedule_metadata'].get(schedule_id) is None or
            DATA['schedule_metadata'][schedule_id].get(key) is None):
        msg = _('Meta %s could not be found for Schedule %s ')
        msg = msg % (key, schedule_id)
        raise exception.NotFound(message=msg)


def schedule_meta_get_all(schedule_id):
    _check_schedule_exists(schedule_id)
    _schedule_meta_init(schedule_id)

    return DATA['schedule_metadata'][schedule_id].values()


def schedule_meta_get(schedule_id, key):
    _check_meta_exists(schedule_id, key)

    return DATA['schedule_metadata'][schedule_id][key]


def schedule_meta_update(schedule_id, key, values):
    global DATA
    _check_meta_exists(schedule_id, key)

    meta = DATA['schedule_metadata'][schedule_id][key]
    meta.update(values)
    meta['updated_at'] = timeutils.utcnow()
    DATA['schedule_metadata'][schedule_id][key] = meta

    return copy.deepcopy(meta)


def schedule_meta_delete(schedule_id, key):
    _check_meta_exists(schedule_id, key)

    del DATA['schedule_metadata'][schedule_id][key]


def worker_get_all():
    return DATA['workers'].values()


def worker_get_by_id(worker_id):
    if worker_id not in DATA['workers']:
        raise exception.NotFound()
    return copy.deepcopy(DATA['workers'][worker_id])


def worker_create(values):
    global DATA
    worker = {}
    worker.update(values)
    worker.update(_gen_base_attributes())
    DATA['workers'][worker['id']] = worker
    return copy.deepcopy(worker)


def worker_delete(worker_id):
    global DATA
    if worker_id not in DATA['workers']:
        raise exception.NotFound()
    del DATA['workers'][worker_id]


def job_create(job_values):
    global DATA
    values = job_values.copy()
    job = {}

    metadata = []
    if 'job_metadata' in values:
        metadata = values['job_metadata']
        del values['job_metadata']

    job['retry_count'] = 0
    job.update(values)
    job.update(_gen_base_attributes())

    DATA['jobs'][job['id']] = job

    for metadatum in metadata:
        job_meta_create(job['id'], metadatum)

    return job_get_by_id(job['id'])


def job_get_all():
    jobs = copy.deepcopy(DATA['jobs'].values())

    for job in jobs:
        job['job_metadata'] =\
            job_meta_get_all_by_job_id(job['id'])

    return jobs


def job_get_by_id(job_id):
    if job_id not in DATA['jobs']:
        raise exception.NotFound()

    job = copy.deepcopy(DATA['jobs'][job_id])

    job['job_metadata'] = \
        job_meta_get_all_by_job_id(job_id)

    return job


def job_updated_at_get_by_id(job_id):
    job = job_get_by_id(job_id)
    return job['updated_at']


def job_status_get_by_id(job_id):
    job = job_get_by_id(job_id)
    return job['status']


def job_update(job_id, job_values):
    global DATA
    values = job_values.copy()
    if job_id not in DATA['jobs']:
        raise exception.NotFound()

    metadata = []
    if 'job_metadata' in values:
        metadata = values['job_metadata']
        del values['job_metadata']

    if len(values) > 0:
        job = DATA['jobs'][job_id]
        #NOTE(ameade): This must come before update specified values since
        # we may be trying to manually set updated_at
        job['updated_at'] = timeutils.utcnow()
        job.update(values)

    if len(metadata) > 0:
        DATA['job_metadata'][job_id] = {}
        for metadatum in metadata:
            job_meta_create(job_id, metadatum)

    return job_get_by_id(job_id)


def job_delete(job_id):
    global DATA
    if job_id not in DATA['jobs']:
        raise exception.NotFound()
    del DATA['jobs'][job_id]


def job_meta_create(job_id, values):
    global DATA
    values['job_id'] = job_id
    _check_job_exists(job_id)

    if DATA['job_metadata'].get(job_id) is None:
        DATA['job_metadata'][job_id] = {}

    try:
        _check_job_meta_exists(job_id, values['key'])
    except exception.NotFound:
        pass
    else:
        raise exception.Duplicate()

    meta = {}
    meta.update(values)
    meta.update(_gen_base_attributes())
    DATA['job_metadata'][job_id][values['key']] = meta
    return copy.deepcopy(meta)


def _check_job_exists(job_id):
    if not job_id in DATA['jobs']:
        msg = _('Job %s could not be found') % job_id
        raise exception.NotFound(message=msg)


def _check_job_meta_exists(job_id, key):

    if (DATA['job_metadata'].get(job_id) is None or
            DATA['job_metadata'][job_id].get(key) is None):
        msg = _('Meta %s could not be found for Job %s ')
        msg = msg % (key, job_id)
        raise exception.NotFound(message=msg)


def job_meta_get_all_by_job_id(job_id):
    _check_job_exists(job_id)

    if job_id not in DATA['job_metadata']:
        DATA['job_metadata'][job_id] = {}

    return copy.deepcopy(DATA['job_metadata'][job_id].values())


def job_meta_get(job_id, key):
    _check_job_exists(job_id)
    _check_job_meta_exists(job_id, key)
    return DATA['job_metadata'][job_id][key]


def job_meta_update(job_id, key, values):
    global DATA
    _check_job_meta_exists(job_id, key)

    meta = DATA['job_metadata'][job_id][key]
    meta.update(values)
    meta['updated_at'] = timeutils.utcnow()
    DATA['job_metadata'][job_id][key] = meta

    return copy.deepcopy(meta)


def job_meta_delete(job_id, key):
    _check_job_meta_exists(job_id, key)

    del DATA['job_metadata'][job_id][key]
