import functools
import uuid

from qonos.common import exception
from qonos.openstack.common import timeutils
from qonos.openstack.common.gettextutils import _


DATA = {
    'schedules': {},
    'schedule_metadata': {},
    'jobs': {},
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
    return values.copy()


def _schedule_create(values):
    global DATA
    DATA['schedules'][values['id']] = values
    return values.copy()


def schedule_get_all():
    return DATA['schedules'].values()


def schedule_get_by_id(schedule_id):
    if schedule_id not in DATA['schedules']:
        raise exception.NotFound()
    return DATA['schedules'][schedule_id].copy()


def schedule_create(values):
    schedule = {}
    schedule.update(values)
    schedule.update(_gen_base_attributes())
    return _schedule_create(schedule)


def schedule_update(schedule_id, values):
    global DATA
    if schedule_id not in DATA['schedules']:
        raise exception.NotFound()
    schedule = DATA['schedules'][schedule_id]
    schedule.update(values)
    schedule['updated_at'] = timeutils.utcnow()
    DATA['schedules'][schedule_id] = schedule
    return schedule


def schedule_delete(schedule_id):
    global DATA
    if schedule_id not in DATA['schedules']:
        raise exception.NotFound()
    del DATA['schedules'][schedule_id]


def schedule_meta_create(schedule_id, values):
    global DATA
    if DATA['schedules'].get(schedule_id) is None:
        msg = _('Schedule %s could not be found') % schedule_id
        raise exception.NotFound(message=msg)

    if DATA['schedule_metadata'].get(schedule_id) is None:
        DATA['schedule_metadata'][schedule_id] = {}

    meta = {}
    values['schedule_id'] = schedule_id
    meta.update(values)
    meta.update(_gen_base_attributes())
    DATA['schedule_metadata'][schedule_id][values['key']] = meta
    return meta.copy()


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

    return meta.copy()


def schedule_meta_delete(schedule_id, key):
    _check_meta_exists(schedule_id, key)

    del DATA['schedule_metadata'][schedule_id][key]


def worker_get_all():
    return DATA['workers'].values()


def worker_get_by_id(worker_id):
    if worker_id not in DATA['workers']:
        raise exception.NotFound()
    return DATA['workers'][worker_id].copy()


def worker_create(values):
    global DATA
    worker = {}
    worker.update(values)
    worker.update(_gen_base_attributes())
    DATA['workers'][worker['id']] = worker
    return worker.copy()


def worker_delete(worker_id):
    global DATA
    if worker_id not in DATA['workers']:
        raise exception.NotFound()
    del DATA['workers'][worker_id]


def job_create(values):
    global DATA
    job = {}
    job['retry_count'] = 0
    job.update(values)
    job.update(_gen_base_attributes())
    DATA['jobs'][job['id']] = job
    return job.copy()


def job_get_all():
    return DATA['jobs'].values()


def job_get_by_id(job_id):
    if job_id not in DATA['jobs']:
        raise exception.NotFound()
    return DATA['jobs'][job_id].copy()


def job_updated_at_get_by_id(job_id):
    if job_id not in DATA['jobs']:
        raise exception.NotFound()
    return DATA['jobs'][job_id]['updated_at']


def job_status_get_by_id(job_id):
    if job_id not in DATA['jobs']:
        raise exception.NotFound()
    return DATA['jobs'][job_id]['status']


def job_update(job_id, values):
    global DATA
    if job_id not in DATA['jobs']:
        raise exception.NotFound()

    job = DATA['jobs'][job_id]
    #NOTE(ameade): This must come before update specified values since
    # we may be trying to manually set updated_at
    job['updated_at'] = timeutils.utcnow()
    job.update(values)
    return job


def job_delete(job_id):
    global DATA
    if job_id not in DATA['jobs']:
        raise exception.NotFound()
    del DATA['jobs'][job_id]
