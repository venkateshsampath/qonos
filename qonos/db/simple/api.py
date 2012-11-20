import uuid

from qonos.common import exception
from qonos.openstack.common import timeutils


DATA = {
    'schedules': {},
    'schedule_metadata': {},
    'jobs': {},
    'workers': {},
    'job_faults': {},
}


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
    DATA['schedules'][values['id']] = values
    return values.copy()


def schedule_get_all():
    return DATA['schedules'].values()


def schedule_get_by_id(schedule_id):
    if schedule_id not in DATA['schedules']:
        raise exception.NotFound()
    return DATA['schedules'][schedule_id].copy()


def schedule_create(values):
    global DATA
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
