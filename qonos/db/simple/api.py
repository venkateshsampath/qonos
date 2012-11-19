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
    values['created_at'] = timeutils.isotime()
    values['updated_at'] = timeutils.isotime()
    return values


def _schedule_create(values):
    DATA['schedules'][values['id']] = values
    return values


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
    schedule = DATA['schedules'][schedule_id]
    schedule.update(values)
    schedule['updated_at'] = timeutils.isotime()
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
    return worker


def worker_delete(worker_id):
    global DATA
    if worker_id not in DATA['workers']:
        raise exception.NotFound()
    del DATA['workers'][worker_id]
