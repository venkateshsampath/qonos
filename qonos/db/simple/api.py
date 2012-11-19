import uuid

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


def worker_get_all():
    return DATA['workers'].values()


def worker_get_by_id(id):
    return DATA['workers'][id].copy()


def worker_create(values):
    global DATA
    worker = {}
    worker.update(values)
    worker.update(_gen_base_attributes())
    DATA['workers'][worker['id']] = worker
    return worker


def worker_delete(id):
    global DATA
    del DATA['workers'][id]
