import functools
import uuid
import copy
import qonos.db.db_utils as db_utils
from datetime import timedelta
from operator import itemgetter
from operator import methodcaller

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


# TODO: Move to config
JOB_TYPES = {
    'default':
    {
        'max_retry': 3,
        'timeout_seconds': 60,
    },
    'snapshot':
    {
        'max_retry': 2,
        'timeout_seconds': 30,
    }
}


def configure_db():
    pass


def reset():
    global DATA
    for k in DATA:
        DATA[k] = {}


def _gen_base_attributes(ID=None):
    values = {}
    if ID is None:
        values['id'] = str(uuid.uuid4())
    values['created_at'] = timeutils.utcnow()
    values['updated_at'] = timeutils.utcnow()
    return copy.deepcopy(values)


def _schedule_create(values):
    global DATA
    DATA['schedules'][values['id']] = values
    _schedule_meta_init(values['id'])
    return copy.deepcopy(values)


def _do_pagination(items, marker, limit):
    items = sorted(items, key=itemgetter('id'))
    start = 0
    end = -1
    if marker is None:
        start = 0
    else:
        for i, item in enumerate(items):
            if item['id'] == marker:
                start = i + 1
                break
        else:
            msg = _('Marker %s not found') % marker
            raise exception.NotFound(explanation=msg)

    end = start + limit if limit is not None else None
    return items[start:end]


def schedule_get_all(filter_args={}):
    schedules = copy.deepcopy(DATA['schedules'].values())
    schedules_mutate = copy.deepcopy(DATA['schedules'].values())

    for schedule in schedules:
        schedule['schedule_metadata'] =\
            copy.deepcopy(schedule_meta_get_all(schedule['id']))

    schedules_mutate = copy.deepcopy(schedules)

    if 'next_run_after' in filter_args and 'next_run_before' in filter_args:
        for schedule in schedules:
            if not((schedule['next_run'] > filter_args['next_run_after'] and
                    schedule['next_run'] < filter_args['next_run_before']) or
                    schedule['next_run'] == filter_args['next_run_after']):
                if schedule in schedules_mutate:
                    del schedules_mutate[schedules_mutate.index(schedule)]

    if ('next_run_after' not in filter_args and
        'next_run_before' in filter_args):
        for schedule in schedules:
            if not (schedule['next_run'] < filter_args['next_run_before']):
                if schedule in schedules_mutate:
                    del schedules_mutate[schedules_mutate.index(schedule)]

    if ('next_run_after' in filter_args and
        'next_run_before' not in filter_args):
        for schedule in schedules:
            if not(schedule['next_run'] > filter_args['next_run_after'] or
                   schedule['next_run'] == filter_args['next_run_after']):
                if schedule in schedules_mutate:
                    del schedules_mutate[schedules_mutate.index(schedule)]

    if filter_args.get('tenant_id') is not None:
        for schedule in schedules:
            if schedule['tenant_id'] != filter_args['tenant_id']:
                if schedule in schedules_mutate:
                    del schedules_mutate[schedules_mutate.index(schedule)]

    if filter_args.get('instance_id') is not None:
        instance_id = filter_args['instance_id']
        for schedule in schedules:
            if schedule['schedule_metadata']:
                for schedule_metadata in schedule['schedule_metadata']:
                    if not(schedule_metadata['key'] == 'instance_id' and
                           schedule_metadata['value'] == instance_id):
                        del schedules_mutate[schedules_mutate.index(schedule)]
                        break
            else:
                if schedule in schedules_mutate:
                    del schedules_mutate[schedules_mutate.index(schedule)]

    marker = filter_args.get('marker')
    limit = filter_args.get('limit')
    schedules_mutate = _do_pagination(schedules_mutate, marker, limit)
    return schedules_mutate


def schedule_get_by_id(schedule_id):
    if schedule_id not in DATA['schedules']:
        raise exception.NotFound()
    schedule = copy.deepcopy(DATA['schedules'][schedule_id])
    schedule['schedule_metadata'] = \
        copy.deepcopy(schedule_meta_get_all(schedule_id))
    return schedule


def schedule_create(schedule_values):
    db_utils.validate_schedule_values(schedule_values)
    values = copy.deepcopy(schedule_values)
    schedule = {}

    metadata = []
    if 'schedule_metadata' in values:
        metadata = values['schedule_metadata']
        del values['schedule_metadata']

    schedule.update(values)
    ID = values.get('id')
    schedule.update(_gen_base_attributes(ID=ID))
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


def _delete_schedule_meta(schedule_id, key):
    del DATA['schedule_metadata'][schedule_id][key]

def schedule_meta_delete(schedule_id, key):
    _check_meta_exists(schedule_id, key)
    _delete_schedule_meta(schedule_id, key)

def worker_get_all(params={}):
    workers = copy.deepcopy(DATA['workers'].values())
    marker = params.get('marker')
    limit = params.get('limit')
    workers = _do_pagination(workers, marker, limit)
    return workers

def worker_get_by_id(worker_id):
    if worker_id not in DATA['workers']:
        raise exception.NotFound()
    return copy.deepcopy(DATA['workers'][worker_id])


def worker_create(values):
    global DATA
    worker = {}
    worker.update(values)
    ID = values.get('id')
    worker.update(_gen_base_attributes(ID=ID))
    DATA['workers'][worker['id']] = worker
    return copy.deepcopy(worker)


def worker_delete(worker_id):
    global DATA
    if worker_id not in DATA['workers']:
        raise exception.NotFound()
    del DATA['workers'][worker_id]


def job_create(job_values):
    global DATA
    db_utils.validate_job_values(job_values)
    values = job_values.copy()
    job = {}

    metadata = []
    if 'job_metadata' in values:
        metadata = values['job_metadata']
        del values['job_metadata']

    if not 'retry_count' in values:
        values['retry_count'] = 0
    job['worker_id'] = None

    now = timeutils.utcnow()

    job_timeout_seconds = _job_get_timeout(values['action'])
    if not 'timeout' in values:
        values['timeout'] = now + timedelta(seconds=job_timeout_seconds)
    values['hard_timeout'] = now + timedelta(seconds=job_timeout_seconds)
    job.update(values)
    ID = values.get('id')
    job.update(_gen_base_attributes(ID=ID))

    DATA['jobs'][job['id']] = job

    for metadatum in metadata:
        job_meta_create(job['id'], metadatum)

    return job_get_by_id(job['id'])


def job_get_all(params={}):
    jobs = copy.deepcopy(DATA['jobs'].values())

    for job in jobs:
        job['job_metadata'] =\
            job_meta_get_all_by_job_id(job['id'])

    marker = params.get('marker')
    limit = params.get('limit')
    jobs = _do_pagination(jobs, marker, limit)

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


def job_get_and_assign_next_by_action(action, worker_id):
    """Get the next available job for the given action and assign it
    to the worker for worker_id.
    This must be an atomic action!"""
    job_ref = None
    now = timeutils.utcnow()
    jobs = _jobs_get_sorted()
    max_retry = _job_get_max_retry(action)
    for job in jobs:
        if job['action'] == action and \
                job['retry_count'] < max_retry and \
                job['hard_timeout'] > now and \
                (job['worker_id'] is None or job['timeout'] <= now):
            job_ref = job
            break

    if job_ref is None:
        return None

    job_id = job_ref['id']
    DATA['jobs'][job_id]['worker_id'] = worker_id
    DATA['jobs'][job_id]['retry_count'] = job_ref['retry_count'] + 1
    job = copy.deepcopy(DATA['jobs'][job_id])
    job['job_metadata'] = job_meta_get_all_by_job_id(job_id)

    return job


def _jobs_get_sorted():
    jobs = copy.deepcopy(DATA['jobs'])
    sorted_jobs = []
    for job_id in jobs:
        sorted_jobs.append(jobs[job_id])

    sorted_jobs = sorted(sorted_jobs, key=itemgetter('created_at'))
    return sorted_jobs


def _job_get_max_retry(action):
    if not action in JOB_TYPES:
        action = 'default'
    return JOB_TYPES[action]['max_retry']


def _job_get_timeout(action):
    if not action in JOB_TYPES:
        action = 'default'
    return JOB_TYPES[action]['timeout_seconds']


def _jobs_cleanup_hard_timed_out():
    """Find all jobs with hard_timeout values which have passed
    and delete them, logging the timeout / failure as appropriate"""
    now = timeutils.utcnow()
    del_ids = []
    for job_id in DATA['jobs']:
        job = DATA['jobs'][job_id]
        print now, job['hard_timeout']
        print now - job['hard_timeout']
        if (now - job['hard_timeout']) > timedelta(microseconds=0):
            del_ids.append(job_id)

    for job_id in del_ids:
        job_delete(job_id)
    return len(del_ids)


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
