"""
Defines interface for DB access
"""

import functools
import logging
import time
import qonos.db.db_utils as db_utils
from datetime import timedelta
from qonos.openstack.common.gettextutils import _

import sqlalchemy
import sqlalchemy.orm as sa_orm
import sqlalchemy.sql as sa_sql

from qonos.common import exception
from qonos.db.sqlalchemy import models
from qonos.openstack.common import cfg
import qonos.openstack.common.log as os_logging
from qonos.openstack.common import timeutils


_ENGINE = None
_MAKER = None
_MAX_RETRIES = None
_RETRY_INTERVAL = None
BASE = models.BASE
sa_logger = None
LOG = os_logging.getLogger(__name__)

db_opts = [
    cfg.IntOpt('sql_idle_timeout', default=3600),
    cfg.IntOpt('sql_max_retries', default=60),
    cfg.IntOpt('sql_retry_interval', default=1),
    cfg.BoolOpt('db_auto_create', default=True),
]

CONF = cfg.CONF
CONF.register_opts(db_opts)

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


def force_dict(func):
    """Ensure returned object is a dict or list of dicts."""

    def convert_list(a_list):
        to_return = []
        for object in a_list:
            if isinstance(object, list):
                to_return.append(convert_list(object))
            else:
                to_return.append(convert_object(object))
        return to_return

    def convert_object(object):
        if (isinstance(object, models.ModelBase) or
                isinstance(object, tuple)):
            to_return = dict(object)
        else:
            raise ValueError()

        if 'parent' in to_return:
            del to_return['parent']

        if '_sa_instance_state' in to_return:
            del to_return['_sa_instance_state']

        for key in to_return:
            if isinstance(to_return[key], list):
                to_return[key] = convert_list(to_return[key])
            elif isinstance(to_return[key], models.ModelBase):
                to_return[key] = convert_object(to_return[key])

        return to_return

    def convert_output(output):
        if output is None:
            to_return = None
        elif isinstance(output, list):
            to_return = convert_list(output)
        else:
            to_return = convert_object(output)
        return to_return

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        output = func(*args, **kwargs)
        return convert_output(output)
    return wrapped


def ping_listener(dbapi_conn, connection_rec, connection_proxy):

    """
    Ensures that MySQL connections checked out of the
    pool are alive.

    Borrowed from:
    http://groups.google.com/group/sqlalchemy/msg/a4ce563d802c929f
    """

    try:
        dbapi_conn.cursor().execute('select 1')
    except dbapi_conn.OperationalError, ex:
        if ex.args[0] in (2006, 2013, 2014, 2045, 2055):
            msg = 'Got mysql server has gone away: %s' % ex
            LOG.warn(msg)
            raise sqlalchemy.exc.DisconnectionError(msg)
        else:
            raise


def configure_db():
    """
    Establish the database, create an engine if needed, and
    register the models.
    """
    global _ENGINE, sa_logger, _MAX_RETRIES, _RETRY_INTERVAL
    if not _ENGINE:
        sql_connection = CONF.sql_connection
        _MAX_RETRIES = CONF.sql_max_retries
        _RETRY_INTERVAL = CONF.sql_retry_interval
        connection_dict = sqlalchemy.engine.url.make_url(sql_connection)
        engine_args = {'pool_recycle': CONF.sql_idle_timeout,
                       'echo': False,
                       'convert_unicode': True
                       }

        try:
            _ENGINE = sqlalchemy.create_engine(sql_connection, **engine_args)

            if 'mysql' in connection_dict.drivername:
                sqlalchemy.event.listen(_ENGINE, 'checkout', ping_listener)

            _ENGINE.connect = wrap_db_error(_ENGINE.connect)
            _ENGINE.connect()
        except Exception, err:
            msg = _("Error configuring registry database with supplied "
                    "sql_connection '%(sql_connection)s'. "
                    "Got error:\n%(err)s") % locals()
            LOG.error(msg)
            raise

        sa_logger = logging.getLogger('sqlalchemy.engine')
        if CONF.debug:
            sa_logger.setLevel(logging.DEBUG)

        if CONF.db_auto_create:
            LOG.info('auto-creating qonos DB')
            models.register_models(_ENGINE)
        else:
            LOG.info('not auto-creating qonos DB')


def reset():
    models.unregister_models(_ENGINE)
    models.register_models(_ENGINE)


def get_session(autocommit=True, expire_on_commit=False):
    """Helper method to grab session"""
    global _MAKER
    if not _MAKER:
        assert _ENGINE
        _MAKER = sa_orm.sessionmaker(bind=_ENGINE,
                                     autocommit=autocommit,
                                     expire_on_commit=expire_on_commit)
    return _MAKER()


def is_db_connection_error(args):
    """Return True if error in connecting to db."""
    # NOTE(adam_g): This is currently MySQL specific and needs to be extended
    #               to support Postgres and others.
    conn_err_codes = ('2002', '2003', '2006')
    for err_code in conn_err_codes:
        if args.find(err_code) != -1:
            return True
    return False


def wrap_db_error(f):
    """Retry DB connection. Copied from nova and modified."""
    def _wrap(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except sqlalchemy.exc.OperationalError, e:
            if not is_db_connection_error(e.args[0]):
                raise

            remaining_attempts = _MAX_RETRIES
            while True:
                LOG.warning(_('SQL connection failed. %d attempts left.'),
                            remaining_attempts)
                remaining_attempts -= 1
                time.sleep(_RETRY_INTERVAL)
                try:
                    return f(*args, **kwargs)
                except sqlalchemy.exc.OperationalError, e:
                    if (remaining_attempts == 0 or
                            not is_db_connection_error(e.args[0])):
                        raise
                except sqlalchemy.exc.DBAPIError:
                    raise
        except sqlalchemy.exc.DBAPIError:
            raise
    _wrap.func_name = f.func_name
    return _wrap


#################### Schedule methods


@force_dict
def schedule_create(schedule_values):
    db_utils.validate_schedule_values(schedule_values)
    # make a copy so we can remove 'schedule_metadata'
    # without affecting the caller
    values = schedule_values.copy()
    metadata = None
    session = get_session()
    schedule_ref = models.Schedule()

    if 'schedule_metadata' in values:
        metadata = values['schedule_metadata']
        _set_schedule_metadata(schedule_ref, metadata)
        del values['schedule_metadata']

    schedule_ref.update(values)
    schedule_ref.save(session=session)

    return _schedule_get_by_id(schedule_ref['id'])


def paginate_query(query, model, sort_keys, limit=None, marker=None,
                   sort_dir=None, sort_dirs=None):
    """
    #Note(nikhil): add the docstring help here
    """
    if marker is not None:
        marker_values = []
        for sort_key in sort_keys:
            v = getattr(marker, sort_key)
            marker_values.append(v)

        criteria_list = []
        crit_attrs = []
        crit_attrs.append((model_attr > marker_values[0]))
        criteria = sa_sql.and_(*crit_attrs)
        criteria_list.append(criteria)

        f = sa_sql.or_(*criteria_list)
        query = query.filter(f)

    if limit is not None:
        query = query.limit(limit)

    return query


@force_dict
def schedule_get_all(filter_args={}):
    session = get_session()
    query = session.query(models.Schedule)\
                   .options(sa_orm.joinedload(
                            models.Schedule.schedule_metadata))

    if 'next_run_after' in filter_args and 'next_run_before' in filter_args:
        query = query.filter(
            models.Schedule.next_run.between(filter_args['next_run_after'],
                                             filter_args['next_run_before']))
    if ('next_run_after' in filter_args and
        'next_run_before' not in filter_args):
        query = query.filter(
            models.Schedule.next_run >= filter_args['next_run_after'])

    if ('next_run_after' not in filter_args and
        'next_run_before' in filter_args):
        query = query.filter(
            models.Schedule.next_run < filter_args['next_run_before'])

    if filter_args.get('tenant_id') is not None:
        query = query.filter(
                models.Schedule.tenant_id == filter_args['tenant_id'])

    if filter_args.get('instance_id') is not None:
        query = query.filter(models.Schedule.schedule_metadata.any(
                    key='instance_id', value=filter_args['instance_id']))

    marker_schedule = None
    if filter_args.get('marker') is not None:
        marker_schedule = _schedule_get_by_id(filter_args['marker'])

    query = paginate_query(query, models.Schedule, ['id'],
                           limit=filter_args.get('limit'), marker=marker_schedule)

    return query.all()


def _schedule_get_by_id(schedule_id):
    session = get_session()
    try:
        schedule = session.query(models.Schedule)\
                          .options(sa_orm.subqueryload('schedule_metadata'))\
                          .filter_by(id=schedule_id)\
                          .one()
    except sa_orm.exc.NoResultFound:
        raise exception.NotFound()

    return schedule


@force_dict
def schedule_get_by_id(schedule_id):
    return _schedule_get_by_id(schedule_id)


@force_dict
def schedule_update(schedule_id, schedule_values):
    # make a copy so we can remove 'schedule_metadata'
    # without affecting the caller
    values = schedule_values.copy()
    session = get_session()
    schedule_ref = _schedule_get_by_id(schedule_id)

    if 'schedule_metadata' in values:
        for metadata_ref in schedule_ref.schedule_metadata:
            schedule_ref.schedule_metadata.remove(metadata_ref)

        metadata = values['schedule_metadata']
        _set_schedule_metadata(schedule_ref, metadata)
        del values['schedule_metadata']

    schedule_ref.update(values)
    schedule_ref.save(session=session)
    return _schedule_get_by_id(schedule_id)


def schedule_delete(schedule_id):
    session = get_session()
    schedule_ref = _schedule_get_by_id(schedule_id)
    schedule_ref.delete(session=session)


def _set_schedule_metadata(schedule_ref, metadata):
    for metadatum in metadata:
        metadata_ref = models.ScheduleMetadata()
        metadata_ref.update(metadatum)
        schedule_ref.schedule_metadata.append(metadata_ref)

#################### Schedule Metadata methods


@force_dict
def schedule_meta_create(schedule_id, values):
    _schedule_get_by_id(schedule_id)
    session = get_session()
    meta_ref = models.ScheduleMetadata()
    values['schedule_id'] = schedule_id
    meta_ref.update(values)

    try:
        meta_ref.save(session=session)
    except sqlalchemy.exc.IntegrityError:
        raise exception.Duplicate()

    return _schedule_meta_get(schedule_id, values['key'])


@force_dict
def schedule_meta_get_all(schedule_id):
    _schedule_get_by_id(schedule_id)
    session = get_session()
    query = session.query(models.ScheduleMetadata)\
                   .filter_by(schedule_id=schedule_id)

    return query.all()


def _schedule_meta_get(schedule_id, key):
    try:
        _schedule_get_by_id(schedule_id)
    except exception.NotFound:
        msg = _('Schedule %s could not be found') % schedule_id
        raise exception.NotFound(message=msg)
    session = get_session()
    try:
        meta = session.query(models.ScheduleMetadata)\
                      .filter_by(schedule_id=schedule_id)\
                      .filter_by(key=key)\
                      .one()
    except sa_orm.exc.NoResultFound:
        raise exception.NotFound()

    return meta


@force_dict
def schedule_meta_get(schedule_id, key):
    return _schedule_meta_get(schedule_id, key)


@force_dict
def schedule_meta_update(schedule_id, key, values):
    _schedule_get_by_id(schedule_id)
    session = get_session()
    meta_ref = _schedule_meta_get(schedule_id, key)
    meta_ref.update(values)
    meta_ref.save(session=session)
    return meta_ref


def schedule_meta_delete(schedule_id, key):
    _schedule_get_by_id(schedule_id)
    session = get_session()
    meta_ref = _schedule_meta_get(schedule_id, key)
    meta_ref.delete(session=session)


##################### Worker methods


@force_dict
def worker_get_all(params={}):
    session = get_session()
    query = session.query(models.Worker)

    marker_worker = None
    if params.get('marker') is not None:
        marker_worker = _worker_get_by_id(params['marker'])

    query = paginate_query(query, models.Worker, ['id'],
                           limit=params.get('limit'), marker=marker_worker)

    return query.all()


@force_dict
def worker_create(values):
    session = get_session()
    worker_ref = models.Worker()
    worker_ref.update(values)
    worker_ref.save(session=session)

    return _worker_get_by_id(worker_ref['id'])


def _worker_get_by_id(worker_id):
    session = get_session()
    query = session.query(models.Worker).filter_by(id=worker_id)

    try:
        worker = query.one()
    except sa_orm.exc.NoResultFound:
        raise exception.NotFound

    return worker


@force_dict
def worker_get_by_id(worker_id):
    return _worker_get_by_id(worker_id)


def worker_delete(worker_id):
    session = get_session()

    with session.begin():
        worker = _worker_get_by_id(worker_id)
        worker.delete(session=session)


#################### Job methods


@force_dict
def job_create(job_values):
    db_utils.validate_job_values(job_values)
    values = job_values.copy()
    session = get_session()
    job_ref = models.Job()

    if 'job_metadata' in values:
        metadata = values['job_metadata']
        _set_job_metadata(job_ref, metadata)
        del values['job_metadata']

    now = timeutils.utcnow()

    job_timeout_seconds = _job_get_timeout(values['action'])
    if not 'timeout' in values:
        values['timeout'] = now + timedelta(seconds=job_timeout_seconds)
    values['hard_timeout'] = now + timedelta(seconds=job_timeout_seconds)
    job_ref.update(values)
    job_ref.save(session=session)

    return _job_get_by_id(job_ref['id'])


@force_dict
def job_get_all():
    session = get_session()
    query = session.query(models.Job)\
                   .options(sa_orm.subqueryload('job_metadata'))

    return query.all()


def _job_get_by_id(job_id):
    session = get_session()
    try:
        job = session.query(models.Job)\
                     .options(sa_orm.subqueryload('job_metadata'))\
                     .filter_by(id=job_id)\
                     .one()
    except sa_orm.exc.NoResultFound:
        raise exception.NotFound()

    return job


@force_dict
def job_get_by_id(job_id):
    return _job_get_by_id(job_id)


def job_updated_at_get_by_id(job_id):
    return _job_get_by_id(job_id)['updated_at']


def job_status_get_by_id(job_id):
    return _job_get_by_id(job_id)['status']


@force_dict
def job_get_and_assign_next_by_action(action, worker_id):
    """Get the next available job for the given action and assign it
    to the worker for worker_id.
    This must be an atomic action!"""
    now = timeutils.utcnow()
    session = get_session()
    job_id = None
    try:
        job_ref = _job_get_next_by_action(session, now, action)

        if job_ref is None:
            return None

        job_ref.update({'worker_id': worker_id,
                        'retry_count': job_ref['retry_count'] + 1})
        job_id = job_ref['id']
        job_ref.save(session)
    except sa_orm.exc.NoResultFound:
        raise exception.NotFound()

    return _job_get_by_id(job_id)


def _job_get_next_by_action(session, now, action):
    max_retry = _job_get_max_retry(action)
    job_ref = session.query(models.Job)\
        .options(sa_orm.subqueryload('job_metadata'))\
        .filter_by(action=action)\
        .filter(models.Job.retry_count < max_retry)\
        .filter(models.Job.hard_timeout > now)\
        .filter(sa_sql.or_(models.Job.worker_id == None,
                           models.Job.timeout <= now))\
        .with_lockmode('update')\
        .order_by(models.Job.created_at.asc())\
        .first()
    return job_ref


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
    session = get_session()
    num_del = session.query(models.Job)\
        .filter(models.Job.hard_timeout <= now)\
        .delete()
    session.flush()
    return num_del


@force_dict
def job_update(job_id, job_values):
    # make a copy so we can remove 'job_metadata'
    # without affecting the caller
    values = job_values.copy()
    session = get_session()
    job_ref = _job_get_by_id(job_id)

    if 'job_metadata' in values:
        for metadata_ref in job_ref.job_metadata:
            job_ref.job_metadata.remove(metadata_ref)

        metadata = values['job_metadata']
        _set_job_metadata(job_ref, metadata)
        del values['job_metadata']

    job_ref.update(values)
    job_ref.save(session=session)
    return _job_get_by_id(job_id)


def job_delete(job_id):
    session = get_session()
    job_ref = _job_get_by_id(job_id)
    job_ref.delete(session=session)


def _set_job_metadata(job_ref, metadata):
    for metadatum in metadata:
        metadata_ref = models.JobMetadata()
        metadata_ref.update(metadatum)
        job_ref.job_metadata.append(metadata_ref)


@force_dict
def job_meta_create(job_id, values):
    values['job_id'] = job_id
    session = get_session()
    meta_ref = models.JobMetadata()
    meta_ref.update(values)

    try:
        meta_ref.save(session=session)
    except sqlalchemy.exc.IntegrityError:
        raise exception.Duplicate()

    return _job_meta_get_by_id(meta_ref['id'])


def _job_meta_get_by_id(meta_id):
    session = get_session()
    try:
        meta = session.query(models.JobMetadata)\
                      .filter_by(id=meta_id)\
                      .one()
    except sa_orm.exc.NoResultFound:
        raise exception.NotFound()

    return meta


def _job_meta_get_all_by_job_id(job_id):
    session = get_session()
    try:
        meta = session.query(models.JobMetadata)\
                      .filter_by(job_id=job_id)\
                      .all()
    except sa_orm.exc.NoResultFound:
        raise exception.NotFound()

    return meta


def _job_meta_get(job_id, key):
    session = get_session()
    try:
        meta = session.query(models.JobMetadata)\
                      .filter_by(job_id=job_id)\
                      .filter_by(key=key)\
                      .one()
    except sa_orm.exc.NoResultFound:
        raise exception.NotFound()

    return meta


@force_dict
def job_meta_get_all_by_job_id(job_id):
    return _job_meta_get_all_by_job_id(job_id)


@force_dict
def job_meta_get(job_id, key):
    return _job_meta_get(job_id, key)


@force_dict
def job_meta_update(job_id, key, values):
    _job_get_by_id(job_id)
    session = get_session()
    meta_ref = _job_meta_get(job_id, key)
    meta_ref.update(values)
    meta_ref.save(session=session)
    return meta_ref


def job_meta_delete(job_id, key):
    job_get_by_id(job_id)
    session = get_session()
    meta_ref = _job_meta_get(job_id, key)
    meta_ref.delete(session=session)
