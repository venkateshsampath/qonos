"""
Defines interface for DB access
"""

import functools
import logging
import time
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


def force_dict(func):
    """Ensure returned object is a dict or list of dicts."""
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        output = func(*args, **kwargs)
        if isinstance(output, list):
            to_return = []
            for i in output:
                to_return.append(dict(i))
            return to_return
        return dict(output)
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
def schedule_create(values):
    session = get_session()
    schedule_ref = models.Schedule()
    schedule_ref.update(values)
    schedule_ref.save(session=session)

    return _schedule_get_by_id(schedule_ref['id'])


@force_dict
def schedule_get_all():
    session = get_session()
    query = session.query(models.Schedule)

    return query.all()


def _schedule_get_by_id(schedule_id):
    session = get_session()
    try:
        schedule = session.query(models.Schedule)\
                          .filter_by(id=schedule_id)\
                          .one()
    except sa_orm.exc.NoResultFound:
        raise exception.NotFound()

    return schedule


@force_dict
def schedule_get_by_id(schedule_id):
    return _schedule_get_by_id(schedule_id)


@force_dict
def schedule_update(schedule_id, values):
    session = get_session()
    schedule_ref = _schedule_get_by_id(schedule_id)
    schedule_ref.update(values)
    schedule_ref.save(session=session)
    return schedule_ref


def schedule_delete(schedule_id):
    session = get_session()
    schedule_ref = _schedule_get_by_id(schedule_id)
    schedule_ref.delete(session=session)


#################### Schedule Metadata methods


@force_dict
def schedule_meta_create(schedule_id, values):
    schedule_get_by_id(schedule_id)
    session = get_session()
    meta_ref = models.ScheduleMetadata()
    values['schedule_id'] = schedule_id
    meta_ref.update(values)
    meta_ref.save(session=session)

    return schedule_meta_get(schedule_id, values['key'])


@force_dict
def schedule_meta_get_all(schedule_id):
    schedule_get_by_id(schedule_id)
    session = get_session()
    query = session.query(models.ScheduleMetadata)\
                   .filter_by(schedule_id=schedule_id)

    return query.all()


def _schedule_meta_get(schedule_id, key):
    try:
        schedule_get_by_id(schedule_id)
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
    schedule_get_by_id(schedule_id)
    session = get_session()
    meta_ref = _schedule_meta_get(schedule_id, key)
    meta_ref.update(values)
    meta_ref.save(session=session)
    return meta_ref


def schedule_meta_delete(schedule_id, key):
    schedule_get_by_id(schedule_id)
    session = get_session()
    meta_ref = _schedule_meta_get(schedule_id, key)
    meta_ref.delete(session=session)


##################### Worker methods


@force_dict
def worker_get_all():
    session = get_session()
    query = session.query(models.Worker)

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
def job_create(values):
    session = get_session()
    job_ref = models.Job()
    job_ref.update(values)
    job_ref.save(session=session)

    return _job_get_by_id(job_ref['id'])


@force_dict
def job_get_all():
    session = get_session()
    query = session.query(models.Job)

    return query.all()


def _job_get_by_id(job_id):
    session = get_session()
    try:
        job = session.query(models.Job)\
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
def job_update(job_id, values):
    session = get_session()
    job_ref = _job_get_by_id(job_id)
    job_ref.update(values)
    job_ref.save(session=session)
    return job_ref


def job_delete(job_id):
    session = get_session()
    job_ref = _job_get_by_id(job_id)
    job_ref.delete(session=session)
