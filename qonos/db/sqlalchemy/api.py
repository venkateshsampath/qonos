# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# Copyright 2010-2011 OpenStack LLC.
# Copyright 2012 Justin Santa Barbara
# All Rights Reserved.
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

"""
Defines interface for DB access
"""

import logging
import time

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
    cfg.BoolOpt('db_auto_create', default=False),
]

CONF = cfg.CONF
CONF.register_opts(db_opts)


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


##################### Worker methods


def worker_get_all():
    session = get_session()
    query = session.query(models.Worker)

    return query.all()


def worker_create(values):
    session = get_session()
    worker_ref = models.Worker()
    worker_ref.update(values)
    worker_ref.save(session=session)

    return worker_ref


def worker_get_by_id(worker_id):
    session = get_session()
    query = session.query(models.Worker).filter_by(id=worker_id)

    try:
        worker = query.one()
    except sa_orm.exc.NoResultFound:
        raise exception.NotFound

    return worker


def worker_delete(worker_id):
    session = get_session()

    with session.begin():
        worker = worker_get_by_id(worker_id)
        worker.delete(session=session)


#################### Job methods


def job_create(values):
    session = get_session()
    job_ref = models.Job()
    job_ref.update(values)
    job_ref.save(session=session)

    return job_ref


def job_get_all():
    session = get_session()
    query = session.query(models.Job)

    return query.all()


def job_get_by_id(job_id):
    session = get_session()
    try:
        job = session.query(models.Job)\
                     .filter_by(id=job_id)\
                     .one()
    except sa_orm.exc.NoResultFound:
        raise exception.NotFound()

    return job


def job_updated_at_get_by_id(job_id):
    return job_get_by_id(job_id)['updated_at']


def job_status_get_by_id(job_id):
    return job_get_by_id(job_id)['status']


def job_update(job_id, values):
    session = get_session()
    job_ref = job_get_by_id(job_id)
    job_ref.update(values)
    job_ref.save(session=session)
    return job_ref


def job_delete(job_id):
    session = get_session()
    job_ref = job_get_by_id(job_id)
    job_ref.delete(session=session)
