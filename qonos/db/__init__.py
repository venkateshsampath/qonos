from qonos.openstack.common import cfg
from qonos.openstack.common import importutils

sql_connection_opt = cfg.StrOpt('sql_connection',
                                default='sqlite:///qonos.sqlite',
                                secret=True,
                                metavar='CONNECTION',
                                help='A valid SQLAlchemy connection '
                                     'string for the database. '
                                     'Default: %default')

CONF = cfg.CONF
CONF.register_opt(sql_connection_opt)


def get_api():
    db_api = importutils.import_module(CONF.db_api)
    db_api.configure_db()
    return db_api
