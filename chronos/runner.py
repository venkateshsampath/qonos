from chronos.common import config
from chronos.openstack.common.wsgi import run_server

run_server(config.load_paste_app(), 8080)
