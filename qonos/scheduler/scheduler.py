from qonos.openstack.common.gettextutils import _
import qonos.openstack.common.log as logging

LOG = logging.getLogger(__name__)


class Scheduler(object):
    def __init__(self):
        pass

    def run(self):
        LOG.debug(_('Starting qonos scheduler service'))

        while True:
            pass

    def get_schedules(self):
        pass

