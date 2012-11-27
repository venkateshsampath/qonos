import os
import sys
from random import randint

from qonos.common import config
from qonos.openstack.common import cfg
from qonos.openstack.common import wsgi
from qonos.tests import utils as utils
from qonos.qonosclient import client
from qonos.qonosclient import exception as client_exc


CONF = cfg.CONF


class TestApi(utils.BaseTestCase):

    def setUp(self):
        super(TestApi, self).setUp()
        CONF.paste_deploy.config_file = './etc/qonos-api-paste.ini'
        self.port = randint(20000, 60000)
        service = wsgi.Service()
        service.start(config.load_paste_app('qonos-api'), self.port)
        self.client = client.Client("localhost", self.port)

    def tearDown(self):
        super(TestApi, self).setUp()

    def test_workers_workflow(self):
        workers = self.client.list_workers()['workers']
        self.assertEqual(len(workers), 0)

        # create
        worker = self.client.create_worker('hostname')['worker']
        self.assertTrue(worker['id'])
        self.assertEqual(worker['host'], 'hostname')

        # get worker
        worker = self.client.get_worker(worker['id'])['worker']
        self.assertTrue(worker['id'])
        self.assertEqual(worker['host'], 'hostname')

        # delete worker
        self.client.delete_worker(worker['id'])

        # make sure worker no longer exists
        self.assertRaises(client_exc.NotFound, self.client.get_worker,
                          worker['id'])
