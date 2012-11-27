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

TENANT1 = '6838eb7b-6ded-434a-882c-b344c77fe8df'
TENANT2 = '2c014f32-55eb-467d-8fcb-4bd706012f81'


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

        # create worker
        worker = self.client.create_worker('hostname')['worker']
        self.assertTrue(worker['id'])
        self.assertEqual(worker['host'], 'hostname')

        # get worker
        worker = self.client.get_worker(worker['id'])['worker']
        self.assertTrue(worker['id'])
        self.assertEqual(worker['host'], 'hostname')

        # list workers
        workers = self.client.list_workers()['workers']
        self.assertEqual(len(workers), 1)
        self.assertDictEqual(workers[0], worker)

        # delete worker
        self.client.delete_worker(worker['id'])

        # make sure worker no longer exists
        self.assertRaises(client_exc.NotFound, self.client.get_worker,
                          worker['id'])

    def test_schedule_workflow(self):
        schedules = self.client.list_schedules()['schedules']
        self.assertEqual(len(schedules), 0)

        # create schedule
        request = {
            'schedule':
            {
                'tenant_id': TENANT1,
                'action': 'snapshot',
                'minute': '30',
                'hour': '12'
            }
        }
        schedule = self.client.create_schedule(request)['schedule']
        self.assertTrue(schedule['id'])
        self.assertEqual(schedule['tenant_id'], TENANT1)
        self.assertEqual(schedule['action'], 'snapshot')
        self.assertEqual(schedule['minute'], '30')
        self.assertEqual(schedule['hour'], '12')

        # get schedule
        schedule = self.client.get_schedule(schedule['id'])['schedule']
        self.assertTrue(schedule['id'])
        self.assertEqual(schedule['tenant_id'], TENANT1)
        self.assertEqual(schedule['action'], 'snapshot')
        self.assertEqual(schedule['minute'], '30')
        self.assertEqual(schedule['hour'], '12')

        #list schedules
        schedules = self.client.list_schedules()['schedules']
        self.assertEqual(len(schedules), 1)
        self.assertDictEqual(schedules[0], schedule)

        #update schedule
        old_hour = schedule['hour']
        schedule['hour'] = '14'
        request = {'schedule': schedule}
        updated_schedule = self.client.update_schedule(schedule['id'], request)
        updated_schedule = updated_schedule['schedule']
        self.assertEqual(updated_schedule['id'], schedule['id'])
        self.assertEqual(updated_schedule['tenant_id'], schedule['tenant_id'])
        self.assertEqual(updated_schedule['action'], schedule['action'])
        self.assertEqual(updated_schedule['minute'], schedule['minute'])
        self.assertNotEqual(updated_schedule['hour'], old_hour)
        self.assertEqual(updated_schedule['hour'], schedule['hour'])

        # delete schedule
        self.client.delete_schedule(schedule['id'])

        # make sure schedule no longer exists
        self.assertRaises(client_exc.NotFound, self.client.get_schedule,
                          schedule['id'])
