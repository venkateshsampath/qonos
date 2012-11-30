import os
import sys
from random import randint

from qonos.common import config
import qonos.db
from qonos.openstack.common import cfg
from qonos.openstack.common import timeutils
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
        self.port = randint(50000, 60000)
        self.service = wsgi.Service()
        self.service.start(config.load_paste_app('qonos-api'), self.port)
        self.client = client.Client("localhost", self.port)

    def tearDown(self):
        super(TestApi, self).tearDown()

        jobs = self.client.list_jobs()['jobs']
        for job in jobs:
            self.client.delete_job(job['id'])

        schedules = self.client.list_schedules()['schedules']
        for schedule in schedules:
            self.client.delete_schedule(schedule['id'])

        workers = self.client.list_workers()['workers']
        for worker in workers:
            self.client.delete_worker(worker['id'])

        self.service.stop()

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
                'minute': 30,
                'hour': 12,
            }
        }
        schedule = self.client.create_schedule(request)['schedule']
        self.assertTrue(schedule['id'])
        self.assertEqual(schedule['tenant_id'], TENANT1)
        self.assertEqual(schedule['action'], 'snapshot')
        self.assertEqual(schedule['minute'], 30)
        self.assertEqual(schedule['hour'], 12)

        # get schedule
        schedule = self.client.get_schedule(schedule['id'])['schedule']
        self.assertTrue(schedule['id'])
        self.assertEqual(schedule['tenant_id'], TENANT1)
        self.assertEqual(schedule['action'], 'snapshot')
        self.assertEqual(schedule['minute'], 30)
        self.assertEqual(schedule['hour'], 12)

        #list schedules
        schedules = self.client.list_schedules()['schedules']
        self.assertEqual(len(schedules), 1)
        print schedules[0]
        print schedule
        self.assertDictEqual(schedules[0], schedule)

        #list schedules, next_run filter
        filter = {}
        filter['next_run_after'] = schedule['next_run']
        filter['next_run_before'] = schedule['next_run']
        schedules = self.client.list_schedules(filter_args=filter)['schedules']
        self.assertEqual(len(schedules), 1)
        self.assertDictEqual(schedules[0], schedule)
        filter['next_run_after'] = '2010-11-30T15:23:00Z'
        filter['next_run_before'] = '2011-11-30T15:23:00Z'
        schedules = self.client.list_schedules(filter_args=filter)['schedules']
        self.assertEqual(len(schedules), 0)

        #update schedule
        request = {'schedule': {'hour': 14}}
        updated_schedule = self.client.update_schedule(schedule['id'], request)
        updated_schedule = updated_schedule['schedule']
        self.assertEqual(updated_schedule['id'], schedule['id'])
        self.assertEqual(updated_schedule['tenant_id'], schedule['tenant_id'])
        self.assertEqual(updated_schedule['action'], schedule['action'])
        self.assertEqual(updated_schedule['minute'], schedule['minute'])
        self.assertEqual(updated_schedule['hour'], request['schedule']['hour'])
        self.assertNotEqual(updated_schedule['hour'], schedule['hour'])

        # delete schedule
        self.client.delete_schedule(schedule['id'])

        # make sure schedule no longer exists
        self.assertRaises(client_exc.NotFound, self.client.get_schedule,
                          schedule['id'])

    def test_schedule_meta_workflow(self):

        # (setup) create schedule
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

        # create meta
        meta = self.client.create_schedule_meta(schedule['id'], 'key1',
                                                'value1')
        meta = meta['meta']
        self.assertEqual(meta['key'], 'key1')
        self.assertEqual(meta['value'], 'value1')

        # make sure duplicate metadata can't be created
        self.assertRaises(client_exc.Duplicate,
                          self.client.create_schedule_meta,
                          schedule['id'],
                          'key1',
                          'value1')

        # list meta
        metadata = self.client.list_schedule_meta(schedule['id'])['metadata']
        self.assertEqual(len(metadata), 1)
        self.assertEqual(metadata[0]['key'], 'key1')
        self.assertEqual(metadata[0]['value'], 'value1')

        # get meta
        value = self.client.get_schedule_meta(schedule['id'], 'key1')
        self.assertEqual(value, 'value1')

        #update schedule
        updated_value = self.client.update_schedule_meta(schedule['id'],
                                                         'key1', 'value2')
        self.assertEqual(updated_value, 'value2')

        # get meta after update
        old_value = value
        value = self.client.get_schedule_meta(schedule['id'], 'key1')
        self.assertNotEqual(value, old_value)
        self.assertEqual(value, 'value2')

        # delete meta
        self.client.delete_schedule_meta(schedule['id'], 'key1')

        # make sure metadata no longer exists
        self.assertRaises(client_exc.NotFound, self.client.get_schedule_meta,
                          schedule['id'], 'key1')

    def test_job_workflow(self):

        # (setup) create schedule
        meta1 = {'key': 'key1', 'value': 'value1'}
        meta2 = {'key': 'key2', 'value': 'value2'}
        request = {
            'schedule':
            {
                'tenant_id': TENANT1,
                'action': 'snapshot',
                'minute': '30',
                'hour': '12',
                'schedule_metadata': [
                    meta1,
                    meta2,
                ]
            }
        }
        schedule = self.client.create_schedule(request)['schedule']

        # create job

        request = {
            'job':
            {
                'schedule_id': schedule['id'],
            }
        }

        new_job = self.client.create_job(request)['job']
        self.assertIsNotNone(new_job.get('id'))
        self.assertEqual(new_job['schedule_id'], schedule['id'])
        self.assertEqual(new_job['tenant_id'], schedule['tenant_id'])
        self.assertEqual(new_job['action'], schedule['action'])
        self.assertEqual(new_job['status'], 'queued')
        self.assertMetadataInList(new_job['job_metadata'], meta1)
        self.assertMetadataInList(new_job['job_metadata'], meta2)

        # list jobs
        jobs = self.client.list_jobs()['jobs']
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]['id'], new_job['id'])
        self.assertEqual(jobs[0]['schedule_id'], new_job['schedule_id'])
        self.assertEqual(jobs[0]['status'], new_job['status'])
        self.assertEqual(jobs[0]['retry_count'], new_job['retry_count'])

        # get job
        job = self.client.get_job(new_job['id'])['job']
        self.assertEqual(job['id'], new_job['id'])
        self.assertEqual(job['schedule_id'], new_job['schedule_id'])
        self.assertEqual(job['status'], new_job['status'])
        self.assertEqual(job['retry_count'], new_job['retry_count'])

        # list job metadata
        metadata = self.client.list_job_metadata(new_job['id'])['metadata']
        self.assertMetadataInList(metadata, meta1)
        self.assertMetadataInList(metadata, meta2)

        # get job metadata
        meta_value = self.client.get_job_metadata(new_job['id'], meta1['key'])
        self.assertEqual(meta_value, meta1['value'])

        # get heartbeat
        heartbeat = self.client.get_job_heartbeat(job['id'])['heartbeat']
        self.assertIsNotNone(heartbeat)

        # heartbeat
        timeutils.set_time_override()
        timeutils.advance_time_seconds(30)
        self.client.job_heartbeat(job['id'])
        new_heartbeat = self.client.get_job_heartbeat(job['id'])['heartbeat']
        self.assertNotEqual(new_heartbeat, heartbeat)
        timeutils.clear_time_override()

        # get status
        status = self.client.get_job_status(job['id'])['status']
        self.assertEqual(status, new_job['status'])

        # update status
        self.client.update_job_status(job['id'], 'done')
        status = self.client.get_job_status(job['id'])['status']
        self.assertNotEqual(status, new_job['status'])
        self.assertEqual(status, 'done')

        # delete job
        self.client.delete_job(job['id'])

        # make sure job no longer exists
        self.assertRaises(client_exc.NotFound, self.client.get_job,
                          job['id'])
