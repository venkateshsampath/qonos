
import os
import sys
from random import randint
from operator import itemgetter

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

        jobs = self.client.list_jobs()
        for job in jobs:
            self.client.delete_job(job['id'])

        #schedules = self.client.list_schedules()
        response = self.client.list_schedules()
        schedules = response[0]
        for schedule in schedules:
            self.client.delete_schedule(schedule['id'])

        workers = self.client.list_workers()
        for worker in workers:
            self.client.delete_worker(worker['id'])

        self.service.stop()

    def test_workers_workflow(self):
        workers = self.client.list_workers()
        self.assertEqual(len(workers), 0)

        # create worker
        worker = self.client.create_worker('hostname', 'workername')
        self.assertTrue(worker['id'])
        self.assertEqual(worker['host'], 'hostname')

        # get worker
        worker = self.client.get_worker(worker['id'])
        self.assertTrue(worker['id'])
        self.assertEqual(worker['host'], 'hostname')

        # list workers
        workers = self.client.list_workers()
        self.assertEqual(len(workers), 1)
        self.assertDictEqual(workers[0], worker)

        # get job for worker no jobs for action
        job = self.client.get_next_job(worker['id'], 'snapshot')
        self.assertIsNone(job['job'])

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
        schedule = self.client.create_schedule(request)

        # (setup) create job

        self.client.create_job(schedule['id'])

        job = self.client.get_next_job(worker['id'], 'snapshot')
        next_job = job['job']
        self.assertIsNotNone(next_job.get('id'))
        self.assertEqual(next_job['schedule_id'], schedule['id'])
        self.assertEqual(next_job['tenant_id'], schedule['tenant_id'])
        self.assertEqual(next_job['action'], schedule['action'])
        self.assertEqual(next_job['status'], 'queued')
        self.assertMetadataInList(next_job['job_metadata'], meta1)
        self.assertMetadataInList(next_job['job_metadata'], meta2)

        # get job for worker no jobs left for action
        job = self.client.get_next_job(worker['id'], 'snapshot')
        self.assertIsNone(job['job'])

        # delete worker
        self.client.delete_worker(worker['id'])

        # make sure worker no longer exists
        self.assertRaises(client_exc.NotFound, self.client.get_worker,
                          worker['id'])

    def test_schedule_workflow(self):
        response = self.client.list_schedules()
        schedules = response[0]
        import pprint
        pp = pprint.PrettyPrinter()
        pp.pprint(schedules)
        self.assertEqual(len(schedules), 0)

        # create invalid schedule
        request = {'not a schedule': 'yes'}

        self.assertRaises(client_exc.BadRequest, self.client.create_schedule,
                          request)

        # create malformed schedule
        request = 'not a schedule'

        self.assertRaises(client_exc.BadRequest, self.client.create_schedule,
                          request)

        # create schedule with no body
        self.assertRaises(client_exc.BadRequest, self.client.create_schedule,
                          None)
        # create schedule
        request = {
            'schedule':
            {
                'tenant_id': TENANT1,
                'action': 'snapshot',
                'minute': 30,
                'hour': 12,
                'schedule_metadata': [{'key': 'instance_id',
                                       'value': 'my_instance_1'}]
            }
        }
        schedule = self.client.create_schedule(request)
        self.assertTrue(schedule['id'])
        self.assertEqual(schedule['tenant_id'], TENANT1)
        self.assertEqual(schedule['action'], 'snapshot')
        self.assertEqual(schedule['minute'], 30)
        self.assertEqual(schedule['hour'], 12)

        # get schedule
        schedule = self.client.get_schedule(schedule['id'])
        self.assertTrue(schedule['id'])
        self.assertEqual(schedule['tenant_id'], TENANT1)
        self.assertEqual(schedule['action'], 'snapshot')
        self.assertEqual(schedule['minute'], 30)
        self.assertEqual(schedule['hour'], 12)

        #list schedules
        response = self.client.list_schedules()
        schedules = response[0]  
        self.assertEqual(len(schedules), 1)
        self.assertDictEqual(schedules[0], schedule)

        #list schedules, next_run filter
        filter = {}
        filter['next_run_after'] = schedule['next_run']
        filter['next_run_before'] = schedule['next_run']
        response = self.client.list_schedules(filter_args=filter)
        schedules = response[0]
        self.assertEqual(len(schedules), 1)
        self.assertDictEqual(schedules[0], schedule)
        filter['next_run_after'] = '2010-11-30T15:23:00Z'
        filter['next_run_before'] = '2011-11-30T15:23:00Z'
        response = self.client.list_schedules(filter_args=filter)
        schedules = response[0]
        self.assertEqual(len(schedules), 0)

        #list schedules, next_run_before filter
        filter = {}
        filter['next_run_before'] = schedule['next_run']
        response = self.client.list_schedules(filter_args=filter)
        schedules = response[0]
        self.assertEqual(len(schedules), 0)

        #list schedules, next_run_after filter
        filter = {}
        filter['next_run_after'] = schedule['next_run']
        response = self.client.list_schedules(filter_args=filter)
        schedules = response[0]
        self.assertEqual(len(schedules), 1)
        self.assertDictEqual(schedules[0], schedule)

        #list schedules, tenant_id filter
        filter = {}
        filter['tenant_id'] = TENANT1
        response = self.client.list_schedules(filter_args=filter)
        schedules = response[0]
        self.assertEqual(len(schedules), 1)
        self.assertDictEqual(schedules[0], schedule)
        filter['tenant_id'] = 'aaaa-bbbb-cccc-dddd'
        response = self.client.list_schedules(filter_args=filter)
        schedules = response[0]
        self.assertEqual(len(schedules), 0)

        #list schedules, instance_id filter
        filter = {}
        filter['instance_id'] = 'my_instance_1'
        response = self.client.list_schedules(filter_args=filter)
        schedules = response[0]
        self.assertEqual(len(schedules), 1)
        self.assertDictEqual(schedules[0], schedule)
        filter['instance_id'] = 'aaaa-bbbb-cccc-dddd'
        response = self.client.list_schedules(filter_args=filter)
        schedules = response[0]
        self.assertEqual(len(schedules), 0)

        #update schedule
        request = {'schedule': {'hour': 14}}
        updated_schedule = self.client.update_schedule(schedule['id'], request)
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
        schedule = self.client.create_schedule(request)

        # create meta
        meta = self.client.create_schedule_meta(schedule['id'], 'key1',
                                                'value1')
        self.assertEqual(meta['key'], 'key1')
        self.assertEqual(meta['value'], 'value1')

        # make sure duplicate metadata can't be created
        self.assertRaises(client_exc.Duplicate,
                          self.client.create_schedule_meta,
                          schedule['id'],
                          'key1',
                          'value1')

        # list meta
        metadata = self.client.list_schedule_meta(schedule['id'])
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
        schedule = self.client.create_schedule(request)

        # create job

        new_job = self.client.create_job(schedule['id'])
        print "Job: %s" % str(new_job)
        self.assertIsNotNone(new_job.get('id'))
        self.assertEqual(new_job['schedule_id'], schedule['id'])
        self.assertEqual(new_job['tenant_id'], schedule['tenant_id'])
        self.assertEqual(new_job['action'], schedule['action'])
        self.assertEqual(new_job['status'], 'queued')
        self.assertIsNotNone(new_job.get('timeout'))
        self.assertIsNotNone(new_job.get('hard_timeout'))
        self.assertMetadataInList(new_job['job_metadata'], meta1)
        self.assertMetadataInList(new_job['job_metadata'], meta2)

        # list jobs
        jobs = self.client.list_jobs()
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]['id'], new_job['id'])
        self.assertEqual(jobs[0]['schedule_id'], new_job['schedule_id'])
        self.assertEqual(jobs[0]['status'], new_job['status'])
        self.assertEqual(jobs[0]['retry_count'], new_job['retry_count'])

        # get job
        job = self.client.get_job(new_job['id'])
        self.assertEqual(job['id'], new_job['id'])
        self.assertEqual(job['schedule_id'], new_job['schedule_id'])
        self.assertEqual(job['status'], new_job['status'])
        self.assertEqual(job['retry_count'], new_job['retry_count'])

        # list job metadata
        metadata = self.client.list_job_metadata(new_job['id'])
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

        # update status without timeout
        self.client.update_job_status(job['id'], 'processing')
        status = self.client.get_job_status(job['id'])['status']
        self.assertNotEqual(status, new_job['status'])
        self.assertEqual(status, 'processing')

        # update status with timeout
        timeout = '2010-11-30T17:00:00Z'
        self.client.update_job_status(job['id'], 'done', timeout)
        updated_job = self.client.get_job(new_job['id'])
        self.assertNotEqual(updated_job['status'], new_job['status'])
        self.assertEqual(updated_job['status'], 'done')
        self.assertNotEqual(updated_job['timeout'], new_job['timeout'])
        self.assertEqual(updated_job['timeout'], timeout)

        # delete job
        self.client.delete_job(job['id'])

        # make sure job no longer exists
        self.assertRaises(client_exc.NotFound, self.client.get_job,
                          job['id'])

    def test_pagination(self):

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
        schedule_1 = self.client.create_schedule(request)
        schedule_2 = self.client.create_schedule(request)
        schedule_3 = self.client.create_schedule(request)
        schedule_4 = self.client.create_schedule(request)
        schedules = [schedule_1, schedule_2, schedule_3, schedule_4]
        schedules = sorted(schedules, key=itemgetter('id'))

        # create worker
        worker_1 = self.client.create_worker('hostname')
        worker_2 = self.client.create_worker('hostname')
        worker_3 = self.client.create_worker('hostname')
        worker_4 = self.client.create_worker('hostname')
        workers = [worker_1, worker_2, worker_3, worker_4]
        workers = sorted(workers, key=itemgetter('id'))

        # create job
        job_1 = self.client.create_job(schedule_1['id'])
        job_2 = self.client.create_job(schedule_2['id'])
        job_3 = self.client.create_job(schedule_3['id'])
        job_4 = self.client.create_job(schedule_4['id'])
        jobs = [job_1, job_2, job_3, job_4]
        jobs = sorted(jobs, key=itemgetter('id'))

        #list schedules
        response = self.client.list_schedules()
        actual_schedules = response[0]
        self.assertEqual(len(actual_schedules), 4)
        actual_ids = set(a['id'] for a in actual_schedules)
        schedule_ids = set(s['id'] for s in schedules)
        self.assertEqual(actual_ids, schedule_ids)

        #list schedules with limit
        filter_args = {'limit': '2'}
        response = self.client.list_schedules(filter_args=filter_args)
        actual_schedules = response[0]
        self.assertEqual(len(actual_schedules), 2)
        actual_ids = set(a['id'] for a in actual_schedules)
        schedule_ids = set(s['id'] for s in schedules[0:2])
        self.assertEqual(actual_ids, schedule_ids)

        #list schedules with marker
        filter_args = {'marker': schedules[0]['id']}
        response = self.client.list_schedules(filter_args=filter_args)
        actual_schedules = response[0]
        self.assertEqual(len(actual_schedules), 3)
        actual_ids = set(a['id'] for a in actual_schedules)
        schedule_ids = set(s['id'] for s in schedules[1:4])
        self.assertEqual(actual_ids, schedule_ids)

        #list schedules with limit and marker
        filter_args = {'limit': '2', 'marker': schedules[0]['id']}
        response = self.client.list_schedules(filter_args=filter_args)
        actual_schedules = response[0]
        self.assertEqual(len(actual_schedules), 2)
        actual_ids = set(a['id'] for a in actual_schedules)
        schedule_ids = set(s['id'] for s in schedules[1:3])
        self.assertEqual(actual_ids, schedule_ids)

        # list workers
        response = self.client.list_workers()
        self.assertEqual(len(response), 4)
        response_ids = set(r['id'] for r in response)
        worker_ids = set(w['id'] for w in workers)
        self.assertEqual(response_ids, worker_ids)

        # list workers with limit
        params = {'limit': '2'}
        response = self.client.list_workers(params=params)
        self.assertEqual(len(response), 2)
        response_ids = set(r['id'] for r in response)
        worker_ids = set(w['id'] for w in workers[0:2])
        self.assertEqual(response_ids, worker_ids)

        # list workers with marker
        params = {'marker': workers[0]['id']}
        response = self.client.list_workers(params=params)
        self.assertEqual(len(response), 3)
        response_ids = set(r['id'] for r in response)
        worker_ids = set(w['id'] for w in workers[1:4])
        self.assertEqual(response_ids, worker_ids)

        # list workers with limit and marker
        params = {'marker': workers[0]['id'], 'limit': '2'}
        response = self.client.list_workers(params=params)
        self.assertEqual(len(response), 2)
        response_ids = set(r['id'] for r in response)
        worker_ids = set(w['id'] for w in workers[1:3])
        self.assertEqual(response_ids, worker_ids)

        # list jobs
        response = self.client.list_jobs()
        self.assertEqual(len(response), 4)
        response_ids = set(r['id'] for r in response)
        job_ids = set(j['id'] for j in jobs)
        self.assertEqual(response_ids, job_ids)

        # list jobs with limit
        params = {'limit': '2'}
        response = self.client.list_jobs(params=params)
        self.assertEqual(len(response), 2)
        response_ids = set(r['id'] for r in response)
        job_ids = set(j['id'] for j in jobs[0:2])
        self.assertEqual(job_ids, response_ids)

        # list jobs with marker
        params = {'marker': jobs[0]['id']}
        response = self.client.list_jobs(params=params)
        self.assertEqual(len(response), 3)
        response_ids = set(r['id'] for r in response)
        job_ids = set(j['id'] for j in jobs[1:4])
        self.assertEqual(response_ids, job_ids)

        # list jobs with limit and marker
        params = {'limit': '2', 'marker': jobs[0]['id']}
        response = self.client.list_jobs(params=params)
        self.assertEqual(len(response), 2)
        response_ids = set(r['id'] for r in response)
        job_ids = set(j['id'] for j in jobs[1:3])
        self.assertEqual(job_ids, response_ids)
