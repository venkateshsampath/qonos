import uuid

from qonos.common import exception
from qonos.openstack.common import timeutils
from qonos.openstack.common import uuidutils
from qonos.tests import utils as utils
from qonos.tests.unit import utils as unit_utils


#NOTE(ameade): This is set in each individual db test module
db_api = None


class TestDBApi(utils.BaseTestCase):

    def setUp(self):
        super(TestDBApi, self).setUp()
        self.db_api = db_api

    def tearDown(self):
        super(TestDBApi, self).setUp()
        self.db_api.reset()

    def test_reset(self):
        fixture = {
            'tenant_id': str(uuid.uuid4()),
            'action': 'snapshot',
            'minute': '30',
            'hour': '2',
        }
        self.db_api.schedule_create(fixture)
        self.db_api.reset()
        self.assertFalse(self.db_api.schedule_get_all())


class TestSchedulesDBApi(utils.BaseTestCase):

    def setUp(self):
        super(TestSchedulesDBApi, self).setUp()
        self.db_api = db_api
        self._create_schedules()

    def tearDown(self):
        super(TestSchedulesDBApi, self).setUp()
        self.db_api.reset()

    def _create_schedules(self):
        fixture = {
            'tenant_id': str(uuid.uuid4()),
            'action': 'snapshot',
            'minute': '30',
            'hour': '2',
        }
        self.schedule_1 = self.db_api.schedule_create(fixture)
        fixture = {
            'tenant_id': str(uuid.uuid4()),
            'action': 'snapshot',
            'minute': 30,
            'hour': 2,
        }
        self.schedule_2 = self.db_api.schedule_create(fixture)

    def test_schedule_get_all(self):
        schedules = self.db_api.schedule_get_all()
        self.assertEqual(len(schedules), 2)

    def test_schedule_get_by_id(self):
        fixture = {
            'tenant_id': str(uuid.uuid4()),
            'action': 'snapshot',
            'minute': 30,
            'hour': 2,
            'schedule_metadata': [
                {
                    'key': 'instance_id',
                    'value': 'my_instance',
                },
            ],
        }
        expected = self.db_api.schedule_create(fixture)
        actual = self.db_api.schedule_get_by_id(expected['id'])
        self.assertEqual(actual['tenant_id'], fixture['tenant_id'])
        self.assertEqual(actual['action'], fixture['action'])
        self.assertEqual(actual['minute'], fixture['minute'])
        self.assertEqual(actual['hour'], fixture['hour'])
        metadata = actual['schedule_metadata']
        self.assertEqual(len(metadata), 1)
        self.assertEqual(metadata[0]['key'],
                         fixture['schedule_metadata'][0]['key'])
        self.assertEqual(metadata[0]['value'],
                         fixture['schedule_metadata'][0]['value'])
        self.assertNotEqual(actual['created_at'], None)
        self.assertNotEqual(actual['updated_at'], None)

    def test_schedule_get_by_id_not_found(self):
        schedule_id = str(uuid.uuid4())
        self.assertRaises(exception.NotFound,
                          self.db_api.schedule_get_by_id, schedule_id)

    def test_schedule_create(self):
        fixture = {
            'tenant_id': str(uuid.uuid4()),
            'action': 'snapshot',
            'minute': 30,
            'hour': 2,
        }
        schedule = self.db_api.schedule_create(fixture)
        self.assertTrue(uuidutils.is_uuid_like(schedule['id']))
        self.assertEqual(schedule['tenant_id'], fixture['tenant_id'])
        self.assertEqual(schedule['action'], fixture['action'])
        self.assertEqual(schedule['minute'], fixture['minute'])
        self.assertEqual(schedule['hour'], fixture['hour'])
        self.assertNotEqual(schedule['created_at'], None)
        self.assertNotEqual(schedule['updated_at'], None)

    def test_schedule_update(self):
        fixture = {
            'id': str(uuid.uuid4()),
            'tenant_id': str(uuid.uuid4()),
            'action': 'snapshot',
            'minute': 30,
            'hour': 2,
        }
        schedule = self.db_api.schedule_create(fixture)
        fixture = {'hour': 3}
        timeutils.set_time_override()
        timeutils.advance_time_seconds(2)
        updated_schedule = self.db_api.schedule_update(schedule['id'], fixture)
        timeutils.clear_time_override()

        self.assertTrue(uuidutils.is_uuid_like(schedule['id']))
        self.assertEqual(updated_schedule['tenant_id'], schedule['tenant_id'])
        self.assertEqual(updated_schedule['action'], schedule['action'])
        self.assertEqual(updated_schedule['minute'], schedule['minute'])
        self.assertEqual(updated_schedule['hour'], fixture['hour'])
        self.assertEqual(updated_schedule['created_at'],
                         schedule['created_at'])
        self.assertNotEqual(updated_schedule['updated_at'],
                            schedule['updated_at'])

    def test_schedule_update_metadata(self):
        fixture = {
            'id': str(uuid.uuid4()),
            'tenant_id': str(uuid.uuid4()),
            'action': 'snapshot',
            'minute': 30,
            'hour': 2,
        }
        schedule = self.db_api.schedule_create(fixture)
        fixture = {
            'schedule_metadata': [
                {
                    'key': 'instance_id',
                    'value': 'my_instance',
                },
            ],
        }

        timeutils.set_time_override()
        timeutils.advance_time_seconds(2)
        updated_schedule = self.db_api.schedule_update(schedule['id'], fixture)
        timeutils.clear_time_override()

        self.assertTrue(uuidutils.is_uuid_like(schedule['id']))
        self.assertEqual(updated_schedule['tenant_id'], schedule['tenant_id'])
        self.assertEqual(updated_schedule['action'], schedule['action'])
        self.assertEqual(updated_schedule['minute'], schedule['minute'])
        self.assertEqual(updated_schedule['hour'], schedule['hour'])
        metadata = updated_schedule['schedule_metadata']
        self.assertEqual(len(metadata), 1)
        self.assertEqual(metadata[0]['key'],
                         fixture['schedule_metadata'][0]['key'])
        self.assertEqual(metadata[0]['value'],
                         fixture['schedule_metadata'][0]['value'])
        self.assertEqual(updated_schedule['created_at'],
                         schedule['created_at'])
        # updated child metadata collection doesn't update the parent schedule
        self.assertEqual(updated_schedule['updated_at'],
                         schedule['updated_at'])

    def test_schedule_delete(self):
        schedules = self.db_api.schedule_get_all()
        self.assertEqual(len(schedules), 2)
        self.db_api.schedule_delete(self.schedule_1['id'])
        schedules = self.db_api.schedule_get_all()
        self.assertEqual(len(schedules), 1)

    def test_schedule_delete_not_found(self):
        schedule_id = str(uuid.uuid4())
        self.assertRaises(exception.NotFound, self.db_api.schedule_delete,
                          schedule_id)

    def test_metadata_create(self):
        schedule = db_api.schedule_create({})
        fixture = {'key': 'key1', 'value': 'value1'}
        meta = db_api.schedule_meta_create(schedule['id'], fixture)
        self.assertEqual(meta['schedule_id'], schedule['id'])
        self.assertEqual(meta['key'], fixture['key'])
        self.assertEqual(meta['value'], fixture['value'])
        self.assertIsNotNone(meta['created_at'])
        self.assertIsNotNone(meta['updated_at'])
        self.assertIsNotNone(meta['id'])

    def test_metadata_get(self):
        schedule = db_api.schedule_create({})
        fixture = {'key': 'key1', 'value': 'value1'}
        db_api.schedule_meta_create(schedule['id'], fixture)
        meta = db_api.schedule_meta_get(schedule['id'], fixture['key'])
        self.assertIsNotNone(meta['created_at'])
        self.assertIsNotNone(meta['updated_at'])
        self.assertIsNotNone(meta['id'])
        self.assertEquals(meta['key'], fixture['key'])
        self.assertEquals(meta['value'], fixture['value'])

    def assertMetadataInList(self, metadata, meta):
        found = False
        for element in metadata:
            if element['key'] == meta['key']:
                found = True
                self.assertEqual(element['value'], meta['value'])
        self.assertTrue(found)

    def test_metadata_get_all(self):
        schedule = db_api.schedule_create({})
        fixture1 = {'key': 'key1', 'value': 'value1'}
        db_api.schedule_meta_create(schedule['id'], fixture1)
        fixture2 = {'key': 'key2', 'value': 'value2'}
        db_api.schedule_meta_create(schedule['id'], fixture2)
        metadata = db_api.schedule_meta_get_all(schedule['id'])
        self.assertEqual(len(metadata), 2)
        for element in metadata:
            self.assertIsNotNone(element['id'])
            self.assertEqual(element['schedule_id'], schedule['id'])
            self.assertIsNotNone(element['created_at'])
            self.assertIsNotNone(element['updated_at'])
            self.assertEqual(element['schedule_id'], schedule['id'])
        self.assertMetadataInList(metadata, fixture1)
        self.assertMetadataInList(metadata, fixture2)

    def test_metadata_delete(self):
        schedule = db_api.schedule_create({})
        fixture = {'key': 'key1', 'value': 'value1'}
        meta = db_api.schedule_meta_create(schedule['id'], fixture)
        db_api.schedule_meta_delete(schedule['id'], fixture['key'])
        metadata = db_api.schedule_meta_get_all(schedule['id'])
        self.assertEqual(0, len(metadata))
        self.assertFalse(meta in metadata)

    def test_metadata_delete_not_found(self):
        schedule = db_api.schedule_create({})
        fixture = {'key': 'key1', 'value': 'value1'}
        db_api.schedule_meta_create(schedule['id'], fixture)
        db_api.schedule_meta_delete(schedule['id'], fixture['key'])
        self.assertRaises(exception.NotFound, db_api.schedule_meta_get,
                          schedule['id'], fixture['key'])

    def test_metadata_update(self):
        schedule = db_api.schedule_create({})
        fixture = {'key': 'key1', 'value': 'value1'}
        meta = db_api.schedule_meta_create(schedule['id'], fixture)
        update_fixture = {'key': 'key1', 'value': 'value2'}
        updated_meta = db_api.schedule_meta_update(schedule['id'],
                                                   fixture['key'],
                                                   update_fixture)
        self.assertEquals(meta['key'], updated_meta['key'])
        self.assertNotEquals(meta['value'], updated_meta['value'])

    def test_metadata_update_schedule_not_found(self):
        schedule_id = str(uuid.uuid4())
        self.assertRaises(exception.NotFound, db_api.schedule_meta_update,
                          schedule_id, 'key2', {})

    def test_metadata_update_key_not_found(self):
        schedule = db_api.schedule_create({})
        fixture = {'key': 'key1', 'value': 'value1'}
        db_api.schedule_meta_create(schedule['id'], fixture)
        self.assertRaises(exception.NotFound, db_api.schedule_meta_update,
                          schedule['id'], 'key2', {})

    def test_metadata_get_all_not_found_when_schedule_doesnt_exists(self):
        schedule_id = str(uuid.uuid4())
        self.assertRaises(exception.NotFound, db_api.schedule_meta_get_all,
                          schedule_id)

    def test_metadata_get_schedule_not_found(self):
        schedule_id = str(uuid.uuid4())
        self.assertRaises(exception.NotFound, db_api.schedule_meta_get,
                          schedule_id, 'key')

    def test_metadata_get_key_not_found(self):
        schedule = db_api.schedule_create({})
        fixture = {'key': 'key1', 'value': 'value1'}
        db_api.schedule_meta_create(schedule['id'], fixture)
        self.assertRaises(exception.NotFound, db_api.schedule_meta_get,
                          schedule['id'], 'key2')


class TestWorkersDBApi(utils.BaseTestCase):

    def setUp(self):
        super(TestWorkersDBApi, self).setUp()
        self.db_api = db_api
        self._create_workers()

    def tearDown(self):
        super(TestWorkersDBApi, self).tearDown()
        self.db_api.reset()

    def _create_workers(self):
        fixture = {'host': ''}
        self.worker_1 = self.db_api.worker_create(fixture)
        self.worker_2 = self.db_api.worker_create(fixture)

    def test_worker_get_all(self):
        workers = self.db_api.worker_get_all()
        self.assertEqual(len(workers), 2)

    def test_worker_get_by_id(self):
        actual = self.db_api.worker_get_by_id(self.worker_1['id'])
        self.assertEquals(actual['id'], self.worker_1['id'])
        self.assertEquals(actual['created_at'], self.worker_1['created_at'])
        self.assertEquals(actual['updated_at'], self.worker_1['updated_at'])
        self.assertEquals(actual['host'], self.worker_1['host'])

    def test_worker_get_by_id_not_found(self):
        worker_id = str(uuid.uuid4())
        self.assertRaises(exception.NotFound,
                          self.db_api.worker_get_by_id, worker_id)

    def test_worker_create(self):
        fixture = {'host': 'i.am.cowman'}
        worker = self.db_api.worker_create(fixture)
        self.assertTrue(uuidutils.is_uuid_like(worker['id']))
        self.assertEqual(worker['host'], fixture['host'])
        self.assertNotEqual(worker['created_at'], None)
        self.assertNotEqual(worker['updated_at'], None)

    def test_worker_delete(self):
        workers = self.db_api.worker_get_all()
        self.assertEqual(len(workers), 2)
        self.db_api.worker_delete(self.worker_1['id'])
        workers = self.db_api.worker_get_all()
        self.assertEqual(len(workers), 1)

    def test_worker_delete_not_found(self):
        worker_id = str(uuid.uuid4())
        self.assertRaises(exception.NotFound,
                          self.db_api.worker_delete, worker_id)


class TestJobsDBApi(utils.BaseTestCase):

    def setUp(self):
        super(TestJobsDBApi, self).setUp()
        self.db_api = db_api
        self._create_jobs()

    def tearDown(self):
        super(TestJobsDBApi, self).tearDown()
        self.db_api.reset()

    def _create_jobs(self):
        fixture = {
            'action': 'snapshot',
            'tenant_id': unit_utils.TENANT1,
            'schedule_id': unit_utils.SCHEDULE_UUID1,
            'worker_id': unit_utils.WORKER_UUID1,
            'status': 'queued',
            'retry_count': 0,
        }
        self.job_1 = self.db_api.job_create(fixture)
        fixture = {
            'action': 'snapshot',
            'tenant_id': unit_utils.TENANT1,
            'schedule_id': unit_utils.SCHEDULE_UUID2,
            'worker_id': unit_utils.WORKER_UUID2,
            'status': 'error',
            'retry_count': 0,
        }
        self.job_2 = self.db_api.job_create(fixture)

    def test_job_create(self):
        fixture = {
            'action': 'snapshot',
            'tenant_id': unit_utils.TENANT1,
            'schedule_id': unit_utils.SCHEDULE_UUID2,
            'worker_id': unit_utils.WORKER_UUID2,
            'status': 'queued',
        }
        job = self.db_api.job_create(fixture)
        self.assertTrue(uuidutils.is_uuid_like(job['id']))
        self.assertNotEqual(job['created_at'], None)
        self.assertNotEqual(job['updated_at'], None)
        self.assertEqual(job['schedule_id'], fixture['schedule_id'])
        self.assertEqual(job['worker_id'], fixture['worker_id'])
        self.assertEqual(job['status'], fixture['status'])
        self.assertEqual(job['retry_count'], 0)

    def test_job_get_all(self):
        workers = self.db_api.job_get_all()
        self.assertEqual(len(workers), 2)

    def test_job_get_by_id(self):
        expected = self.job_1
        actual = self.db_api.job_get_by_id(self.job_1['id'])
        self.assertEqual(actual['schedule_id'], expected['schedule_id'])
        self.assertEqual(actual['worker_id'], expected['worker_id'])
        self.assertEqual(actual['status'], expected['status'])
        self.assertEqual(actual['retry_count'], expected['retry_count'])
        self.assertEqual(actual['action'], expected['action'])
        self.assertEqual(actual['tenant_id'], expected['tenant_id'])

    def test_job_get_by_id_not_found(self):
        self.assertRaises(exception.NotFound,
                          self.db_api.job_get_by_id, str(uuid.uuid4))

    def test_job_updated_at_get_by_id(self):
        expected = self.job_1['updated_at']
        actual = self.db_api.job_updated_at_get_by_id(self.job_1['id'])
        self.assertEqual(actual, expected)

    def test_job_updated_at_get_by_id_job_not_found(self):
        self.assertRaises(exception.NotFound,
                          self.db_api.job_updated_at_get_by_id,
                          str(uuid.uuid4))

    def test_job_status_get_by_id(self):
        expected = self.job_1['status']
        actual = self.db_api.job_status_get_by_id(self.job_1['id'])
        self.assertEqual(actual, expected)

    def test_job_status_get_by_id_job_not_found(self):
        self.assertRaises(exception.NotFound,
                          self.db_api.job_status_get_by_id, str(uuid.uuid4))

    def test_job_update(self):
        fixture = {
            'status': 'error',
            'retry_count': 2,
        }
        old = self.db_api.job_get_by_id(self.job_1['id'])
        self.db_api.job_update(self.job_1['id'], fixture)
        updated = self.db_api.job_get_by_id(self.job_1['id'])

        self.assertEqual(old['schedule_id'], updated['schedule_id'])
        self.assertEqual(old['worker_id'], updated['worker_id'])
        self.assertNotEqual(old['status'], updated['status'])
        self.assertNotEqual(old['retry_count'], updated['retry_count'])

        self.assertEqual(updated['status'], 'error')
        self.assertEqual(updated['retry_count'], 2)

    def test_job_delete(self):
        self.assertEqual(len(self.db_api.job_get_all()), 2)
        self.db_api.job_delete(self.job_1['id'])
        self.assertEqual(len(self.db_api.job_get_all()), 1)

    def test_job_delete_not_found(self):
        self.assertRaises(exception.NotFound,
                          self.db_api.job_delete, str(uuid.uuid4))

    def test_metadata_create(self):
        job = db_api.job_create({})
        fixture = {'key': 'key1', 'value': 'value1'}
        meta = db_api.job_meta_create(job['id'], fixture)
        self.assertEqual(meta['job_id'], job['id'])
        self.assertEqual(meta['key'], fixture['key'])
        self.assertEqual(meta['value'], fixture['value'])
        self.assertIsNotNone(meta['created_at'])
        self.assertIsNotNone(meta['updated_at'])
        self.assertIsNotNone(meta['id'])

    def test_metadata_create(self):
        job = db_api.job_create({})
        fixture = {'key': 'key1', 'value': 'value1'}
        meta = db_api.job_meta_create(job['id'], fixture)
        fixture = {'key': 'key1', 'value': 'value1'}

        self.assertRaises(exception.Duplicate, db_api.job_meta_create,
                          job['id'], fixture)

    def test_metadata_get_all_by_job_id(self):
        job = db_api.job_create({})
        fixture = {'key': 'key1', 'value': 'value1'}
        meta = db_api.job_meta_create(job['id'], fixture)
        fixture = {'key': 'key2', 'value': 'value2'}
        meta = db_api.job_meta_create(job['id'], fixture)
        meta_list = db_api.job_meta_get_all_by_job_id(job['id'])
        self.assertEqual(len(meta_list), 2)

    def test_metadata_get_all_by_job_id_no_meta(self):
        job = db_api.job_create({})
        meta_list = db_api.job_meta_get_all_by_job_id(job['id'])
        self.assertEqual(len(meta_list), 0)

    def test_metadata_get(self):
        job = db_api.job_create({})
        fixture = {'key': 'key1', 'value': 'value1'}
        meta = db_api.job_meta_create(job['id'], fixture)
        meta = db_api.job_meta_get(job['id'], 'key1')
        self.assertIsNotNone(meta['created_at'])
        self.assertIsNotNone(meta['updated_at'])
        self.assertIsNotNone(meta['id'])
        self.assertEquals(meta['key'], fixture['key'])
        self.assertEquals(meta['value'], fixture['value'])

    def test_metadata_get_not_found(self):
        job = db_api.job_create({})
        self.assertRaises(exception.NotFound, db_api.job_meta_get,
                          job['id'], 'key1')

    def test_metadata_delete(self):
        job = db_api.job_create({})
        fixture = {'key': 'key1', 'value': 'value1'}
        meta = db_api.job_meta_create(job['id'], fixture)
        db_api.job_meta_delete(job['id'], fixture['key'])
        metadata = db_api.job_meta_get_all_by_job_id(job['id'])
        self.assertEqual(0, len(metadata))
        self.assertFalse(meta in metadata)

    def test_metadata_delete_not_found(self):
        job = db_api.job_create({})
        fixture = {'key': 'key1', 'value': 'value1'}
        db_api.job_meta_create(job['id'], fixture)
        db_api.job_meta_delete(job['id'], fixture['key'])
        self.assertRaises(exception.NotFound, db_api.job_meta_get,
                          job['id'], fixture['key'])

    def test_metadata_update(self):
        job = db_api.job_create({})
        fixture = {'key': 'key1', 'value': 'value1'}
        meta = db_api.job_meta_create(job['id'], fixture)
        update_fixture = {'key': 'key1', 'value': 'value2'}
        updated_meta = db_api.job_meta_update(job['id'],
                                              fixture['key'],
                                              update_fixture)
        self.assertEquals(meta['key'], updated_meta['key'])
        self.assertNotEquals(meta['value'], updated_meta['value'])

    def test_metadata_update_job_not_found(self):
        job_id = str(uuid.uuid4())
        self.assertRaises(exception.NotFound, db_api.job_meta_update,
                          job_id, 'key2', {})

    def test_metadata_update_key_not_found(self):
        job = db_api.job_create({})
        fixture = {'key': 'key1', 'value': 'value1'}
        db_api.job_meta_create(job['id'], fixture)
        self.assertRaises(exception.NotFound, db_api.job_meta_update,
                          job['id'], 'key2', {})

    def test_metadata_get_job_not_found(self):
        job_id = str(uuid.uuid4())
        self.assertRaises(exception.NotFound, db_api.job_meta_get,
                          job_id, 'key')

    def test_metadata_get_key_not_found(self):
        job = db_api.job_create({})
        fixture = {'key': 'key1', 'value': 'value1'}
        db_api.job_meta_create(job['id'], fixture)
        self.assertRaises(exception.NotFound, db_api.job_meta_get,
                          job['id'], 'key2')
