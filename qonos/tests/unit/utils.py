from qonos.openstack.common import wsgi

SERVER_UUID1 = 'ba3f1c5d-89a0-34c1-b44d-ef78d46b90a3'

TENANT1 = '6838eb7b-6ded-434a-882c-b344c77fe8df'
TENANT2 = '2c014f32-55eb-467d-8fcb-4bd706012f81'
TENANT3 = '2c014f32-55eb-467d-8fcb-4bd706012f81'
TENANT4 = '2c014f32-55eb-467d-8fcb-4bd706012f81'

SCHEDULE_UUID1 = 'c80a1a6c-bd1f-41c5-90ee-81afedb1d58d'
SCHEDULE_UUID2 = '971ec09a-8067-4bc8-a91f-ae3557f1c4c7'

JOB_UUID1 = '54492ba0-f4df-4e4e-be62-27f4d76b29cf'
JOB_UUID2 = '0b3b3006-cb76-4517-ae32-51397e22c754'

WORKER_UUID1 = '2hss8dkl-d8jh-88yd-uhs9-879sdjsd8skd'
WORKER_UUID2 = '92ba0dkl-8jhd-4517-hu9s-1397e22c75kd'


def get_fake_request(path='', method='GET'):
    req = wsgi.Request.blank(path)
    req.method = method

    return req


def get_schedule(id=None, tenant_id=TENANT1, job_type=None, schedule=None,
                 metadata=None):
    schedule = {'schedule': {
        'id': id,
        'tenant_id': tenant_id,
        'job_type': job_type,
        'schedule': schedule,
        'metadata': metadata
    }}
    return schedule


def get_job(id=None, schedule_id=SCHEDULE_UUID1, status=None, heartbeat=None):
    job = {'job': {
        'id': id,
        'schedule_id': schedule_id,
        'status': status,
        'heartbeat': heartbeat
    }}
    return job
