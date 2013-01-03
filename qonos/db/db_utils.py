from qonos.common import exception


def validate_schedule_values(values):
    keys = ['action', 'tenant_id']
    _validate_values('Job', values, keys)


def validate_job_values(values):
    keys = ['action', 'tenant_id']
    _validate_values('Job', values, keys)


def _validate_values(object_name, values, keys):
    missing_values = []
    for key in keys:
        _validate_value(values, key, missing_values)

    if missing_values:
        raise exception.MissingValue(
            '[%s] Values for %s must be provided' %
            (object_name, missing_values))


def _validate_value(values, key, missing_values):
    if not key in values:
        missing_values.append(key)
