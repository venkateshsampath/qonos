DATA = {
    'schedules': {},
    'schedule_metadata': {},
    'jobs': {},
    'workers': {},
    'job_faults': {},
}

def reset():
    global DATA
    for k in DATA:
        DATA[k] = {}
