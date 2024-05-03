import pytest
import worker as w
import os
import redis

REDIS_IP = os.environ.get("REDIS_IP")
res = redis.Redis(host=REDIS_IP, port=6379, db=4)

@pytest.fixture
def setup_job():
    jid = "test_job_id"
    job_dict = {
        'id': jid,
        'status': 'submitted',
        'job parameters': {
            'plot_type': 'trip_duration',
            'start_date': '01/31/2023',
            'end_date': '01/31/2024',
            'kiosk1': '4055',
            'kiosk2': '2498'
        }
    }
    return jid, job_dict

def test_process_job(setup_job):
    jid, job_dict = setup_job
    w.process_job(jid)
    result = res.get(jid)
    assert result is not None
    assert isinstance(result, bytes)

def test_trip_duration_histogram_job():
    job_params = {
        'plot_type': 'trip_duration',
        'start_date': '01/31/2023',
        'end_date': '01/31/2024',
        'kiosk1': '4055',
        'kiosk2': '2498'
    }
    result = w.trip_duration_histogram_job(job_params)
    assert result is not None
    assert isinstance(result, bytes)

def test_trips_per_day_job():
    job_params = {
        'plot_type': 'trips_per_day',
        'start_date': '01/31/2023',
        'end_date': '01/31/2024',
        'lat': '30.2862730619728',
        'long': '-97.73937727490916',
        'radius': '3'
    }
    result = w.trips_per_day_job(job_params)
    assert result is not None
    assert isinstance(result, bytes)
