import pytest
import requests

job_id = None

@pytest.fixture
def base_url():
    return 'http://localhost:5000'

@pytest.fixture
def job_id(base_url):
    # Create a job and return its ID
    response = requests.post(f'{base_url}/jobs', json={"kiosk1":"4055", "kiosk2":"2498", "start_date":"01/31/2023", "end_date":"01/31/2024", "plot_type":"trip_duration"})
    assert response.status_code == 200
    return response.json()["id"]

def test_get_trip_data(base_url):
    response = requests.get(f'{base_url}/trips')
    assert response.status_code == 200
    assert isinstance(response.json(), list)

def test_get_kiosk_keys(base_url):
    response = requests.get(f'{base_url}/kiosk_ids')
    assert response.status_code == 200
    assert isinstance(response.json(), list)

def test_get_job(base_url, job_id):
    response = requests.get(f'{base_url}/jobs/{job_id}')
    assert response.status_code == 200

def test_get_results(base_url, job_id):
    response = requests.get(f'{base_url}/results/{job_id}')
    assert response.status_code == 200

def test_nearest(base_url):
    response = requests.get(f'{base_url}/nearest', params={"n":"5","lat":"30.2862730619728","long":"-97.73937727490916"})
    assert response.status_code == 200