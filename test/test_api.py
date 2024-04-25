import pytest
import requests

job_id = None

@pytest.fixture
def base_url():
    return 'http://localhost:5000'

@pytest.fixture
def job_id(base_url):
    # Create a job and return its ID
    response = requests.post(f'{base_url}/jobs', json={"gene1": "HGNC:24523", "gene2": "HGNC:29027"})
    assert response.status_code == 201
    return response.json()["id"]

def test_get_data(base_url):
    response = requests.get(f'{base_url}/data')
    assert response.status_code == 200
    assert isinstance(response.json(), str) or isinstance(response.json(), dict)

def test_get_genes(base_url):
    response = requests.get(f'{base_url}/genes')
    assert response.status_code == 200
    assert isinstance(response.json(), list)

def test_get_gene_data(base_url):
    response = requests.get(f'{base_url}/genes/HGNC:24523')
    assert response.status_code == 200
    assert isinstance(response.json(), dict)

def test_list_jobs(base_url):
    response = requests.get(f'{base_url}/jobs')
    assert response.status_code == 200

def test_get_job(base_url, job_id):
    response = requests.get(f'{base_url}/jobs/{job_id}')
    assert response.status_code == 200

def test_get_results(base_url, job_id):
    response = requests.get(f'{base_url}/results/{job_id}')
    assert response.status_code == 200