import pytest
import jobs as j
import os
import redis

REDIS_IP = os.environ.get("REDIS_IP")
jdb = redis.Redis(host=REDIS_IP, port=6379, db=2)

@pytest.fixture
def setup_job():
    jid = j._generate_jid()
    job_dict = j._instantiate_job(jid, "submitted", "HGNC:24523", "HGNC:29027")
    assert isinstance(job_dict, dict)
    return jid, job_dict

def test_generate_jid():
    jid = j._generate_jid()
    assert isinstance(jid, str)
    assert len(jid) == 36  # UUID length

def test_save_and_get_job(setup_job):
    jid, job_dict = setup_job
    j._save_job(jid, job_dict)
    retrieved_job_dict = j.get_job_by_id(jid)
    assert retrieved_job_dict == job_dict

def test_update_job_status(setup_job):
    jid, job_dict = setup_job
    new_status = "in progress"
    j._save_job(jid, job_dict)
    j.update_job_status(jid, new_status)
    updated_job_dict = j.get_job_by_id(jid)
    assert updated_job_dict['status'] == new_status
    jdb.flushdb()