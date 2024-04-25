import json
import uuid
import redis
from hotqueue import HotQueue
import os
import logging

# Initialize logging
log_level = os.environ.get("LOG_LEVEL")
logging.basicConfig(level=log_level)

REDIS_IP = os.environ.get("REDIS_IP")

rd = redis.Redis(host=REDIS_IP, port=6379, db=0)
q = HotQueue("queue", host=REDIS_IP, port=6379, db=1)
jdb = redis.Redis(host=REDIS_IP, port=6379, db=2)

def _generate_jid():
    """
    Generate a pseudo-random identifier for a job.
    """
    logging.info("Generating job ID...")
    jid = str(uuid.uuid4())
    logging.info(f"Generated job ID: {jid}")
    return jid

def _instantiate_job(jid, status, gene1, gene2):
    """
    Create the job object description as a python dictionary. Requires the job id,
    status, genes, and operation parameters.
    """
    logging.info(f"Instantiating job with ID {jid}...")
    job_dict = {'id': jid, 'status': status, 'gene1': gene1, 'gene2': gene2}
    logging.info("Job instantiated successfully.")
    return job_dict

def _save_job(jid, job_dict):
    """Save a job object in the Redis database."""
    logging.info(f"Saving job with ID {jid} to the database...")
    jdb.set(jid, json.dumps(job_dict))
    logging.info("Job saved successfully.")

def _queue_job(jid):
    """Add a job to the redis queue."""
    logging.info(f"Queueing job with ID {jid}...")
    q.put(jid)
    logging.info("Job queued successfully.")
    return

def add_job(gene1, gene2, status="submitted"):
    """Add a job to the redis queue."""
    logging.info("Adding job to the system...")
    jid = _generate_jid()
    job_dict = _instantiate_job(jid, status, gene1, gene2)
    _save_job(jid, job_dict)
    _queue_job(jid)
    logging.info("Job added successfully.")
    return job_dict

def get_job_by_id(jid):
    """Return job dictionary given jid"""
    logging.info(f"Retrieving job with ID {jid}...")
    job_json = jdb.get(jid)
    if job_json:
        job_dict = json.loads(job_json)
        logging.info("Job retrieved successfully.")
        return job_dict
    else:
        logging.error(f"No job found with ID {jid}.")
        return None

def update_job_status(jid, status):
    """Update the status of job with job id `jid` to status `status`."""
    job_dict = get_job_by_id(jid)
    if job_dict:
        logging.info(f"Updating job status for job ID {jid} to '{status}'")
        job_dict['status'] = status
        _save_job(jid, job_dict)
        logging.info(f"Job status updated successfully.")
    else:
        logging.error(f"Job with ID {jid} not found.")
        raise Exception("Job not found")