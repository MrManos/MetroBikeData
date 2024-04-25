import time
import logging
import json
import os
from hotqueue import HotQueue
import redis
import api
import jobs

# Initialize logging
log_level = os.environ.get("LOG_LEVEL")
logging.basicConfig(level=log_level)

# Initialize Redis connections
REDIS_IP = os.environ.get("REDIS_IP")
trips_db = redis.Redis(host=REDIS_IP, port=6379, db=0)
kiosk_db = redis.Redis(host=REDIS_IP, port=6379, db=1)
q = HotQueue("queue", host=REDIS_IP, port=6379, db=2)
jdb = redis.Redis(host=REDIS_IP, port=6379, db=3)
res = redis.Redis(host=REDIS_IP, port=6379, db=4)


@q.worker
def process_job(job_id):
    """
    Process a job from the queue.

    Args:
        job_id (str): The ID of the job to process.
    """
    logging.info(f"Processing job with ID: {job_id}")

    # Update job status to "in progress"
    jobs.update_job_status(job_id, "in progress")

    # Simulate processing time
    time.sleep(5)

   
    result = None
    job_info = jobs.get_job_by_id(job_id)

    ## This is where the worker function goes 
    
    
    
    
    res.set(job_id, result)

    # Simulate processing time
    time.sleep(5)

    # Update job status to "complete"
    jobs.update_job_status(job_id, "complete")

    logging.info(f"Job with ID {job_id} processed successfully")

# Start processing jobs
process_job()