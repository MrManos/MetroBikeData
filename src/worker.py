import time
import logging
import json
import os
from hotqueue import HotQueue
import redis
import api
import jobs
from jobs import trips_db, kiosk_db, q, jdb, res

# Initialize logging
log_level = os.environ.get("LOG_LEVEL")
logging.basicConfig(level=log_level)

## Would this be a list or dict 
# I think list u dont have to say rn tho
# def plotBetweenKiosk( kiosk_1, kiosk_2) -> list:
#     '''
#     This returns the distribution of 
    
#     '''




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