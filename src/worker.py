import time
import logging
import json
import os
from hotqueue import HotQueue
import redis
import api
import jobs
from jobs import trips_db, kiosk_db, q, jdb, res
from data_lib import filter_by_location
from gcd_algorithm import great_circle_distance

# Initialize logging
log_level = os.environ.get("LOG_LEVEL")
logging.basicConfig(level=log_level)

# def plotBetweenKiosk( kiosk_1, kiosk_2) -> list:
#     '''
#     This returns the distribution of 
    
#     '''

def nearest_kiosk( coordinates: tuple, kiosk_data: list[dict], radius: float) -> dict:
    '''
    Tells the user the nearest kiosk location 
    
    Args:
    location[tuple]: The coordinates the user inputs

    Returns:

    (will name the variable here) [dict]: Returns the name/location of nearby kiosks and their eclidian distance magnitude 
    '''
    ## Creates an empty dict 
    Kiosk_locations = {}

    lat_1,long_1 = coordinates

    for kiosk_dict in kiosk_data:
        ## All the kiosk ids
        kiosk_ids = kiosk_data['Kiosk_ID']
        ## All the kiosk coordinates
        kiosk_coordinates = kiosk_data['Location']
        ## Should pull the lat and long from Location
        lat_2, long_2 = float(kiosk_coordinates[0]), float(kiosk_coordinates[1])
        dist = great_circle_distance(lat_1, long_1, lat_2, long_2)
        Kiosk_locations[kiosk_ids] = dist

    nearby_kiosks = [kiosk for kiosk in kiosk_data if Kiosk_locations.get(kiosk['kiosk_id'], float('inf')) <= radius]

    # Find the nearest kiosk
    nearest_kiosk = min(nearby_kiosks, key=lambda k: Kiosk_locations.get(k['kiosk_id'], float('inf')))

    # Return relevant information
    return {
        'kiosk_id': nearest_kiosk['kiosk_id'],
        'name': nearest_kiosk.get('name', 'Unknown'),
        'location': nearest_kiosk.get('location', {}),
        'distance_km': Kiosk_locations.get(nearest_kiosk['kiosk_id'], float('inf'))
    }
    










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