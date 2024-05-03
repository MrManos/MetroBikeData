import time
import logging
import json
import os
from datetime import datetime
import matplotlib.pyplot as plt
import io

import jobs
from jobs import trips_db, kiosk_db, q, jdb, res
from data_lib import filter_by_date, filter_by_location, nearest_kiosks, get_trips, get_kiosks
from gcd_algorithm import great_circle_distance

# Initialize logging
log_level = os.environ.get("LOG_LEVEL")
logging.basicConfig(level=log_level)

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

    # Generate the desired plot
    result = None
    job_params = jobs.get_job_by_id(job_id)['job parameters']
    job_type = job_params['plot_type']
    if job_type == 'trip_duration':
        result = trip_duration_histogram_job(job_params)
    elif job_type == 'trips_per_day':
        result = trips_per_day_job(job_params)
    else: 
        # Simulate processing time
        logging.warning('Invalid plot/job type')
        time.sleep(5)
    
    res.set(job_id, result)

    # Update status
    jobs.update_job_status(job_id, "complete")
    logging.info(f"Job with ID {job_id} processed successfully")

def trip_duration_histogram_job(job_parameters):
    """
    Function that plots the route data for a given time interval between two kiosk locations.
    """
    start_date = datetime(year=2023, month=1, day=31) if job_parameters['start_date'] == 'default' else datetime.strptime(job_parameters['start_date'], "%m/%d/%Y")
    end_date = datetime(year=2024, month=1, day=31) if job_parameters['end_date'] == 'default' else datetime.strptime(job_parameters['end_date'], "%m/%d/%Y")

    k1 = job_parameters['kiosk1']
    k2 = job_parameters['kiosk2']
    # Check if parameters are provided and valid
    if not all([start_date,end_date, k1, k2]):
        logging.error("Missing or invalid parameters. Please provide 'day', 'kiosk1', and 'kiosk2' parameters.")
        return "Missing or invalid parameters. Please provide 'day', 'kiosk1', and 'kiosk2' parameters.", 400

    # Get all the trips on that day between the two kiosks
    trips = []

    trips_data = filter_by_date(get_trips(trips_db), start_date, end_date)
    for trip in trips_data:
        if 'checkout_kiosk_id' in trip and 'return_kiosk_id' in trip:
            kiosk_set = {trip['checkout_kiosk_id'], trip['return_kiosk_id']}
            if kiosk_set == {k1, k2} or kiosk_set == {k2, k1}:
                trips.append(trip)

    # Get trip durations
    trip_durations = [int(trip['trip_duration_minutes']) for trip in trips]

    # Plot trip durations on histogram and save figure
    fig = plt.figure()
    plt.hist(trip_durations, bins=range(0, 31))
    plt.xlabel('Trip Duration (minutes)')
    plt.ylabel('Number of Trips')
    plt.title(f"Trip Durations between {k1} and {k2} ({start_date.strftime('%m/%d/%y')} - {end_date.strftime('%m/%d/%y')})")

    if type(serialize_fig(fig)) == None:
        logging.warning('serialized fig is none')
    return serialize_fig(fig)

def trips_per_day_job(job_data:dict):
    '''
    Creates a plot of the number of trips per day within the specific time interval/locations

    Expected keys in Job_parameters: 
        - 'start_date', 
        - 'end_date', 
        - 'lat', 
        - 'long',
        - 'radius'
    
    Returns bytes data for a png image. Image is of a plot of the number of trips per day. 
    '''
    # get data
    trips_data, kiosk_data = get_trips(trips_db), get_kiosks(kiosk_db)
    
    # parse job parameters
    radius = (1 if job_data['radius'] == 'default' else float(job_data['radius']))*1.609344
    lat = 30.2862730619728 if job_data['lat'] == 'default' else float(job_data['lat'])
    long = -97.73937727490916 if job_data['long'] == 'default' else float(job_data['long'])
    start_date = datetime(year=2023, month=1, day=31) if job_data['start_date'] == 'default' else datetime.strptime(job_data['start_date'], "%m/%d/%Y")
    end_date = datetime(year=2024, month=1, day=31) if job_data['end_date'] == 'default' else datetime.strptime(job_data['end_date'], "%m/%d/%Y")

    # filter trip data
    filtered_trips_temp = filter_by_date(trips_data, start_date, end_date)
    filtered_trips = filter_by_location(filtered_trips_temp, kiosk_data, (lat, long), radius)

    # trips_by_date = { <datetime1> : [<trip dict1>, <trip dict2> ...] ... }
    trips_by_date = {}
    for trip_dict in filtered_trips:
        date_str = trip_dict['checkout_date']
        trip_date = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f') # datetime object
        if trip_date not in trips_by_date.keys():
            trips_by_date[trip_date] = [trip_dict]
        else:
            trips_by_date[trip_date].append(trip_dict)

    logging.debug(f"Collected dates: {trips_by_date.keys()}")

    dates = sorted(trips_by_date.keys())
    number_trips = [len(trips_by_date[date]) for date in dates]

    fig = plt.figure(figsize=(15,8))
    plt.plot(dates, number_trips)
    plt.xlabel('Date')
    plt.ylabel('Number of Trips')
    plt.title(f"Trips per day {start_date.strftime('%m/%d/%y')} - {end_date.strftime('%m/%d/%y')}, Location: ({lat:.3f}, {long:.3f}), Radius: {radius}")

    return serialize_fig(fig)


def serialize_fig(figure)->bytes:
    '''
    Converts a matplotlibfigure into png binary data that
    can be stored in redis.

    Parameters:
    - figure (matplotlib.Figure): The figure to be serialized.
    '''
    buf = io.BytesIO()
    figure.savefig(buf)
    return buf.getvalue() #BytesIO object

# Start processing jobs
if __name__ == '__main__':
    process_job()