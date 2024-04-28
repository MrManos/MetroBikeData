# standard library
import logging
import os
from datetime import datetime
from typing import List

# 3rd party
import matplotlib.pyplot as plt
import numpy as np
import redis
import requests
from flask import Flask, request, app, send_file
import json

# Project defined
from gcd_algorithm import great_circle_distance
from jobs import trips_db, kiosk_db, get_job_by_id, res, add_job
from data_lib import get_data, filter_by_date, filter_by_location

# Initialize Flask app
app = Flask(__name__)

# Initialize logging
log_level = os.environ.get("LOG_LEVEL")
logging.basicConfig(level=log_level)

trips_url = "https://data.austintexas.gov/resource/tyfh-5r8s.json?"
kiosk_url = "https://data.austintexas.gov/resource/qd73-bsdg.json"



@app.route('/data', methods=['POST'])
def load_data():
    """
    Route to load data to Redis via POST request.

    Example command: curl -X POST localhost:5000/data -d '{"rows":"100000"}' -H "Content-Type: application/json"

    Returns:
        tuple: A tuple containing a message indicating the success or failure of data loading (str) and an HTTP status code.
    """
    params = request.get_json()

    # Check if 'rows' parameter is provided and valid
    if 'rows' not in params:
        logging.error("Missing parameters. Please provide 'rows' parameter.")
        return "Missing parameters. Please provide 'rows' parameter.", 400
    try:
        rows = int(params['rows'])
        if rows <= 0:
            logging.error("The value of 'rows' must be greater than 0.")
            return "The value of 'rows' must be greater than 0.", 400
    except:
        logging.error("The value of 'rows' must be an integer.")
        return "The value of 'rows' must be an integer.", 400

    # Load trips data to trips_db in chunks
    chunk_size = 1000000
    response = requests.get(trips_url + f"$limit={rows}&$order=checkout_date DESC")
    if response.status_code != 200:
        logging.error("Failed to load data to Redis")
        return 'Failed to load data to Redis', 500
    trips_db.flushall()
    trips_data = response.json()
    logging.debug(f"Number of trips retrieved: {len(trips_data)}")  

    n = len(trips_data)//chunk_size     # number of chunks
    if n > 0:
        # Store in chunks if there are more than 1M rows
        for i in range(n):
            trips_db.set(f'chunk {i}',json.dumps(trips_data[i*chunk_size:(i+1)*chunk_size]))
        trips_db.set(f'chunk {i+1}',json.dumps(trips_data[(i+1)*chunk_size:]))
    else:
        # Store in single key called "trips" if less than 1M rows
        trips_db.set(f'trips',json.dumps(trips_data))

    # Load kiosk data to trips_db in chunks
    response = requests.get(kiosk_url)
    if response.status_code != 200:
        logging.error("Failed to load data to Redis")
        return 'Failed to load data to Redis', 500
    kiosk_data = response.json()
    logging.debug(f"Number of kiosks retrieved: {len(kiosk_data)}")
    kiosk_db.set('kiosks', json.dumps(kiosk_data))

    return f'Loaded {len(trips_data)} trips and {len(kiosk_data)} kiosks into Redis databases.', 200


@app.route('/plot', methods=['GET'])
def plot():
    """
    Route to plot routes data for a given day between two kiosk locations to Redis via GET request.

    Example command: curl -o plot.png "localhost:5000/plot?day=01/31/2024&kiosk1=4055&kiosk2=2498"
    """
    day = request.args.get('day')
    k1 = request.args.get('kiosk1')
    k2 = request.args.get('kiosk2')
    # Check if parameters are provided and valid
    if not all([day, k1, k2]):
        logging.error("Missing or invalid parameters. Please provide 'day', 'kiosk1', and 'kiosk2' parameters.")
        return "Missing or invalid parameters. Please provide 'day', 'kiosk1', and 'kiosk2' parameters.", 400

    # Get all the trips on that day between the two kiosks
    trips = []
    day = datetime.strptime(day, "%m/%d/%Y")
    trips_data, kiosk_data = get_data(trips_db,kiosk_db)
    for trip in trips_data:
        if 'checkout_kiosk_id' in trip and 'return_kiosk_id' in trip:
            kiosk_set = {trip['checkout_kiosk_id'], trip['return_kiosk_id']}
            trips_day = datetime.strptime(trip['checkout_date'][:10], "%Y-%m-%d")
            if trips_day == day and kiosk_set == {k1, k2} or kiosk_set == {k2, k1}:
                trips.append(trip)

    # Get trip durations
    trip_durations = [int(trip['trip_duration_minutes']) for trip in trips]

    # Plot trip durations on histogram and save figure
    plt.hist(trip_durations, bins=range(0, 31))
    plt.xlabel('Trip Duration (minutes)')
    plt.ylabel('Frequency')
    plt.title('Histogram of Trip Durations')
    plt.savefig('plot.png')

    return send_file('plot.png', mimetype='image/png', as_attachment=True)

@app.route('/kiosk_ids', methods = ['GET'])
def get_kiosk_keys():
    '''
    Returns all the available kiosk IDs
    '''
    return [kiosk['kiosk_id'] for kiosk in get_data(trips_db, kiosk_db)[1]]

@app.route('/jobs', methods = ['POST'])
def submit_job():
    '''
    Check if a job request is valid and then submits the request.

    job parameters
    - start date
    - end date
    - checkout location
    - checkout radius
    - return location
    - return radius
    - plot type - e.g trip duration histogram, number of trips per day, etc.

    curl -X POST localhost:5000/jobs -d '{"kiosk1":"4055", "kiosk2":"2498", "start_date":"01/31/2023", "end_date":"01/31/2024", "plot_type":"trip_duration"}' -H "Content-Type: application/json"
    '''
    
    job_data = request.get_json()
    allowed_params = ['kiosk1','kiosk2','start_date','end_date','latitude','longitude','radius','plot_type']
    for param in job_data:
        if param not in allowed_params:
            return f"Invalid parameters. Allowed parameters are {allowed_params}.", 400
    if 'plot_type' not in allowed_params:
        return "Must include a plot type.", 400
    
    if job_data['plot_type'] == 'trip_duration':
        if all(key in job_data for key in ['kiosk1', 'kiosk2', 'start_date', 'end_date']):
            try:
                int(job_data['kiosk1'])
                int(job_data['kiosk2'])
                datetime.strptime(job_data['start_date'], "%m/%d/%Y")
                datetime.strptime(job_data['end_date'], "%m/%d/%Y")
            except:
                "Invalid job parameters.", 400
            try:
                job_info = add_job({
                    'kiosk1': job_data['kiosk1'],
                    'kiosk2': job_data['kiosk2'],
                    'start_date': job_data['start_date'],
                    'end_date': job_data['end_date']
                })
            except:
                "Unable to add job.", 500
        else:
            return "Invalid parameters for trip duration plot. Please provide start_date, end_date, kiosk1, kiosk2.", 400
    elif job_data['plot_type'] == 'trips_per_day':
        if all(key in job_data for key in ['start_date', 'end_date', 'lat', 'long', 'radius']):
            try:
                float(job_data['radius'])
                float(job_data['lat'])
                float(job_data['long'])
                datetime.strptime(job_data['start_date'], "%m/%d/%Y")
                datetime.strptime(job_data['end_date'], "%m/%d/%Y")
            except:
                "Invalid job parameters.", 400

            try:
                job_info = add_job({
                    'start_date': job_data['start_date'],
                    'end_date': job_data['end_date'],
                    'lat': job_data['lat'],
                    'long': job_data['long'],
                    'radius': job_data['radius']
                })
            except:
                "Unable to add job.", 500
        else:
            return "Invalid parameters for trip duration plot. Please provide start_date, end_date, lat, long and radius.", 400
    else:
        return "Invalid plot type.", 400
    
    #return job_dict
    # 

    return job_info

@app.route('/jobs/<job_id>', methods = ['GET'])
def get_job(job_id):
    '''
    Returns job information associated with the job id.
    '''
    try:
        return get_job_by_id(job_id)
    except:
        return f"Job {job_id} not found"
    
# @app.route('/results/<job_id>', methods = ['GET'])
# def get_results(job_id):
#     '''
#     Returns job results associated with the job id. If the job
#     has not yet completed, it will return message indicating the
#     current status.
#     '''

#     # check if the job exists
#     try:
#         job_dict = get_job_by_id(job_id)
#     except:
#         return f"Job {job_id} not found."
    
#     # check if the job is complete
#     status = job_dict['status']
#     if job_dict['status'] != 'complete':
#         return f"Job {job_id} not complete. Current status: {status}"

#     # get results
#     results = get_results_by_id(job_id)
#     if not results:
#         # no results found
#         return f"Results for job {job_id} not found."
#     else:
#         return Response(results, mimetype="image/png'")
        
if __name__ == '__main__':
    app.run(debug=True, host = '0.0.0.0', port = 5000)