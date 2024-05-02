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
from flask import Flask, request, app, send_file, Response
import json
import folium
import io
from PIL import Image

# Project defined
from gcd_algorithm import great_circle_distance
from jobs import trips_db, kiosk_db, get_job_by_id, res, add_job, get_results_by_id
from data_lib import filter_by_date, filter_by_location, nearest_kiosks, get_kiosks, get_trips

# Initialize Flask app
app = Flask(__name__)

# Initialize logging
log_level = os.environ.get("LOG_LEVEL")
logging.basicConfig(level=log_level)

trips_url = "https://data.austintexas.gov/resource/tyfh-5r8s.json?"
kiosk_url = "https://data.austintexas.gov/resource/qd73-bsdg.json"

@app.route('/data', methods=['POST', 'DELETE'])
def load_data():
    """
    Route to load data to Redis via POST request.

    Example command: curl -X POST localhost:5000/data -d '{"rows":"100000"}' -H "Content-Type: application/json"

    Returns:
        tuple: A tuple containing a message indicating the success or failure of data loading (str) and an HTTP status code.
    """
    if request.method == 'POST':
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


    elif request.method == 'DELETE':
        trips_deleted = 0
        for key in trips_db.keys():
            trips_db.delete(key)
            trips_deleted += 1

        kiosks_deleted = 0
        for key in kiosk_db.keys():
            kiosk_db.delete(key)
            kiosks_deleted += 1

        return f"Deleted {trips_deleted} trips and {kiosks_deleted} kiosks"


@app.route('/kiosk_ids', methods = ['GET'])
def get_kiosk_keys():
    '''
    Returns all the available kiosk IDs

    Example command: curl localhost:5000/kiosk_ids
    '''
    return str([kiosk['kiosk_id'] for kiosk in get_kiosks(kiosk_db)])

@app.route('/show_nearest', methods = ['GET'])
def show_nearest_kiosks():
    """
    Route to plot the n nearest kiosks to a given location.

    Example route to paste into browser: localhost:5000/show_nearest?n=5&lat=30.2862730619728&long=-97.73937727490916
    """
    try:
        n, lat, long = int(request.args.get('n')), float(request.args.get('lat')), float(request.args.get('long'))
    except ValueError:
        return "Invalid parameters. Please provide valid 'n', 'lat', and 'long' parameters.", 400
    # Check if parameters are provided and valid
    if not all([lat,long]):
        logging.error("Missing parameters. Please provide 'n', 'lat', and 'long' parameters.")
        return "Missing parameters. Please provide 'n', 'lat', and 'long' parameters.", 400
    
    # Get nearest kiosks
    nearest = nearest_kiosks((lat,long),get_kiosks(kiosk_db),n)

    # Use Folium to output a map with HTML
    map = folium.Map()
    locations = [(float(kiosk['location']['latitude']), float(kiosk['location']['longitude'])) for kiosk in nearest]
    marker_colors = ['red' if kiosk['kiosk_status'] != 'active' else 'green' for kiosk in nearest]

    # Add markers for n closest kiosks
    for loc,color in zip(locations,marker_colors):
        folium.Marker(location=loc, icon=folium.Icon(color=color)).add_to(map)
    
    # Requested location
    folium.Marker(location=(lat,long), icon=folium.Icon(color='blue')).add_to(map)

    # Fit the map to the bounds of the markers
    map.fit_bounds(locations)

    # Save map
    map.save("map.html")

    return send_file('map.html', mimetype='text/html', as_attachment=False)
    
@app.route('/nearest', methods = ['GET'])
def get_nearest_kiosks():
    """
    Route to print the n nearest kiosks to a given location.

    Example command: curl "localhost:5000/nearest?n=5&lat=30.2862730619728&long=-97.73937727490916"
    """
    try:
        n, lat, long = int(request.args.get('n')), float(request.args.get('lat')), float(request.args.get('long'))
    except ValueError:
        return "Invalid parameters. Please provide valid 'n', 'lat', and 'long' parameters.", 400
    # Check if parameters are provided and valid
    if not all([lat,long]):
        logging.error("Missing parameters. Please provide 'n', 'lat', and 'long' parameters.")
        return "Missing parameters. Please provide 'n', 'lat', and 'long' parameters.", 400
    
    # Get nearest kiosks
    nearest = nearest_kiosks((lat,long),get_kiosks(kiosk_db),n)
    response_string = "Nearest Kiosks:\n"
    for kiosk in nearest:
        distance = great_circle_distance(lat, long, float(kiosk['location']['latitude']), float(kiosk['location']['longitude']))
        response_string += f"- Kiosk Name: {kiosk['kiosk_name']}, Kiosk ID: {kiosk['kiosk_id']}, Distance: {distance:.2f} km, Status: {kiosk['kiosk_status']} \n"

    return response_string

@app.route('/jobs', methods = ['POST'])
def submit_job():
    '''
    Check if a job request is valid and then submits the request.

    job parameters
    - start date
    - end date
    - radius
    - latitude
    - longitude
    - plot type - e.g trip duration histogram, number of trips per day, etc.

    curl -X POST localhost:5000/jobs -d '{"kiosk1":"4055", "kiosk2":"2498", "start_date":"01/31/2023", "end_date":"01/31/2024", "plot_type":"trip_duration"}' -H "Content-Type: application/json"
    curl -X POST localhost:5000/jobs -d '{"start_date": "01/31/2023", "end_date":"01/31/2024", "latitude":"30.286", "longitude":"-97.739", "radius":"3", "plot_type":"trips_per_day"}' -H "Content-Type: application/json"
    '''
    
    job_data = request.get_json()
    allowed_params = ['kiosk1','kiosk2','start_date','end_date','latitude','longitude','radius','plot_type']
    for param in job_data:
        if param not in allowed_params:
            return f"Invalid parameters. Allowed parameters are {allowed_params}.", 400
    if 'plot_type' not in job_data:
        return "Must include a plot type.", 400
    
    if job_data['plot_type'] == 'trip_duration':
        if all(key in job_data for key in ['kiosk1', 'kiosk2', 'start_date', 'end_date']):
            try:
                int(job_data['kiosk1'])
                int(job_data['kiosk2'])
                datetime.strptime(job_data['start_date'], "%m/%d/%Y")
                datetime.strptime(job_data['end_date'], "%m/%d/%Y")
            except:
                return "Invalid job parameters.", 400
            try:
                job_info = add_job({
                    'kiosk1': job_data['kiosk1'],
                    'kiosk2': job_data['kiosk2'],
                    'start_date': job_data['start_date'],
                    'end_date': job_data['end_date'],
                    'plot_type': job_data['plot_type']
                })
            except:
                return "Unable to add job.", 500
        else:
            return "Invalid parameters for trip duration plot. Please provide start_date, end_date, kiosk1, kiosk2.", 400
    
    elif job_data['plot_type'] == 'trips_per_day':
        if all(key in job_data for key in ['start_date', 'end_date', 'latitude', 'longitude', 'radius']):
            try:
                float(job_data['radius'])
                float(job_data['latitude'])
                float(job_data['longitude'])
                datetime.strptime(job_data['start_date'], "%m/%d/%Y")
                datetime.strptime(job_data['end_date'], "%m/%d/%Y")
            except:
                return "Invalid job parameters.", 400

            try:
                job_info = add_job({
                    'start_date': job_data['start_date'],
                    'end_date': job_data['end_date'],
                    'lat': job_data['latitude'],
                    'long': job_data['longitude'],
                    'radius': job_data['radius'],
                    'plot_type': job_data['plot_type']
                })
            except:
                return "Unable to add job.", 500
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
    
@app.route('/help', methods=['GET'])
def help_route() -> str:
    """
    Displays a menu for the user to know the commands of the program.
    """
    help_message = '''
    These are the available routes and their functionalities:

    /data (POST):
        Load data (trips and kiosks) into Redis databases.
        Example: curl -X POST localhost:5000/data -d '{"rows":"100000"}' -H "Content-Type: application/json"

    /kiosk_ids (GET):
        Get a list of available kiosk IDs.
        Example: curl localhost:5000/kiosk_ids

    /show_nearest (GET):
        Plot the n nearest kiosks to a given location on a map and return the map as an HTML file.
        Example: localhost:5000/show_nearest?n=5&lat=30.2862730619728&long=-97.73937727490916

    /nearest (GET):
        Print the n nearest kiosks to a given location.
        Example: curl "localhost:5000/nearest?n=5&lat=30.2862730619728&long=-97.73937727490916"

    /jobs (POST):
        Submit a job request with various parameters (e.g., start date, end date, checkout location, return location, plot type).
        Example: curl -X POST localhost:5000/jobs -d '{"kiosk1":"4055", "kiosk2":"2498", "start_date":"01/31/2023", "end_date":"01/31/2024", "plot_type":"trip_duration"}' -H "Content-Type: application/json"

    /jobs/<job_id> (GET):
        Get job information associated with the given job ID.
        Example: curl localhost:5000/jobs/1234
    '''
    return help_message

    
@app.route('/results/<job_id>', methods = ['GET'])
def get_results(job_id):
    '''
    Returns job results associated with the job id. If the job
    has not yet completed, it will return message indicating the
    current status.
    '''

    # check if the job exists
    try:
        job_dict = get_job_by_id(job_id)
    except:
        return f"Job {job_id} not found."
    
    # check if the job is complete
    status = job_dict['status']
    if job_dict['status'] != 'complete':
        return f"Job {job_id} not complete. Current status: {status}"

    # get results
    results = get_results_by_id(job_id)
    if not results:
        # no results found
        return f"Results for job {job_id} not found."
    else:
        return Response(results, mimetype="image/png'")

if __name__ == '__main__':
    app.run(debug=True, host = '0.0.0.0', port = 5000)