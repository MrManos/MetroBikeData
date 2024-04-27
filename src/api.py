import requests
from flask import Flask, request, app, send_file
import json
from jobs import trips_db, kiosk_db, get_job_by_id, res
import logging
import os
import redis
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
from typing import List

# Initialize Flask app
app = Flask(__name__)

# Initialize logging
log_level = os.environ.get("LOG_LEVEL")
logging.basicConfig(level=log_level)

trips_url = "https://data.austintexas.gov/resource/tyfh-5r8s.json?"
kiosk_url = "https://data.austintexas.gov/resource/qd73-bsdg.json"

def get_data(trips_db: redis.client.Redis, kiosk_db: redis.client.Redis) -> tuple:
    """
    Retrieve trips and kiosk data from Redis databases.

    Args:
        trips_db (redis.client.Redis): Redis connection for trips database.
        kiosk_db (redis.client.Redis): Redis connection for kiosk database.

    Returns:
        tuple: A tuple containing trips data (list) and kiosk data (list).
    """
    # Retrieve trips data
    trips_data = []
    for key in sorted(trips_db.keys()):
        trips_data.extend(json.loads(trips_db.get(key)))

    # Retrieve kiosks data
    kiosk_data = json.loads(kiosk_db.get('kiosks'))

    return trips_data, kiosk_data

def filter_by_date(trips_data: List[dict], start_datetime: datetime, end_datetime:datetime) -> List[dict]:
    '''
    Filters trip data within the interval [start_data, end_date]

    Args:
        trips_data: List of dicts, each dict is data for one trip
        start_date: start date of the interval in TBD format
        end_date: end date of the interval in TBD format

    Returns:
        List[dict]: filtered trips_data

    Example:

    '''

    # helper function to check if trip is within time interval
    def _in_interval(trip_dict):
        date_str = trip_dict['checkout_datetime']
        trip_datetime = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f')
        return start_datetime <= trip_datetime <= end_datetime
    
    return [trip for trip in trips_data if _in_interval(trip)]

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
    except ValueError:
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
        for i in range(n):
            trips_db.set(f'chunk {i}',json.dumps(trips_data[i*chunk_size:(i+1)*chunk_size]))
        trips_db.set(f'chunk {i+1}',json.dumps(trips_data[(i+1)*chunk_size:]))
    else:
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
    Route to plot routes data for a given day between two kiosk locations to Redis via POST request.

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

if __name__ == '__main__':
    app.run(debug=True, host = '0.0.0.0', port = 5000)