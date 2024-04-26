import requests
from flask import Flask, request, app
import json
from jobs import trips_db, kiosk_db, q, jdb, res
import logging
import os
import redis

# Initialize Flask app
app = Flask(__name__)

# Initialize logging
log_level = os.environ.get("LOG_LEVEL")
logging.basicConfig(level=log_level)

trips_url = "https://data.austintexas.gov/resource/tyfh-5r8s.json?"
kiosk_url = "https://data.austintexas.gov/resource/qd73-bsdg.json"

def get_data(trips_db, kiosk_db):
    # Retrieve trips data
    trips_data = []
    for key in sorted(trips_db.keys()):
        trips_data.extend(json.loads(trips_db.get(key)))

    # Retrieve kiosks data
    kiosk_data = json.loads(kiosk_db.get('kiosks'))

    return trips_data, kiosk_data

@app.route('/data', methods=['POST'])
def load_data():
    """
    Route to load data to Redis via POST request.

    Example command: curl -X POST localhost:5000/data -d '{"rows":"100000"}' -H "Content-Type: application/json"
    """
    params = request.get_json()

    # Check if 'rows' parameter is provided
    if 'rows' not in params:
        logging.error("Missing parameters. Please provide 'rows' parameter.")
        return "Missing parameters. Please provide 'rows' parameter."
    try:
        rows = int(params['rows'])
        if rows <= 0:
            logging.error("'rows' must be greater than 0.")
            return "'rows' must be between 1000 and 1000000."
    except ValueError:
        logging.error("The value of 'rows' must be an integer.")
        return "The value of 'rows' must be an integer."

    if rows <= 0:
        logging.error("The value of 'rows' must be greater than 0.")
        return "The value of 'rows' must be greater than 0."

    # Load trips data to trips_db in chunks
    chunk_size = 1000000
    response = requests.get(trips_url + f"$limit={rows}&$order=checkout_date DESC")
    if response.status_code != 200:
        logging.error("Failed to load data to Redis")
        return 'Failed to load data to Redis'
    trips_db.flushall()
    trips_data = response.json()
    logging.debug(f"Number of trips retrieved: {len(trips_data)}")
    for i in range(len(trips_data)//chunk_size):
        trips_db.set(f'chunk {i}',json.dumps(trips_data[i*chunk_size:(i+1)*chunk_size]))
    trips_db.set(f'chunk {i+1}',json.dumps(trips_data[(i+1)*chunk_size:]))

    # Load kiosk data to trips_db in chunks
    response = requests.get(kiosk_url)
    if response.status_code != 200:
        logging.error("Failed to load data to Redis")
        return 'Failed to load data to Redis'
    kiosk_data = response.json()
    logging.debug(f"Number of kiosks retrieved: {len(kiosk_data)}")
    kiosk_db.set('kiosks', json.dumps(kiosk_data))

    return f'Loaded {len(trips_data)} trips and {len(kiosk_data)} kiosks into Redis databases.', 200

if __name__ == '__main__':
    app.run(debug=True, host = '0.0.0.0', port = 5000)