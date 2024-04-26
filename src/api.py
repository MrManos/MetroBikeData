import requests
from flask import Flask, request, app
import json
from jobs import trips_db, kiosk_db, q, jdb, res
import logging
import os

# Initialize Flask app
app = Flask(__name__)

# Initialize logging
log_level = os.environ.get("LOG_LEVEL")
logging.basicConfig(level=log_level)

url = "https://data.austintexas.gov/resource/tyfh-5r8s.json?"

@app.route('/data', methods=['POST'])
def load_data():
    """
    Route to load data to Redis via POST request.
    """
    params = request.get_json()

    # Check if 'rows' parameter is provided
    if 'rows' not in params:
        logging.error("Missing parameters. Please provide 'rows' parameter.")
        return "Missing parameters. Please provide 'rows' parameter."
    try:
        rows = int(params['rows'])
    except ValueError:
        logging.error("The value of 'rows' must be an integer.")
        return "The value of 'rows' must be an integer."

    if not 1000 <= rows <= 1000000:
        logging.error("'rows' must be between 1000 and 1000000.")
        return "'rows' must be between 1000 and 1000000."

    response = requests.get(url + f"$limit={rows}&$order=checkout_date DESC")
    if response.status_code != 200:
        logging.error("Failed to load data to Redis")
        return 'Failed to load data to Redis'
    trips_db.delete('data')
    trips_db.set('data', response.content)

    return 'Data loaded to Redis successfully'

if __name__ == '__main__':
    app.run(debug=True, host = '0.0.0.0', port = 5000)