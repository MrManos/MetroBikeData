import requests
import redis
from flask import Flask, request, app
from hotqueue import HotQueue
import json
from jobs import trips_db, kiosk_db
from jobs import rd, get_job_by_id, res
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

    # Check if 'gene1' and 'gene2' parameters are provided
    if 'rows' not in params:
        logging.error("Missing parameters. Please provide 'rows' parameter.")
        return json.dumps({"error": "Missing parameters. Please provide 'rows' parameter."}), 400
    
    if 1000 < params['rows'] < 500000:
        logging.error("Number of rows must be between 1000 and 500000.")
        return json.dumps({"error": "Number of rows must be between 1000 and 500000."}), 400
    
    for i in range(params['rows']):
        response = requests.get(url + f"$offset={i*1000}&$order=checkout_date DESC")
        if response.status_code != 200:
            logging.error("Failed to load data to Redis")
            return 'Failed to load data to Redis', 500
        rd.set('data', json.dumps(response.json()))

    return 'Data loaded to Redis successfully', 200
    
    return json.dumps(job_info), 201

    logging.info("Loading data to Redis...")
    response = requests.get('https://ftp.ebi.ac.uk/pub/databases/genenames/hgnc/json/hgnc_complete_set.json')
    if response.status_code == 200:
        rd.set('gene_data', json.dumps(response.json()))
        logging.info("Data loaded to Redis successfully")
        return 'Data loaded to Redis successfully', 200
    else:
        logging.error("Failed to load data to Redis")
        return 'Failed to load data to Redis', 500
    
@app.route('/jobs/<job_id>', methods = ['GET'])
def get_job(job_id):
    """
    Gets specific job details
    """
    job_info = get_job_by_id(job_id)
    if not job_info:
        logging.warning("Job not found")
        return "Job not found", 404
    return json.dumps(job_info), 200

    
@app.route('/results/<job_id>', methods=['GET'])
def get_results(job_id):
    """
    Gets the results of a specific job 
    """
    # Check if the job ID exists
    job_info = get_job_by_id(job_id)
    if not job_info:
        logging.warning("Job not found")
        return "Job not found", 404

    # Check if the job is complete
    if job_info['status'] != "complete":
        logging.warning("Job is still in progress")
        return "Job is still in progress", 200
    
    # Retrieve the results from the separate database (db=3)
    result_data = res.get(job_id)
    if result_data:
        return result_data, 200
    else:
        logging.warning("No results for this job")
        return "No results for this job", 404



if __name__ == '__main__':
    app.run(debug=True, host = '0.0.0.0', port = 5000)