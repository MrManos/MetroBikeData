import requests
import redis
from flask import Flask, request
from hotqueue import HotQueue
import json
import jobs
import logging
import os

# Initialize Flask app
app = Flask(__name__)

# Initialize logging
log_level = os.environ.get("LOG_LEVEL")
logging.basicConfig(level=log_level)

# Initialize Redis connections
REDIS_IP = os.environ.get("REDIS_IP")
rd = redis.Redis(host=REDIS_IP, port=6379, db=0)
q = HotQueue("queue", host=REDIS_IP, port=6379, db=1)
jdb = redis.Redis(host=REDIS_IP, port=6379, db=2)
res = redis.Redis(host=REDIS_IP, port=6379, db=3) # results database

@app.route('/data', methods=['POST'])
def load_data():
    """
    Route to load data to Redis via POST request.
    """
    logging.info("Loading data to Redis...")
    response = requests.get('https://ftp.ebi.ac.uk/pub/databases/genenames/hgnc/json/hgnc_complete_set.json')
    if response.status_code == 200:
        rd.set('gene_data', json.dumps(response.json()))
        logging.info("Data loaded to Redis successfully")
        return 'Data loaded to Redis successfully', 200
    else:
        logging.error("Failed to load data to Redis")
        return 'Failed to load data to Redis', 500

@app.route('/data', methods=['GET'])
def get_data():
    """
    Route to retrieve all data from Redis via GET request.
    """
    logging.info("Retrieving data from Redis...")
    data = rd.get('gene_data')
    if data:
        return data, 200
    else:
        logging.warning("No data found in Redis")
        return 'No data found in Redis', 404

@app.route('/data', methods=['DELETE'])
def delete_data():
    """
    Route to delete all data from Redis via DELETE request.
    """
    logging.info("Deleting data from Redis...")
    rd.delete('gene_data')
    return 'Data deleted from Redis', 200

@app.route('/genes', methods=['GET'])
def get_genes():
    """
    Route to retrieve list of hgnc_id fields.
    """
    logging.info("Retrieving list of hgnc_id fields...")
    data = rd.get('gene_data')
    if data:
        data = json.loads(data)['response']['docs']
        return json.dumps([gene['hgnc_id'] for gene in data]), 200
    else:
        logging.warning("No data found in Redis")
        return 'No data found in Redis', 404
    
@app.route('/genes/<hgnc_id>', methods=['GET'])
def get_gene_data(hgnc_id):
    """
    Route to retrieve data associated with a given hgnc_id.
    """
    logging.info(f"Retrieving data for hgnc_id: {hgnc_id}...")
    data = rd.get('gene_data')
    if data:
        data = json.loads(data)['response']['docs']
        gene_dict = {gene['hgnc_id']: gene for gene in data}
        if hgnc_id in gene_dict:
            result = {'hgnc_id': hgnc_id}
            result.update({k: v for k, v in gene_dict[hgnc_id].items() if k != 'hgnc_id'})
            return json.dumps(result), 200
        return 'No data found for the provided hgnc_id', 404
    else:
        logging.warning("No data found in Redis")
        return 'No data found in Redis', 404

@app.route('/jobs', methods=['POST'])
def create_job():
    """
    Route to create a new job.
    """
    logging.info("Creating new job...")
    params = request.get_json()

    # Check if 'gene1' and 'gene2' parameters are provided
    if 'gene1' not in params or 'gene2' not in params:
        logging.error("Missing parameters. Please provide 'gene1' and 'gene2' parameters.")
        return json.dumps({"error": "Missing parameters. Please provide 'gene1' and 'gene2' parameters."}), 400
    
    job_info = jobs.add_job(params['gene1'],params['gene2'])
    logging.info("New job created")
    return json.dumps(job_info), 201

@app.route('/jobs', methods=['GET'])
def list_jobs():
    """
    Route to list all jobs.
    """
    logging.info("Listing all jobs...")
    job_ids = []

    # Get all job IDs from the database
    all_job_ids = jdb.keys()
    
    # Fetch job details for each job ID
    for job_id in all_job_ids:
        job_info = jobs.get_job_by_id(job_id.decode())
        job_ids.append(f"Job ID: {job_info['id']} | Status: {job_info['status']} | gene1: {job_info['gene1']} | gene2: {job_info['gene2']}")

    # Check if any jobs were retrieved
    if not job_ids:
        logging.info("No jobs available")
        return "No jobs available", 200

    return "\n".join(job_ids), 200

@app.route('/jobs/<job_id>', methods=['GET'])
def get_job(job_id):
    """
    Route to retrieve a specific job by ID.
    """
    logging.info(f"Retrieving job with ID: {job_id}...")
    job_info = jobs.get_job_by_id(job_id)
    if not job_info:
        logging.warning("Job not found")
        return "Job not found", 404
    return json.dumps(job_info), 200

@app.route('/results/<job_id>', methods=['GET'])
def get_results(job_id):
    """
    Route to retrieve results for a specific job by ID.
    """
    logging.info(f"Retrieving results for job with ID: {job_id}...")
    # Check if the job ID exists
    job_info = jobs.get_job_by_id(job_id)
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
        logging.warning("No results found for this job")
        return "No results found for this job", 404

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)