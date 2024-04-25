import time
import logging
import json
import os
from hotqueue import HotQueue
import redis
import api
import jobs

# Initialize logging
log_level = os.environ.get("LOG_LEVEL")
logging.basicConfig(level=log_level)

# Initialize Redis connections
REDIS_IP = os.environ.get("REDIS_IP")
rd = redis.Redis(host=REDIS_IP, port=6379, db=0)
q = HotQueue("queue", host=REDIS_IP, port=6379, db=1)
res = redis.Redis(host=REDIS_IP, port=6379, db=3)

def calculate_similarities(gene1, gene2):
    """
    Calculate similarities between two genes based on their attributes.

    Args:
        gene1 (str): HGNC ID of the first gene.
        gene2 (str): HGNC ID of the second gene.

    Returns:
        str: A string describing the similarities between the two genes.
    """
    logging.info(f"Calculating similarities between {gene1} and {gene2}...")
    similarities = {}

    gene1_data, gene2_data = json.loads(api.get_gene_data(gene1)[0]), json.loads(api.get_gene_data(gene2)[0])

    similarities = {k: v for k, v in gene1_data.items() if k in gene2_data and gene2_data[k] == v}

    return f"Similarities between {gene1_data['hgnc_id']} and {gene2_data['hgnc_id']}:\n" + "\n".join([f"{k}: {v}" for k, v in similarities.items()])

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

    # Calculate similarities between genes
    result = None
    job_info = jobs.get_job_by_id(job_id)

    if 'gene1' in job_info and 'gene2' in job_info:
        gene1 = job_info['gene1']
        gene2 = job_info['gene2']
        result = calculate_similarities(gene1, gene2)
    else:
    # Handle the case where gene1 or gene2 doesn't exist
        result = "Gene 1 or Gene 2 not found in the database. Please provide valid HGNC IDs."

    res.set(job_id, result)

    # Simulate processing time
    time.sleep(5)

    # Update job status to "complete"
    jobs.update_job_status(job_id, "complete")

    logging.info(f"Job with ID {job_id} processed successfully")

# Start processing jobs
process_job()