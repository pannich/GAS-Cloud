# hw5 job.py
#
# from t2.nano

import json
import os
from datetime import datetime

# Directory to store job files
JOB_DIRECTORY = '/home/ec2-user/mpcs-cc/job_data'

def create_job(key, job_id_path, input_file):
    """Create a new job and store its data in a file.
    Return local file path"""
    # Ensure the jobs directory exists
    os.makedirs(JOB_DIRECTORY, exist_ok=True)

    # Create a directory specifically for this job
    job_folder, extension = os.path.splitext(key)

    job_specific_directory = os.path.join(JOB_DIRECTORY, job_id_path)
    os.makedirs(job_specific_directory, exist_ok=True)

    # Job data json
    job_data = {
        "key": key,
        "input_file": input_file,
        "created_at": datetime.now().isoformat()
    }

    # Write the job data to a file
    job_file_path = os.path.join(job_specific_directory, f'data.json')
    with open(job_file_path, 'w') as file:
        json.dump(job_data, file)

    return job_specific_directory
