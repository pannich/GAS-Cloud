# final proj run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import sys
import os
import time
import driver
import json
from decimal import Decimal


import boto3
from botocore.exceptions import ClientError

import logging
# Configure logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

# Get util configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'ann_config.ini'))

# Access configurations
s3_results_bucket = config['aws']['ResultBucketName']
table_name = config['aws']['TableName']
queue_url = config['aws']['QueueUrl']


"""A rudimentary timer for coarse-grained profiling
"""
class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")

# Custom JSON encoder for handling Decimal objects
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)


if __name__ == '__main__':
    """
    Example attributes:
    input_file_path : "/home/ec2-user/mpcs-cc/job_data/nichada/UserX/b94b246e-5d69-4b50-942f-99b465eb0d22/b94b246e-5d69-4b50-942f-99b465eb0d22~test.vcf"
    job_id : "123somerandomnumber"
    """
    input_file_path = sys.argv[1]
    job_id = sys.argv[2] #new
    CNETID, USER, _job_id, input_file = input_file_path.split('job_data')[1].split('/')[1:]

    filename_noext, _ext = os.path.splitext(input_file) # a2434f26-3ee4-499d-ba88-1d6dd9410197~free_1
    result_file = os.path.splitext(input_file_path)[0]+'.annot.vcf' #full path locally
    log_file = os.path.splitext(input_file_path)[0]+'.vcf.count.log'

    result_bucket_path = f'{CNETID}/{USER}/{job_id}/{filename_noext}'

    # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        with Timer():
            driver.run(input_file_path, 'vcf')

        ## JOB COMPLETED
        complete_time = int(time.time())

        # Upload results
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.upload_file(result_file, s3_results_bucket, f'{result_bucket_path}.annot.vcf')
        # print(f"uploaded {s3_results_bucket}")

        s3.upload_file(result_file, s3_results_bucket, f'{result_bucket_path}.vcf.count.log')

        # 3. Clean up (delete) local job files
        try:
            os.remove(result_file)
            print(f"File result file deleted successfully.")
            os.remove(log_file)
            print(f"File log file deleted successfully.")
        except OSError as e:
            print(f"Error deleting file '{result_file}' or '{log_file}': {str(e)}")

    else:
        print("A valid .vcf file must be provided as input to this program.")

    ##---------------- Update job detail to DB --------------------------
    dynamo = boto3.resource('dynamodb')
    table = dynamo.Table('nichada_annotations')

    # Define the new fields and their values you want to add
    new_fields = {
        's3_results_bucket': s3_results_bucket,
        's3_key_result_file': f'{result_bucket_path}.annot.vcf',
        's3_key_log_file': f'{result_bucket_path}.vcf.count.log',
        'complete_time': complete_time,
        'job_status': "COMPLETED"
    }

    update_expression = 'SET ' + ', '.join(f'{k} = :{k}' for k in new_fields.keys())
    expression_attribute_values = {f':{k}': v for k, v in new_fields.items()}

    try:
        response = table.update_item(
            Key={'job_id': job_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues='ALL_NEW'  # Returns all attributes of the item after the update
        )
        print("Update Item succeeded:", response)
    except Exception as e:
        print("Error updating item:", e)

    # Send message to request queue results
    # Move your code here...
    # Publish to SNS Topic
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
    sns_client = boto3.client('sns')
    topic_arn = 'arn:aws:sns:us-east-1:659248683008:nichada_job_results.fifo'  # TODO hardcode?

    try:
        message = json.dumps(response['Attributes'], cls=DecimalEncoder)
        # print(f"\n\n Posting : {message}\n")
        # Publish the message to the SNS topic
        sns_response = sns_client.publish(
            TopicArn=topic_arn,
            Message=message,      # TODO
            MessageGroupId='annotations_jobs',
            MessageDeduplicationId=job_id
        )
        message_id = sns_response["MessageId"]
        # print("Published message to SNS topic: %s", sns_response) # debug
        logger.info("Published message to SNS topic: %s", sns_response)  # Logging at INFO level

    except ClientError as e:
        logger.exception("Couldn't publish message to topic %s.", topic_arn)
        raise  # Re-raises the last exception ??
