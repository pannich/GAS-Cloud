# thaw.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import ast
import time

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
# import helpers

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('thaw_config.ini')
thaws_queue_url = config['aws']['ThawsQueueUrl']
s3_results_bucket=config['aws']['ResultBucketName']


# s3
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
sqs = boto3.client('sqs')
vault_name = config['aws']['VaultName']
table_name = config['aws']['TableName']

# make local path
download_dir = config['local']['DownloadDir']
os.makedirs(download_dir, exist_ok=True)

def check_job_status(job_id):
    glacier_client = boto3.client('glacier')
    try:
        response = glacier_client.describe_job(vaultName=vault_name, jobId=job_id)
        return response['StatusCode']
    except ClientError as e:
        print(f"Error checking job status: {e}")
        raise

def download_restored_file(job_id, download_path):
    glacier_client = boto3.client('glacier')
    try:
        response = glacier_client.get_job_output(vaultName=vault_name, jobId=job_id)

        # Write the response data to a file
        with open(download_path, 'wb') as file:
            file.write(response['body'].read())
        # print(f"Downloaded to local successfully. Job id: {job_id}")
    except ClientError as e:
        print(f"Error downloading restored file: {e}")
        raise

def upload_file_to_s3(file_path, bucket_name, object_name):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_path, bucket_name, object_name)
        # print("Uploaded retrived file to s3 successfully")
    except ClientError as e:
        print(f"Error uploading file to S3: {e}")
        raise

def update_restore_message(job_id):
    """update restore message to '' DynamoDb"""
    dynamo = boto3.resource('dynamodb')
    table = dynamo.Table(table_name)

    # Define the new fields and their values you want to add
    new_fields = {
        'restore_message': "",
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
        # print("Update Restore Message succeeded for job_id: ", job_id)
    except Exception as e:
        print("Error updating item:", e)
    return
# ----------------------- End Utility ---------------------------------------
## ----------------------Start Polling----------------------------------------

def poll_messages():
    # listen to thaw topic
    while True:
        messages = sqs.receive_message(
            QueueUrl=thaws_queue_url,
            MaxNumberOfMessages=10,      # Adjust if you want to process more messages per request
            WaitTimeSeconds=20          # Enable long polling, up to 20 seconds
        )

        if 'Messages' in messages:  # Check if any retrieval initations were submitted
            # print("--- thaw poll ---")
            for message in messages['Messages']:
                try :   # parse retrieval_job_id, s3_key_result_file, job_id from message
                    body = ast.literal_eval(message['Body'])
                    data = ast.literal_eval(body['Message'])

                    retrieval_job_id = data['retrieval_job_id']
                    s3_key_result_file = data['s3_key_result_file']
                    job_id = data['job_id']

                    # print(f"poll {job_id}")
                except Exception as e:
                    print(f'Error while parsing message. {e}')
                    sqs.delete_message(
                        QueueUrl=thaws_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    print(f"Deleted bad request message from the queue")
                    continue  # go to next message

                # ---- process the retrieval_job_id -----
                # download restore files to local
                status_code = check_job_status(retrieval_job_id)
                download_path = os.path.join(download_dir, retrieval_job_id)

                if status_code == 'Succeeded':
                    download_restored_file(retrieval_job_id, download_path)

                    # move restore files to s3
                    upload_file_to_s3(download_path, s3_results_bucket, s3_key_result_file)

                    # Delete the message from the queue if job was successfully submitted
                    try :
                        sqs.delete_message(
                            QueueUrl=thaws_queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        print(f"Deleted : {s3_key_result_file} from the queue\n")
                    except Exception as e:
                        print(f"Failed to delete message from the queue: {e} \n")

                    # Update database to signal web to show download link
                    update_restore_message(job_id)

        time.sleep(60) # wait between each poll

if __name__ == "__main__":
    poll_messages()

### EOF
