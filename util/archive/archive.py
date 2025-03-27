# archive.py
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

from ann_package import job

import boto3
from botocore.exceptions import ClientError
from botocore.client import Config

# Import utility helpers ??
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# --------
# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'archive_config.ini'))
vault_name = config['aws']['VaultName']
archive_queue_url = config['aws']['ArchiveQueueUrl']
table_name = config['aws']['TableName']

# s3
sqs = boto3.client('sqs')
s3 = boto3.resource('s3', config=Config(signature_version='s3v4', region_name='us-east-1'))
s3_client = boto3.client('s3')

# Add utility code here
def upload_to_glacier(file_path):
    # Create a Glacier client
    glacier_client = boto3.client('glacier')

    # Open the file you want to upload
    with open(file_path, 'rb') as file:
        try:
            # Upload the archive to Glacier
            response = glacier_client.upload_archive(vaultName=vault_name, body=file)
            # print(f"Archive uploaded: {response['archiveId']}")
            return response['archiveId']
        except ClientError as e:
            print(f"Error uploading archive: {e}")
            return None

def upload_archive_id(job_id, archive_id):
    """upload Glacier archive id to DynamoDb"""
    dynamo = boto3.resource('dynamodb')
    table = dynamo.Table(table_name)

    # Define the new fields and their values you want to add
    new_fields = {
        'results_file_archive_id': archive_id,
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
        # print("Update Item succeeded:", response)
    except Exception as e:
        print("Error updating item:", e)
    return

def poll_messages():
    while True:
        # Poll the message queue
        messages = sqs.receive_message(
            QueueUrl=archive_queue_url,
            MaxNumberOfMessages=1,  # Adjust if you want to process more messages per request
            WaitTimeSeconds=5  # Enable long polling, up to 20 seconds
        )

        if 'Messages' in messages:  # Check if any messages were returned
            for message in messages['Messages']:
                # ast.iteraleval : parse dict from str
                # https://medium.com/@aniruddhapal/the-power-of-the-ast-literal-eval-method-in-python-8fb4014a2574
                try :
                    body = ast.literal_eval(message['Body'])
                    # print("message body: " , body, "\n")
                    data = ast.literal_eval(body['Message'])

                    job_id=data['job_id']
                    user_id=data['user_id']
                    s3_key_result_file=data['s3_key_result_file']
                    s3_results_bucket=data['s3_results_bucket']

                    # Create local directory
                    result_file_name = s3_key_result_file.split('/')[-1]
                    job_id_path, _f = s3_key_result_file.split('~')         # "nichada/UserX/ce848421-d5dc-4899-afb9-4043381cac5f"
                    job_specific_directory = job.create_job(s3_key_result_file, job_id_path, result_file_name) #"./job_data/nichada/UserX/ce848421-d5dc-4899-afb9-4043381cac5f/"

                except Exception as e:
                    print(f'Error while parsing message. {e}')
                    sqs.delete_message(
                        QueueUrl=archive_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    print(f"Deleted failed message: {job_id} from the queue")
                    continue  # go to next message

                profile = helpers.get_user_profile(id=user_id)

                if profile['role'] != 'premium_user':
                    # Download result file S3 object to a local file for free users
                    # https://boto3.amazonaws.com/v1/documentation/api/1.9.42/guide/s3-example-download-file.html
                    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/download_file.html
                    try:
                        s3.meta.client.download_file(s3_results_bucket, s3_key_result_file, f'{job_specific_directory}/{result_file_name}')
                        # print("File download succeeded!\n")
                    except ClientError as e:
                        error_code = e.response['Error']['Code']
                        if error_code == "403":
                            print(f"Key not found. \n{e}")
                        else:
                            print(f"An unexpected error occurred: {e}")
                    except Exception as e:
                        print(f"An unexpected error occurred: {e}")

                    # ------------------------------------------
                    # ---------- Upload to Glacier -------------
                    result_file = f'{job_specific_directory}/{result_file_name}'
                    archive_id = upload_to_glacier(result_file)
                    upload_archive_id(job_id, archive_id)

                    # Clean up (delete) local job files
                    try:
                        os.remove(result_file)
                        # print(f"File result file deleted successfully.")
                    except OSError as e:
                        print(f"Error deleting file '{result_file}' : {str(e)}")


                    # Delete the message from the queue if job was successfully submitted
                    try :
                        sqs.delete_message(
                            QueueUrl=archive_queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        # print(f"Deleted message: {job_id} from the queue\n")
                    except Exception as e:
                        print(f"Failed to delete message from the queue: {e} \n")

                    # Delete result files from s3 bucket
                    s3_client.delete_object(Bucket=config['aws']['ResultBucketName'], Key=s3_key_result_file)
                    # print(f"Deleted result file from from s3 bucket\n")


if __name__ == "__main__":
    poll_messages()

### EOF
