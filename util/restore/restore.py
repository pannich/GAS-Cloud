# restore.py
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
import json

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('restore_config.ini')
vault_name = config['aws']['VaultName']
subscription_queue_url = config['aws']['SubscriptionUpgradeQueueUrl']
table_name = config['aws']['TableName']
thaws_topic_arn = config['aws']['ThawsTopicArn']

# s3
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
sqs = boto3.client('sqs')


def initiate_retrieval(archive_id, retrieval_type='Expedited'):
    glacier_client = boto3.client('glacier')
    try:
        # Initiate the retrieval job
        response = glacier_client.initiate_job(
            vaultName=vault_name,
            jobParameters={
                'Type': 'archive-retrieval',
                'ArchiveId': archive_id,
                'Tier': retrieval_type
            }
        )
        return response['jobId']
    except ClientError as e:
        print(e.response['Error']['Code']) # debugging
        if e.response['Error']['Code'] == 'InsufficientCapacityException':
            print("Expedited retrieval failed, falling back to standard retrieval.")
            return initiate_retrieval(archive_id, 'Standard')       # fall back to standard type
        else:
            print(f"Error initiating retrieval job: {e}")
            raise e

def publish_sns_topic(topic_arn, data, MessageDeduplicationId):
    # Publish retrieval_job_id to thaw SNS Topic - - wait for thaw queue to check job complete
    sns_client = boto3.client('sns')
    try:
        # Publish the message to the SNS topic
        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=str(data),
            MessageGroupId='annotations_jobs',
            MessageDeduplicationId=MessageDeduplicationId
        )
        # print(f"Published message to SNS topic for key: {data['s3_key_result_file']}")
        return
    except ClientError as e:
        print(f"Couldn't publish message to topic {topic_arn}. Error {e}. \n")
        raise

def put_restore_message(job_id):
    """put restore message to DynamoDb"""
    dynamo = boto3.resource('dynamodb')
    table = dynamo.Table(table_name)

    # Define the new fields and their values you want to add
    new_fields = {
        'restore_message': "Your file is being restored from archive and will be available shortly.",
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


def poll_messages():
    while True:
        # Poll the message queue for the new upgraded users
        # query all jobs associate to this users
        # submit retrival initiation and post to thaw queue.
        messages = sqs.receive_message(
            QueueUrl=subscription_queue_url,
            MaxNumberOfMessages=10,  # Adjust if you want to process more messages per request
            WaitTimeSeconds=5  # Enable long polling, up to 20 seconds
        )

        if 'Messages' in messages:  # Check if any messages (newly upgrade user) were returned
            for message in messages['Messages']:
                # ast.iteraleval : parse dict from str
                # https://medium.com/@aniruddhapal/the-power-of-the-ast-literal-eval-method-in-python-8fb4014a2574
                try :
                    body = ast.literal_eval(message['Body'])
                    data = ast.literal_eval(body['Message'])

                    user_id=data['user_id']
                except Exception as e:
                    print(f'Error while parsing message. {e}')
                    sqs.delete_message(
                        QueueUrl=subscription_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    print(f"Deleted bad request message from the queue")
                    continue  # go to next message

                # --------------------------------
                # Get glacier archive id from database
                # Initialize a DynamoDB client
                dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
                table = dynamodb.Table(table_name)

                try :
                    response = table.query(
                        IndexName='user_id_index',  # If querying on a secondary index
                        KeyConditionExpression=Key('user_id').eq(user_id))
                except ClientError as e:
                    error_code = e.response['Error']['Code']
                    print(f"An unexpected error occurred: {e}. Error Code : {error_code}")
                except Exception as e:
                    print(e)

                # ---- retrieve from Glacier ----
                # for each result files associate with user_id, initiate retrieval, send to thaw topic
                result_files = response['Items']
                # print(f"get result_files : {result_files}") # debug

                for result in result_files:
                    archive_id = result['results_file_archive_id']
                    s3_key_result_file = result['s3_key_result_file']
                    job_id = result['job_id']

                    # send retrieval_job_idto thaw SNS topic
                    retrieval_job_id = initiate_retrieval(archive_id)
                    data = {
                        "retrieval_job_id" : retrieval_job_id,
                        "s3_key_result_file" : s3_key_result_file,
                        "job_id" : job_id
                        }
                    publish_sns_topic(thaws_topic_arn, data, MessageDeduplicationId=data['retrieval_job_id'])

                    # update db with restore message
                    put_restore_message(job_id)

                # Delete the message from the queue if job was successfully submitted
                try :
                    sqs.delete_message(
                        QueueUrl=subscription_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    # print(f"Deleted message: {retrieval_job_id} from the queue")
                except Exception as e:
                    print(f"Failed to delete message from the queue: {e}")


if __name__ == "__main__":
    poll_messages()

### EOF
