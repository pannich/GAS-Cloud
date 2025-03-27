# annotator.py
#
#

from ann_package import job
import boto3
import subprocess, os, ast
from botocore.client import Config
from botocore.exceptions import ClientError

# Get util configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'ann_config.ini'))

# Access configurations
table_name = config['aws']['TableName']
request_queue_url = config['aws']['QueueUrl']

# Initialize Boto3 clients for SQS and S3
sqs = boto3.client('sqs')
s3 = boto3.resource('s3', config=Config(signature_version='s3v4', region_name='us-east-1'))

mpcs_path = '/home/ec2-user/mpcs-cc'    # local path

def poll_messages():
    while True:
        # Poll the message queue
        messages = sqs.receive_message(
            QueueUrl=request_queue_url,
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
                    input_file_name=data['input_file_name']
                    s3_inputs_bucket=data['s3_inputs_bucket']
                    s3_key_input_file=data['s3_key_input_file']
                    job_status=data['job_status']
                    # Local directory
                    job_id_path, _f = s3_key_input_file.split('~') #"nichada/UserX/ce848421-d5dc-4899-afb9-4043381cac5f"
                    job_specific_directory = job.create_job(s3_key_input_file, job_id_path, input_file_name) #"/home/ec2-user/mpcs-cc/job_data/nichada/UserX/ce848421-d5dc-4899-afb9-4043381cac5f"

                except Exception as e:
                    print(f'Error while parsing message. {e}')
                    sqs.delete_message(
                        QueueUrl=request_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    print(f"Deleted failed message: {job_id} from the queue")
                    continue  # go to next message

                # Download input file S3 object to a local file
                # https://boto3.amazonaws.com/v1/documentation/api/1.9.42/guide/s3-example-download-file.html
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/download_file.html
                try:
                   s3.meta.client.download_file(s3_inputs_bucket, s3_key_input_file, f'{job_specific_directory}/{input_file_name}')
                #    print("File download succeeded!\n")
                except ClientError as e:
                    error_code = e.response['Error']['Code']
                    if error_code == "403":
                        print(f"Key not found. \n{e}")
                    else:
                        print(f"An unexpected error occurred: {e}")
                except Exception as e:
                    print(f"An unexpected error occurred: {e}")

                # Update job status to DB
                # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.UpdateItem.html
                dynamo = boto3.resource('dynamodb')
                table = dynamo.Table(table_name)   # move

                update_expression = 'SET job_status = :new_status'
                expression_attribute_values = {
                    ':new_status': 'RUNNING',
                    ':current_status': 'PENDING'
                }
                condition_expression = 'job_status = :current_status'

                #--  1. Start the annotation process in bg
                try:
                    # $ python anntools/hw4_run.py <path-of-file-to-run> <job_id>
                    child = subprocess.Popen(['python', '/home/ec2-user/mpcs-cc/anntools/run.py', os.path.join(mpcs_path, 'job_data', job_id_path, input_file_name), job_id]
                                            , stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    # ---- debugging ----
                    # out, err = child.communicate()
                    # print(out.decode())  # This prints the stdout
                    # print(err.decode())  # This prints the stderr

                    print('Start the annotation process \n')
                except Exception as e:
                    try :
                        response = table.update_item(
                        Key={ 'job_id': job_id },
                        UpdateExpression='SET job_status = :job_status',
                        ExpressionAttributeValues={':job_status': 'FAILED'},
                        ReturnValues='ALL_NEW'  # Returns all attributes of the item after the update
                        )
                        print('Error running popen. Update job status : FAILED \n')
                    except ClientError as e:
                        print(f"Error while updating job_status to FAILURE: {e.response['Error']['Message']}")
                        # go to delete message

                    print(f'Job Failed \n An exception occurred while running job: {e}')
                    # go to delete message


                #-- 2. Update conditional status
                try:
                    response = table.update_item(
                        Key={ 'job_id': job_id },
                        UpdateExpression=update_expression,
                        ExpressionAttributeValues=expression_attribute_values,
                        ConditionExpression=condition_expression,
                        ReturnValues='ALL_NEW'                  # Returns all attributes of the item after the update
                    )
                    # print("Update Item succeeded:", response['Attributes'])

                except Exception as e:
                    error_code = e.response['Error']['Code']
                    if error_code == 'ConditionalCheckFailedException':
                        print("Condition not met, item's job_status is not 'PENDING'. Update not performed.")
                    else:
                        print(f'Error while updating job status {e}. Error Code: {error_code}')
                    # go to delete message


                #-- 3. Delete the message from the queue if job was successfully submitted
                try :
                    sqs.delete_message(
                        QueueUrl=request_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    print(f"Deleted message: {job_id} from the queue")
                except Exception as e:
                    print("Failed to delete message from the queue: {e}")


if __name__ == "__main__":
    poll_messages()
