# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template,
  request, session, url_for)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile

import logging
# Configure logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)
subscription_topic_arn = app.config['AWS_SNS_SUBSCRIPTION_UPGRADE_TOPIC']

"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Create a session client to the S3 service
  s3 = boto3.client('s3',
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + '/job'

  # Define policy fields/conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name,
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL for upload: {e}")
    return abort(500)

  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html', s3_post=presigned_post)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():

  # Get bucket name, key, and job ID from the S3 redirect URL
  bucket_name = str(request.args.get('bucket'))
  s3_key = str(request.args.get('key'))

  # Extract the job ID from the S3 key
  input_file = s3_key.split('/')[-1]
  job_id = input_file.split('~')[0]

  # Persist job to database
  # Move your code here..
  dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
  table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  print(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  submit_time = int(time.time())
  user_id = session['primary_identity']
  user_email = session['email']

  data = {
    "job_id": job_id,
    "user_id": user_id,
    "user_email": user_email, # new
    "input_file_name": input_file,
    "s3_inputs_bucket": bucket_name,
    "s3_key_input_file": s3_key,
    "submit_time": submit_time,
    "job_status": "PENDING"
  }

  # error handling for put item to DynamoDB
  # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html
  try:
    response = table.put_item(Item=data)
    print("Item successfully inserted: PENDING \n")
  except ClientError as e:
    # Handle specific DynamoDB errors
    error_code = e.response['Error']['Code']
    if error_code == 'ConditionalCheckFailedException':
        print("Conditional check failed, item may already exist or condition not met")
    elif error_code == 'ProvisionedThroughputExceededException':
        print("Provisioned throughput exceeded, consider increasing throughput or implementing backoff/retry strategies")
    elif error_code == 'ValidationException':
        print("Validation error:", e.response['Error']['Message'])
    else:
        print("Unexpected error:", e.response['Error']['Message'])
  except Exception as e:
    print("Error inserting item:", str(e))

  # Send message to request queue
  # Move your code here...
  # Publish to SNS Topic
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
  sns_client = boto3.client('sns')
  topic_arn = app.config['AWS_SNS_JOB_REQUEST_TOPIC']

  try:
    # Publish the message to the SNS topic
    response = sns_client.publish(
        TopicArn=topic_arn,
        Message=str(data),
        MessageGroupId='annotations_jobs',
        MessageDeduplicationId=job_id
    )
    message_id = response["MessageId"]

    logger.info("Published message to SNS topic: %s", response)  # Logging at INFO level

  except ClientError as e:
    logger.exception("Couldn't publish message to topic %s.", topic_arn)
    raise  # Re-raises the last exception ??

  return render_template('annotate_confirm.html', job_id=job_id)


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
  user_id = session['primary_identity']

  # Initialize a DynamoDB client
  dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
  table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])  # Replace 'YourTableName' with your actual table name

  try :
    response = table.query(
          IndexName='user_id_index',  # If querying on a secondary index
          KeyConditionExpression=Key('user_id').eq(user_id))
  except ClientError as e:
    error_code = e.response['Error']['Code']
    print(f"An unexpected error occurred: {e}. Error Code : {error_code}")
  except Exception as e:
    print(e)
    return internal_error(e)
  annotations = response['Items']
  for annotation in annotations:
    annotation['submit_time'] = convert_epoch_to_datetime(annotation['submit_time'])

  return render_template('annotations.html', annotations=annotations)


"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
  # Create a session client to the S3 service
  s3 = boto3.client('s3',
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))

  # Initialize a DynamoDB client
  dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
  table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])  # Replace 'YourTableName' with your actual table name

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']

  # query for job
  try :
    response = table.get_item(Key = { 'job_id': id })
  except ClientError as e:
    error_code = e.response['Error']['Code']
    print(f"An unexpected error occurred: {e}. Error Code : {error_code}")
  except Exception as e:
    print(e)
    return internal_error(e)

  # parsing the results
  annotation = response['Item']

  # -- Authorize user --
  if user_id != annotation['user_id']:
    print('Unauthorized User')
    return forbidden('Unauthorized')

  # Generate the presigned URL for input download
  try:
    presigned_url_input = s3.generate_presigned_url(
      'get_object',
      Params={
      'Bucket': app.config['AWS_S3_INPUTS_BUCKET'],
      'Key': annotation['s3_key_input_file'],
      },
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL for download input: {e}")
    return abort(500)
  annotation['input_file_url'] = presigned_url_input

  if 's3_key_result_file' in annotation:
    # Generate the presigned URL for result download
    try:
      presigned_url_result = s3.generate_presigned_url('get_object',
              Params={
                  'Bucket': app.config['AWS_S3_RESULTS_BUCKET'],
                  'Key': annotation['s3_key_result_file']
              },
        ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
    except ClientError as e:
      app.logger.error(f"Unable to generate presigned URL for download result: {e}")
      return abort(500)
    annotation['result_file_url'] = presigned_url_result

  annotation['submit_time'] = convert_epoch_to_datetime(annotation['submit_time'])
  if 'complete_time' in annotation:
    annotation['complete_time'] = convert_epoch_to_datetime(annotation['complete_time'])

  free_access_expired = False
  if session['role'] != 'premium_user':
    if 'results_file_archive_id' in annotation:     # passed 5 minutes
      free_access_expired = True

  if annotation.get('restore_message') == '':
      del annotation['restore_message']

  return render_template('annotation_details.html', annotation=annotation, free_access_expired=free_access_expired)


"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
  # Initialize a DynamoDB client
  dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
  table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])  # Replace 'YourTableName' with your actual table name
  user_id = session['primary_identity']

  # query for job
  try :
    response = table.get_item(Key = { 'job_id': id })
  except ClientError as e:
    error_code = e.response['Error']['Code']
    print(f"An unexpected error occurred: {e}. Error Code : {error_code}")
  except Exception as e:
    print(e)
    return internal_error(e)
  # parsing the results
  annotation = response['Item']
  # -- Authorize user --
  if user_id != annotation['user_id']:
    print('Unauthorized User')
    return forbidden('Unauthorized')
  log_file_key = annotation['s3_key_log_file']

  # Create a session client to the S3 service
  s3 = boto3.client('s3',
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))
  try:
    # Fetch the file object from S3
    file_obj = s3.get_object(Bucket=app.config['AWS_S3_RESULTS_BUCKET'], Key=log_file_key)
    # Read the file content
    log_file_contents = file_obj['Body'].read().decode('utf-8')
  except Exception as e:
    print(f"Error fetching file from S3: {e}")

  return render_template('view_log.html', log_file_contents=log_file_contents, job_id=id)

"""Subscription management handler
"""
@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
    if (session.get('role') == "free_user"):
      return render_template('subscribe.html')
    else:
      return redirect(url_for('profile'))

  elif (request.method == 'POST'):
    # Update user role to allow access to paid features
    update_profile(
      identity_id=session['primary_identity'],
      role="premium_user"
    )

    # Update role in the session
    session['role'] = "premium_user"
    user_id = session['primary_identity']

    # Request restoration of the user's data from Glacier
    # Add code here to initiate restoration of archived user data
    # Make sure you handle files not yet archived!

    # Publish user_id to upgrade subscription SNS Topic - - send to util instance to retrieve Glacier
    sns_client = boto3.client('sns')
    data = {"user_id" : user_id}

    try:
        # Publish the message to the SNS topic
        response = sns_client.publish(
            TopicArn=subscription_topic_arn,
            Message=str(data),
            MessageGroupId='annotations_jobs',
            MessageDeduplicationId=user_id
        )

        logger.info("Published message to SNS topic: %s", response)  # Logging at INFO level

    except ClientError as e:
        logger.exception("Couldn't publish message to topic %s.", subscription_topic_arn)
        raise  # Re-raises the last exception ??

    # Display confirmation page
    return render_template('subscribe_confirm.html')

"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )
  return redirect(url_for('profile'))

# --------------------Util functions----------------------------
def convert_epoch_to_datetime(epoch):
    return datetime.utcfromtimestamp(int(epoch)).strftime('%Y-%m-%d %H:%M:%S UTC')




"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html',
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
    ), 500

### EOF
