#!/usr/bin/env python

import os
import sys
import boto3
from boto3 import session
from botocore.client import Config
import json
import base64

# Get the service resource
sqs = boto3.resource('sqs')

# Get the queue
#queue = sqs.get_queue_by_name(QueueName='DawnyQueue')
queue = sqs.Queue('https://us-west-2.queue.amazonaws.com/080983167913/DawnyQueue')
dynamo = boto3.resource('dynamodb')
table = dynamo.Table('iocheck')

messages_available = queue.attributes['ApproximateNumberOfMessages']

# Process messages by printing out body and optional author name
for message in queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=5):

    session = boto3.session.Session()
    s3client = session.client('s3', config = boto3.session.Config(signature_version = 's3v4'))
    s3 = boto3.resource('s3')

    bucket_name = 'sourcebucketepi'

    body_json = json.loads(message.body)

    sqs_key = body_json['Records'][0]['s3']['object']['key']

    decoded_key = str(base64.decodestring(sqs_key.replace('%3D', '=').replace('%0A', '')))

    table.update_item(Key={'filename': sqs_key ,'status': 'Done',})


    body = s3client.get_object(Bucket=bucket_name, Key=decoded_key)['Body'].read()
    s3client.put_object(Key=sqs_key, Body=body, Bucket='destbucketepi')

    message.delete()

    # Let the queue know that the message is processed
