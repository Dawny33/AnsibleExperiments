#!/usr/bin/python

"""
This script takes the following iput:
1. No. of files to be processed
2. Last processed time
3. No. of slaves to be launched
4. Full Bucket url
"""
import os
import sys
import boto3
from boto3 import session
from botocore.client import Config


N = str(sys.argv[1])
bucket_name = str(sys.argv[2])

def recent_N(N, bucket_name):

	nrecent = 'aws s3 ls s3://' + bucket_name + '/ | sort | tail -n' + N + "| awk '{ print $4 }'"

	output = os.popen(nrecent).read().split()

	return output



def get_details(N, bucket_name):

	session = boto3.session.Session()
	s3client = session.client('s3', config = boto3.session.Config(signature_version='s3v4'))
	s3 = boto3.resource('s3')
	files = recent_N(str(N), bucket_name)
	bucket = s3.Bucket(bucket_name)

	for obj in bucket.objects.all():
		key = obj.key
		if key in files:
			body = s3client.get_object(Bucket=bucket_name, Key=key)['Body'].read()
			s3client.put_object(Key=key, Body=body, Bucket='ansibletest1')





print get_details(N, str(bucket_name))
