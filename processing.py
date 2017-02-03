#!/usr/bin/python

"""
This script takes the following iput:
1. No. of files to be processed
2. Last processed time
3. No. of slaves to be launched
4. Full Bucket url
"""
import os
import boto3
import sys


N = str(sys.argv[1])
bucket_name = str(sys.argv[2])

def recent_N(N, bucket_name):

	#"s3cmd get $(s3cmd ls s3://episourceexperiment2/ | tail -5 | awk '{ print $4 }')"
	nrecent = 's3cmd ls ' + 's3://' + bucket_name + '/ | head -' + N +  "| awk '{ print $4 }'" + "| sed 's/s3:\/\/" + bucket_name +"\///'"

	output = os.popen(nrecent).read().split()

	return output



# print recent_N(str(5), "episourceexperiment2")

def get_details(N, bucket_path):

	s3 = boto3.resource('s3')
	files = recent_N(str(N), bucket_path)
	bucket = s3.Bucket(bucket_path)

	for obj in bucket.objects.all():
		key = obj.key
		print key
		if key in files:
			body = obj.get()['Body'].read()
			s3.Bucket('episourceexperiment2resized').put_object(Key=key, Body=body)





print get_details(N, str(bucket_name))










'''

s3 = boto3.resource('s3')
bucket = s3.Bucket('test-bucket')
# Iterates through all the objects, doing the pagination for you. Each obj
# is an ObjectSummary, so it doesn't contain the body. You'll need to call
# get to get the whole body.
for obj in bucket.objects.all():
    key = obj.key
    body = obj.get()['Body'].read()

'''