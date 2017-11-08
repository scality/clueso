#!/bin/python
import boto3
import json
from botocore.exceptions import ClientError

session = boto3.session.Session()

s3 = session.client(
    service_name='s3',
    endpoint_url='http://localhost:8000',
)

s3Resource = session.resource(
    service_name='s3',
    endpoint_url='http://localhost:8000',
)

bucketName = "foo"
sparkPrefix = "landing/_spark_metadata/"
try:
    s3.create_bucket(Bucket=bucketName)
    print "Created bucket: %s" % bucketName
except ClientError as e:
    if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
        print "Bucket already owned by you"
    else:
        print "Unexpected error: %s" % e
        raise ValueError('Unable to create METADATA bucket')

keysToDelete = []
bucket = s3Resource.Bucket(bucketName)

for obj in bucket.objects.filter(Prefix=sparkPrefix):
    keysToDelete.append({'Key': obj.key})
    print "Found object to delete: %s" % obj.key

response = s3.delete_objects(
    Bucket=bucketName,
    Delete={
        'Objects': keysToDelete,
        'Quiet': False
    },
)

print response
