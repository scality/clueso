#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import httplib
import os
import random
import socket
import string
import sys
import time
import urllib
from botocore.exceptions import ClientError
from botocore.exceptions import ConnectionError

session = boto3.session.Session()

S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL', 'http://lb')

# Carbon settings
CARBON_SERVER = os.getenv('CARBON_SERVER', '0.0.0.0')
CARBON_PORT = int(os.getenv('CARBON_PORT', '2003'))

session = boto3.session.Session()
s3 = session.client(
    service_name='s3',
    endpoint_url=S3_ENDPOINT_URL,
)

s3Resource = session.resource(
    service_name='s3',
    endpoint_url=S3_ENDPOINT_URL,
)

if CARBON_SERVER != '':
    sock = socket.socket()
    sock.connect((CARBON_SERVER, CARBON_PORT))

BUCKET_NAME = 'perf-testing'
METADATA_PROPS = ['make', 'color']


def setup():
    global s3
    while (True):
        try:
            s3.create_bucket(Bucket=BUCKET_NAME)
            print "Created bucket: %s" % BUCKET_NAME
            b = s3Resource.Bucket(BUCKET_NAME)
            bucketAcl = s3Resource.BucketAcl(BUCKET_NAME)
            bucketAcl.put(ACL='public-read')
            print("Set ACL = public-read for bucket %s" % BUCKET_NAME)
            break
        except ClientError as e:
            if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                print "Bucket already owned by you"
                break
            else:
                print "Unexpected error: %s" % e
                raise ValueError("Unable to create %s bucket" % BUCKET_NAME)
        except ConnectionError as e:
            print "Connection error: %s. Retrying in 3 seconds..." % e
            time.sleep(3)



def search(bucket_name, search_query):
    params = urllib.urlencode({'search': search_query})
    httpconn = httplib.HTTPConnection(S3_ENDPOINT_URL.replace('http://',''))
    httpconn.request("GET", "/%s?%s" % (BUCKET_NAME, params))
    response = httpconn.getresponse().read()
    print(" RESPONSE = %s" % response)
    return response


def createFile(bucket_name, key):
    global s3
    print("Creating " + bucket_name + "/" + key)
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body='')

def send_metrics(metric_name, value):
    message = "%s %d %d\n" % (metric_name, value, int(time.time()))
    if sock is not None:
        sock.sendall(message)

def searchQueryForMetadata(metadata):
    return ' AND '.join([ "userMd.`x-amz-meta-%s`='%s'" % (k,v) for k,v in metadata.iteritems() ])

def randomMetadata():
    return { random.choice(METADATA_PROPS): randomString(7) }

def randomString(N):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))

def applyMetadata(bucket_name, key, metadata):
    global s3Resource
    s3Resource.Object(bucket_name, key).put(Metadata=metadata)

def print_usage():
    print "./search.py <bucket_name> <search query> [<limit>]"

if __name__ == "__main__":
    setup()

    if len(sys.argv) < 3 || len(sys.argv) > 5:
        print_usage()
        sys.exit(-1)

    bucket_name = sys.argv[1]
    search_query = sys.argv[2]


    print("Clueso Search Tool – bucket = %s query: %s" % (bucket_name, search_query))

    start_ts = time.time() * 1000
    response = search(bucket_name, search_query)
    print(response)
    duration = time.time() * 1000 - start_ts

    print("Took %d milli." % (duration,))
    send_metrics("search_time.%s.milli" % (BUCKET_NAME,), duration)
    sock.close()
