#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import httplib
import os
import socket
import sys
import time
import urllib

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


def search(bucket_name, search_query):
    params = urllib.urlencode({'search': search_query})
    httpconn = httplib.HTTPConnection(S3_ENDPOINT_URL.replace('http://',''))
    httpconn.request("GET", "/%s?%s" % (bucket_name, params))
    response = httpconn.getresponse().read()
    print(" RESPONSE = %s" % response)
    return response


def send_metrics(metric_name, value):
    message = "%s %d %d\n" % (metric_name, value, int(time.time()))
    if sock is not None:
        sock.sendall(message)

def searchQueryForMetadata(metadata):
    return ' AND '.join([ "userMd.`x-amz-meta-%s`='%s'" % (k,v) for k,v in metadata.iteritems() ])


def print_usage():
    print "./search.py <bucket_name> <search query> [<limit>]"

if __name__ == "__main__":
    if len(sys.argv) < 3 or len(sys.argv) > 5:
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
    send_metrics("search_time.%s.milli" % (bucket_name,), duration)
    sock.close()
