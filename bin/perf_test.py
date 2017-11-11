#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import time
import random
import string
import socket
from subprocess import call, check_output, STDOUT
import xml.etree.ElementTree as ET

# Carbon settings
CARBON_SERVER = '0.0.0.0'
CARBON_PORT = 2003

sock = socket.socket()
sock.connect((CARBON_SERVER, CARBON_PORT))

BUCKET_NAMES = ['wednesday', 'gobig']
METADATA_PROPS = ['make', 'color']

def search(bucket_name, search_query):
  cmd = "./search.sh %s %s" % (bucket_name, search_query)
  print("  Exec cmd " + cmd)
  return check_output(cmd, stderr=STDOUT, shell=True)

def createFile(bucket_name, key):
  print("Creating " + bucket_name + "/" + key)
  call(["s3cmd", "put", "./a_file", "s3://%s/%s" % (bucket_name, key)])

def send_metrics(metric_name, value):
  message = "%s %d %d\n" % (metric_name, value, int(time.time()))
  sock.sendall(message)

def searchQueryForMetadata(metadata):
  return ' AND '.join([ "userMd.\`x-amz-meta-%s\`=\\\\\\\"%s\\\\\\\"" % (k,v) for k,v in metadata.iteritems() ])

def randomMetadata():
 return { random.choice(METADATA_PROPS): randomString(7) }
  
def randomString(N):
  return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))

def applyMetadata(bucket_name, key, metadata):
  metadata_str = ",".join([ "%s=%s" % (k,v) for k,v in metadata.iteritems() ])
  call(["aws","s3api","put-object","--bucket", bucket_name, "--key", key, "--metadata", metadata_str, "--endpoint-url", "http://127.0.0.1"])

if __name__ == "__main__":
  # create an empty file for us to upload
  call(["touch","a_file"])

  i = 0
  loops = int(sys.argv[1])

  print("Clueso Perf Test Tool – loops = %d" % (loops,))
  while (i < loops):
    bucket_name = random.choice(BUCKET_NAMES)
    key = randomString(10)
    createFile(bucket_name, key)
    metadata = randomMetadata()
    applyMetadata(bucket_name, key, metadata)
    search_query = searchQueryForMetadata(metadata)

    has_valid_result = False
    start_ts = time.time() * 1000
    duration = None

    while (not has_valid_result):
      print("Executing search")
      response = search(bucket_name, search_query) 
      print("  Got " + response)
      search_results_str = response[response.find("<?xml"):]
      # parse xml results
      xml_root = ET.fromstring(search_results_str)
      for content in xml_root.iter('{http://s3.amazonaws.com/doc/2006-03-01/}Contents'):
        if (content.find('{http://s3.amazonaws.com/doc/2006-03-01/}Key').text == key):
          has_valid_result = True
          duration = time.time() * 1000 - start_ts
          break

    print("Took %d milli." % (duration,))
    send_metrics("data_availability.%s.milli" % (bucket_name,), duration)

    i += 1
    if (i % 1 == 0):
      print("Completed %d iterations." % (i,))

  sock.close()
