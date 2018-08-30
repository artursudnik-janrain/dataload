#!/usr/bin/env python3
"""
This script will delete records in listed success.csv (or same format)
Use for rollback script 

"""
import os
import csv
import sys
import json
import time
import requests
from dataload.reader import CsvBatchReader
from janrain.capture.cli import ApiArgumentParser
from janrain.capture import config
from janrain.capture import ApiResponseError

class DataLoadArgumentParser(ApiArgumentParser):
    def __init__(self, *args, **kwargs):
        super(DataLoadArgumentParser, self).__init__(*args, **kwargs)
        self.add_argument('-t', '--type-name', default="user",
                          help="entity type name (default: user)")
        self.add_argument('data_file', metavar="DATA_FILE",
                          help="full path to the data file being loaded")
        self.add_argument('-o', '--timeout', type=int, default=10,
                          help="timeout in seconds for API calls (default: 10)")
        self.add_argument('-r', '--rate-limit', type=float, default=1.0,
                          help="max API calls per second (default: 1)")

    def parse_args(self, args=None, namespace=None):
        args = super(ApiArgumentParser, self).parse_args(args, namespace)
        # Parse the YAML configuration here so that init_api() does not need to
        # read the config each time it's called which would not be thread safe.
        if args.config_key:
            credentials = config.get_settings_at_path(args.config_key)
        elif args.default_client:
            credentials = config.default_client()

        if args.config_key or args.default_client:
            args.client_id = credentials['client_id']
            args.client_secret = credentials['client_secret']
            args.apid_uri = credentials['apid_uri']

        self._parsed_args = args
        return self._parsed_args

parser = DataLoadArgumentParser()
args = parser.parse_args()
api = parser.init_api()

# Setup rate limiting
if args.rate_limit > 0:
    min_time = round(1 / args.rate_limit, 2)
else:
    min_time = 0

def entityDelete(uuid):
    try:
        response = api.call('entity.delete',type_name=args.type_name,uuid=uuid)
        stat = response['stat']
    except:
        stat = 'ERROR'
    return stat

# Iterate csv file
print('Loading'+' '+args.data_file)
deleteList = set();
readerRow = 1

with open(args.data_file , newline='') as csvfile:
    reader = csv.DictReader(csvfile,delimiter=',')
    for row in reader:
        #print(row)
        #print(readerRow)
        uuid= row['uuid']
        #print(uuid)
        deleteList.add(uuid)
        readerRow = readerRow + 1

count = len(deleteList)

for n in deleteList:
    last_time = time.time()
    print("deleting"+" "+n)
    response = entityDelete(n)
    print(response)
    count = count - 1
    countStr = str(count)
    print(countStr+" records left")

    # Gentle... gentle!
    if (time.time() - last_time) < min_time:
        sleepTime = min_time - (time.time() - last_time)
        sleepStr = str(sleepTime)
        print("sleep"+" "+sleepStr)
        time.sleep(sleepTime)

print('removing results logs')

os.remove("success.csv")
os.remove("fail.csv")
os.remove("500.csv")

print('Done!')

