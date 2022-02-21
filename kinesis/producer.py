# Based on https://github.com/aws-samples/kinesis-poster-worker.
# Updated to work with boto3 and Python 3.

import argparse
import datetime
import time
import uuid

import boto3
import botocore
import json
import csv

from argparse import RawTextHelpFormatter
from collections import namedtuple
from typing import List

# To preclude inclusion of aws keys into this code, you may temporarily add
# your AWS credentials to the file:
#     ~/aws/credentials
# as follows:
#     [default]
#     aws_access_key_id = <your access key>
#     aws_secret_access_key = <your secret key>

PutResults = namedtuple('PutResults', '''success_count error_count''')


def put_batch(data: List[dict],
              stream_name: str,
              partition_key: str = None) -> PutResults:
    """
    Publish a collection of records to the stream as a single put_records batch.
    :param data: a list of python dicts containing your data. (1 <= batch size <= 500).
    :param stream_name: the name of the Kinesis stream to write to
    :param partition_key: the field in the data dict to use as a partition key. Ignore if you
                          don't care for processing order or if this stream only has 1 shard.
    :return: PutResults
    """
    def get_partition_key(d: dict) -> str:
        # If no partition key is given, or it isn't in the dict,
        # assume random sharding for even shard write load
        this_key = str(uuid.uuid4())
        if partition_key:
            this_key = d.get(partition_key, str(uuid.uuid4()))
        return this_key

    batch = []
    for j, rec in enumerate(data):
        this_partition_key = get_partition_key(rec)
        # print(f'[{j}] {this_partition_key} {rec}')

        # build the kinesis record and add it to the collection
        kinesis_record = {'Data': json.dumps(rec), 'PartitionKey': this_partition_key}
        batch.append(kinesis_record)

    recs, errs = 0, 0
    try:
        response = kinesis.put_records(Records=batch,
                                       StreamName=stream_name)
        recs = len(response['Records'])
        errs = response['FailedRecordCount']
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
            print('Provisioned throughput exceeded. Worker {worker.name} sleeping... ')
            time.sleep(5)
        else:
            raise
    return PutResults(recs, errs)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='''Connect to a Kinesis stream and write data to it from a CSV file.''',
        formatter_class=RawTextHelpFormatter)
    parser.add_argument('stream_name',
                        help='''the name of the Kinesis stream to connect to''')
    parser.add_argument('file_name',
                        help='''the name of the CSV file to read from. It requires a header.''')
    parser.add_argument('--region', type=str, default='us-east-1',
                        help='''the name of the Kinesis region to connect with [default: us-east-1]''')
    parser.add_argument('--sleep_interval', type=float, default=0.2,
                        help='''the loop sleep interval in seconds [default: 0.2]''')
    parser.add_argument('--batch_size', type=int, default=10,
                        help='''the batch size for each put request [default: 10]''')
    parser.add_argument('--max_records', type=int, default=10,
                        help='''the maximum number of records to write to the stream [default: 10]''')
    parser.add_argument('--partition_key', type=str, default=None,
                        help='''the field in the data dict to use as a partition key. Ignore if you
                          don't care for processing order or if this stream only has 1 shard. 
                          [default: None (ie randomly generated)]''')
    args = parser.parse_args()
    kinesis = boto3.client('kinesis', region_name=args.region)
    # stream = kinesis.describe_stream(StreamName=args.stream_name)
    # print(json.dumps(stream, default=str, sort_keys=True, indent=2, separators=(',', ': ')))

    # read the csv file, batch up records, and send them to the stream in batches
    print(f'~> opening file: {args.file_name}')
    start_time = datetime.datetime.now()
    records = []
    put_results = None
    total_records, total_successes, total_errors = 0, 0, 0
    with open(args.file_name, 'r') as content_file:
        content = csv.DictReader(content_file)
        for i, row in enumerate(content):
            if i == args.max_records:
                # we hit our max mark. pop out of the loop.
                break
            records.append(row)
            total_records += 1
            if (i + 1) % args.batch_size == 0:
                # write the batch, and reset
                put_results = put_batch(records, args.stream_name, args.partition_key)
                total_successes += put_results.success_count
                total_errors += put_results.error_count
                records = []

                # sleep for a bit to avoid throughput errors
                time.sleep(args.sleep_interval)

        # flush any remaining records
        if records:
            put_results = put_batch(records, args.stream_name, args.partition_key)
            total_successes += put_results.success_count
            total_errors += put_results.error_count

    finish_time = datetime.datetime.now()
    duration = (finish_time - start_time).total_seconds()
    print('-=> Exiting Producer Main <=-')
    print(f'  Total Records: {total_records}')
    print(f'     Total Successes: {total_successes}')
    print(f'     Total Errors   : {total_errors}')
    print(f'     Total Time (s) : {duration: .2f}')
    print(f'  Records / sec: {(total_records / duration):.2f}')
    print(f'  Failure rate: {total_records and ((total_errors / total_records) * 100): .2f}%')  # avoid div by 0
    print(f'  Worker sleep interval: {args.sleep_interval}')
