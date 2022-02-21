# Modified from https://github.com/aws-samples/kinesis-poster-worker.
# Updated to work with boto3 and Python 3.

import sys
import argparse
import datetime
import concurrent.futures
import time

import boto3
import botocore
import json

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

def echo_records(records):
    for record in records:
        text = record['Data']
        print(f'+--> echo record:\n{text}')


KinesisWorker = namedtuple('KinesisWorker', '''stream_name 
                                               shard_id 
                                               iterator_type 
                                               worker_time 
                                               sleep_interval 
                                               name''')


def start_work(workers: List[KinesisWorker], max_workers) -> int:
    total = 0

    def do_work(worker: KinesisWorker) -> int:
        print('+ KinesisWorker:', worker.name)
        print('+-> working with iterator:', worker.iterator_type)
        response = kinesis.get_shard_iterator(StreamName=worker.stream_name,
                                              ShardId=worker.shard_id,
                                              ShardIteratorType=worker.iterator_type)
        next_iterator = response['ShardIterator']
        print('+-> getting next records using iterator:', next_iterator)
        start = datetime.datetime.now()
        finish = start + datetime.timedelta(seconds=worker.worker_time)
        worker_total = 0
        while finish > datetime.datetime.now():
            try:
                response = kinesis.get_records(ShardIterator=next_iterator, Limit=25)
                worker_total += len(response['Records'])

                if len(response['Records']) > 0:
                    print(f'\n+-> Worker {worker.name} got {len(response["Records"])} records.')
                    echo_records(response['Records'])
                else:
                    sys.stdout.write('.')
                    sys.stdout.flush()
                next_iterator = response['NextShardIterator']
                time.sleep(worker.sleep_interval)
            except botocore.exceptions.ClientError as error:
                if error.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                    print('Provisioned throughput exceeded. Worker {worker.name} sleeping... ')
                    time.sleep(5)
                else:
                    raise
        return worker_total

    # set the workers running
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_workers = {executor.submit(do_work, wkr): wkr for wkr in workers}
        for future in concurrent.futures.as_completed(future_workers):
            finished_worker = future_workers[future]
            try:
                data = future.result()
            except Exception as exc:
                print(f'\n{finished_worker.name} generated an exception: {exc}')
            else:
                total = total + data
                print(f'\n{finished_worker.name} returned {data} records.')
    return total


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='''Connect to a Kinesis stream and create workers
                       that read and echo records from each shard.''',
        formatter_class=RawTextHelpFormatter)
    parser.add_argument('stream_name',
                        help='''the name of the Kinesis stream to connect to''')
    parser.add_argument('--region', type=str, default='us-east-1',
                        help='''the name of the Kinesis region to connect with [default: us-east-1]''')
    parser.add_argument('--worker_time', type=int, default=30,
                        help='''the worker's duration of operation in seconds [default: 30]''')
    parser.add_argument('--sleep_interval', type=float, default=0.1,
                        help='''the worker's work loop sleep interval in seconds [default: 0.1]''')

    args = parser.parse_args()
    kinesis = boto3.client('kinesis', region_name=args.region)
    stream = kinesis.describe_stream(StreamName=args.stream_name)
    print(json.dumps(stream, default=str, sort_keys=True, indent=2, separators=(',', ': ')))
    shards = stream['StreamDescription']['Shards']
    print('# Shard Count:', len(shards))

    # set up a worker for each shard in the stream
    workers_lst = []
    start_time = datetime.datetime.now()
    for shard_idx in range(len(shards)):
        worker_name = 'shard_worker:%s' % shard_idx
        shard_id = shards[shard_idx]['ShardId']
        print('#-> shardId:', shard_id)
        a_worker = KinesisWorker(
            stream_name=args.stream_name,
            shard_id=shard_id,
            iterator_type='TRIM_HORIZON',
            worker_time=args.worker_time,
            sleep_interval=args.sleep_interval,
            name=worker_name
        )
        workers_lst.append(a_worker)

    # get the workers running
    total_records = start_work(workers_lst, 3)

    finish_time = datetime.datetime.now()
    duration = (finish_time - start_time).total_seconds()
    print('-=> Exiting Worker Main <=-')
    print(f'  Total Records: {total_records}')
    print(f'     Total Time (s): {duration: .2f}')
    print(f'  Records / sec: {total_records / duration}')
    print(f'  Worker sleep interval: {args.sleep_interval}')
