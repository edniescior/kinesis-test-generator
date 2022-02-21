# Kinesis Test Generator
This is a simple python implementation of a Kinesis producer and 
consumer to use for basic testing.

## Pre-requisites
 1. The AWS CLI is installed on your machine.
 2. Test data in a CSV file with header line. (Producer only.) 

## Managing Test Streams using the AWS CLI

To list data streams: 
```
aws kinesis list-streams
```

To create a data stream:
```
aws kinesis create-stream \
    --stream-name samplestream \
    --shard-count 1
```

To describe a shard:
```
aws kinesis describe-stream-summary \
    --stream-name samplestream
```
To delete a data stream:
```
aws kinesis delete-stream \
    --stream-name samplestream
```

## Using the Producer

The `producer.py` python script reads records from a CSV file and writes 
each row as a record to a Kinesis data stream. 

The CSV file requires a header.

Calling `producer.py --help` provides:
```
usage: producer.py [-h] [--region REGION] [--sleep_interval SLEEP_INTERVAL] [--batch_size BATCH_SIZE] [--max_records MAX_RECORDS] [--partition_key PARTITION_KEY]
                   stream_name file_name

Connect to a Kinesis stream and write data to it from a CSV file.

positional arguments:
  stream_name           the name of the Kinesis stream to connect to
  file_name             the name of the CSV file to read from. It requires a header.

options:
  -h, --help            show this help message and exit
  --region REGION       the name of the Kinesis region to connect with [default: us-east-1]
  --sleep_interval SLEEP_INTERVAL
                        the loop sleep interval in seconds [default: 0.2]
  --batch_size BATCH_SIZE
                        the batch size for each put request [default: 10]
  --max_records MAX_RECORDS
                        the maximum number of records to write to the stream [default: 10]
  --partition_key PARTITION_KEY
                        the field in the data dict to use as a partition key. Ignore if you
                                                  don't care for processing order or if this stream only has 1 shard. 
                                                  [default: None (ie randomly generated)]

```

### Writing
Each row record in the CSV file is converted into a python dictionary. 
The keys for the dictionary are taken from the header line of the CSV file.

The records are published in a batch to Kinesis using the `put_records` API. 
The batch size is configured by setting the `batch_size` parameter. The 
default is to send 10 records per batch.

If you want to set the partition key of each record to be a taken from 
a certain field in the CSV record, pass in the field name to the
`partition_key` parameter. (This should match one of the field names in the 
header line of the CSV.) The default behaviour is to randomize the partition 
key and spread the records out evenly across the shards. You can ignore it 
if you only have one shard.

## Using the Consumer

The `consumer.py` python script reads records from Kinesis data stream 
and prints them to the console.

Calling `consumer.py --help` provides:
```
usage: consumer.py [-h] [--region REGION] [--worker_time WORKER_TIME] [--sleep_interval SLEEP_INTERVAL] stream_name

Connect to a Kinesis stream and create workers
                       that read and echo records from each shard.

positional arguments:
  stream_name           the name of the Kinesis stream to connect to

options:
  -h, --help            show this help message and exit
  --region REGION       the name of the Kinesis region to connect with [default: us-east-1]
  --worker_time WORKER_TIME
                        the worker's duration of operation in seconds [default: 30]
  --sleep_interval SLEEP_INTERVAL
                        the worker's work loop sleep interval in seconds [default: 0.1]
```

### Reading
The consumer creates one worker for each shard in the stream. Each worker
reads from its assigned shard.

The workers will keep reading for a set amount of time and then terminate.
By default, this is 30 seconds.

Between fetches, each worker sleeps for set amount of time to
prevent throughput errors. By default, this is 0.1 seconds, but tweak it
depending on your load.

The shard iterator type is set to TRIM_HORIZON by default. (This will
start reading from the oldest record in the stream.) Options: 
AT_SEQUENCE_NUMBER | AFTER_SEQUENCE_NUMBER | TRIM_HORIZON | 
LATEST | AT_TIMESTAMP.

TO DO: Make this a parameter ^^^

