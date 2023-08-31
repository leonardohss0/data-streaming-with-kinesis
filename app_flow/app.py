import json
import boto3

from app_flow.utils.wiki import fetch_latest_wikipedia_data


def run():

    # Create a Kinesis client
    kinesis_client = boto3.client('kinesis')

    last_timestamp = 0

    while True:
        for data in fetch_latest_wikipedia_data(last_timestamp):

            # Publish a record to the Kinesis stream
            response = kinesis_client.put_record(
                StreamName='demo-kinesis-bench-de',
                Data=data,
                PartitionKey='partitionKey-03'
            )

            print(f"The message was sent to Kinesis.")
            print(f"Stream Name: demo-kinesis-bench-de")
            print(f"Partition Key: partitionKey-03")
            print(f"Sequence Number: {response['SequenceNumber']}")
            print(f"Shard ID: {response['ShardId']}")
            print(f"Data: {data}")

            change = json.loads(data)
            last_timestamp = int(change.get("timestamp", last_timestamp))


run()
