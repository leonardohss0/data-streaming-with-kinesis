import json
import boto3
import time
import base64

# Initialize the DynamoDB client and specify your table name
dynamodb = boto3.resource('dynamodb')
table_name = 'demo_wikipedia_bench_de'
table = dynamodb.Table(table_name)


def lambda_handler(event, context):

    output = []

    for record in event['Records']:

        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')

        # Split payload into individual records
        # Assuming each record is separated by a new line
        records = payload.split('\n')

        for data in records:
            output_record = {
                # Add a timestamp based on your data
                'timestamp': int(time.time()),
                'recordId': record['eventID'],
                'result': 'Ok',
                'data': base64.b64encode(data.encode('utf-8')).decode('utf-8')
            }
            output.append(output_record)

            # Put the item into DynamoDB for each record
            response = table.put_item(Item=output_record)

    print('Successfully processed {} records.'.format(len(event['Records'])))
    print('Output contains {} records.'.format(len(output)))

    return {
        'statusCode': 200,
        'body': json.dumps('Data loaded into DynamoDB successfully!')
    }
