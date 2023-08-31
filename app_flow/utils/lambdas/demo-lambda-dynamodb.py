import boto3
import json
from decimal import Decimal

print('Loading function')

# Initialize DynamoDB client and table
dynamodb = boto3.resource('dynamodb')
table_name = 'demo_wikipedia_bench_de'
table = dynamodb.Table(table_name)


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)  # Convert Decimal to string
        return super(DecimalEncoder, self).default(obj)


def respond(err, res=None):
    return {
        'statusCode': '400' if err else '200',
        # Use the custom encoder
        'body': err.message if err else json.dumps(res, cls=DecimalEncoder),
        'headers': {
            'Content-Type': 'application/json',
        },
    }


def lambda_handler(event, context):
    '''Demonstrates a simple HTTP endpoint using API Gateway. You have full
    access to the request and response payload, including headers and
    status code.

    To scan a DynamoDB table, make a GET request with the TableName as a
    query string parameter. To put, update, or delete an item, make a POST,
    PUT, or DELETE request respectively, passing in the payload to the
    DynamoDB API as a JSON body.
    '''
    # Get the HTTP method from the request context
    operation = event['requestContext']['http']['method']

    # Define supported operations and their corresponding DynamoDB methods
    operations = {
        'DELETE': lambda dynamo, x: dynamo.delete_item(**x),
        'GET': lambda dynamo, x: dynamo.scan(**x),
        'POST': lambda dynamo, x: dynamo.put_item(**x),
        'PUT': lambda dynamo, x: dynamo.update_item(**x),
    }

    # Check if the operation is supported
    if operation in operations:
        # Perform the requested operation and return the response
        return respond(None, operations[operation](table, {}))
    else:
        return respond(ValueError('Unsupported method "{}"'.format(operation)))
