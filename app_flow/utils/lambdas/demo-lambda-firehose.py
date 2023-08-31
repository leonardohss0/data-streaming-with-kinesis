import base64

print('Loading function')


def lambda_handler(event, context):
    output = []

    for record in event['records']:
        print(record['recordId'])
        payload = base64.b64decode(record['data']).decode('utf-8')

        # Split payload into individual records
        # Assuming each record is separated by a new line
        records = payload.split('\n')

        for data in records:
            output_record = {
                'recordId': record['recordId'],
                'result': 'Ok',
                'data': base64.b64encode(data.encode('utf-8')).decode('utf-8')
            }
            output.append(output_record)

    print('Successfully processed {} records.'.format(len(event['records'])))
    print('Output contains {} records.'.format(len(output)))

    return {'records': output}
