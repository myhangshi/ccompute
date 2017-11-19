from __future__ import print_function # Python 2/3 compatibility

import boto3
#client = boto3.client('dynamodb') 

dynamodb = boto3.resource('dynamodb', region_name='us-east-1') 

table = dynamodb.create_table(
    TableName='g1e1',
    KeySchema=[
        {
            'AttributeName': 'carrier',
            'KeyType': 'HASH'  #Partition key
        },
        {
            'AttributeName': 'delay',
            'KeyType': 'RANGE'  #Sort key
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'carrier',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'delay',
            'AttributeType': 'N'
        },

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
)

print("Table status:", table.table_status)

