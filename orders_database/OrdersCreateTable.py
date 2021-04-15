import boto3


def create_order_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.create_table(
        TableName='Orders',
        AttributeDefinitions=[
            {
                'AttributeName': 'order_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'customer_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'order_time',
                'AttributeType': 'S'  # timestamp, an ISO 8601 string, e.g., 2015-12-21T17:42:34Z
            },
            {
                'AttributeName': 'product_id',
                'AttributeType': 'S'
            },
            # {
            #     'AttributeName': 'product_quantity',
            #     'AttributeType': 'N'
            # },
            # {
            #     # Suppose price and tax are converted to local currency before entering datbase
            #     'AttributeName': 'total_price',
            #     'AttributeType': 'N'
            # },
            # {
            #     'AttributeName': 'tax',
            #     'AttributeType': 'N'
            # },
            # {
            #     'AttributeName': 'country',
            #     'AttributeType': 'S'
            # }
        ],
        KeySchema=[
            {
                'AttributeName': 'order_id',
                'KeyType': 'HASH'  # Partition key
            }
        ],
        BillingMode='PAY_PER_REQUEST',
        GlobalSecondaryIndexes=[ 
          {
             'IndexName': 'CustomerID_Index',
             'KeySchema': [
                {
                   'AttributeName': 'customer_id',
                   'KeyType': 'HASH'
                }
             ],
             'Projection': { 
                'ProjectionType': 'KEYS_ONLY'
             },
             'ProvisionedThroughput': { 
                'ReadCapacityUnits': 3,
                'WriteCapacityUnits': 3
             }
          },
          {
             'IndexName': 'ProductID_OrderTime_Index',
             'KeySchema': [
                {
                   'AttributeName': 'product_id',
                   'KeyType': 'HASH'
                },
                {
                   'AttributeName': 'order_time',
                   'KeyType': 'RANGE'
                }
             ],
             'Projection': { 
                'ProjectionType': 'INCLUDE',
                'NonKeyAttributes': ['country', 'product_quantity']
             },
             'ProvisionedThroughput': { 
                'ReadCapacityUnits': 3,
                'WriteCapacityUnits': 3
             }
          }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    return table


if __name__ == '__main__':
    order_table = create_order_table()
    print("Table status:", order_table.table_status)
