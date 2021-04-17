import boto3


def create_order_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.create_table(
        TableName='Orders',
        AttributeDefinitions=[
            {
                'AttributeName': 'seller_id',
                'AttributeType': 'S'
            },
            {
                # timestamp with random number
                # an ISO 8601 string, e.g., 2015-12-21T17:42:34Z, plus a 5 digit random number
                'AttributeName': 'order_time_rand',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'customer_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'product_id',
                'AttributeType': 'S'
            },
            {
                # Suppose price and tax are converted to local currency before entering datbase
                'AttributeName': 'total_price',
                'AttributeType': 'N'
            },
            # {
            #     'AttributeName': 'product_quantity',
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
                'AttributeName': 'seller_id',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'order_time_rand',
                'KeyType': 'RANGE'  # Sorted key
            }
        ],
        BillingMode='PAY_PER_REQUEST',
        LocalSecondaryIndexes=[
            {
                'IndexName': 'CustomerID_Index',
                'KeySchema': [
                    {
                       'AttributeName': 'seller_id',
                       'KeyType': 'HASH'
                    },
                    {
                       'AttributeName': 'customer_id',
                       'KeyType': 'RANGE'
                    }
                ],
                'Projection': {
                    'ProjectionType': 'KEYS_ONLY'
                },
                # 'ProvisionedThroughput': {
                #     'ReadCapacityUnits': 3,
                #     'WriteCapacityUnits': 3
                # }
            },
            {
                'IndexName': 'ProductID_Index',
                'KeySchema': [
                    {
                       'AttributeName': 'seller_id',
                       'KeyType': 'HASH'
                    },
                    {
                       'AttributeName': 'product_id',
                       'KeyType': 'RANGE'
                    }
                ],
                'Projection': {
                    'ProjectionType': 'KEYS_ONLY'
                },
            },
            {
                'IndexName': 'Total_Price_Index',
                'KeySchema': [
                    {
                       'AttributeName': 'seller_id',
                       'KeyType': 'HASH'
                    },
                    {
                       'AttributeName': 'total_price',
                       'KeyType': 'RANGE'
                    }
                ],
                'Projection': {
                    'ProjectionType': 'KEYS_ONLY'
                },
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
