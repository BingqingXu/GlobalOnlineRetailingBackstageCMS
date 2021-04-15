import boto3
from decimal import Decimal
from pprint import pprint
from botocore.exceptions import ClientError


def put_order(order_id: str, customer_id: str, order_time: str,
              product_id: str, product_quantity: int, total_price: str,
              tax: str, country: str, dynamodb=None):
    """Create a new item"""
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Orders')
    response = table.put_item(
       Item={
            'order_id': order_id,
            'customer_id': customer_id,
            'order_time' : order_time,
            'product_id' : product_id,
            'product_quantity' : product_quantity,
            'total_price' : total_price,
            'tax' : tax,
            'country' : country
        }
    )
    return response


def get_order(order_id: str, dynamodb=None):
    """Read an item"""
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Orders')

    try:
        response = table.get_item(Key={'order_id': order_id})
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item']



def delete_order(order_id: str, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Orders')

    try:
        response = table.delete_item(
            Key={
                'order_id': order_id
            }
        )
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(e.response['Error']['Message'])
        else:
            raise
    else:
        return response


if __name__ == '__main__':
    # Test for create
    order_resp = put_order("o2", "c1", "2020-01-18T00:00:00Z", "p1", 1, Decimal('5.2'), Decimal('0.9'), "United States")
    print('\n--- Put order succeeded: ---')
    pprint(order_resp, sort_dicts=False)
    
    # Test for read
    order = get_order("o2")
    if order:
        print('\n=== ORDER INTERFACE 1 ===')
        print('--- Get order succeeded: ---')
        pprint(order, sort_dicts=False)
    
    # Test for delete
    print('\nAttempting a conditional delete...')
    delete_response = delete_order("o2")
    if delete_response:
        print('--- Delete order succeeded: ---')
        pprint(delete_response, sort_dicts=False)
