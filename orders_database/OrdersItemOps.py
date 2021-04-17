import boto3
from decimal import Decimal
from pprint import pprint
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key


def put_order(seller_id: str, customer_id: str, order_time_rand: str,
              product_id: str, total_price, product_quantity: int,
              tax, country: str, dynamodb=None):
    """Create a new item"""
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Orders')
    response = table.put_item(
       Item={
            'seller_id': seller_id,
            'order_time_rand' : order_time_rand,
            'customer_id': customer_id,
            'product_id' : product_id,
            'total_price' : total_price,
            'product_quantity' : product_quantity,
            'tax' : tax,
            'country' : country
        }
    )
    return response


def get_order(seller_id: str, dynamodb=None):
    """Read an item"""
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Orders')

    try:
        response = table.scan(
            FilterExpression=Key('seller_id').eq(seller_id)
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Items']



# def delete_order(order_id: str, dynamodb=None):
#     if not dynamodb:
#         dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

#     table = dynamodb.Table('Orders')

#     try:
#         response = table.delete_item(
#             Key={
#                 'order_id': order_id
#             }
#         )
#     except ClientError as e:
#         if e.response['Error']['Code'] == "ConditionalCheckFailedException":
#             print(e.response['Error']['Message'])
#         else:
#             raise
#     else:
#         return response


if __name__ == '__main__':
    # Test for create
    order_resp = put_order("s2", "2020-01-18T00:00:00Z00007", "c1", "p1", Decimal('10'), 1, Decimal('1'), "United States")
    print('\n=== ORDER INTERFACE 1 ===')
    print('\n--- Create order succeeded: ---')
    pprint(order_resp, sort_dicts=False)
    
    # Test for read
    order = get_order("s1")
    if order:
        print('--- Get order by seller_id succeeded: ---')
        pprint(order, sort_dicts=False)
    
    # Test for delete
    # print('\nAttempting a conditional delete...')
    # delete_response = delete_order("o2")
    # if delete_response:
    #     print('--- Delete order succeeded: ---')
    #     pprint(delete_response, sort_dicts=False)
