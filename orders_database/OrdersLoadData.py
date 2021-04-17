from decimal import Decimal
import json
import boto3


def load_orders(orders, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')

    table = dynamodb.Table('Orders')
    for order in orders:
        seller_id = order['seller_id']
        order_time_rand = order['order_time_rand']
        print('Adding order: ', seller_id, order_time_rand)
        table.put_item(Item=order)


if __name__ == '__main__':
    with open('../resources/orderdata.json') as json_file:
        order_list = json.load(json_file, parse_float=Decimal)
    load_orders(order_list)
