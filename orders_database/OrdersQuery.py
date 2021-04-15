import boto3
from boto3.dynamodb.conditions import Key, Attr

import heapq
import collections
from pprint import pprint
from decimal import Decimal
from typing import Tuple, List, Dict


def query_orders_by_gsi(query_gsi: str, query_attr: str, attr_val, query_op: str, dynamodb=None):
    """Query by Global Secondary Index (GSI), supported query_op: 'eq'
    since condition for HASH type must be eq"""
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Orders')
    if query_op == 'eq':
        response = table.query(
            IndexName=query_gsi,
            KeyConditionExpression=Key(query_attr).eq(attr_val)
        )
    return response['Items']


def scan_orders_filter(filter_attr: str, attr_val, filter_op, dynamodb=None):
    """Scan and filter by an attribute. Currently supported filter_op 'eq', 'gt'."""
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Orders')
    if filter_op == 'eq':
        response = table.scan(
            FilterExpression=Attr(filter_attr).eq(attr_val)
        )
    elif filter_op == 'gt':
        response = table.scan(
            FilterExpression=Attr(filter_attr).gt(attr_val)
        )
    else:
        raise ValueError(f'filter_op={filter_op} not supported.')
    return response['Items']


def query_orders_product_and_timerange(query_gsi: str, query_product_id: str, query_time_range: Tuple[str, str], dynamodb=None):
    """Return all orders of a product in a time range
    query_time_range: (starttime, endtime) both inclusive
    """
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Orders')
    response = table.query(
            IndexName=query_gsi,
            KeyConditionExpression=Key('product_id').eq(query_product_id)
            # KeyConditionExpression=Key('product_id').eq(query_product_id) \
            #                        & Key('order_time').between(*query_time_range)
        )
    return response['Items']


def calc_top_countries(query_gsi: str, query_product_id: str, query_time_range: Tuple[str, str]):
    def calc_top_no_parallel(country_to_quantity: Dict[str, int]) -> Dict[str, int]:
        minheap = []
        if len(country_to_quantity) <= 3:
            top_countries_dict = country_to_quantity
        else:
            for country, quantity in country_to_quantity.items():
                quantity_country_pair = (quantity, country)
                if len(minheap) < 3:
                    heapq.heappush(minheap, quantity_country_pair)
                else:
                    heapq.heappushpop(minheap, quantity_country_pair)
            top_countries_dict = {country: int(quantity) for quantity, country in reversed(minheap)}
        return top_countries_dict

    def calc_top_parallel(country_to_quantity: Dict[str, int]) -> Dict[int, List[str]]:
        if len(country_to_quantity) <= 3:
            top_countries_dict = country_to_quantity
        else:
            quantity_to_country = collections.defaultdict(list)
            minheap = []
            for country, quantity in country_to_quantity.items():
                if quantity not in quantity_to_country:
                    if len(minheap) < 3:
                        heapq.heappush(minheap, quantity)
                    else:
                        heapq.heappushpop(minheap, quantity)
                quantity_to_country[quantity].append(country)
            top_countries_dict = dict()
            for quantity in reversed(minheap):
                top_countries_dict[int(quantity)] = quantity_to_country[quantity]
        return top_countries_dict

    orders = query_orders_product_and_timerange(query_gsi, query_product_id, query_time_range)
    if not orders:
        return dict()
    for order in orders:
        # TODO: aggregate and return top 3 countries
        country_to_quantity = dict()
        for order in orders:
            country_to_quantity[order['country']] = country_to_quantity.get(order['country'], 0) \
                                                     + order['product_quantity']
    
    return calc_top_parallel(country_to_quantity)



# Testing for each functionality
if __name__ == '__main__':
    # Query by Customer ID
    query_customer_id = 'c1'
    print('\n=== ORDER INTERFACE 2 ===\n'
          f"Orders from {query_customer_id}")
    orders = query_orders_by_gsi('CustomerID_Index', 'customer_id', query_customer_id, 'eq')
    for order in orders:
        print(order['order_id'])
    
    # Query by Product ID
    query_product_id = 'p1'
    print('\n=== ORDER INTERFACE 3 ===\n'
          f"Orders from {query_product_id}")
    orders = query_orders_by_gsi('ProductID_OrderTime_Index', 'product_id', query_product_id, 'eq')
    for order in orders:
        print(order['order_id'])

    # Query orders whose total price is greater than a specified value
    query_total_price_min = Decimal(2)
    print('\n=== ORDER INTERFACE 4 ===\n'
          f"Get orders with total price greater than {query_total_price_min}")
    orders = scan_orders_filter('total_price', query_total_price_min, 'gt')
    for order in orders:
        print(f"{order['order_id']} : {order['total_price']}")

    # Query top three most popular markets (countries) of a product during a period
    query_product_id = 'p1'
    query_time_range = ("2013-01-18T00:00:00Z", "2014-01-18T00:00:00Z")
    print('\n=== ORDER INTERFACE 5 ===')
    print(f'Top countries are: '
          f'{calc_top_countries("ProductID_OrderTime_Index", query_product_id, query_time_range)}.')
