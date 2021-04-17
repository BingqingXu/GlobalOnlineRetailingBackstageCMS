import boto3
from boto3.dynamodb.conditions import Key, Attr

import heapq
import collections
from pprint import pprint
from decimal import Decimal
from typing import Tuple, List, Dict


def query_orders_by_lsi(lsi_name: str, lsi_partitionkey: str, ptnkey_val, lsi_sortedkey: str, stdkey_val, query_op: str, dynamodb=None):
    """Query by Llobal Secondary Index (LSI), supported query_op: 'eq'
    since condition for HASH type must be eq"""
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Orders')
    if query_op == 'eq':
        response = table.query(
            IndexName=lsi_name,
            KeyConditionExpression=Key(lsi_partitionkey).eq(ptnkey_val) & Key(lsi_sortedkey).eq(stdkey_val)
        )
    elif query_op == 'gt':
        response = table.query(
            IndexName=lsi_name,
            KeyConditionExpression=Key(lsi_partitionkey).eq(ptnkey_val) & Key(lsi_sortedkey).gt(stdkey_val)
        )
    else:
        raise ValueError(f'query_op {query_op} not supported.')
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


def query_orders_timerange_and_product(query_seller_id: str, query_time_range: Tuple[str, str], query_product_id: str, dynamodb=None):
    """Return all orders of a product in a time range
    query_time_range: (starttime, endtime) both inclusive
    """
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Orders')
    response = table.query(
            KeyConditionExpression=Key('seller_id').eq(query_seller_id) & Key('order_time_rand').between(*query_time_range),
            FilterExpression=Attr('product_id').eq(query_product_id)
            # KeyConditionExpression=Key('product_id').eq(query_product_id) \
            #                        & Key('order_time').between(*query_time_range)
        )
    return response['Items']


def calc_top_countries(query_seller_id: str, query_time_range: Tuple[str, str], query_product_id: str):
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

    orders = query_orders_timerange_and_product(query_seller_id, query_time_range, query_product_id)
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
    query_seller_id = 's1'
    query_customer_id = 'c1'
    print('\n=== ORDER INTERFACE 2 ===\n'
          f"Orders from {query_customer_id}")
    orders = query_orders_by_lsi('CustomerID_Index', 'seller_id', query_seller_id, 'customer_id', query_customer_id, 'eq')
    for order in orders:
        print(f"order_time_rand = {order['order_time_rand']}")
    
    # Query by Product ID
    query_seller_id = 's1'
    query_product_id = 'p1'
    print('\n=== ORDER INTERFACE 3 ===\n'
          f"Orders from {query_product_id}")
    orders = query_orders_by_lsi('ProductID_Index', 'seller_id', query_seller_id, 'product_id', query_product_id, 'eq')
    for order in orders:
        print(f"order_time_rand = {order['order_time_rand']}")

    # Query orders whose total price is greater than a specified value
    query_seller_id = 's1'
    query_total_price_min = Decimal(2)
    print('\n=== ORDER INTERFACE 4 ===\n'
          f"Get orders with total price greater than {query_total_price_min}")
    orders = query_orders_by_lsi('Total_Price_Index', 'seller_id', query_seller_id, 'total_price', query_total_price_min, 'gt')
    for order in orders:
        print(f"{order['seller_id']} : {order['total_price']}")

    # Query top three most popular markets (countries) of a product during a period
    query_seller_id = 's1'
    query_product_id = 'p1'
    query_time_range = ("2021-03-01T00:00:00Z00000", "2021-04-30T23:59:59Z99999")
    print('\n=== ORDER INTERFACE 5 ===')
    print(f'Top countries are: '
          f'{calc_top_countries(query_seller_id, query_time_range, query_product_id)}.')
