import boto3


def delete_order_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Orders')
    table.delete()


if __name__ == '__main__':
    delete_order_table()
    print("Orders table deleted.")
