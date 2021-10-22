import boto3
import requests
from util import wait_until


def create_dog_table(table_name, dynamodb):
    print("Creating DynamoDB table: {0}".format(table_name))
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'breed',
                'KeyType': 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'breed',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    )
    return table


def load_dog_data(data, table_name, dynamodb):
    print("Loading data into {0}".format(table_name))
    for dog in data:
        breed = dog.get('breed')
        intelligence = dog.get('intelligence')
        dog_item = {
            'breed': {'S': breed},
            'intelligence': {'N': str(intelligence)}
        }
        print("Adding dog: {0}, with intelligence: {1}".format(breed,
                                                               intelligence))
        dynamodb.put_item(TableName=table_name, Item=dog_item)


def query_table(value, table_name, dynamodb):
    print("Querying table: {0}".format(table_name))
    scan_kwargs = {
        'TableName': table_name,
        'ExpressionAttributeValues': {
            ":n": {'N': str(value)},
        },
        'FilterExpression': "intelligence = :n",
        'ProjectionExpression': "breed",
    }
    response = dynamodb.scan(**scan_kwargs)

    return response['Items']


def delete_item(key, table_name, dynamodb):
    print("Deleting item {0} from {1}".format(key, table_name))
    deleteParams = {
           'Key': {
               'breed': {'S': key},
           },
           'TableName': table_name
    }
    res = dynamodb.delete_item(**deleteParams)
    deleted_item = get_item(key, table_name, client)
    if deleted_item.get("Item") is not None:
        raise Exception("Deleting item {0} failed!".format(key))
    else:
        print("Succesfully deleted {0} from {1}".format(key, table_name))
    return res


def get_item(key, table_name, dynamodb):
    print("Getting item {0} from {1}".format(key, table_name))
    getParams = {
           'Key': {
               'breed': {'S': key},
           },
           'TableName': table_name
    }
    return dynamodb.get_item(**getParams)


def delete_table(table_name, dynamodb):
    print('Deleting table {0}'.format(table_name))
    return dynamodb.delete_table(
        TableName=table_name
    )


def wait_for_table_status(status, table_name, dynamodb):
    print("Waiting for table {0} to be in {1} status...".format(table_name,
                                                                status))
    response = dynamodb.describe_table(
        TableName=table_name
    )
    return response['Table']['TableStatus'] == status.upper()


if __name__ == '__main__':
    # set AWS credentials
    # TODO: change to your credentials
    ACCESS_KEY = "EXAMPLE_ASIA6FNSSPX4KMXLQQGQ"
    SECRET_KEY = "EXAMPLE_H7GWbfg56U25W3jXJjaRz4P"
    SESSION_TOKEN = "EXAMPLE_FwoGZXIvYXdzEPn//////////wEaDHk4SbnENfCajc/RxiLFAc2ok4QQpn5uyRYJhVe/UDdxZwNdCrJ90X3Vkb+bucAUSXBi00/TQ9bcVAnnonqDlE7NlxDlPm9Atrha7JRAQpxr3AH0q/WxxUJg+r/WZPstmn1lacb/A5cmA9hfLu9aI/GdCm51l5P3zYijDXSh4fmEuNQGidfa6NLuoe5AOsVm7fLhUcEIQPdVteeE1v3gi3G7ulL2PApzM5Pl08QKv29LnQlPxM6p6Buk/icq2HVH29eNeWj8jwzCtw3WbgNJnSKaEELZ9A="
    # Create EC2 client
    client = boto3.client(
        'dynamodb',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        aws_session_token=SESSION_TOKEN,
        region_name="us-east-1"
    )
    table_name = 'dogs'
    create_dog_table(table_name, client)
    wait_until(somepredicate=wait_for_table_status, timeout=120, period=12,
               status='ACTIVE', table_name=table_name, dynamodb=client)
    print("Table {0} is ready!".format(table_name))
    url = "https://gist.githubusercontent.com/kastriotadili/acc722c9858189440d964db976303078/raw/ba63ffd45a76e54fd816ff471e9f3ac348e983b7/dog-breeds-data.json"
    result = requests.get(url).json()
    data = result.get('dogBreeds')
    load_dog_data(data, table_name, client)
    dogs_with_intel_9 = query_table(9, table_name, client)
    print("Dogs with intelligence 9 are: {0}".format(dogs_with_intel_9))
    key = 'German Shorthaired'
    delete_item(key, table_name, client)
    delete_table(table_name, client)
    wait_until(somepredicate=wait_for_table_status, timeout=120, period=12,
               status='DELETING', table_name=table_name, dynamodb=client)
    print("Table {0} is being deleted".format(table_name))

