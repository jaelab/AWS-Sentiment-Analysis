import json
import boto3
import time
import pandas as pd
from decimal import Decimal


def lambda_handler(event, context):
    path_to_csv_result = "s3://project-8415/" + event["results_csv"]
    res_csv = pd.read_csv(path_to_csv_result)

    # DynamoDB part
    dynamo_start = time.time()
    dynamo_client = boto3.resource("dynamodb")
    table = dynamo_client.Table("res")
    with table.batch_writer(overwrite_by_pkeys=['id', 'date']) as batch:
        for tweet in res_csv.to_dict(orient="index").values():
            tweet["score"] = Decimal(str(tweet["score"]))
            batch.put_item(
                Item=tweet)
    dynamo_end = time.time()
    print("Dynamo_time")
    print(dynamo_end - dynamo_start)

    table2 = dynamo_client.Table("total-number-requested-items")
    response = table2.update_item(
        Key={
            'table_name': "res"
        },
        UpdateExpression="set is_done=:d",
        ExpressionAttributeValues={
            ':d': True
        },
        ReturnValues="UPDATED_NEW"
    )
    print("Item updated: ")
    print(response)
    return {
        'statusCode': 200,
        'body': json.dumps('Done storing data in DynamoDB!')
    }
