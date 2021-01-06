import json
import boto3
import time


def lambda_handler(event, context):
    id = int(event["queryStringParameters"]["id"])
    date = event["queryStringParameters"]["date"]
    dynamo_client = boto3.resource("dynamodb")
    table = dynamo_client.Table("res")
    response = table.get_item(Key={"id": id, "date": date})

    # Update counter item in other dynamoDB table
    number_items_table = dynamo_client.Table("total-number-requested-items")
    number_items_response = number_items_table.update_item(
        Key={"table_name": "res"},
        UpdateExpression="SET #attr = #attr + :val",
        ExpressionAttributeNames={
            "#attr": "count"
        },
        ExpressionAttributeValues={
            ":val": 1
        },
        ReturnValues="UPDATED_NEW"
    )
    formatted_response = {}
    formatted_response[str(number_items_response["Attributes"]["count"])] = {
        "id": int(response["Item"]["id"]),
        "sentiment": response["Item"]["sentiment"],
        "score": float(response["Item"]["score"])
    }
    print(formatted_response)
    return {
        'statusCode': response["ResponseMetadata"]["HTTPStatusCode"],
        'body': json.dumps(formatted_response)
    }
