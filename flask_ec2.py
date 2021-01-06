import json
from flask import Flask
from flask import request
import boto3
import time

app = Flask(__name__)


@app.route('/sentiment/', methods=['POST'])
def postJsonHandler():
    start = time.time()
    csv_path = request.args.get('data')
    data_to_send = {}
    data_to_send["csv_path"] = csv_path
    lambda_client = boto3.client("lambda")
    dynamo_client = boto3.resource("dynamodb")
    table2 = dynamo_client.Table("total-number-requested-items")
    response = lambda_client.invoke(FunctionName="sentimental-lambda",
                                    InvocationType="Event",
                                    Payload=json.dumps(data_to_send))
    print(response["Payload"].read())

    while table2.get_item(Key={'table_name': "res"})["Item"]["is_done"] != True:
        continue
    else:
        updated_item = table2.update_item(
            Key={
                'table_name': "res"
            },
            UpdateExpression="set is_done=:d",
            ExpressionAttributeValues={
                ':d': False
            },
            ReturnValues="UPDATED_NEW"
        )
    print("Item updated: ")
    print(updated_item)
    end = time.time()
    print("Full time of sentiment analysis and storing results in S3 and DynamoDB in: ")
    print(end - start)

    return "Sentiment Analysis System is done !"


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
