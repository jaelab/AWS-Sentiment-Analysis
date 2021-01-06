import json
import pandas as pd
import nltk
import boto3
import os
import time
from io import StringIO
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Download vader from nltk
nltk.data.path.append("/tmp")
nltk.download('vader_lexicon', download_dir="/tmp")


def lambda_handler(event, context):
    sent_s = time.time()
    s3_resource = boto3.resource('s3')
    all_infos = {}
    bucket_name = "project-8415"
    csv_path = event["csv_path"]
    csv_file_name = str(os.path.basename(csv_path))
    json_file_name = csv_file_name.replace("csv", "json")

    # Read rawdata CSV
    raw_data_csv = pd.read_csv(csv_path)

    # Save JSON rawdata
    json_rawdata = f"rawdata/json/{json_file_name}"
    # Save JSON file with rawdata
    s3_resource.Object(bucket_name, json_rawdata).put(Body=raw_data_csv.to_json(orient='index'))

    # Replace nan to None
    raw_data_csv.fillna("None", inplace=True)
    texts_list = raw_data_csv['text'].tolist()

    # Create sentiment analyzer
    sentiment_analyzer = SentimentIntensityAnalyzer()
    scores_list = []
    sentiments_list = []
    for sentence in texts_list:
        if sentence == "None":
            sentiments_list.append("Neutral")
            scores_list.append(0.0)
        else:
            sentiment_results = sentiment_analyzer.polarity_scores(sentence)
            scores_list.append(sentiment_results["compound"])
            if sentiment_results["compound"] >= 0.05:
                sentiments_list.append("Positive")
            elif sentiment_results["compound"] > -0.05 and sentiment_results["compound"] < 0.05:
                sentiments_list.append("Neutral")
            else:
                sentiments_list.append("Negative")

    raw_data_csv["score"] = scores_list
    raw_data_csv["sentiment"] = sentiments_list

    # Save results
    csv_results_name = "sentiments/csv/" + csv_file_name
    json_results_name = csv_results_name.replace("csv", "json", 2)
    all_infos["results_csv"] = csv_results_name
    final_dict = {}
    for key, value in raw_data_csv.to_dict(orient="index").items():
        final_dict[value["id"]] = value
    csv_buffer = StringIO()
    raw_data_csv.to_csv(csv_buffer, index=False)
    s3_resource.Object(bucket_name, csv_results_name).put(Body=csv_buffer.getvalue())
    s3_resource.Object(bucket_name, json_results_name).put(Body=json.dumps(final_dict))
    lambda_client = boto3.client("lambda")
    sent_e = time.time()
    print(sent_e - sent_s)
    s = time.time()
    response = lambda_client.invoke(FunctionName="store-results",
                                    InvocationType="RequestResponse",
                                    Payload=json.dumps(all_infos))
    print(response["Payload"].read())
    e = time.time()
    print(e - s)

    return {
        'statusCode': 200,
        'body': json.dumps('Sentiment Analysis is done and results are stored in S3!')
    }
