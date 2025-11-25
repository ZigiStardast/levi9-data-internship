'''
Enable S3 Event Notifications, SQS Trigger, and DynamoDB Insertion Trello task.

Develop Lambda Function:
- Develop a Lambda function that is triggered by messages from the SQS queue.
- Lambda function should process files only from the "pollution", "sensor" and "weather" folders.
'''

import json
import os
import boto3
from datetime import datetime
from urllib.parse import unquote_plus

VALID_PREFIXES = ("pollution/", "sensor/", "weather/")

dynamodb = boto3.resource("dynamodb")
table_name = os.environ["DDB_TABLE_NAME"]
table = dynamodb.Table(table_name)

def lambda_handler(event, context):
    for record in event.get("Records", []):
        body = json.loads(record["body"])

        for s3_rec in body.get("Records", []):
            bucket = s3_rec["s3"]["bucket"]["name"]
            key = s3_rec["s3"]["object"]["key"]  

            key = unquote_plus(key)

            if not key.startswith(VALID_PREFIXES):
                print(f"Ignoring object {bucket}/{key}")
                continue

            file_name = key.split("/")[-1]

            if not file_name:
                print(f"Skipping folder-like key {key}")
                continue

            if file_name == "dlq-test.txt":
                raise Exception("Forcing failure for DLQ test")

            event_time = s3_rec.get("eventTime") or datetime.utcnow().isoformat()

            status = 0

            print(f"Processing object {bucket}/{key} -> file_name={file_name}")

            table.put_item(
                Item={
                    "file_name": file_name,
                    "timestamp": event_time,
                    "status": status
                }
            )

    return {
        "statusCode": 200,
        "body": "Processed batch from SQS and written to DynamoDB"
    }
