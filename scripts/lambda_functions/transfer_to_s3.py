'''
S3 file extraction using Lambda Trello task,
za sve 3 lambda funkcije kod mi je isti samo je razlika u env variables (putanjama u S3)
'''
import os
import boto3

def lambda_handler(event, context):
    source_bucket = os.environ["SOURCE_BUCKET"]
    dest_bucket = os.environ["DEST_BUCKET"]
    source_prefix = os.environ["SOURCE_PREFIX"]
    dest_prefix = os.environ["DEST_PREFIX"]
    
    s3 = boto3.client("s3")

    paginator = s3.get_paginator("list_objects_v2")

    copied_files = 0

    for page in paginator.paginate(Bucket=source_bucket, Prefix=source_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if key.endswith("/") and key == source_prefix:
                continue

            suffix = key[len(source_prefix):]
            dest_key = dest_prefix + suffix

            copy_source = {"Bucket": source_bucket, "Key": key}

            s3.copy_object(
                Bucket=dest_bucket,
                Key=dest_key,
                CopySource=copy_source
            )

            copied_files += 1

    return {
        "statusCode": 200,
        "body": f"Copied {copied_files} objects from {source_bucket}/{source_prefix} "
                f"to {dest_bucket}/{dest_prefix}"
    }
