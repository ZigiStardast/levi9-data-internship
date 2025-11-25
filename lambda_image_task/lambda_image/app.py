import os
import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3")

DEFAULT_SOURCE_BUCKET = os.environ.get("SOURCE_BUCKET", "").strip()
DEFAULT_SOURCE_KEY = os.environ.get("SOURCE_KEY", "").strip()
DEFAULT_DEST_BUCKET = os.environ.get("DEST_BUCKET", "").strip()
DEFAULT_DEST_KEY = os.environ.get("DEST_KEY", "").strip()


def _first_non_empty(*values):
    for value in values:
        if value:
            return value
    return ""


def lambda_handler(event, context):
    """Copy a CSV between buckets using server-side S3 copy.

    Event can optionally override bucket/key values with the fields
    `source_bucket`, `source_key`, `dest_bucket`, `dest_key`.
    """

    event = event or {}
    source_bucket = _first_non_empty(
        event.get("source_bucket"),
        event.get("SourceBucket"),
        event.get("bucket"),
        DEFAULT_SOURCE_BUCKET,
    ).strip()
    source_key = _first_non_empty(
        event.get("source_key"),
        event.get("SourceKey"),
        event.get("key"),
        DEFAULT_SOURCE_KEY,
    ).strip()
    dest_bucket = _first_non_empty(
        event.get("dest_bucket"),
        event.get("DestBucket"),
        DEFAULT_DEST_BUCKET or source_bucket,
    ).strip()
    dest_key = _first_non_empty(
        event.get("dest_key"),
        event.get("DestKey"),
        DEFAULT_DEST_KEY or source_key,
    ).strip()

    if not source_bucket or not source_key:
        msg = "Missing source bucket/key; provide via env or event."
        print(msg)
        return {"statusCode": 400, "body": msg}

    if not dest_bucket or not dest_key:
        msg = "Missing destination bucket/key; provide via env or event."
        print(msg)
        return {"statusCode": 400, "body": msg}

    print(f"Copying s3://{source_bucket}/{source_key} -> s3://{dest_bucket}/{dest_key}")

    try:
        head = s3.head_object(Bucket=source_bucket, Key=source_key)
        content_type = head.get("ContentType", "text/csv")
        s3.copy_object(
            Bucket=dest_bucket,
            Key=dest_key,
            CopySource={"Bucket": source_bucket, "Key": source_key},
            ContentType=content_type,
        )
    except ClientError as exc:
        print(f"Copy failed: {exc}")
        return {"statusCode": 500, "body": f"Copy failed: {exc}"}

    return {
        "statusCode": 200,
        "body": f"Copied s3://{source_bucket}/{source_key} -> s3://{dest_bucket}/{dest_key}",
    }
