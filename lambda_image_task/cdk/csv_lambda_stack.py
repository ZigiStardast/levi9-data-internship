import os
from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_logs as logs,
)
from constructs import Construct
from pathlib import Path


SOURCE_BUCKET_NAME = os.getenv("CSV_SOURCE_BUCKET_NAME")
DEST_BUCKET_NAME = os.getenv("CSV_DEST_BUCKET_NAME")
CSV_COPY_FUNCTION_NAME = os.getenv("CSV_COPY_FUNCTION_NAME")
CSV_SOURCE_KEY = os.getenv("CSV_SOURCE_KEY", "input/data.csv")
CSV_DEST_KEY = os.getenv("CSV_DEST_KEY", "output/data-copy.csv")
MY_LAYER_VERSION_NAME = os.getenv("MY_UTILS_LAYER_NAME")
LAYER_DEMO_FUNCTION_NAME = os.getenv("LAYER_DEMO_FUNCTION_NAME")

class CsvLambdaStack(Stack):

    def _init_(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super()._init_(scope, construct_id, **kwargs)

        lifecycle_policy = [s3.LifecycleRule(expiration=Duration.days(7))]

        source_bucket = s3.Bucket(
            self,
            "SourceBucket",
            bucket_name=SOURCE_BUCKET_NAME,
            versioned=False,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            lifecycle_rules=lifecycle_policy,
        )

        dest_bucket = s3.Bucket(
            self,
            "DestBucket",
            bucket_name=DEST_BUCKET_NAME,
            versioned=False,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            lifecycle_rules=lifecycle_policy,
        )

        lambda_image_dir = (
            Path(_file_).resolve().parent.parent / "lambda_image"
        )

        csv_copy_lambda = _lambda.DockerImageFunction(
            self,
            "CsvCopyImageLambda",
            function_name=CSV_COPY_FUNCTION_NAME,
            code=_lambda.DockerImageCode.from_image_asset(
                directory=str(lambda_image_dir)
            ),
            timeout=Duration.seconds(30),
            memory_size=512,
            architecture=_lambda.Architecture.ARM_64,
            reserved_concurrent_executions=1,
            log_retention=logs.RetentionDays.ONE_WEEK,
            environment={
                "SOURCE_BUCKET": source_bucket.bucket_name,
                "SOURCE_KEY": CSV_SOURCE_KEY,
                "DEST_BUCKET": dest_bucket.bucket_name,
                "DEST_KEY": CSV_DEST_KEY,
            },
        )

        source_bucket.grant_read(csv_copy_lambda)
        dest_bucket.grant_write(csv_copy_lambda)

        layer_dir = Path(_file_).resolve().parent.parent / "layer_src"

        my_layer = _lambda.LayerVersion(
            self,
            "MyUtilsLayer",
            layer_version_name=MY_LAYER_VERSION_NAME,
            code=_lambda.Code.from_asset(str(layer_dir)),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_11],
            description="Example layer with my_utils library",
        )

        lambda_with_layer_dir = (
            Path(_file_).resolve().parent.parent / "lambda_with_layer"
        )

        layer_demo_fn = _lambda.Function(
            self,
            "LayerDemoFunction",
            function_name=LAYER_DEMO_FUNCTION_NAME,
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="app.handler",
            code=_lambda.Code.from_asset(str(lambda_with_layer_dir)),
            layers=[my_layer],
            timeout=Duration.seconds(10),
            memory_size=256,
            log_retention=logs.RetentionDays.ONE_WEEK,
        )