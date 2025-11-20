import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch, MagicMock


def _ensure_boto3_stub():
    module = sys.modules.get("boto3")
    if module is None:
        module = SimpleNamespace()
        sys.modules["boto3"] = module

    if not hasattr(module, "client"):
        module.client = lambda *args, **kwargs: MagicMock(name="boto3.client")  # type: ignore

    if not hasattr(module, "resource"):
        module.resource = lambda *args, **kwargs: MagicMock(name="boto3.resource")  # type: ignore


_ensure_boto3_stub()

from lambda_functions import transfer_to_s3 as lambda_mod


class TestS3CopyLambda(unittest.TestCase):

    @patch.dict(os.environ, {
        "SOURCE_BUCKET": "src-bucket",
        "DEST_BUCKET": "dst-bucket",
        "SOURCE_PREFIX": "pollution/",
        "DEST_PREFIX": "archive/"
    }, clear=True)
    @patch("lambda_functions.transfer_to_s3.boto3.client")
    def test_copies_objects_and_counts_them(self, mock_boto_client):
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        paginator_mock = MagicMock()
        paginator_mock.paginate.return_value = [
            {"Contents": [
                {"Key": "pollution/file1.csv"},
                {"Key": "pollution/file2.csv"},
                {"Key": "pollution/"}  
            ]}
        ]
        mock_s3.get_paginator.return_value = paginator_mock

        response = lambda_mod.lambda_handler({}, None)

        self.assertEqual(response["statusCode"], 200)
        self.assertIn("Copied 2 objects", response["body"])

        self.assertEqual(mock_s3.copy_object.call_count, 2)
        mock_s3.copy_object.assert_any_call(
            Bucket="dst-bucket",
            Key="archive/file1.csv",
            CopySource={"Bucket": "src-bucket", "Key": "pollution/file1.csv"}
        )


if __name__ == "__main__":
    unittest.main()
