import json
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
os.environ.setdefault("DDB_TABLE_NAME", "unit-test-table")

from lambda_functions import sqs_to_dynamo as lambda_mod


class TestSqsToDynamoLambda(unittest.TestCase):

    @patch("lambda_functions.sqs_to_dynamo.table")
    def test_happy_path_puts_item(self, mock_table):
        event = {
            "Records": [
                {
                    "body": json.dumps({
                        "Records": [
                            {
                                "s3": {
                                    "bucket": {"name": "my-bucket"},
                                    "object": {"key": "pollution/data.csv"}
                                },
                                "eventTime": "2025-11-19T10:00:00Z"
                            }
                        ]
                    })
                }
            ]
        }

        response = lambda_mod.lambda_handler(event, None)

        self.assertEqual(response["statusCode"], 200)
        mock_table.put_item.assert_called_once()
        args, kwargs = mock_table.put_item.call_args
        self.assertEqual(kwargs["Item"]["file_name"], "data.csv")
        self.assertEqual(kwargs["Item"]["status"], 0)

    @patch("lambda_functions.sqs_to_dynamo.table")
    def test_ignores_invalid_prefix(self, mock_table):
        event = {
            "Records": [
                {
                    "body": json.dumps({
                        "Records": [
                            {
                                "s3": {
                                    "bucket": {"name": "my-bucket"},
                                    "object": {"key": "other-folder/file.txt"}
                                }
                            }
                        ]
                    })
                }
            ]
        }

        response = lambda_mod.lambda_handler(event, None)

        self.assertEqual(response["statusCode"], 200)
        mock_table.put_item.assert_not_called()

    def test_raises_for_dlq_test_file(self):
        event = {
            "Records": [
                {
                    "body": json.dumps({
                        "Records": [
                            {
                                "s3": {
                                    "bucket": {"name": "my-bucket"},
                                    "object": {"key": "pollution/dlq-test.txt"}
                                }
                            }
                        ]
                    })
                }
            ]
        }

        with self.assertRaises(Exception) as ctx:
            lambda_mod.lambda_handler(event, None)

        self.assertIn("DLQ", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
