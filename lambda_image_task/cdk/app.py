#!/usr/bin/env python3
import aws_cdk as cdk
from csv_lambda_stack import CsvLambdaStack

app = cdk.App()

CsvLambdaStack(app, "CsvLambdaStack")

app.synth()
