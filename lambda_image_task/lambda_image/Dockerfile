FROM public.ecr.aws/lambda/python:3.11

# Keep the image lean; boto3 is already included in the base runtime.
COPY app.py ${LAMBDA_TASK_ROOT}

CMD ["app.lambda_handler"]
