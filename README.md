```
coverage report -m
```
### Test coverage

| Name                                               | Stmts | Miss | Cover | Missing                    |
|----------------------------------------------------|:-----:|:----:|:-----:|----------------------------|
| `lambda_functions/sqs_to_dynamo.py`               |  30   |  2   |  93%  | 38–39                     |
| `lambda_functions/transfer_to_s3.py`              |  21   |  0   | 100%  | –                          |
| `unit_testing_example/lambda_function_random.py`  |  30   |  4   |  87%  | 27–28, 31, 54             |
| `unit_testing_example/test_lambda_function_random.py` | 43 |  1   |  98%  | 65                        |
| `unit_testing_example/test_s3_copy_lambda.py`     |  32   |  1   |  97%  | 63                        |
| `unit_testing_example/test_sqs_to_dynamodb_lambda.py` | 41 |  5   |  88%  | 12–13, 16, 19, 107        |
| **TOTAL**                                         | **197** | **13** | **93%** | –                      |                                               197     13    93%


Kod weather_job.ipynb spark zadatka javlja mi se greska Py4JJavaError pri zavrsnom result.write. Javljalo mi se i pre kad sam koristila spark, nisam uspela da resim, pa sam se sada snasla sa pandas. Pokusacu da resim taj problem.
