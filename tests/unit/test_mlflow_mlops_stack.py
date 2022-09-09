import aws_cdk as core
import aws_cdk.assertions as assertions

from mlflow_mlops.mlflow_mlops_stack import MlflowMlopsStack

# example tests. To run these tests, uncomment this file along with the example
# resource in mlflow_mlops/mlflow_mlops_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = MlflowMlopsStack(app, "mlflow-mlops")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
