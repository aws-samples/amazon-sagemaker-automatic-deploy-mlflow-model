import aws_cdk as core
import aws_cdk.assertions as assertions

from deployment_cicd.deployment_cicd_stack import DeploymentCiCdStack

# example tests. To run these tests, uncomment this file along with the example
# resource in model_cicd_seed/model_cicd_seed_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = DeploymentCiCdStack(app, "deployment-cicd")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
