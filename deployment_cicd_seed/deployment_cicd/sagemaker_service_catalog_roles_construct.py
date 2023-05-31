import aws_cdk as cdk
import aws_cdk.aws_iam as iam
from constructs import Construct


class SageMakerSCRoles(Construct):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        self.model_role = iam.Role.from_role_arn(
            self,
            "SMModelDeploymentRole",
            role_arn=format_role(role_name="AmazonSageMakerServiceCatalogProductsExecutionRole")
        )

        self.events_role = iam.Role.from_role_arn(
            self,
            "SMEventsRole",
            role_arn=format_role(role_name="AmazonSageMakerServiceCatalogProductsEventsRole")
        )

        self.code_build_role = iam.Role.from_role_arn(
            self,
            "SMCodeBuildRole",
            role_arn=format_role(role_name="AmazonSageMakerServiceCatalogProductsCodeBuildRole")
        )

        self.code_pipeline_role = iam.Role.from_role_arn(
            self,
            "SMCodePipelineRole",
            role_arn=format_role(role_name="AmazonSageMakerServiceCatalogProductsCodePipelineRole")
        )

        self.lambda_role = iam.Role.from_role_arn(
            self,
            "SMLambdaRole",
            role_arn=format_role(role_name="AmazonSageMakerServiceCatalogProductsLambdaRole")
        )

        self.api_gw_role = iam.Role.from_role_arn(
            self,
            "SMApiGatewayRole",
            role_arn=format_role(role_name="AmazonSageMakerServiceCatalogProductsApiGatewayRole")
        )

        self.firehose_role = iam.Role.from_role_arn(
            self,
            "SMFirehoseRole",
            role_arn=format_role(role_name="AmazonSageMakerServiceCatalogProductsFirehoseRole")
        )

        self.glue_role = iam.Role.from_role_arn(
            self,
            "SMGlueRole",
            role_arn=format_role(role_name="AmazonSageMakerServiceCatalogProductsGlueRole")
        )

        self.cloudformation_role = iam.Role.from_role_arn(
            self,
            "SMCloudformationRole",
            role_arn=format_role(role_name="AmazonSageMakerServiceCatalogProductsCloudformationRole")
        )




def format_role(role_name: str):
    return f"arn:aws:iam::{cdk.Aws.ACCOUNT_ID}:role/service-role/{role_name}"
