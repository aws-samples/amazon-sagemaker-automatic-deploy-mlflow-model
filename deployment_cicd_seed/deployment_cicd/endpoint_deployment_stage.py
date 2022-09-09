import logging
from pathlib import Path

import aws_cdk as cdk
import aws_cdk.aws_iam as iam
import boto3
import yaml
from constructs import Construct

from deployment_cicd.sagemaker_endpoint_construct import SageMakerModel, Endpoint
from deployment_cicd.sagemaker_model_registry_utils import get_models_descriptions

logger = logging.getLogger(__name__)
sm_client = boto3.client("sagemaker")
conf_path = Path(__file__).resolve().parent / "config/inference.yml"

with conf_path.open() as f:
    conf = yaml.safe_load(f)


class ModelDeployStage(cdk.Stage):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        stage_name: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        _ = ModelDeployStack(self, "ModelDeploy", stage=stage_name)


class ModelDeployStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        stage: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        model_name = self.node.try_get_context("ModelName")
        model_name_aws_friendly = model_name.replace("_", "-")
        descriptions_list = get_models_descriptions(
            model_name=model_name_aws_friendly,
            stage=stage,
        )

        model_role = iam.Role.from_role_arn(
            self,
            "ModelDeploymentRole",
            role_arn=self.format_arn(
                resource="role",
                service="iam",
                region="",
                resource_name="service-role/AmazonSageMakerServiceCatalogProductsExecutionRole",
            ),
        )

        for k in descriptions_list:
            deploy_conf = conf[stage]
            model_package_arn = k["ModelPackageArn"]
            model_name = k["ModelPackageGroupName"]
            model_version = k["CustomerMetadataProperties"]["mlflow_version"]

            endpoint_name = f"{model_name}-{model_version}-{stage}"

            sm_model = SageMakerModel(
                self,
                f"ModelVersion{model_version}",
                model_package_name=model_package_arn,
                role=model_role,
            )
            sm_endpoint = Endpoint(
                self,
                f"Endpoint-Version{model_version}",
                endpoint_name=endpoint_name,
                model=sm_model,
                instance_type=deploy_conf.get("InstanceType"),
                initial_instance_count=deploy_conf.get("InstanceCount"),
                serverless_max_concurrency=deploy_conf.get("ServerlessMaxConcurrency"),
                serverless_size_in_mb=deploy_conf.get("ServerlessSizeInMB"),
                capture_destination_s3_uri=deploy_conf.get("CaptureDestinationS3Uri"),
            )
