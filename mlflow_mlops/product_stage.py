from pathlib import Path

import aws_cdk as cdk
import aws_cdk.aws_codebuild as codebuild
import aws_cdk.aws_codecommit as codecommit
import aws_cdk.aws_servicecatalog as sc
from aws_cdk.aws_iam import PolicyStatement
from aws_cdk.aws_s3_assets import Asset
from aws_cdk.pipelines import (
    CodeBuildOptions,
    CodePipeline,
    CodePipelineSource,
    ShellStep,
)
from constructs import Construct

from .service_catalog_utils import create_zip


class ScProductStage(cdk.Stage):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        _ = ServiceCatalogProductStack(self, "ScProductStack")


class ServiceCatalogProductStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        ## Seed code for the repository
        compressed_seed_path = "seed.zip"
        seed_folder = "deployment_cicd_seed"
        create_zip("seed.zip", Path(seed_folder))

        seed_asset = Asset(
            self,
            "SeedAsset",
            path=compressed_seed_path,
        )

        # Create a portfolio
        portfolio = sc.Portfolio(
            self,
            "mlflow-mlops-portfolio",
            display_name="MLflow-MLOps",
            provider_name="MLOpsAdmin",
        )

        # Create a pipeline product from a Product Stack
        product = sc.CloudFormationProduct(
            self,
            "PipelineProductStack",
            product_name="DeployMlflowModel",
            owner="MLOpsAdmin",
            product_versions=[
                sc.CloudFormationProductVersion(
                    cloud_formation_template=sc.CloudFormationTemplate.from_product_stack(
                        PipelineProduct(self, "PipelineProduct", seed_asset)
                    ),
                    product_version_name="RealTimeEndpoint",
                    description="A CDK pipeline for CI/CD of a SageMaker model to a real time endpoint",
                ),
            ],
        )

        portfolio.add_product(product)


class PipelineProduct(sc.ProductStack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        seed_asset: Asset,
    ):
        super().__init__(scope, id)

        model_name = cdk.CfnParameter(self, "ModelName")

        # ToDo option to use 3p code repository
        repo_name = cdk.Fn.join(
            delimiter="-",
            list_of_values=[
                "sagemaker-mlflow",
                model_name.value_as_string,
                "deployment",
            ],
        )
        repo = codecommit.Repository(
            self,
            "Repo",
            repository_name=repo_name,
            code=codecommit.Code.from_asset(
                asset=seed_asset,
                branch="main",
            ),
        )
        repo.apply_removal_policy(cdk.RemovalPolicy.RETAIN)

        # CICD pipeline
        pipeline_name = cdk.Fn.join(
            delimiter="-",
            list_of_values=[
                "sagemaker-mlflow",
                model_name.value_as_string,
                "pipeline",
            ],
        )

        pipeline = CodePipeline(
            self,
            "CiCdPipeline",
            pipeline_name=pipeline_name,
            synth=ShellStep(
                "Synth",
                input=CodePipelineSource.code_commit(repository=repo, branch="main"),
                commands=[
                    "npm install -g aws-cdk",
                    "python -m pip install -r requirements.txt",
                    f"cdk synth --context ModelName={model_name.value_as_string} --context StackName={cdk.Aws.STACK_NAME}",
                ],
            ),
            self_mutation_code_build_defaults=CodeBuildOptions(
                partial_build_spec=codebuild.BuildSpec.from_object(
                    {
                        "phases": {
                            "build": {
                                "commands": [
                                    f"cdk -a . deploy {cdk.Aws.STACK_NAME} --require-approval=never --verbose"
                                ]
                            }
                        }
                    }
                )
            ),
            synth_code_build_defaults=CodeBuildOptions(
                role_policy=[
                    PolicyStatement(
                        actions=[
                            "sagemaker:ListModelPackages",
                            "sagemaker:DescribeModelPackage",
                        ],
                        resources=[
                            f"arn:aws:sagemaker:{self.region}:{self.account}:model-package/*"
                        ],
                    ),
                ],
            ),
        )
