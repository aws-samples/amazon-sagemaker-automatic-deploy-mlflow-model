import aws_cdk as cdk
import aws_cdk.aws_codecommit as codecommit
import aws_cdk.aws_ssm as ssm
from aws_cdk.aws_iam import ManagedPolicy, PolicyStatement, Role, ServicePrincipal
from aws_cdk.pipelines import CodeBuildStep, CodePipeline, CodePipelineSource
from constructs import Construct

from mlflow_mlops.product_stage import ScProductStage
from mlflow_mlops.registry_sync_stage import ModelRepackStage


class MlflowMlopsStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        repo_name = cdk.CfnParameter(self, "RepositoryName", default="mlflow-mlops")
        secret_name = cdk.CfnParameter(self, "SecretName")
        ssm.StringParameter(
            self,
            "Parameter",
            string_value=secret_name.value_as_string,
            parameter_name="/mlflow-mlops/secret_name",
        )

        repo = codecommit.Repository.from_repository_name(
            self,
            "Repository",
            repository_name=repo_name.value_as_string,
        )

        synth_step = CodeBuildStep(
            "Synth",
            input=CodePipelineSource.code_commit(repo, branch="main"),
            commands=[
                "npm install -g aws-cdk",
                "python -m pip install -r requirements.txt",
                "cdk synth",
            ],
            primary_output_directory="cdk.out",
            role_policy_statements=[
                PolicyStatement(
                    actions=[
                        "sagemaker:ListDomains",
                        "sagemaker:DescribeDomain",
                    ],
                    resources=[
                        f"arn:aws:sagemaker:{self.region}:{self.account}:domain/*"
                    ],
                ),
            ],
        )

        pipeline = CodePipeline(
            self,
            "Pipeline",
            pipeline_name="MLflow-MLOps",
            synth=synth_step,
            docker_enabled_for_synth=True,
        )

        wave = pipeline.add_wave("wave")
        wave.add_stage(ModelRepackStage(self, "ModelRepack"))
        wave.add_stage(ScProductStage(self, "ServiceCatalogProduct"))
