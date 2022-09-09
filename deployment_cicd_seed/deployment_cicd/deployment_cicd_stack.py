import aws_cdk as cdk
import aws_cdk.aws_codecommit as codecommit
import aws_cdk.aws_events as events
import aws_cdk.aws_events_targets as events_targets
import aws_cdk.aws_ssm as ssm
from aws_cdk.aws_codepipeline import Pipeline
from aws_cdk.aws_iam import PolicyStatement
from aws_cdk.pipelines import (
    CodeBuildOptions,
    CodePipeline,
    CodePipelineSource,
    ShellStep,
    ManualApprovalStep,
)
from constructs import Construct

from deployment_cicd.sagemaker_service_catalog_roles_construct import SageMakerSCRoles
from deployment_cicd.endpoint_deployment_stage import ModelDeployStage


class DeploymentCiCdStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        sm_roles = SageMakerSCRoles(self, "SageMakerSCRoles")

        model_name = self.node.try_get_context("ModelName")
        model_name_aws_friendly = model_name.replace("_", "-")

        repo_name = f"sagemaker-mlflow-{model_name}-deployment"
        pipeline_name = f"sagemaker-mlflow-{model_name}-pipeline"

        repo = codecommit.Repository.from_repository_name(
            self,
            "Repository",
            repository_name=repo_name,
        )

        pipeline = CodePipeline(
            self,
            "CiCdPipeline",
            pipeline_name=pipeline_name,
            synth=ShellStep(
                "Synth",
                input=CodePipelineSource.code_commit(
                    repository=repo,
                    branch="main",
                    event_role=sm_roles.code_build_role,
                ),
                commands=[
                    "npm install -g aws-cdk",
                    "python -m pip install -r requirements.txt",
                    f"cdk synth --context ModelName={model_name} --context StackName={cdk.Aws.STACK_NAME}",
                ],
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

        wave = pipeline.add_wave("wave")
        wave.add_stage(
            stage=ModelDeployStage(
                self, f"{model_name_aws_friendly}-Staging", stage_name="Staging"
            ),
            # pre=ManualApprovalStep("Deploy staging Endpoints"),
        )
        wave.add_stage(
            stage=ModelDeployStage(
                self, f"{model_name_aws_friendly}-Production", stage_name="Production"
            ),
            # pre=ManualApprovalStep("Deploy production Endpoints"),
        )

        # trigger the execution of the pipeline for every change in the status of the Model group in SageMaker Model Registry
        target_pipeline = Pipeline.from_pipeline_arn(
            self,
            "TargetPipeline",
            pipeline_arn=f"arn:aws:codepipeline:{self.region}:{self.account}:{pipeline_name}",
        )
        events.Rule(
            self,
            "DeployModelRule",
            rule_name=f"sagemaker-DeployModelRule-{model_name_aws_friendly}",
            description="Rule to trigger a new deployment when a model changes status",
            event_pattern=events.EventPattern(
                source=["aws.sagemaker"],
                detail_type=["SageMaker Model Package State Change"],
                detail={
                    "ModelPackageGroupName": [
                        model_name_aws_friendly,
                    ],
                    "ModelApprovalStatus": [
                        "Approved",
                    ],
                },
            ),
            targets=[
                events_targets.CodePipeline(
                    pipeline=target_pipeline,
                    event_role=sm_roles.events_role,
                )
            ],
        )

        # Create Webhook using a custom resource
        create_webhook_lambda_arn_parameter = (
            ssm.StringParameter.from_string_parameter_name(
                self,
                "CreateWebHookLambdaParameter",
                string_parameter_name="/mlflow-mlops/create_webhook_lambda_arn",
            )
        )

        cdk.CustomResource(
            self,
            "CRCreateWebHook",
            service_token=create_webhook_lambda_arn_parameter.string_value,
            properties=dict(ModelName=model_name),
        )
