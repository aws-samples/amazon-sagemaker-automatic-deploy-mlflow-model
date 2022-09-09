from pathlib import Path

import aws_cdk as cdk
import aws_cdk.aws_apigateway as apigw
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_lambda_python_alpha as lambda_python
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_ssm as ssm
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_stepfunctions_tasks as tasks
from aws_cdk.aws_iam import PolicyStatement
from aws_cdk.aws_secretsmanager import Secret
from constructs import Construct

from mlflow_mlops.custom_container_construct import FlavorCustomContainer
from mlflow_mlops.mlflow_dockerfile import create_docker_file
from mlflow_mlops.prepare_sagemaker_layer import create_sagemaker_libraries


class ModelRepackStage(cdk.Stage):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        _ = ModelRepackStack(self, "ModelSyncStack")


class ModelRepackStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        secret_name = ssm.StringParameter.from_string_parameter_name(
            self,
            "SecretNameParameter",
            string_parameter_name="/mlflow-mlops/secret_name",
        )

        mlflow_token = Secret.from_secret_name_v2(
            self,
            "RegistryToken",
            secret_name.string_value,
        )

        webhook_shared_secret = Secret(
            self,
            "WebhookSharedSecret",
            description="Shared secret to validate webhook invocations",
            secret_name="mlflow-mlops/webhook_shared_secret",
        )

        bucket = s3.Bucket(
            self,
            "ModelArtifactBucket",
            bucket_name=f"sagemaker-mlflow-{self.region}-{self.account}",
        )
        ssm.StringParameter(
            self,
            "s3-artifact-bucket-name-param",
            parameter_name="/mlflow-mlops/mlflow-artifact-bucket",
            string_value=bucket.bucket_name,
        )

        sm_execution_role_arn = self.format_arn(
            resource="role",
            service="iam",
            region="",
            resource_name="service-role/AmazonSageMakerServiceCatalogProductsExecutionRole",
        )

        ### Step function
        lambdas_runtime = lambda_.Runtime.PYTHON_3_9

        mlflow_layer = lambda_python.PythonLayerVersion(
            self,
            "MlflowClientLayer",
            entry="lambdas/layers/mlflow",
            compatible_runtimes=[lambdas_runtime],
            description="MLflow skinny client",
            layer_version_name="mlflow-skinny",
        )

        powertools_layer = lambda_.LayerVersion.from_layer_version_arn(
            self,
            id="lambda-powertools",
            layer_version_arn=f"arn:aws:lambda:{self.region}:017000801446:layer:AWSLambdaPowertoolsPython:33",
        )

        create_diff_lambda = lambda_.Function(
            self,
            "CreateDiffLambda",
            code=lambda_.Code.from_asset("lambdas/functions/create_model_diff"),
            handler="lambda.handler",
            runtime=lambdas_runtime,
            layers=[mlflow_layer, powertools_layer],
            environment={
                "MLFLOW_SECRET_NAME": mlflow_token.secret_name,
            },
            timeout=cdk.Duration.minutes(5),
            memory_size=128,
        )

        lambda_role = create_diff_lambda.role

        mlflow_token.grant_read(create_diff_lambda)

        lambda_role.add_to_principal_policy(
            PolicyStatement(
                actions=[
                    "sagemaker:ListModelPackages",
                    "sagemaker:ListModelPackageGroups",
                    "sagemaker:DescribeModelPackage",
                    "sagemaker:UpdateModelPackage",
                    "sagemaker:DeleteModelPackage",
                    "sagemaker:CreateModelPackage",
                    "sagemaker:CreateModelPackageGroup",
                    "sagemaker:AddTags",
                ],
                resources=[
                    self.format_arn(
                        service="sagemaker",
                        resource="model-package",
                        resource_name="*",
                    ),
                    self.format_arn(
                        service="sagemaker",
                        resource="model-package-group",
                        resource_name="*",
                    ),
                ],
            )
        )
        lambda_role.add_to_principal_policy(
            PolicyStatement(
                actions=["sagemaker:ListModelPackageGroups"],
                resources=["*"],
            )
        )

        lambda_role.add_to_principal_policy(
            PolicyStatement(
                actions=["ssm:GetParameters", "ssm:GetParameter"],
                resources=[
                    self.format_arn(
                        service="ssm",
                        resource="parameter",
                        resource_name="mlflow-mlops*",
                    )
                ],
            )
        )

        lambda_role.add_to_principal_policy(
            PolicyStatement(
                actions=[
                    "ecr:BatchCheckLayerAvailability",
                    "ecr:BatchGetImage",
                    "ecr:DescribeImages",
                    "ecr:DescribeRepositories",
                    "ecr:ListImages",
                    "ecr:ListTagsForResource",
                ],
                resources=[
                    self.format_arn(
                        service="ecr",
                        account="*",
                        resource="repository",
                        resource_name="*sagemaker*",
                    )
                ],
            )
        )
        lambda_role.add_to_principal_policy(
            PolicyStatement(
                actions=[
                    "ecr:DescribeRegistry",
                ],
                resources=["*"],
            )
        )

        create_diff_task = tasks.LambdaInvoke(
            self,
            "Create Registry Diff",
            lambda_function=create_diff_lambda,
            result_path="$.result",
        )

        parallel_model_state = sfn.Parallel(
            self, "Sync Model Registries", input_path="$.result.Payload"
        )

        update_model_stage_lambda = lambda_.Function(
            self,
            "UpdateModelLambda",
            code=lambda_.Code.from_asset("lambdas/functions/update_model_stage"),
            handler="lambda.handler",
            runtime=lambdas_runtime,
            layers=[mlflow_layer, powertools_layer],
            timeout=cdk.Duration.minutes(1),
            memory_size=128,
            role=lambda_role,
        )
        update_model_task = tasks.LambdaInvoke(
            self,
            "Update Model Stage",
            lambda_function=update_model_stage_lambda,
        )
        update_model_map = sfn.Map(
            self,
            "Models Update Map",
            max_concurrency=3,
            items_path=sfn.JsonPath.string_at("$.models_to_update"),
        )
        parallel_model_state.branch(update_model_map.iterator(update_model_task))

        remove_model_task = tasks.CallAwsService(
            self,
            "Delete Models",
            service="sagemaker",
            action="deleteModelPackage",
            parameters={"ModelPackageName": sfn.JsonPath.string_at("$")},
            iam_resources=[
                self.format_arn(
                    resource="model-package",
                    service="sagemaker",
                    resource_name="*",
                )
            ],
        )
        remove_model_map = sfn.Map(
            self,
            "Models Remove Map",
            max_concurrency=3,
            items_path=sfn.JsonPath.string_at("$.models_id_to_drop"),
        )
        parallel_model_state.branch(remove_model_map.iterator(remove_model_task))

        lambda_layer_path = "lambdas/layers/sagemaker"
        create_sagemaker_libraries(lambda_layer_path)
        sagemaker_layer = lambda_.LayerVersion(
            self,
            "SageMakerLayer",
            code=lambda_.Code.from_asset(lambda_layer_path),
            compatible_architectures=[lambda_.Architecture.X86_64],
            compatible_runtimes=[lambdas_runtime],
        )

        numpy_layer = lambda_.LayerVersion.from_layer_version_arn(
            self,
            "NumpyLayer",
            f"arn:aws:lambda:{self.region}:336392948345:layer:AWSDataWrangler-Python39:5",
        )

        transfer_model_lambda = lambda_.Function(
            self,
            "TransferModelLambda",
            code=lambda_.Code.from_asset(path="lambdas/functions/transfer_model"),
            handler="lambda.handler",
            runtime=lambdas_runtime,
            layers=[powertools_layer, sagemaker_layer, numpy_layer],
            environment={
                "MLFLOW_SECRET_NAME": mlflow_token.secret_name,
                "MODEL_ROLE_ARN": sm_execution_role_arn,
                "BUCKET_NAME": bucket.bucket_name,
            },
            timeout=cdk.Duration.minutes(15),
            memory_size=512,
            role=lambda_role,
            ephemeral_storage_size=cdk.Size.mebibytes(5000),
        )

        bucket.grant_read_write(transfer_model_lambda)
        transfer_model_task = tasks.LambdaInvoke(
            self,
            "Model Transfer Stage",
            lambda_function=transfer_model_lambda,
        )
        transfer_model_map = sfn.Map(
            self,
            "Models Transfer Map",
            max_concurrency=3,
            items_path=sfn.JsonPath.string_at("$.models_id_to_create"),
        )
        parallel_model_state.branch(transfer_model_map.iterator(transfer_model_task))

        definition = create_diff_task.next(parallel_model_state).next(
            sfn.Succeed(self, "Done")
        )
        state_machine = sfn.StateMachine(
            self,
            "RepackStateMachine",
            definition=definition,
        )

        state_machine.role.add_to_principal_policy(
            PolicyStatement(
                actions=["sagemaker:DeleteModelPackage"],
                resources=[
                    f"arn:aws:sagemaker:{self.region}:{self.account}:model-package/*"
                ],
            )
        )

        start_state_machine_lambda = lambda_.Function(
            self,
            "StartSfnLambda",
            code=lambda_.Code.from_asset("lambdas/functions/start_state_machine"),
            handler="lambda.handler",
            runtime=lambdas_runtime,
            layers=[powertools_layer],
            timeout=cdk.Duration.seconds(30),
            memory_size=128,
            environment={
                "SFN_ARN": state_machine.state_machine_arn,
                "WEBHOOK_SHARED_SECRET_NAME": webhook_shared_secret.secret_name,
            },
        )
        state_machine.grant_start_execution(start_state_machine_lambda)
        webhook_shared_secret.grant_read(start_state_machine_lambda)
        rest_api = apigw.LambdaRestApi(
            self,
            "MLflowRegistrySyncRestApi",
            handler=start_state_machine_lambda,
        )

        ### Lambda Fn supporting a custom resource to create webhooks in MLflow Model Registry
        webhook_layer = lambda_python.PythonLayerVersion(
            self,
            "WebhookLayer",
            entry="lambdas/layers/mlflow_webhooks",
            compatible_runtimes=[lambdas_runtime],
            description="MLflow skinny client",
            layer_version_name="databricks-registry-webhooks",
        )

        create_wh_lambda = lambda_.Function(
            self,
            "CreateWebHookLambda",
            code=lambda_.Code.from_asset(path="lambdas/functions/create_webhook"),
            handler="lambda.handler",
            runtime=lambdas_runtime,
            layers=[powertools_layer, webhook_layer],
            environment={
                "WEBHOOK_SECRET_NAME": webhook_shared_secret.secret_name,
                "MLFLOW_SECRET_NAME": mlflow_token.secret_name,
                "TARGET_URL": rest_api.url,
            },
            timeout=cdk.Duration.seconds(30),
        )
        webhook_shared_secret.grant_read(create_wh_lambda)
        mlflow_token.grant_read(create_wh_lambda)

        ssm.StringParameter(
            self,
            "CreateWebHookLambdaParameter",
            string_value=create_wh_lambda.function_arn,
            parameter_name="/mlflow-mlops/create_webhook_lambda_arn",
        )

        # Custom docker image for flavor `python_function`
        flavor = "python_function"
        mlflow_version = create_docker_file(Path("docker_images") / flavor)

        python_function_image = FlavorCustomContainer(
            self,
            "PyfuncImage",
            flavor=flavor,
            version=mlflow_version,
        )