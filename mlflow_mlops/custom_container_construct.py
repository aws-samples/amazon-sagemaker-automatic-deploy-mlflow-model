import aws_cdk.aws_ecr as ecr
import aws_cdk.aws_ssm as ssm
import cdk_ecr_deployment as ecrdeploy
from aws_cdk.aws_ecr_assets import DockerImageAsset
from constructs import Construct


class FlavorCustomContainer(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id,
        flavor: str,
        version: str = None,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)
        ecr_repo = ecr.Repository(
            self,
            "EcrRepo",
            repository_name=f"sagemaker-mlflow-mlops/{flavor}",
        )

        image_asset = DockerImageAsset(
            self, "ImageAsset", directory=f"docker_images/{flavor}"
        )

        mlflow_image_uri = ecr_repo.repository_uri_for_tag(tag=version)
        ecrdeploy.ECRDeployment(
            self,
            "DeployDockerImage",
            src=ecrdeploy.DockerImageName(image_asset.image_uri),
            dest=ecrdeploy.DockerImageName(mlflow_image_uri),
        )

        ssm.StringParameter(
            self,
            "MlflowDockerImageParameter",
            string_value=mlflow_image_uri,
            parameter_name=f"/mlflow-mlops/{flavor}_image_uri",
        )