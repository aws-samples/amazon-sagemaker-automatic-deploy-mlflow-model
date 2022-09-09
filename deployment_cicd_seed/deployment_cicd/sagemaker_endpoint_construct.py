from typing import Sequence, Dict, Any, Union

import aws_cdk as cdk
import aws_cdk.aws_ec2 as ec2
from aws_cdk import aws_sagemaker as sagemaker

from aws_cdk.aws_iam import PolicyDocument, PolicyStatement, Role, ServicePrincipal
from aws_cdk.aws_kms import Key
from aws_cdk.aws_s3_assets import Asset
from constructs import Construct


class SageMakerModel(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        model_name: str = None,
        role: Role = None,
        model_package_name: str = None,
        model_artifact: Union[Asset, str] = None,
        image: Union[str, Dict[str, Any]] = None,
        environment: Dict[str, Any] = None,
        enable_network_isolation: bool = False,
        vpc: ec2.Vpc = None,
        vpc_subnets: Sequence[ec2.SubnetSelection] = None,
        security_groups: Sequence[ec2.ISecurityGroup] = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.role = role
        # create role with permissions suggested in the documentation
        # https://docs.aws.amazon.com/sagemaker/latest/dg/sagemaker-roles.html#sagemaker-roles-createmodel-perms
        if role is None:
            self.role = Role(
                self,
                f"Role",
                assumed_by=ServicePrincipal("sagemaker.amazonaws.com"),
                inline_policies={
                    "policy0": PolicyDocument(
                        statements=[
                            PolicyStatement(
                                actions=[
                                    "cloudwatch:PutMetricData",
                                    "logs:CreateLogStream",
                                    "logs:PutLogEvents",
                                    "logs:CreateLogGroup",
                                    "logs:DescribeLogStreams",
                                    "s3:GetObject",
                                    "s3:ListBucket",
                                    "ecr:GetAuthorizationToken",
                                    "ecr:BatchCheckLayerAvailability",
                                    "ecr:GetDownloadUrlForLayer",
                                    "ecr:BatchGetImage",
                                ],
                                resources=["*"],
                            )
                        ]
                    )
                },
            )
        if model_package_name:
            if any([model_artifact, image]):
                raise ValueError(
                    "model_package_name is mutually exclusive with model_artifact and image"
                )
            container_definition = sagemaker.CfnModel.ContainerDefinitionProperty(
                model_package_name=model_package_name,
            )

        else:
            if (model_artifact is None) or (image is None):
                raise ValueError(
                    "model_artifact and image, or model_package_name must be provided"
                )

            model_artifact_uri = model_artifact
            if isinstance(model_artifact, Asset):
                model_artifact_uri = model_artifact.s3_object_url

            image_config = None
            if isinstance(image, dict):
                image_config = image["ImageConfig"]
                image = image["Image"]

            if isinstance(image, dict):
                image_config = image["ImageConfig"]
                image = image["Image"]

            container_definition = sagemaker.CfnModel.ContainerDefinitionProperty(
                model_data_url=model_artifact_uri,
                environment=environment,
                image=image,
                image_config=image_config,
            )

        # prepare the network configuration for the endpoint, providing reasonable defaults if only the VPC is provided
        if vpc is not None:
            if vpc_subnets is None:
                vpc_subnets = [
                    # vpc.select_subnets(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED),
                    vpc.select_subnets(subnet_type=ec2.SubnetType.PRIVATE_WITH_NAT),
                ]
            if security_groups is None:
                security_groups = [
                    ec2.SecurityGroup(
                        self,
                        "SecurityGroup",
                        vpc=vpc,
                        description=f"Security group for {construct_id}",
                    )
                ]

        vpc_config = None
        if (vpc_subnets is not None) and (security_groups is not None):
            vpc_subnets_ids = [j.subnet_id for k in vpc_subnets for j in k.subnets]
            security_groups_ids = [k.security_group_id for k in security_groups]
            vpc_config = sagemaker.CfnModel.VpcConfigProperty(
                security_group_ids=security_groups_ids,
                subnets=vpc_subnets_ids,
            )
        self._cfn_model = sagemaker.CfnModel(
            self,
            construct_id,
            model_name=model_name,
            execution_role_arn=self.role.role_arn,
            containers=[container_definition],
            enable_network_isolation=enable_network_isolation,
            vpc_config=vpc_config,
        )
        self.model_name = self._cfn_model.attr_model_name
        self.network_isolation = enable_network_isolation


class ModelVariant(sagemaker.CfnEndpointConfig.ProductionVariantProperty):
    def __init__(
        self,
        model: SageMakerModel,
        initial_variant_weight: float = None,
        instance_type: str = None,
        initial_instance_count: int = None,
        variant_name: str = None,
        serverless_max_concurrency: int = None,
        serverless_size_in_mb: int = None,
    ) -> None:

        if variant_name is None:
            variant_name = "variant0"
        if initial_variant_weight is None:
            initial_variant_weight = 1

        serverless_config = None
        if any([serverless_max_concurrency, serverless_size_in_mb]):
            if any([instance_type, initial_instance_count]):
                raise ValueError(
                    "instance_type and initial_instance_count are mutually exclusive with serverless_max_concurrency and serverless_size_in_mb"
                )
            if model.network_isolation:
                raise ValueError(
                    "The network isolation is not supported for serverless endpoint. Please disable the network isolation"
                )
            # Handle default values for serverless arguments
            if serverless_max_concurrency is None:
                serverless_max_concurrency = 1
            if serverless_size_in_mb is None:
                serverless_size_in_mb = 1024
            if serverless_size_in_mb not in serverless_allowed_memory_in_mb:
                raise ValueError(
                    f"serverless_size_in_mb must be one of {serverless_allowed_memory_in_mb}"
                )
            serverless_config = sagemaker.CfnEndpointConfig.ServerlessConfigProperty(
                max_concurrency=serverless_max_concurrency,
                memory_size_in_mb=serverless_size_in_mb,
            )

        super().__init__(
            initial_variant_weight=initial_variant_weight,
            initial_instance_count=initial_instance_count,
            instance_type=instance_type,
            model_name=model.model_name,
            variant_name=variant_name,
            serverless_config=serverless_config,
        )


class DataCaptureConfig(object):
    def __init__(
        self,
        destination_s3_uri: str,
        capture_input: bool = True,
        capture_output: bool = True,
        initial_sampling_percentage: int = 100,
        kms_key: Key = None,
    ):
        kms_key_id = None
        if kms_key is not None:
            kms_key_id = kms_key.key_id

        capture_property = (
            [sagemaker.CfnEndpointConfig.CaptureOptionProperty("Input")]
            if capture_input
            else []
        ) + (
            [sagemaker.CfnEndpointConfig.CaptureOptionProperty("Output")]
            if capture_output
            else []
        )
        capture_content_type_header = (
            sagemaker.CfnEndpointConfig.CaptureContentTypeHeaderProperty(
                csv_content_types=["text/csv"],
                json_content_types=["application/json", "application/jsonlines"],
            )
        )
        sagemaker.CfnEndpointConfig.DataCaptureConfigProperty(
            capture_options=capture_property,
            destination_s3_uri=destination_s3_uri,
            initial_sampling_percentage=initial_sampling_percentage,
            capture_content_type_header=capture_content_type_header,
            enable_capture=True,
            kms_key_id=kms_key_id,
        )


class EndpointConfig(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        variants: Sequence[ModelVariant],
        data_capture_config: DataCaptureConfig = None,
        kms_key: Key = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        kms_key_id = None
        if kms_key is not None:
            kms_key_id = kms_key.key_id
        self._endpoint_config = sagemaker.CfnEndpointConfig(
            self,
            construct_id,
            production_variants=variants,
            data_capture_config=data_capture_config,
            kms_key_id=kms_key_id,
        )
        self.endpoint_config_name = self._endpoint_config.attr_endpoint_config_name


class Endpoint(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        endpoint_name: str = None,
        role: Role = None,
        endpoint_config: EndpointConfig = None,
        model: SageMakerModel = None,
        instance_type: str = None,
        initial_instance_count: int = None,
        serverless_max_concurrency: int = None,
        serverless_size_in_mb: int = None,
        capture_destination_s3_uri: str = None,
        endpoint_kms_key: Key = None,
        capture_kms_key: Key = None,
        model_artifact: Union[Asset, str] = None,
        image: Union[str, Dict[str, Any]] = None,
        environment: Dict[str, Any] = None,
        enable_network_isolation: bool = False,
        vpc: ec2.Vpc = None,
        vpc_subnets: Sequence[ec2.SubnetSelection] = None,
        security_groups: Sequence[ec2.ISecurityGroup] = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        if endpoint_config:
            if any(
                [
                    model,
                    role,
                    instance_type,
                    initial_instance_count,
                    capture_destination_s3_uri,
                    endpoint_kms_key,
                    capture_kms_key,
                    vpc,
                    vpc_subnets,
                    security_groups,
                ]
            ):
                raise ValueError(
                    "endpoint_config is mutually exclusive with model,\
                    role, \
                    instance_type,\
                    initial_instance_count,\
                    capture_destination_s3_uri,\
                    endpoint_kms_key,\
                    capture_kms_key,\
                    vpc,\
                    vpc_subnets,\
                    security_groups"
                )
        if all([model_artifact, image]):
            if any([endpoint_config, model]):
                raise ValueError(
                    "Conflicting definition of Model and Endpoint configuration"
                )
            model = SageMakerModel(
                self,
                "Model",
                model_artifact=model_artifact,
                image=image,
                environment=environment,
                enable_network_isolation=enable_network_isolation,
                vpc=vpc,
                vpc_subnets=vpc,
                security_groups=security_groups,
                role=role,
            )

        if endpoint_config is None:
            if not any(
                [
                    all([model, instance_type]),
                    all(
                        [
                            model,
                            any([serverless_max_concurrency, serverless_size_in_mb]),
                        ]
                    ),
                ]
            ):
                raise ValueError(
                    "Without an endpoint configuration it's necessary to indicate "
                    "at least a model and either an instance_type, or serverless_max_concurrency and/or serverless_size_in_mb"
                )
            data_capture_config = None
            if capture_destination_s3_uri is not None:
                data_capture_config = DataCaptureConfig(
                    destination_s3_uri=capture_destination_s3_uri,
                    kms_key=capture_kms_key,
                )
            endpoint_config = EndpointConfig(
                self,
                "Config",
                variants=[
                    ModelVariant(
                        model=model,
                        instance_type=instance_type,
                        initial_instance_count=initial_instance_count,
                        serverless_max_concurrency=serverless_max_concurrency,
                        serverless_size_in_mb=serverless_size_in_mb,
                    )
                ],
                data_capture_config=data_capture_config,
                kms_key=endpoint_kms_key,
            )

        self._endpoint = sagemaker.CfnEndpoint(
            self,
            f"{construct_id}",
            endpoint_config_name=endpoint_config.endpoint_config_name,
            endpoint_name=endpoint_name,
        )

        self.endpoint_name = self._endpoint.attr_endpoint_name


allowed_instance_type_list = [
    "ml.c4.2xlarge",
    "ml.c4.4xlarge",
    "ml.c4.8xlarge",
    "ml.c4.large",
    "ml.c4.xlarge",
    "ml.c5.18xlarge",
    "ml.c5.2xlarge",
    "ml.c5.4xlarge",
    "ml.c5.9xlarge",
    "ml.c5.large",
    "ml.c5.xlarge",
    "ml.c5d.18xlarge",
    "ml.c5d.2xlarge",
    "ml.c5d.4xlarge",
    "ml.c5d.9xlarge",
    "ml.c5d.large",
    "ml.c5d.xlarge",
    "ml.c6i.12xlarge",
    "ml.c6i.16xlarge",
    "ml.c6i.24xlarge",
    "ml.c6i.2xlarge",
    "ml.c6i.32xlarge",
    "ml.c6i.4xlarge",
    "ml.c6i.8xlarge",
    "ml.c6i.large",
    "ml.c6i.xlarge",
    "ml.g4dn.12xlarge",
    "ml.g4dn.16xlarge",
    "ml.g4dn.2xlarge",
    "ml.g4dn.4xlarge",
    "ml.g4dn.8xlarge",
    "ml.g4dn.xlarge",
    "ml.g5.12xlarge",
    "ml.g5.16xlarge",
    "ml.g5.24xlarge",
    "ml.g5.2xlarge",
    "ml.g5.48xlarge",
    "ml.g5.4xlarge",
    "ml.g5.8xlarge",
    "ml.g5.xlarge",
    "ml.inf1.24xlarge",
    "ml.inf1.2xlarge",
    "ml.inf1.6xlarge",
    "ml.inf1.xlarge",
    "ml.m4.10xlarge",
    "ml.m4.16xlarge",
    "ml.m4.2xlarge",
    "ml.m4.4xlarge",
    "ml.m4.xlarge",
    "ml.m5.12xlarge",
    "ml.m5.24xlarge",
    "ml.m5.2xlarge",
    "ml.m5.4xlarge",
    "ml.m5.large",
    "ml.m5.xlarge",
    "ml.m5d.12xlarge",
    "ml.m5d.24xlarge",
    "ml.m5d.2xlarge",
    "ml.m5d.4xlarge",
    "ml.m5d.large",
    "ml.m5d.xlarge",
    "ml.p2.16xlarge",
    "ml.p2.8xlarge",
    "ml.p2.xlarge",
    "ml.p3.16xlarge",
    "ml.p3.2xlarge",
    "ml.p3.8xlarge",
    "ml.p4d.24xlarge",
    "ml.r5.12xlarge",
    "ml.r5.24xlarge",
    "ml.r5.2xlarge",
    "ml.r5.4xlarge",
    "ml.r5.large",
    "ml.r5.xlarge",
    "ml.r5d.12xlarge",
    "ml.r5d.24xlarge",
    "ml.r5d.2xlarge",
    "ml.r5d.4xlarge",
    "ml.r5d.large",
    "ml.r5d.xlarge",
    "ml.t2.2xlarge",
    "ml.t2.large",
    "ml.t2.medium",
    "ml.t2.xlarge",
]


serverless_allowed_memory_in_mb = [1024, 2048, 3072, 4096, 5120, 6144]
