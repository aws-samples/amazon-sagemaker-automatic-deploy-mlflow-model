import json
import os
import tarfile
import tempfile
from functools import cached_property
from pathlib import Path
from typing import Dict

import boto3
import mlflow
import sagemaker
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities import parameters
from aws_lambda_powertools.utilities.parameters import GetParameterError
from packaging import version
from pkg_resources import resource_filename

logger = Logger()
region = os.getenv("AWS_REGION")

sm_client = boto3.client("sagemaker")

model_data_path_dict = {
    "tensorflow": "tfmodel",
    "xgboost": ".",
    "python_function": ".",
    "sklearn": ".",
}


def model_data_path(flavor: str):
    return model_data_path_dict.get(flavor, "data/model")


sm_model_data_path_dict = {
    "tensorflow": "model/1",
    "keras": "model/1",
}


def sm_model_data_path(flavor: str):
    return sm_model_data_path_dict.get(flavor, "")


def get_fw_versions(framework: str):
    try:
        with open(
            resource_filename(
                sagemaker.__name__,
                f"image_uri_config/{framework}.json",
            ),
            "r",
        ) as f:
            fw_dict = json.load(f)
    except:
        logger.exception(f"No matching framework for {framework}")
        raise ValueError()
    if "inference" in fw_dict:
        fw_dict = fw_dict["inference"]
    return fw_dict["versions"].keys()


def get_latest_fw_versions(framework: str):
    a = {version.parse(k): k for k in get_fw_versions(framework)}
    return a[max(a.keys())]


class MlflowModel(object):
    def __init__(self, model_object):
        self._model_object = model_object
        self.name = model_object.name
        self.version = model_object.version
        self._model_uri = f"models:/{model_object.name}/{model_object.version}"
        self._get_model_artifact()
        self._get_conf()

    def _get_model_artifact(self):
        self.artifact_tmpdir = tempfile.TemporaryDirectory()
        self.artifact_path = Path(self.artifact_tmpdir.name)
        mlflow.tracking.artifact_utils._download_artifact_from_uri(
            self._model_uri, output_path=self.artifact_path.as_posix()
        )

    def _get_conf(self):
        self.config = mlflow.models.Model.load(self.artifact_path / "MLmodel")
        try:
            self.flavor = [k for k in self.config.flavors if k != "python_function"][0]
        except IndexError:
            self.flavor = [k for k in self.config.flavors if k][0]

    def create_tarfile(self, tarfile_path: Path):
        with tarfile.open(tarfile_path.as_posix(), mode="w:gz") as archive:
            flavor_default_path = Path(__file__).parent / self.flavor

            if not (
                requirements_path := self.artifact_path / "sagemaker/requirements.txt"
            ).exists():
                requirements_path = flavor_default_path / "requirements.txt"
                logger.info(
                    f"No requirement.txt for SageMaker deployment, using the default one {requirements_path}"
                )

            if requirements_path.exists():
                archive.add(
                    requirements_path,
                    arcname=requirements_path.relative_to(flavor_default_path),
                )

            if not (
                inference_path := self.artifact_path / "sagemaker/inference.py"
            ).exists():
                inference_path = flavor_default_path / "inference.py"
                logger.info(
                    f"No inference.py for SageMaker deployment, using the default one {inference_path}"
                )

            if inference_path.exists():
                archive.add(
                    inference_path,
                    arcname=inference_path.relative_to(flavor_default_path),
                )

            archive.add(
                self.artifact_path / model_data_path(self.flavor),
                arcname=sm_model_data_path(self.flavor),
                recursive=True,
            )

        return tarfile_path.stat().st_size / 10**6

    @cached_property
    def image_uri(self) -> str:
        if retval := self.tags.get("sagemaker_deploy_image"):
            logger.info(f"Using image found in the model tags")
            return retval

        logger.info(
            f"Searching for image to flavor {self.flavor}. "
            "Starting from the Parameter Store"
        )

        if flavor := self.tags.get("sagemaker_deploy_flavor"):
            logger.info(f"Using flavor found in the model tags")
            self.flavor = flavor

        try:
            uri = parameters.get_parameter(
                name=f"/mlflow-mlops/{self.flavor}_image_uri",
            )
            logger.info(f"Found image uri {uri}")
            return uri
        except GetParameterError:
            logger.exception(
                f"No parameter found for flavor {self.flavor}. "
                "Trying to guess the right framework and version"
            )
            pass
        fw = self.sagemaker_framework

        if not fw:
            logger.info(
                "Failed to find a matching framework image from the model configuration"
            )
        if fw["framework"] == "python_function":
            logger.info("Using python_function image")
            return parameters.get_parameter(
                name=f"/mlflow-mlops/python_function_image_uri",
            )
        try:
            uri = sagemaker.image_uris.retrieve(
                framework=fw["framework"],
                region=region,
                image_scope="inference",
                version=fw["fw_version"],
                instance_type="ml.g",
            )
        except ValueError:
            uri = sagemaker.image_uris.retrieve(
                framework=fw["framework"],
                region=region,
                image_scope="inference",
                version=fw["fw_version"],
                instance_type="ml.c",
            )
        logger.info(f"Found image uri {uri}")
        return uri

    @cached_property
    def sagemaker_framework(self) -> Dict:
        return _get_matching_framework(self)

    @cached_property
    def model_metadata(self) -> Dict:
        return {
            f"mlflow{k}": str(o)
            for k, o in self._model_object.__dict__.items()
            if k != "_tags"
            if len(str(o)) > 0
        }

    @property
    def tags(self) -> Dict:
        return self._model_object.tags

    def env(self, model_data_uri) -> Dict:
        if self.sagemaker_framework["framework"] in ["xgboost", "sklearn"]:
            return {
                "SAGEMAKER_SUBMIT_DIRECTORY": model_data_uri,
                "SAGEMAKER_PROGRAM": "inference.py",
            }
        return {}


def create_model(model_artifact_uri: str, mlflow_model: MlflowModel):
    """
    Register a model in model registry

    """
    model_name_aws_friendly = mlflow_model.name.replace("_", "-")

    retval = sm_client.create_model_package(
        ModelPackageGroupName=model_name_aws_friendly,
        ModelApprovalStatus="Approved",
        InferenceSpecification={
            "Containers": [
                {
                    "Image": mlflow_model.image_uri,
                    "ModelDataUrl": model_artifact_uri,
                    "Environment": mlflow_model.env(model_artifact_uri),
                },
            ],
            "SupportedContentTypes": [
                "application/json",
                "text/csv",
                "application/x-npy",
            ],
            "SupportedResponseMIMETypes": [
                "application/json",
                "text/csv",
                "application/x-npy",
            ],
        },
        CustomerMetadataProperties=mlflow_model.model_metadata,
        ModelPackageDescription=f"mlflow {mlflow_model.name}-v{mlflow_model.version}",
    )
    return retval["ModelPackageArn"]


def create_model_group(model_name: str):
    """
    Create a model group if it doesn't exist yet
    """
    model_list = [
        k["ModelPackageGroupName"]
        for k in sm_client.list_model_package_groups(NameContains=model_name)[
            "ModelPackageGroupSummaryList"
        ]
    ]
    if model_name not in model_list:
        sm_client.create_model_package_group(
            ModelPackageGroupName=model_name,
            Tags=[dict(Key="model-source", Value="mlflow")],
        )


def _match_fw_version(v: version, framework: str, match_minor: bool = False) -> str:
    """returns the best matching version of a framework

    Args:
        v (version): version as parsed from mlflow metadata
        framework (str): sagemaker framework matching the mlflow flavors
        match_minor (bool, optional): True is necessary to also match the minor version. Defaults to False.

    Returns:
        str: framework version for retrieving the image uri
    """
    if v is None:
        return
    matching_versions = [
        k
        for k in get_fw_versions(framework)
        if v.major == version.parse(k).major
        if (v.minor == version.parse(k).minor) or not match_minor
    ]
    if len(matching_versions) > 0:
        a = {version.parse(k): k for k in matching_versions}
        return a[max(a.keys())]


def _get_flavor_version(model_obj: MlflowModel, flavor: str) -> version:
    """extract the framework version from the mlflow metadata

    Args:
        model_obj (MlflowModel): _description_
        flavor (str): _description_

    Returns:
        version: _description_
    """
    flavors_dict = model_obj.config.flavors
    conf_dict = flavors_dict.get(flavor)
    if conf_dict is None:
        return None
    if flavor == "xgboost":
        flavor = "xgb"
    try:
        v = version.parse(conf_dict[f"{flavor}_version"])
        return v
    except Exception as e:
        return


def _get_fw_version_from_req(model_obj: MlflowModel, fw: str) -> version:
    with (model_obj.artifact_path / "requirements.txt").open("r") as f:
        a = [k[1] for line in f if (k := line.split("=="))[0] == fw]
        if len(a) > 0:
            return version.parse(a[0])


def _get_mlflow_framework_versions(model_obj: MlflowModel, framework: str) -> version:
    if not (ver := _get_flavor_version(model_obj, framework)):
        ver = _get_fw_version_from_req(model_obj, framework)
    return ver


def _get_matching_framework(model_obj):
    tentative_fw = model_obj.flavor
    if tentative_fw == "keras":
        tentative_fw = "tensorflow"

    match_minor = False
    # https://xgboost.readthedocs.io/en/stable/tutorials/saving_model.html#a-note-on-backward-compatibility-of-models-and-memory-snapshots
    # if tentative_fw in ["xgboost"]:
    #     match_minor = True

    v = _get_mlflow_framework_versions(model_obj, tentative_fw)
    if ver := _match_fw_version(v, tentative_fw, match_minor=match_minor):
        logger.info(f"Found a matching framework: {tentative_fw}, {ver}")
        return dict(framework=tentative_fw, fw_version=ver)

    if "python_function" in model_obj.config.flavors:
        logger.info(f"No matching framework found, using python_function")
        return dict(
            framework="python_function",
            fw_version=model_obj.config.mlflow_version,
        )
    return
