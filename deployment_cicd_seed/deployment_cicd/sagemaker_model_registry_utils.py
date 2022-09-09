import logging

import boto3

logger = logging.getLogger(__name__)

sm_client = boto3.client("sagemaker")


def get_models_descriptions(model_name: str, stage: str = None) -> list:
    """
    Get the model description from the SageMaker API

    Args:
        model_name: the name of the model
        stage: the name of the stage environment metadata in MLflow

    Returns:
        A list of dictionaries with the model description
    """
    model_name_aws_friendly = model_name.replace("_", "-")
    paginator = sm_client.get_paginator("list_model_packages")


    try:
        descriptions_list = [
            sm_client.describe_model_package(ModelPackageName=j["ModelPackageArn"])
            for k in paginator.paginate(ModelPackageGroupName=model_name_aws_friendly)
            for j in k["ModelPackageSummaryList"]
        ]
    except:
        logger.exception("failed to retrieve list of models")
        return []
    if stage is not None:
        descriptions_list = [
            k
            for k in descriptions_list
            if k["CustomerMetadataProperties"]["mlflow_current_stage"] == stage
        ]
    return descriptions_list
