import os

import boto3
import jsonpickle
from aws_lambda_powertools import Logger
from mlflow.tracking.client import MlflowClient

logger = Logger()
secret_manager = boto3.client("secretsmanager")
sm_client = boto3.client("sagemaker")

mlflow_secret_name = os.getenv("MLFLOW_SECRET_NAME")

secret = jsonpickle.loads(
    secret_manager.get_secret_value(SecretId=mlflow_secret_name)["SecretString"]
)

os.environ["MLFLOW_TRACKING_URI"] = "databricks"
os.environ["DATABRICKS_HOST"] = secret["DATABRICKS_HOST"]
os.environ["DATABRICKS_TOKEN"] = secret["DATABRICKS_TOKEN"]


mlflow_client = MlflowClient()
active_stages = ["production", "staging"]


@logger.inject_lambda_context(log_event=True)
def handler(event, context):
    if "webhook_id" in event:
        retval = from_webhook(event)
    else:
        retval = direct_invocation(event)

    retval["status"] = "success"
    retval["message"]: prepare_message(retval)

    return retval


def parse_even(event):
    model_name = event["model_name"]
    model_name_aws_friendly = model_name.replace("_", "-")

    mlf_version = event.get("version")

    mlf_model_reg = {
        k.run_id: k for k in mlflow_client.search_model_versions(f"name='{model_name}'")
    }
    sm_models_reg = get_sm_models(model_name_aws_friendly)
    logger.info(
        f"There are {len(sm_models_reg)} models registered in SageMaker model registry"
    )
    return model_name, mlf_version, mlf_model_reg, sm_models_reg


def from_webhook(event):
    model_name, mlf_version, mlf_model_reg, sm_models_reg = parse_even(event)
    mlf_model_ids = [
        k.run_id for _, k in mlf_model_reg.items() if k.version == mlf_version
    ]
    if len(mlf_model_ids) != 1:
        return

    model_id = mlf_model_ids[0]
    mlf_model = mlf_model_reg[model_id]
    mlf_stage = mlf_model.current_stage.lower()

    sm_model = sm_models_reg.get(model_id)

    ret_val = {
        "models_to_update": [],
        "models_id_to_drop": [],
        "models_id_to_create": [],
    }
    if (sm_model is None) and (mlf_stage in active_stages):
        ret_val["models_id_to_create"] = (
            [{"mlf_model": jsonpickle.encode(mlf_model)}],
        )
        logger.info(f"Transfer {model_name} version {mlf_version}, stage: {mlf_stage}")
        return ret_val

    if mlf_stage in active_stages:
        ret_val["models_to_update"] = [
            {
                "arn": sm_model["model_arn"],
                "mlflow_model": jsonpickle.encode(mlf_model),
            }
        ]
        logger.info(
            f"Updating {model_name} version {mlf_version} to stage: {mlf_stage}"
        )
        return ret_val

    ret_val["models_id_to_drop"] = [sm_model["model_arn"]]
    logger.info(f"Removing {model_name} version {mlf_version}")
    return ret_val


def direct_invocation(event):
    model_name, mlf_version, mlf_model_reg, sm_models_reg = parse_even(event)
    mlf_model_reg = {
        k: o
        for k, o in mlf_model_reg.items()
        if o.current_stage.lower() in active_stages
    }
    logger.info(
        f"There are {len(mlf_model_reg)} models in Staging or Production stage in MLflow model registry"
    )

    id_to_transfer = mlf_model_reg.keys() - sm_models_reg.keys()
    id_to_drop = sm_models_reg.keys() - mlf_model_reg.keys()
    models_to_update = [
        {"arn": sm_models_reg[k]["model_arn"], "mlflow_model": jsonpickle.encode(o)}
        for k, o in mlf_model_reg.items()
        if k in sm_models_reg
        if sm_models_reg[k]["mlflow_current_stage"] != o.current_stage
    ]

    return {
        "models_to_update": models_to_update,
        "models_id_to_drop": [sm_models_reg[idx]["model_arn"] for idx in id_to_drop],
        "models_id_to_create": [
            {"mlf_model": jsonpickle.encode(mlf_model_reg[k])} for k in id_to_transfer
        ],
    }


def prepare_message(payload):
    return f"{len(payload['models_id_to_drop'])} models to delete, {len(payload['models_to_update'])} models to update, {len(payload['models_id_to_create'])} new models to create"


def get_sm_models(model_name: str):
    model_name_aws_friendly = model_name.replace("_", "-")

    list_model_arn = [
        k["ModelPackageArn"]
        for k in sm_client.list_model_packages(
            ModelPackageGroupName=model_name_aws_friendly
        )["ModelPackageSummaryList"]
    ]
    ret_dict = {}
    for arn in list_model_arn:
        description = sm_client.describe_model_package(ModelPackageName=arn)
        if not description["CustomerMetadataProperties"]["mlflow_run_id"]:
            continue
        ret_dict[description["CustomerMetadataProperties"]["mlflow_run_id"]] = {
            "model_arn": description["ModelPackageArn"],
            "model_status": description["ModelApprovalStatus"]
            if "ModelApprovalStatus" in description
            else None,
            "mlflow_current_stage": description["CustomerMetadataProperties"][
                "mlflow_current_stage"
            ],
            "CustomerMetadataProperties": description["CustomerMetadataProperties"],
        }
    return ret_dict
