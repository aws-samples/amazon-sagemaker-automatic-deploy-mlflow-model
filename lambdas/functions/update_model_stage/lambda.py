import boto3
import jsonpickle
import mlflow
from aws_lambda_powertools import Logger

logger = Logger()
sm_client = boto3.client("sagemaker")


@logger.inject_lambda_context(log_event=True)
def handler(event, context):
    model_arn = event["arn"]
    mlflow_model = jsonpickle.decode(event["mlflow_model"])
    try:
        update_stage(mlflow_model, model_arn)
        message = f"Updated model {model_arn}"

    except:
        message = f"Failed to update stage of model {model_arn}"
        logger.exception(message)

    return {
        "status": "success",
        "message": message,
    }


def update_stage(mlflow_model: mlflow.models.Model, model_arn: str):

    """
    Update the stage of the model in SageMaker Model Registry
    to match the stage in MLflow
    """

    ml_flow_metadata = format_metadata(mlflow_model)
    new_stage = ml_flow_metadata["mlflow_current_stage"]

    logger.info(f"Updating model {model_arn} to {new_stage}")
    retval = sm_client.update_model_package(
        ModelPackageArn=model_arn,
        CustomerMetadataProperties={**ml_flow_metadata},
    )
    return retval


def format_metadata(mlflow_model: mlflow.models.Model):
    """
    Formats the model metadata into a dictionary that can be used as CustomerMetadataProperties
    """

    return {
        f"mlflow{k}": str(o)
        for k, o in mlflow_model.__dict__.items()
        if k != "_tags"
        if len(str(o)) > 0
    }
