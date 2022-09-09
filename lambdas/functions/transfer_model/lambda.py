import os
import tempfile
from pathlib import Path

import boto3
import jsonpickle
from aws_lambda_powertools import Logger
from mlflow.tracking.client import MlflowClient

from utils import MlflowModel, create_model, create_model_group

logger = Logger()
secret_manager = boto3.client("secretsmanager")
s3 = boto3.resource("s3")

mlflow_secret_name = os.getenv("MLFLOW_SECRET_NAME")
model_role_arn = os.getenv("MODEL_ROLE_ARN")
region = os.getenv("AWS_REGION")

secret = jsonpickle.loads(
    secret_manager.get_secret_value(SecretId=mlflow_secret_name)["SecretString"]
)

os.environ["MLFLOW_TRACKING_URI"] = "databricks"
os.environ["DATABRICKS_HOST"] = secret["DATABRICKS_HOST"]
os.environ["DATABRICKS_TOKEN"] = secret["DATABRICKS_TOKEN"]

bucket_name = os.getenv("BUCKET_NAME")
model_name = os.getenv("MODEL_NAME")


mlflow_client = MlflowClient()


@logger.inject_lambda_context(log_event=True)
def handler(event, context):
    mlf_model = jsonpickle.decode(event["mlf_model"])
    logger.info("mlf_model:")
    logger.info(mlf_model)
    key = f"{mlf_model.name}/{mlf_model.version}/model.tar.gz"
    model_artifact_uri = f"s3://{bucket_name}/{key}"

    model = MlflowModel(mlf_model)
    model_name_aws_friendly = model.name.replace("_", "-")

    with tempfile.TemporaryDirectory() as tmp_d:
        model_file_path = Path(f"{tmp_d}/model.tar.gz")
        model.create_tarfile(model_file_path)
        obj = s3.Object(
            bucket_name=bucket_name,
            key=key,
        )
        try:
            obj.upload_file(model_file_path.as_posix())
            logger.info(f"tar.gz archive uploaded to {model_artifact_uri}")
        except:
            logger.exception("Failed to upload tar.gz archive to S3")
    create_model_group(model_name_aws_friendly)
    create_model(model_artifact_uri, model)
