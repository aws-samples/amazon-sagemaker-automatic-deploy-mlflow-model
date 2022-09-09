import os

import boto3
import jsonpickle
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities import parameters
from crhelper import CfnResource
from databricks_registry_webhooks import HttpUrlSpec, RegistryWebhooksClient
from databricks_registry_webhooks.exceptions import RestException

logger = Logger()
secret_manager = boto3.client("secretsmanager")

target_url = os.getenv("TARGET_URL")
webhook_secret_name = os.getenv("WEBHOOK_SECRET_NAME")
mlflow_secret_name = os.getenv("MLFLOW_SECRET_NAME")

# Set env parameters to connect to mlflow
secret = jsonpickle.loads(
    secret_manager.get_secret_value(SecretId=mlflow_secret_name)["SecretString"]
)
os.environ["MLFLOW_TRACKING_URI"] = "databricks"
os.environ["DATABRICKS_HOST"] = secret["DATABRICKS_HOST"]
os.environ["DATABRICKS_TOKEN"] = secret["DATABRICKS_TOKEN"]

# Get the shared secret for the webhook
web_hook_secret = parameters.get_secret(webhook_secret_name)



helper = CfnResource(
    json_logging=False,
    log_level="DEBUG",
    boto_level="CRITICAL",
    sleep_on_delete=120,
    ssl_verify=None,
)


@helper.create
def create(event, context):
    props = event["ResourceProperties"]
    model_name = props["ModelName"]
    http_url_spec = HttpUrlSpec(
        url=target_url,
        secret=web_hook_secret,
    )
    http_webhook = RegistryWebhooksClient().create_webhook(
        model_name=model_name,
        events=[
            "MODEL_VERSION_CREATED",
            "MODEL_VERSION_TRANSITIONED_STAGE",
        ],
        http_url_spec=http_url_spec,
        description="Sync Registry to SageMaker",
        status="ACTIVE",
    )

    return http_webhook.id


@helper.update
def update(event, context):
    logger.info("Got Update. This is a no-ops operation")


@helper.delete
def delete(event, context):
    logger.info("Got Delete")
    physical_id = event["PhysicalResourceId"]
    try:
        http_webhook = RegistryWebhooksClient().delete_webhook(id=physical_id)
        logger.info(f"Deleted webhook with id {physical_id}")
    except RestException:
        logger.exception("Webhook does not exist")



# @logger.inject_lambda_context(log_event=False)
def handler(event, context):
    helper(event, context)
