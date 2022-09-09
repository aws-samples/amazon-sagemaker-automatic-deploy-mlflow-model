import hashlib
import hmac
import json
import os

import boto3
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities import parameters
from aws_lambda_powertools.utilities.data_classes import APIGatewayProxyEvent

logger = Logger()

sfn_client = boto3.client("stepfunctions")

stateMachineArn = os.getenv("SFN_ARN")
webhook_shared_secret_name = os.getenv("WEBHOOK_SHARED_SECRET_NAME")
secret = parameters.get_secret(webhook_shared_secret_name)


@logger.inject_lambda_context(log_event=True)
def handler(event, context):
    event: APIGatewayProxyEvent = APIGatewayProxyEvent(event)
    validate_signature(event)

    model_name = json.loads(event.body)["model_name"]

    try:
        sfn_client.start_execution(
            stateMachineArn=stateMachineArn,
            input=json.dumps({"model_name": model_name}),
        )
        message = "State machine execution started"
        logger.info(message)
    except:
        logger.exception(
            f"Failed to start execution of State Machine {stateMachineArn}"
        )
        message = "Something went wrong, check the logs"
    return {"statusCode": 200, "body": json.dumps({"message": message})}


def validate_signature(request):
    signature_key = "X-Databricks-Signature"
    if signature_key not in request.headers:
        raise Exception("No X-Signature. Webhook not be trusted.")

    x_sig = request.headers[signature_key]
    body = request.body.encode("utf-8")
    h = hmac.new(secret.encode("utf-8"), body, hashlib.sha256)
    computed_sig = h.hexdigest()

    if not hmac.compare_digest(computed_sig, x_sig):
        raise Exception("X-Signature mismatch. Webhook not be trusted.")
