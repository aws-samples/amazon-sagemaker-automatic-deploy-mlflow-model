#!/usr/bin/env python3

import aws_cdk as cdk

from deployment_cicd.deployment_cicd_stack import DeploymentCiCdStack



app = cdk.App()
stack_name = app.node.try_get_context("StackName")

DeploymentCiCdStack(
    app,
    stack_name,
)

app.synth()
