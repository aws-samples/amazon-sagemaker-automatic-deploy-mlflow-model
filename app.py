#!/usr/bin/env python3
import os

import aws_cdk as cdk

from mlflow_mlops.mlflow_mlops_stack import MlflowMlopsStack


app = cdk.App()
MlflowMlopsStack(
    app,
    "MlflowMlopsStack",
)
app.synth()
