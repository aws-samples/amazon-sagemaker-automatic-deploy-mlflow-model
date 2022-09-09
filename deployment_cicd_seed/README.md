
# Model deployment repository

This repository contains the source code for deploying an realtime inference Endpoint infrastructure using Amazon SageMaker. This code repository is created as part of creating a MLflow model deployment product in Service Catalog.

- [1. Introduction](#1-introduction)
- [2. Layout of the repository](#2-layout-of-the-repository)
  - [2.1. `deployment_cicd_stack.py`](#21-deployment_cicd_stackpy)
  - [2.2. endpoint_deployment_stage.py](#22-endpoint_deployment_stagepy)
  - [2.3. config/inference.yaml](#23-configinferenceyaml)

## 1. Introduction

This repository is part of a [CDK Pipelines](https://docs.aws.amazon.com/cdk/api/v2/docs/aws-cdk-lib.pipelines-readme.html) (a construct library module for painless continuous delivery of AWS CDK applications), and every commit pushed or merged into the `main` branch will trigger the automatic building and deployment of **Staging** and **Production** CloudFormation stacks, including the relevant SageMaker endpoints.

The automatic build and deployment is also triggered by the change in status of any of the [`model versions`](https://docs.aws.amazon.com/sagemaker/latest/dg/model-registry-version.html) registered in [*Amazon SageMaker Model Registry*](https://docs.aws.amazon.com/sagemaker/latest/dg/model-registry.html) in the [`model package group`](https://docs.aws.amazon.com/sagemaker/latest/dg/model-registry-model-group.html) with the same name as the CI/CD pipeline.

## 2. Layout of the repository

The repository is structured as a [CDK App project](https://docs.aws.amazon.com/cdk/v2/guide/work-with-cdk-python.html), and uses [CDK Pipelines](https://docs.aws.amazon.com/cdk/api/v2/docs/aws-cdk-lib.pipelines-readme.html) to define a CI/CD pipeline using [AWS CodeCommit](https://aws.amazon.com/codecommit/) for version control fo the source code, [AWS CodePipeline](https://aws.amazon.com/codepipeline/) for continuos delivery, [AWS CodeBuild](https://aws.amazon.com/codebuild/) for continuos integration, and [AWS CloudFormation](https://aws.amazon.com/cloudformation/) for defining and provisioning the infrastructure.

### 2.1. [`deployment_cicd_stack.py`](deployment_cicd/deployment_cicd_stack.py)

This file contains the definition of the the CI/CD pipeline (CloudFormation stack with name defined by ServiceCatalog at creation time), and the stages (`Staging`, `Production`, stacks by default) of the application.  
Additional resource defined:

- a CloudFormation Custom Resource to create a [MLflow Model Registry WebHook](https://docs.databricks.com/applications/mlflow/model-registry-webhooks.html) associated with the model with the same name as this repository.
- An [Amazon EventBridge Rule](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-rules.html) that triggers the execution fo the CI/CD pipelines when a change is detected in the Amazon SageMaker Model Registry.

### 2.2. [endpoint_deployment_stage.py](deployment_cicd_seed/deployment_cicd/endpoint_deployment_stage.py)

This file contains the definition of the resource deployed in each stage of the app.

### 2.3. [config/inference.yaml](deployment_cicd/config/inference.yml)

This files configures the parameters of the `Staging` and `Production` endpoints. The example configuration define a real-time endpoint using a single `ml.m5.large` instance for `Staging`, and a serverless endpoint with a max concurrency of 2 for `Production`.

```yaml
Staging:
  InstanceCount: 1
  InstanceType: ml.m5.large
Production:
  ServerlessMaxConcurrency: 2
```