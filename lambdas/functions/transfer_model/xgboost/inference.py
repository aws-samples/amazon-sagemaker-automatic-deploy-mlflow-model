import mlflow
from sagemaker_inference import decoder, encoder

from xgboost import DMatrix


def model_fn(model_dir):
    """Deserialize and return fitted model.

    Note that this should have the same name as the serialized model in the _xgb_train method
    """
    model = mlflow.xgboost.load_model(model_dir)
    return model


def input_fn(input_data, content_type):
    return decoder.decode(input_data, content_type=content_type)


def output_fn(prediction, accept):
    return encoder.encode(prediction, accept)


def predict_fn(input_data, model):
    return model.predict(DMatrix(input_data))
