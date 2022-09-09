import os
import shutil
from pathlib import Path

import mlflow
from mlflow.models import docker_utils


def setup_container(_):
    return "\n".join(
        [
            'ENV {disable_env}="false"',
            'RUN python -c "from mlflow.models.container import _install_pyfunc_deps;'
            '_install_pyfunc_deps(None, False)"',
        ]
    )


entrypoint = """
ENTRYPOINT ["python", "-c", "import sys; from mlflow.models import container as C; \
C._init(sys.argv[1], '{env_manager}')"]
""".format(
    env_manager="conda"
)


def create_docker_file(destination_path: Path):

    destination_path.mkdir(parents=True, exist_ok=True)

    def copy_dockerfile(context_dir: str, image_name: str):
        shutil.copy(os.path.join(context_dir, "Dockerfile"), destination_path)

    # Monkey patching original method to extract the dockerfile
    docker_utils._build_image_from_context = copy_dockerfile

    docker_utils._build_image(
        None,
        entrypoint,
        custom_setup_steps_hook=setup_container,
        env_manager="conda",
    )
    return mlflow.__version__


if __name__ == "__main__":
    mlflow_version = create_docker_file(destination_path=Path("."))
    print(f"MLflow version: {mlflow_version}")
