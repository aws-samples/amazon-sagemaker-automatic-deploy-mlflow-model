import shutil
import subprocess
import sys
from pathlib import Path
from typing import Union


def create_sagemaker_libraries(
    base_path: Union[str, Path], requirements_file_name: str = "requirements.txt"
):
    if isinstance(base_path, str):
        base_path = Path(base_path)
    req_file = base_path / requirements_file_name
    layer_path = base_path / "python"
    cmd = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--target",
        layer_path.as_posix(),
        "--no-compile",
        "-r",
        req_file.as_posix(),
    ]
    subprocess.run(cmd, check=True)

    [
        shutil.rmtree(k)
        for j in ["boto", "numpy", "pandas", "test"]
        for k in layer_path.glob(f"{j}*")
    ]


if __name__ == "__main__":
    create_sagemaker_libraries("lambdas/layers/sagemaker")
