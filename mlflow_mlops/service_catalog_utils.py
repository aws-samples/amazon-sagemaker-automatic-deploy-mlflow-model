from pathlib import Path
from zipfile import ZipFile


def create_zip(zipfile_name: str, local_path: Path):
    """
    Create a zip archive with the content of `local_path`

    :param zipfile_name: The name of the zip archive
    :param local_path: The path to the directory to zip
    """
    with ZipFile(zipfile_name, mode="w") as archive:
        [
            archive.write(k, arcname=f"{k.relative_to(local_path)}")
            for k in local_path.glob("**/*.*")
            if not f"{k.relative_to(local_path)}".startswith(("cdk.out", "."))
            if not "__pycache__" in f"{k.relative_to(local_path)}"
            if not f"{k.relative_to(local_path)}".endswith(".zip")
        ]
        if (gitignore_path := local_path / ".gitignore").exists:
            archive.write(gitignore_path, arcname=".gitignore")

    zip_size = Path(zipfile_name).stat().st_size / 10**6
    return zip_size
