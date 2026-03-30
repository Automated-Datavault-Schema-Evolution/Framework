import subprocess
from typing import List

from config.config import DBT_PROJECT_DIR


def run_dbt(extra_args: List[str]) -> int:
    proc = subprocess.run(["dbt", *extra_args], cwd=DBT_PROJECT_DIR, check=False)
    return proc.returncode
