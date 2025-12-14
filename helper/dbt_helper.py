import subprocess
from typing import List

from config.config import DBT_PROJECT_DIR


def run_dbt(extra_args: List[str]) -> int:
    """
    Thin wrapper to run dbt via subprocess.
    Not directly used by SEF core, but available if policies/plan want to trigger dbt jobs.
    """
    cmd = ["dbt"] + extra_args
    proc = subprocess.run(cmd, cwd=DBT_PROJECT_DIR, check=False)
    return proc.returncode
