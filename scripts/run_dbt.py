import os
import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
PROJECT_DIR = BASE_DIR / "dbt" / "order_intelligence"
DUCKDB_PATH = BASE_DIR / "data" / "order_intelligence.duckdb"


def run_command(args: list[str]) -> int:
    env = os.environ.copy()
    env["DUCKDB_PATH"] = str(DUCKDB_PATH)
    print("Running:", " ".join(args))
    result = subprocess.run(args, cwd=PROJECT_DIR, env=env)
    return result.returncode


def main() -> int:
    if not DUCKDB_PATH.exists():
        print(f"DuckDB file not found yet: {DUCKDB_PATH}")
        print("Start the consumer and trigger changes first.")
        return 1

    for cmd in [
        [sys.executable, "-m", "dbt", "debug", "--profiles-dir", "."],
        [sys.executable, "-m", "dbt", "run", "--profiles-dir", "."],
        [sys.executable, "-m", "dbt", "test", "--profiles-dir", "."],
    ]:
        code = run_command(cmd)
        if code != 0:
            return code
    print("dbt finished successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
