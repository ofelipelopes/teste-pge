"""Create the private Airflow Simple Auth Manager password file."""

from __future__ import annotations

import json
import os
from pathlib import Path


def main() -> None:
    """Write the configured local administrator password without logging it."""
    target = Path(
        os.environ.get(
            "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE",
            "/opt/airflow/config/simple_auth_passwords.json",
        )
    )
    password = os.environ["AIRFLOW_ADMIN_PASSWORD"]
    if not password:
        raise ValueError("AIRFLOW_ADMIN_PASSWORD must not be empty")

    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(f"{target.suffix}.tmp")
    temporary.write_text(json.dumps({"admin": password}), encoding="utf-8")
    temporary.chmod(0o600)
    temporary.replace(target)


if __name__ == "__main__":
    main()
