"""
model_runner.py
Model runner. Call run_model() from your flow scripts.
"""

import json
import logging

from sqlalchemy import text
from sqlalchemy.engine import Engine

from lib.validation import run_validations

logger = logging.getLogger(__name__)


def run_model(
    engine: Engine,
    sp_config: list[dict] | str,
    val_config: list[dict] | str,
) -> tuple[bool, list[str]]:
    """
    Validates source data then executes stored procedures in order.
    Returns early without running SPs if validation fails.

    Args:
        engine:     SQLAlchemy engine.
        sp_config:  Ordered SP list or JSON string.
                    Each entry: {"name": "usp_foo", "order": 1, "timeout": 300}
        val_config: Validation check list or JSON string.
                    Passed directly to run_validations().

    Returns:
        (success: bool, messages: list[str])
    """
    if isinstance(sp_config, str):
        sp_config = json.loads(sp_config)
    if isinstance(val_config, str):
        val_config = json.loads(val_config)

    # Validation gate
    if not val_config:
        logger.warning("No validation config provided — skipping validation.")
        passed, val_messages = True, []
    else:
        passed, val_messages = run_validations(engine, val_config)

    if not passed:
        logger.warning("Validation failed. Existing data model preserved. SPs will not run.")
        return False, val_messages

    # Execute SPs in order
    ordered = sorted(sp_config, key=lambda x: x["order"])
    logger.info(f"Validation passed. Running {len(ordered)} stored procedure(s).")
    sp_messages = []

    for sp in ordered:
        name = sp["name"]
        timeout = sp.get("timeout", 300)
        try:
            with engine.begin() as conn:
                conn.execute(text(f"EXEC {name}"), execution_options={"timeout": timeout})
            msg = f"[sp] ✓ {name}"
            logger.info(msg)
        except Exception as e:
            msg = f"[sp] ✗ {name} — {e}"
            logger.error(msg)
            sp_messages.append(msg)
            return False, val_messages + sp_messages

        sp_messages.append(msg)

    logger.info("Model build complete.")
    return True, val_messages + sp_messages
