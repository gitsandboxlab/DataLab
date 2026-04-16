"""
validation.py
Single validation function. Reads check type from config and runs accordingly.
"""

import logging
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def run_validations(engine: Engine, val_config: list[dict]) -> tuple[bool, list[str]]:
    """
    Runs all validation checks defined in val_config.
    All checks run regardless of failures so full results are always returned.

    Args:
        engine:     SQLAlchemy engine.
        val_config: List of check dicts, each with a "check" key.

    Returns:
        (all_passed: bool, messages: list[str])

    Supported check types:
        {"check": "row_count",           "table": "dbo.x", "min_rows": 1000}
        {"check": "freshness",           "table": "dbo.x", "column": "load_date", "hours": 24}
        {"check": "null_rate",           "table": "dbo.x", "column": "customer_id", "max_pct": 0.05}
        {"check": "referential_integrity","fact_table": "dbo.fact", "fact_column": "id",
                                          "dim_table": "dbo.dim", "dim_column": "id", "max_orphan_pct": 0.01}
        {"check": "custom_sql",          "sql": "SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM dbo.x"}
    """
    messages = []
    all_passed = True

    for entry in val_config:
        check = entry.get("check")
        passed = False

        try:
            if check == "row_count":
                df = pd.read_sql(f"SELECT COUNT(*) AS cnt FROM {entry['table']}", engine)
                count = int(df["cnt"].iloc[0])
                passed = count >= entry["min_rows"]
                msg = f"[row_count] {entry['table']}: {count} rows (min={entry['min_rows']})"

            elif check == "freshness":
                df = pd.read_sql(f"SELECT MAX({entry['column']}) AS max_date FROM {entry['table']}", engine)
                max_date = df["max_date"].iloc[0]
                if max_date is None:
                    passed = False
                    msg = f"[freshness] {entry['table']}.{entry['column']}: no data found"
                else:
                    max_date = pd.Timestamp(max_date).to_pydatetime().replace(tzinfo=None)
                    cutoff = datetime.utcnow() - timedelta(hours=entry["hours"])
                    passed = max_date >= cutoff
                    msg = f"[freshness] {entry['table']}.{entry['column']}: max={max_date} (cutoff={cutoff})"

            elif check == "null_rate":
                df = pd.read_sql(
                    f"SELECT COUNT(*) AS total, SUM(CASE WHEN {entry['column']} IS NULL THEN 1 ELSE 0 END) AS nulls FROM {entry['table']}",
                    engine,
                )
                total = int(df["total"].iloc[0])
                nulls = int(df["nulls"].iloc[0])
                rate = nulls / total if total > 0 else 1.0
                passed = rate <= entry["max_pct"]
                msg = f"[null_rate] {entry['table']}.{entry['column']}: {rate:.2%} nulls (max={entry['max_pct']:.2%})"

            elif check == "referential_integrity":
                df = pd.read_sql(
                    f"""
                    SELECT COUNT(*) AS total,
                           SUM(CASE WHEN d.{entry['dim_column']} IS NULL THEN 1 ELSE 0 END) AS orphans
                    FROM {entry['fact_table']} f
                    LEFT JOIN {entry['dim_table']} d ON f.{entry['fact_column']} = d.{entry['dim_column']}
                    """,
                    engine,
                )
                total = int(df["total"].iloc[0])
                orphans = int(df["orphans"].iloc[0])
                rate = orphans / total if total > 0 else 0.0
                max_pct = entry.get("max_orphan_pct", 0.0)
                passed = rate <= max_pct
                msg = f"[ref_integrity] {entry['fact_table']}.{entry['fact_column']} -> {entry['dim_table']}.{entry['dim_column']}: {rate:.2%} orphans (max={max_pct:.2%})"

            elif check == "custom_sql":
                df = pd.read_sql(entry["sql"], engine)
                result = bool(df.iloc[0, 0])
                expected = entry.get("expected", True)
                passed = result == expected
                msg = f"[custom_sql] result={result} (expected={expected}) | sql={entry['sql'][:80]}..."

            else:
                passed = False
                msg = f"[unknown_check] '{check}' is not a supported check type"

        except Exception as e:
            passed = False
            msg = f"[{check}] Exception: {e}"

        messages.append(msg)
        logger.info(f"{'✓' if passed else '✗'} {msg}")
        if not passed:
            all_passed = False

    return all_passed, messages

def _check_null_rate(engine: Engine, table: str, column: str, max_pct: float) -> tuple[bool, str]:
    df = pd.read_sql(
        f"SELECT COUNT(*) AS total, SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) AS nulls FROM {table}",
        engine,
    )
    total = int(df["total"].iloc[0])
    nulls = int(df["nulls"].iloc[0])
    rate = nulls / total if total > 0 else 1.0
    passed = rate <= max_pct
    return passed, f"[null_rate] {table}.{column}: {rate:.2%} nulls (max={max_pct:.2%})"


def _check_referential_integrity(
    engine: Engine,
    fact_table: str,
    fact_column: str,
    dim_table: str,
    dim_column: str,
    max_orphan_pct: float = 0.0,
) -> tuple[bool, str]:
    df = pd.read_sql(
        f"""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN d.{dim_column} IS NULL THEN 1 ELSE 0 END) AS orphans
        FROM {fact_table} f
        LEFT JOIN {dim_table} d ON f.{fact_column} = d.{dim_column}
        """,
        engine,
    )
    total = int(df["total"].iloc[0])
    orphans = int(df["orphans"].iloc[0])
    rate = orphans / total if total > 0 else 0.0
    passed = rate <= max_orphan_pct
    return passed, (
        f"[ref_integrity] {fact_table}.{fact_column} -> {dim_table}.{dim_column}: "
        f"{rate:.2%} orphans (max={max_orphan_pct:.2%})"
    )


def _check_custom_sql(engine: Engine, sql: str, expected: bool = True) -> tuple[bool, str]:
    df = pd.read_sql(sql, engine)
    result = bool(df.iloc[0, 0])
    passed = result == expected
    return passed, f"[custom_sql] result={result} (expected={expected}) | sql={sql[:80]}..."


_CHECK_REGISTRY = {
    "row_count": _check_row_count,
    "freshness": _check_freshness,
    "null_rate": _check_null_rate,
    "referential_integrity": _check_referential_integrity,
    "custom_sql": _check_custom_sql,
}


# ---------------------------------------------------------------------------
# Validation engine — single entry point
# ---------------------------------------------------------------------------

def run_validations(engine: Engine, val_config: list[dict]) -> tuple[bool, list[str]]:
    """
    Runs all validation checks from config. All checks run regardless of failures
    so the full picture is available in logs.

    Args:
        engine:     SQLAlchemy engine.
        val_config: List of check dicts from Prefect Variable.
                    Each must have a "check" key matching a registered check type.

    Returns:
        (all_passed: bool, messages: list[str])

    Example config:
        [
          {"check": "row_count",  "table": "dbo.customers", "min_rows": 1000},
          {"check": "freshness",  "table": "dbo.orders", "column": "load_date", "hours": 24},
          {"check": "null_rate",  "table": "dbo.orders", "column": "customer_id", "max_pct": 0.05}
        ]
    """
    messages = []
    all_passed = True

    for entry in val_config:
        check_name = entry.get("check")
        if check_name not in _CHECK_REGISTRY:
            msg = f"[unknown_check] '{check_name}' not in registry. Available: {list(_CHECK_REGISTRY)}"
            messages.append(msg)
            logger.error(msg)
            all_passed = False
            continue

        fn = _CHECK_REGISTRY[check_name]
        kwargs = {k: v for k, v in entry.items() if k != "check"}

        try:
            passed, msg = fn(engine, **kwargs)
            messages.append(msg)
            if passed:
                logger.info(f"✓ {msg}")
            else:
                logger.warning(f"✗ {msg}")
                all_passed = False
        except Exception as e:
            msg = f"[{check_name}] Exception: {e}"
            messages.append(msg)
            logger.error(f"✗ {msg}")
            all_passed = False

    return all_passed, messages
