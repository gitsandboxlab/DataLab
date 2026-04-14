“””
snowflake_model_a_flow.py
Orchestration flow for Snowflake Model A.

Prefect Variables required (set in Prefect UI):

- model_a_sp_config          (JSON array)
- model_a_validation_config  (JSON array)

Example model_a_sp_config:
[
{“name”: “usp_load_dim_customer”, “order”: 1, “timeout”: 120},
{“name”: “usp_load_dim_product”,  “order”: 2, “timeout”: 120},
{“name”: “usp_load_fact_sales”,   “order”: 3, “timeout”: 300}
]

Example model_a_validation_config:
[
{“check”: “row_count”,  “table”: “dbo.customers”,  “min_rows”: 1000},
{“check”: “freshness”,  “table”: “dbo.orders”,     “column”: “load_date”, “hours”: 24},
{“check”: “null_rate”,  “table”: “dbo.orders”,     “column”: “customer_id”, “max_pct”: 0.05},
{“check”: “referential_integrity”,
“fact_table”: “dbo.fact_sales”, “fact_column”: “customer_id”,
“dim_table”: “dbo.dim_customer”, “dim_column”: “customer_id”, “max_orphan_pct”: 0.01}
]
“””

from prefect import flow
from prefect.variables import Variable
from sqlalchemy.engine import Engine

from lib.model_runner import run_model
from lib.validation import build_checks

# —————————————————————————

# Replace with your engine factory or inject at call time

# —————————————————————————

def get_engine() -> Engine:
from your_ingestion_lib import create_engine  # your existing engine
return create_engine()

@flow(name=“snowflake_model_a”, log_prints=True)
def snowflake_model_a_flow():
engine = get_engine()

```
sp_config = Variable.get("model_a_sp_config")
val_config = Variable.get("model_a_validation_config")

checks = build_checks(val_config)
run_model(engine, sp_config, checks)
```

if **name** == “**main**”:
snowflake_model_a_flow()