CREATE TABLE fact_goals (

    -- Surrogate key
    goal_fact_key           BIGINT          NOT NULL,

    -- Grain keys (what makes each row unique)
    date_key                INT             NOT NULL,   -- FK → dim_date (YYYYMMDD)
    employee_key            INT             NOT NULL,   -- FK → dim_employee (use -1 for branch-level)
    branch_key              INT             NOT NULL,   -- FK → dim_branch
    product_key             INT             NOT NULL,   -- FK → dim_product
    metric_key              INT             NOT NULL,   -- FK → dim_metric

    -- Degenerate dimensions (no separate table needed yet)
    goal_level_cd           VARCHAR(10)     NOT NULL,   -- 'RM' or 'BRANCH'
    goal_version_cd         VARCHAR(20)     NOT NULL,   -- e.g. 'FY2025_BUDGET'

    -- Annual goal reference (preserved for auditability)
    annual_goal_amount      DECIMAL(18,2),              -- Original annual target
    total_biz_days_in_year  INT,                        -- e.g. 261

    -- Daily measures (annual goal prorated across business days)
    goal_amount             DECIMAL(18,2),              -- annual_goal / total_biz_days
    actual_amount           DECIMAL(18,2),              -- Actual for this business day

    -- Derived measures (stored for query performance)
    variance_amount         DECIMAL(18,2),              -- actual - goal
    attainment_pct          DECIMAL(8,4),               -- actual / goal

    -- Audit
    load_date               DATE            NOT NULL,
    source_system_cd        VARCHAR(20),

    CONSTRAINT pk_fact_goals PRIMARY KEY (goal_fact_key),
    CONSTRAINT fk_fg_date     FOREIGN KEY (date_key)     REFERENCES dim_date(date_key),
    CONSTRAINT fk_fg_employee FOREIGN KEY (employee_key) REFERENCES dim_employee(employee_key),
    CONSTRAINT fk_fg_branch   FOREIGN KEY (branch_key)   REFERENCES dim_branch(branch_key),
    CONSTRAINT fk_fg_product  FOREIGN KEY (product_key)  REFERENCES dim_product(product_key),
    CONSTRAINT fk_fg_metric   FOREIGN KEY (metric_key)   REFERENCES dim_metric(metric_key)
);
