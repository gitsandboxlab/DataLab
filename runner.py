CREATE TABLE fact_goals_ytd_snap (

    snap_key                BIGINT          NOT NULL,

    -- Grain
    date_key                INT             NOT NULL,   -- Today's date (the snapshot date)
    employee_key            INT             NOT NULL,
    branch_key              INT             NOT NULL,
    product_key             INT             NOT NULL,
    metric_key              INT             NOT NULL,
    goal_level_cd           VARCHAR(10)     NOT NULL,
    goal_version_cd         VARCHAR(20)     NOT NULL,

    -- Annual reference
    annual_goal_amount      DECIMAL(18,2),
    total_biz_days_in_year  INT,

    -- Cumulative YTD measures (the key difference)
    biz_days_elapsed        INT,            -- How many biz days have passed YTD
    ytd_goal_amount         DECIMAL(18,2),  -- Prorated goal through today
    ytd_actual_amount       DECIMAL(18,2),  -- Cumulative actual through today
    ytd_variance_amount     DECIMAL(18,2),  -- ytd_actual - ytd_goal
    ytd_attainment_pct      DECIMAL(8,4),   -- ytd_actual / ytd_goal

    -- Projection
    projected_year_end      DECIMAL(18,2),  -- At current pace, full year estimate

    -- Audit
    load_date               DATE            NOT NULL,

    CONSTRAINT pk_goals_ytd PRIMARY KEY (snap_key)
);
