-- =============================================================================
-- Mart: fct_daily_trade
-- Daily trade aggregation with volume, value, and risk metrics.
-- =============================================================================

WITH declarations AS (

    SELECT * FROM {{ ref('stg_declarations') }}

),

dates AS (

    SELECT * FROM {{ source('customs', 'dim_date') }}

),

aggregated AS (

    SELECT
        d.date_key,
        d.full_date,
        d.year,
        d.quarter,
        d.month,
        d.month_name,
        d.day_of_week,
        d.day_name,
        d.is_weekend,

        -- Volume metrics
        COUNT(f.declaration_id)                             AS total_declarations,
        COUNT(DISTINCT f.importer_key)                      AS distinct_importers,
        COUNT(DISTINCT f.exporter_key)                      AS distinct_exporters,
        COUNT(DISTINCT f.commodity_key)                     AS distinct_commodities,

        -- Value metrics
        SUM(f.declared_value)                               AS total_declared_value,
        SUM(f.statistical_value)                            AS total_statistical_value,
        SUM(f.weight_kg)                                    AS total_weight_kg,
        SUM(f.duty_amount)                                  AS total_duty_amount,
        SUM(f.tax_amount)                                   AS total_tax_amount,
        AVG(f.declared_value)                               AS avg_declared_value,

        -- Risk metrics
        AVG(f.risk_score)                                   AS avg_risk_score,
        MAX(f.risk_score)                                   AS max_risk_score,
        COUNT(CASE WHEN f.is_flagged THEN 1 END)            AS flagged_count,
        COUNT(CASE WHEN f.risk_level = 'HIGH' THEN 1 END)   AS high_risk_count,
        COUNT(CASE WHEN f.risk_level = 'CRITICAL' THEN 1 END) AS critical_risk_count,

        ROUND(
            COUNT(CASE WHEN f.is_flagged THEN 1 END)::NUMERIC
            / NULLIF(COUNT(f.declaration_id), 0) * 100, 2
        )                                                   AS flagged_pct

    FROM declarations f
    INNER JOIN dates d
        ON f.date_key = d.date_key
    GROUP BY
        d.date_key, d.full_date, d.year, d.quarter,
        d.month, d.month_name, d.day_of_week, d.day_name, d.is_weekend

)

SELECT * FROM aggregated
ORDER BY full_date
