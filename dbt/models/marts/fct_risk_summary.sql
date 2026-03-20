-- =============================================================================
-- Mart: fct_risk_summary
-- Risk summary aggregated by entity (importer) and time period.
-- =============================================================================

WITH declarations AS (

    SELECT * FROM {{ ref('stg_declarations') }}

),

dates AS (

    SELECT * FROM {{ source('customs', 'dim_date') }}

),

importers AS (

    SELECT * FROM {{ source('customs', 'dim_importer') }}

),

risk_agg AS (

    SELECT
        d.year,
        d.quarter,
        d.month,
        d.month_name,

        i.importer_key,
        i.importer_id,
        i.importer_name,
        i.country_code                                      AS importer_country,

        -- Volume
        COUNT(f.declaration_id)                             AS total_declarations,

        -- Value
        SUM(f.declared_value)                               AS total_declared_value,
        SUM(f.duty_amount)                                  AS total_duty_amount,
        AVG(f.declared_value)                               AS avg_declared_value,

        -- Risk scoring
        AVG(f.risk_score)                                   AS avg_risk_score,
        MAX(f.risk_score)                                   AS max_risk_score,
        MIN(f.risk_score)                                   AS min_risk_score,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.risk_score)
                                                            AS median_risk_score,
        STDDEV(f.risk_score)                                AS stddev_risk_score,

        -- Risk level breakdown
        COUNT(CASE WHEN f.risk_level = 'LOW' THEN 1 END)       AS low_risk_count,
        COUNT(CASE WHEN f.risk_level = 'MEDIUM' THEN 1 END)    AS medium_risk_count,
        COUNT(CASE WHEN f.risk_level = 'HIGH' THEN 1 END)      AS high_risk_count,
        COUNT(CASE WHEN f.risk_level = 'CRITICAL' THEN 1 END)  AS critical_risk_count,

        -- Flags
        COUNT(CASE WHEN f.is_flagged THEN 1 END)            AS flagged_count,
        ROUND(
            COUNT(CASE WHEN f.is_flagged THEN 1 END)::NUMERIC
            / NULLIF(COUNT(f.declaration_id), 0) * 100, 2
        )                                                   AS flagged_pct,

        -- Diversity (more diverse = potentially more scrutiny)
        COUNT(DISTINCT f.commodity_key)                     AS distinct_commodities,
        COUNT(DISTINCT f.route_key)                         AS distinct_routes,
        COUNT(DISTINCT f.exporter_key)                      AS distinct_exporters

    FROM declarations f
    INNER JOIN dates d       ON f.date_key     = d.date_key
    INNER JOIN importers i   ON f.importer_key = i.importer_key
    GROUP BY
        d.year, d.quarter, d.month, d.month_name,
        i.importer_key, i.importer_id, i.importer_name, i.country_code

)

SELECT * FROM risk_agg
ORDER BY year, month, avg_risk_score DESC
