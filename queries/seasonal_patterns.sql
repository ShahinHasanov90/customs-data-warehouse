-- =============================================================================
-- Query: Seasonal Patterns
-- Identifies seasonality in trade volume, value, and risk across months.
-- Useful for capacity planning and targeted inspection scheduling.
-- =============================================================================

WITH monthly AS (
    SELECT
        d.year,
        d.month,
        d.month_name,
        COUNT(f.declaration_id)                         AS total_declarations,
        SUM(f.declared_value)                           AS total_value,
        SUM(f.weight_kg)                                AS total_weight_kg,
        SUM(f.duty_amount)                              AS total_duty,
        AVG(f.risk_score)                               AS avg_risk_score,
        COUNT(CASE WHEN f.is_flagged THEN 1 END)        AS flagged_count,
        COUNT(DISTINCT f.importer_key)                  AS active_importers,
        COUNT(DISTINCT f.commodity_key)                 AS active_commodities
    FROM fact_declarations f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY d.year, d.month, d.month_name
),

averages AS (
    SELECT
        month,
        TRIM(month_name)                                AS month_name,
        ROUND(AVG(total_declarations), 0)               AS avg_declarations,
        ROUND(AVG(total_value), 2)                      AS avg_value,
        ROUND(AVG(total_weight_kg), 2)                  AS avg_weight_kg,
        ROUND(AVG(total_duty), 2)                       AS avg_duty,
        ROUND(AVG(avg_risk_score), 2)                   AS avg_risk_score,
        ROUND(AVG(flagged_count), 0)                    AS avg_flagged,
        ROUND(AVG(active_importers), 0)                 AS avg_importers,
        COUNT(DISTINCT year)                            AS years_observed
    FROM monthly
    GROUP BY month, month_name
),

with_index AS (
    SELECT
        a.*,
        ROUND(
            avg_declarations / NULLIF(
                (SELECT AVG(avg_declarations) FROM averages), 0
            ) * 100, 1
        )                                               AS volume_index,
        ROUND(
            avg_value / NULLIF(
                (SELECT AVG(avg_value) FROM averages), 0
            ) * 100, 1
        )                                               AS value_index
    FROM averages a
)

SELECT
    month_name,
    avg_declarations,
    avg_value,
    avg_weight_kg,
    avg_duty,
    avg_risk_score,
    avg_flagged,
    avg_importers,
    volume_index,
    value_index,
    years_observed,
    CASE
        WHEN volume_index >= 120 THEN 'PEAK'
        WHEN volume_index >= 80  THEN 'NORMAL'
        ELSE 'LOW'
    END                                                 AS season_category
FROM with_index
ORDER BY month;
