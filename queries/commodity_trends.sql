-- =============================================================================
-- Query: Commodity Trends
-- Tracks month-over-month trends for each HS chapter, showing volume changes,
-- value shifts, and emerging risk patterns.
-- =============================================================================

WITH monthly_commodity AS (
    SELECT
        d.year,
        d.month,
        d.month_name,
        c.hs_chapter,
        c.chapter_desc,
        COUNT(f.declaration_id)                         AS declarations,
        SUM(f.declared_value)                           AS total_value,
        SUM(f.weight_kg)                                AS total_weight_kg,
        SUM(f.duty_amount)                              AS total_duty,
        ROUND(AVG(f.risk_score), 2)                     AS avg_risk_score,
        COUNT(CASE WHEN f.is_flagged THEN 1 END)        AS flagged_count,
        COUNT(DISTINCT f.importer_key)                  AS distinct_importers,
        ROUND(
            AVG(f.declared_value / NULLIF(f.weight_kg, 0)), 2
        )                                               AS avg_unit_value
    FROM fact_declarations f
    JOIN dim_date d      ON f.date_key     = d.date_key
    JOIN dim_commodity c ON f.commodity_key = c.commodity_key
    GROUP BY d.year, d.month, d.month_name, c.hs_chapter, c.chapter_desc
),

with_lag AS (
    SELECT
        *,
        LAG(total_value) OVER (
            PARTITION BY hs_chapter ORDER BY year, month
        )                                               AS prev_month_value,
        LAG(declarations) OVER (
            PARTITION BY hs_chapter ORDER BY year, month
        )                                               AS prev_month_decl,
        LAG(avg_risk_score) OVER (
            PARTITION BY hs_chapter ORDER BY year, month
        )                                               AS prev_month_risk
    FROM monthly_commodity
),

trend_analysis AS (
    SELECT
        year,
        month,
        TRIM(month_name)                                AS month_name,
        hs_chapter,
        chapter_desc,
        declarations,
        total_value,
        total_weight_kg,
        avg_unit_value,
        avg_risk_score,
        flagged_count,
        distinct_importers,

        -- Month-over-month changes
        ROUND(
            (total_value - prev_month_value)
            / NULLIF(prev_month_value, 0) * 100, 1
        )                                               AS value_change_pct,
        ROUND(
            (declarations - prev_month_decl)::NUMERIC
            / NULLIF(prev_month_decl, 0) * 100, 1
        )                                               AS volume_change_pct,
        ROUND(avg_risk_score - COALESCE(prev_month_risk, avg_risk_score), 2)
                                                        AS risk_score_delta
    FROM with_lag
)

SELECT
    year,
    month_name,
    hs_chapter,
    chapter_desc,
    declarations,
    total_value,
    avg_unit_value,
    avg_risk_score,
    flagged_count,
    distinct_importers,
    value_change_pct,
    volume_change_pct,
    risk_score_delta,
    CASE
        WHEN value_change_pct > 50  THEN 'SURGE'
        WHEN value_change_pct > 10  THEN 'GROWING'
        WHEN value_change_pct < -50 THEN 'DECLINING_FAST'
        WHEN value_change_pct < -10 THEN 'DECLINING'
        ELSE 'STABLE'
    END                                                 AS trend_status
FROM trend_analysis
ORDER BY year DESC, month DESC, total_value DESC;
