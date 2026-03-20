-- =============================================================================
-- Query: Trade Corridor Analysis
-- Analyses trade corridors (origin → destination) by volume, value,
-- and risk profile. Highlights high-risk corridors for targeting.
-- =============================================================================

WITH corridor_metrics AS (
    SELECT
        r.origin_country_code,
        r.origin_country_name,
        r.dest_country_code,
        r.dest_country_name,
        r.transport_mode,
        COUNT(f.declaration_id)                         AS total_declarations,
        COUNT(DISTINCT f.importer_key)                  AS distinct_importers,
        COUNT(DISTINCT f.commodity_key)                 AS distinct_commodities,
        SUM(f.declared_value)                           AS total_declared_value,
        SUM(f.weight_kg)                                AS total_weight_kg,
        SUM(f.duty_amount)                              AS total_duty_collected,
        ROUND(AVG(f.risk_score), 2)                     AS avg_risk_score,
        ROUND(MAX(f.risk_score), 2)                     AS max_risk_score,
        COUNT(CASE WHEN f.is_flagged THEN 1 END)        AS flagged_count,
        ROUND(
            COUNT(CASE WHEN f.is_flagged THEN 1 END)::NUMERIC
            / NULLIF(COUNT(f.declaration_id), 0) * 100, 1
        )                                               AS flagged_pct
    FROM fact_declarations f
    JOIN dim_route r ON f.route_key = r.route_key
    JOIN dim_date d  ON f.date_key  = d.date_key
    WHERE d.year = EXTRACT(YEAR FROM CURRENT_DATE)
    GROUP BY
        r.origin_country_code, r.origin_country_name,
        r.dest_country_code, r.dest_country_name,
        r.transport_mode
),

ranked AS (
    SELECT
        *,
        RANK() OVER (ORDER BY total_declared_value DESC)    AS value_rank,
        RANK() OVER (ORDER BY avg_risk_score DESC)          AS risk_rank,
        NTILE(4) OVER (ORDER BY avg_risk_score DESC)        AS risk_quartile
    FROM corridor_metrics
)

SELECT
    origin_country_code || ' → ' || dest_country_code       AS corridor,
    origin_country_name,
    dest_country_name,
    transport_mode,
    total_declarations,
    distinct_importers,
    total_declared_value,
    total_weight_kg,
    total_duty_collected,
    avg_risk_score,
    flagged_count,
    flagged_pct,
    value_rank,
    risk_rank,
    CASE risk_quartile
        WHEN 1 THEN 'HIGH RISK'
        WHEN 2 THEN 'ELEVATED'
        WHEN 3 THEN 'MODERATE'
        WHEN 4 THEN 'LOW RISK'
    END                                                     AS risk_category
FROM ranked
ORDER BY avg_risk_score DESC, total_declared_value DESC;
