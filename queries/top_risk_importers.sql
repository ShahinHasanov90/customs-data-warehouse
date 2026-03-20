-- =============================================================================
-- Query: Top Risk Importers
-- Identifies importers with the highest average risk scores, filtered to
-- those with meaningful volume (>= 5 declarations).
-- =============================================================================

SELECT
    i.importer_id,
    i.importer_name,
    i.country_name                                  AS importer_country,
    COUNT(f.declaration_id)                         AS total_declarations,
    SUM(f.declared_value)                           AS total_declared_value,
    ROUND(AVG(f.risk_score), 2)                     AS avg_risk_score,
    MAX(f.risk_score)                               AS max_risk_score,
    COUNT(CASE WHEN f.is_flagged THEN 1 END)        AS flagged_count,
    ROUND(
        COUNT(CASE WHEN f.is_flagged THEN 1 END)::NUMERIC
        / COUNT(f.declaration_id) * 100, 1
    )                                               AS flagged_pct,
    COUNT(DISTINCT f.commodity_key)                 AS distinct_commodities,
    COUNT(DISTINCT f.route_key)                     AS distinct_routes,
    STRING_AGG(DISTINCT rl.risk_level, ', ' ORDER BY rl.risk_level)
                                                    AS risk_levels_seen
FROM fact_declarations f
JOIN dim_importer i ON f.importer_key = i.importer_key
CROSS JOIN LATERAL (SELECT f.risk_level) rl(risk_level)
WHERE f.date_key >= (
    SELECT date_key FROM dim_date
    WHERE full_date = CURRENT_DATE - INTERVAL '12 months'
    LIMIT 1
)
GROUP BY i.importer_id, i.importer_name, i.country_name
HAVING COUNT(f.declaration_id) >= 5
ORDER BY avg_risk_score DESC, total_declared_value DESC
LIMIT 25;
