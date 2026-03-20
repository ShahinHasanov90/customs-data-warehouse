-- =============================================================================
-- Customs Data Warehouse — Analytical Views
-- =============================================================================

-- ---------------------------------------------------------------------------
-- View: daily_trade_summary
-- Aggregated daily trade metrics across all declarations.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_trade_summary AS
SELECT
    d.full_date,
    d.year,
    d.month,
    d.day_of_week,
    d.day_name,
    COUNT(f.declaration_id)                         AS total_declarations,
    SUM(f.declared_value)                           AS total_declared_value,
    SUM(f.weight_kg)                                AS total_weight_kg,
    SUM(f.duty_amount)                              AS total_duty_amount,
    SUM(f.tax_amount)                               AS total_tax_amount,
    AVG(f.declared_value)                           AS avg_declared_value,
    AVG(f.risk_score)                               AS avg_risk_score,
    COUNT(CASE WHEN f.is_flagged THEN 1 END)        AS flagged_count,
    ROUND(
        COUNT(CASE WHEN f.is_flagged THEN 1 END)::NUMERIC
        / NULLIF(COUNT(f.declaration_id), 0) * 100, 2
    )                                               AS flagged_pct
FROM fact_declarations f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.full_date, d.year, d.month, d.day_of_week, d.day_name
ORDER BY d.full_date;

CREATE UNIQUE INDEX IF NOT EXISTS idx_dts_full_date
    ON daily_trade_summary (full_date);

-- ---------------------------------------------------------------------------
-- View: monthly_risk_trends
-- Month-over-month risk scoring trends.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS monthly_risk_trends AS
SELECT
    d.year,
    d.month,
    d.month_name,
    f.risk_level,
    COUNT(f.declaration_id)                         AS declaration_count,
    SUM(f.declared_value)                           AS total_value,
    AVG(f.risk_score)                               AS avg_risk_score,
    MAX(f.risk_score)                               AS max_risk_score,
    COUNT(CASE WHEN f.is_flagged THEN 1 END)        AS flagged_count
FROM fact_declarations f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name, f.risk_level
ORDER BY d.year, d.month, f.risk_level;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mrt_year_month_level
    ON monthly_risk_trends (year, month, risk_level);

-- ---------------------------------------------------------------------------
-- View: top_risk_importers
-- Importers ranked by average risk score with volume context.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS top_risk_importers AS
SELECT
    i.importer_key,
    i.importer_id,
    i.importer_name,
    i.country_code                                  AS importer_country,
    COUNT(f.declaration_id)                         AS total_declarations,
    SUM(f.declared_value)                           AS total_declared_value,
    AVG(f.risk_score)                               AS avg_risk_score,
    MAX(f.risk_score)                               AS max_risk_score,
    COUNT(CASE WHEN f.is_flagged THEN 1 END)        AS flagged_count,
    ROUND(
        COUNT(CASE WHEN f.is_flagged THEN 1 END)::NUMERIC
        / NULLIF(COUNT(f.declaration_id), 0) * 100, 2
    )                                               AS flagged_pct,
    COUNT(DISTINCT f.commodity_key)                 AS distinct_commodities,
    COUNT(DISTINCT f.route_key)                     AS distinct_routes
FROM fact_declarations f
JOIN dim_importer i ON f.importer_key = i.importer_key
GROUP BY i.importer_key, i.importer_id, i.importer_name, i.country_code
ORDER BY avg_risk_score DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_tri_importer_key
    ON top_risk_importers (importer_key);

-- ---------------------------------------------------------------------------
-- View: commodity_concentration
-- Commodity-level trade concentration and risk metrics.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS commodity_concentration AS
SELECT
    c.commodity_key,
    c.hs_code,
    c.hs_chapter,
    c.description                                   AS commodity_description,
    COUNT(f.declaration_id)                         AS total_declarations,
    COUNT(DISTINCT f.importer_key)                  AS distinct_importers,
    COUNT(DISTINCT f.exporter_key)                  AS distinct_exporters,
    SUM(f.declared_value)                           AS total_declared_value,
    SUM(f.weight_kg)                                AS total_weight_kg,
    AVG(f.declared_value / NULLIF(f.weight_kg, 0))  AS avg_unit_value,
    AVG(f.risk_score)                               AS avg_risk_score,
    COUNT(CASE WHEN f.is_flagged THEN 1 END)        AS flagged_count
FROM fact_declarations f
JOIN dim_commodity c ON f.commodity_key = c.commodity_key
GROUP BY c.commodity_key, c.hs_code, c.hs_chapter, c.description
ORDER BY total_declared_value DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_cc_commodity_key
    ON commodity_concentration (commodity_key);

-- ---------------------------------------------------------------------------
-- View: route_analysis
-- Trade corridor metrics with volume and risk breakdown.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS route_analysis AS
SELECT
    r.route_key,
    r.origin_country_code,
    r.origin_country_name,
    r.dest_country_code,
    r.dest_country_name,
    r.transport_mode,
    COUNT(f.declaration_id)                         AS total_declarations,
    SUM(f.declared_value)                           AS total_declared_value,
    SUM(f.weight_kg)                                AS total_weight_kg,
    SUM(f.duty_amount)                              AS total_duty_collected,
    AVG(f.risk_score)                               AS avg_risk_score,
    COUNT(CASE WHEN f.is_flagged THEN 1 END)        AS flagged_count,
    COUNT(DISTINCT f.importer_key)                  AS distinct_importers,
    COUNT(DISTINCT f.commodity_key)                 AS distinct_commodities
FROM fact_declarations f
JOIN dim_route r ON f.route_key = r.route_key
GROUP BY r.route_key, r.origin_country_code, r.origin_country_name,
         r.dest_country_code, r.dest_country_name, r.transport_mode
ORDER BY total_declared_value DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_ra_route_key
    ON route_analysis (route_key);

-- ---------------------------------------------------------------------------
-- Refresh helper (call periodically or after bulk loads)
-- ---------------------------------------------------------------------------
-- REFRESH MATERIALIZED VIEW CONCURRENTLY daily_trade_summary;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY monthly_risk_trends;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY top_risk_importers;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY commodity_concentration;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY route_analysis;
