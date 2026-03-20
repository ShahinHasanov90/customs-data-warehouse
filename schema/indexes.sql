-- =============================================================================
-- Customs Data Warehouse — Performance Indexes
-- =============================================================================
-- Indexes designed around common analytical query patterns:
--   1. Date-range filtering on fact table
--   2. Risk-based lookups (flagged, risk level, risk score)
--   3. Importer / exporter drill-downs
--   4. Commodity and route aggregations
--   5. Composite covering indexes for dashboards
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Fact table: date-range queries
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_fact_decl_date_key
    ON fact_declarations (date_key);

CREATE INDEX IF NOT EXISTS idx_fact_decl_date_value
    ON fact_declarations (date_key, declared_value);

-- ---------------------------------------------------------------------------
-- Fact table: risk-based queries
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_fact_decl_risk_level
    ON fact_declarations (risk_level);

CREATE INDEX IF NOT EXISTS idx_fact_decl_risk_score
    ON fact_declarations (risk_score DESC);

CREATE INDEX IF NOT EXISTS idx_fact_decl_flagged
    ON fact_declarations (is_flagged)
    WHERE is_flagged = TRUE;

CREATE INDEX IF NOT EXISTS idx_fact_decl_risk_composite
    ON fact_declarations (risk_level, risk_score DESC, is_flagged);

-- ---------------------------------------------------------------------------
-- Fact table: importer / exporter drill-downs
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_fact_decl_importer_key
    ON fact_declarations (importer_key);

CREATE INDEX IF NOT EXISTS idx_fact_decl_exporter_key
    ON fact_declarations (exporter_key);

CREATE INDEX IF NOT EXISTS idx_fact_decl_importer_date
    ON fact_declarations (importer_key, date_key);

CREATE INDEX IF NOT EXISTS idx_fact_decl_exporter_date
    ON fact_declarations (exporter_key, date_key);

-- ---------------------------------------------------------------------------
-- Fact table: commodity and route aggregations
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_fact_decl_commodity_key
    ON fact_declarations (commodity_key);

CREATE INDEX IF NOT EXISTS idx_fact_decl_route_key
    ON fact_declarations (route_key);

CREATE INDEX IF NOT EXISTS idx_fact_decl_customs_post_key
    ON fact_declarations (customs_post_key);

CREATE INDEX IF NOT EXISTS idx_fact_decl_commodity_date
    ON fact_declarations (commodity_key, date_key);

CREATE INDEX IF NOT EXISTS idx_fact_decl_route_date
    ON fact_declarations (route_key, date_key);

-- ---------------------------------------------------------------------------
-- Fact table: composite covering indexes for common dashboard queries
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_fact_decl_dashboard_summary
    ON fact_declarations (date_key, importer_key, commodity_key)
    INCLUDE (declared_value, weight_kg, duty_amount, risk_score, is_flagged);

CREATE INDEX IF NOT EXISTS idx_fact_decl_risk_dashboard
    ON fact_declarations (date_key, risk_level)
    INCLUDE (declared_value, risk_score, is_flagged);

-- ---------------------------------------------------------------------------
-- Dimension table indexes (supplementary)
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_dim_commodity_hs_chapter
    ON dim_commodity (hs_chapter);

CREATE INDEX IF NOT EXISTS idx_dim_commodity_hs_heading
    ON dim_commodity (hs_heading);

CREATE INDEX IF NOT EXISTS idx_dim_route_origin
    ON dim_route (origin_country_code);

CREATE INDEX IF NOT EXISTS idx_dim_route_dest
    ON dim_route (dest_country_code);

CREATE INDEX IF NOT EXISTS idx_dim_route_transport
    ON dim_route (transport_mode);

CREATE INDEX IF NOT EXISTS idx_dim_importer_country
    ON dim_importer (country_code);

CREATE INDEX IF NOT EXISTS idx_dim_exporter_country
    ON dim_exporter (country_code);

CREATE INDEX IF NOT EXISTS idx_dim_date_year_month
    ON dim_date (year, month);
