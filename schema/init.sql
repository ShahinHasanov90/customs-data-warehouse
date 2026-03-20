-- =============================================================================
-- Customs Data Warehouse — Star Schema DDL
-- =============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- Dimension: dim_date
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER     PRIMARY KEY,
    full_date       DATE        NOT NULL UNIQUE,
    year            SMALLINT    NOT NULL,
    quarter         SMALLINT    NOT NULL,
    month           SMALLINT    NOT NULL,
    month_name      VARCHAR(9)  NOT NULL,
    week            SMALLINT    NOT NULL,
    day_of_month    SMALLINT    NOT NULL,
    day_of_week     SMALLINT    NOT NULL,
    day_name        VARCHAR(9)  NOT NULL,
    is_weekend      BOOLEAN     NOT NULL DEFAULT FALSE,
    is_holiday      BOOLEAN     NOT NULL DEFAULT FALSE,
    fiscal_year     SMALLINT    NOT NULL,
    fiscal_quarter  SMALLINT    NOT NULL
);

-- ---------------------------------------------------------------------------
-- Dimension: dim_importer
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_importer (
    importer_key    SERIAL      PRIMARY KEY,
    importer_id     VARCHAR(20) NOT NULL UNIQUE,
    importer_name   VARCHAR(200) NOT NULL,
    tax_id          VARCHAR(30),
    country_code    CHAR(2)     NOT NULL,
    country_name    VARCHAR(100) NOT NULL,
    city            VARCHAR(100),
    registration_date DATE,
    importer_type   VARCHAR(50) DEFAULT 'STANDARD',
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP   NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP   NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Dimension: dim_exporter
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_exporter (
    exporter_key    SERIAL      PRIMARY KEY,
    exporter_id     VARCHAR(20) NOT NULL UNIQUE,
    exporter_name   VARCHAR(200) NOT NULL,
    country_code    CHAR(2)     NOT NULL,
    country_name    VARCHAR(100) NOT NULL,
    city            VARCHAR(100),
    exporter_type   VARCHAR(50) DEFAULT 'STANDARD',
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP   NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP   NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Dimension: dim_commodity
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_commodity (
    commodity_key   SERIAL      PRIMARY KEY,
    hs_code         VARCHAR(10) NOT NULL UNIQUE,
    hs_chapter      VARCHAR(2)  NOT NULL,
    hs_heading      VARCHAR(4)  NOT NULL,
    hs_subheading   VARCHAR(6)  NOT NULL,
    description     VARCHAR(500) NOT NULL,
    chapter_desc    VARCHAR(200),
    heading_desc    VARCHAR(200),
    unit_of_measure VARCHAR(20) DEFAULT 'KG',
    duty_rate       NUMERIC(5,2) DEFAULT 0.00,
    is_restricted   BOOLEAN     NOT NULL DEFAULT FALSE,
    is_prohibited   BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMP   NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP   NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Dimension: dim_route
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_route (
    route_key           SERIAL      PRIMARY KEY,
    origin_country_code CHAR(2)     NOT NULL,
    origin_country_name VARCHAR(100) NOT NULL,
    origin_port         VARCHAR(100),
    dest_country_code   CHAR(2)     NOT NULL,
    dest_country_name   VARCHAR(100) NOT NULL,
    dest_port           VARCHAR(100),
    transport_mode      VARCHAR(20)  NOT NULL,
    route_description   VARCHAR(300),
    distance_km         INTEGER,
    avg_transit_days    SMALLINT,
    created_at          TIMESTAMP   NOT NULL DEFAULT NOW(),
    UNIQUE (origin_country_code, origin_port, dest_country_code, dest_port, transport_mode)
);

-- ---------------------------------------------------------------------------
-- Dimension: dim_customs_post
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_customs_post (
    customs_post_key SERIAL      PRIMARY KEY,
    post_code        VARCHAR(10) NOT NULL UNIQUE,
    post_name        VARCHAR(150) NOT NULL,
    region           VARCHAR(100),
    post_type        VARCHAR(50) NOT NULL DEFAULT 'BORDER',
    country_code     CHAR(2)     NOT NULL,
    is_active        BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMP   NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Fact: fact_declarations
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_declarations (
    declaration_id  BIGSERIAL   PRIMARY KEY,
    declaration_no  VARCHAR(30) NOT NULL UNIQUE,
    date_key        INTEGER     NOT NULL REFERENCES dim_date(date_key),
    importer_key    INTEGER     NOT NULL REFERENCES dim_importer(importer_key),
    exporter_key    INTEGER     NOT NULL REFERENCES dim_exporter(exporter_key),
    commodity_key   INTEGER     NOT NULL REFERENCES dim_commodity(commodity_key),
    route_key       INTEGER     NOT NULL REFERENCES dim_route(route_key),
    customs_post_key INTEGER    NOT NULL REFERENCES dim_customs_post(customs_post_key),
    declared_value  NUMERIC(15,2) NOT NULL,
    statistical_value NUMERIC(15,2),
    weight_kg       NUMERIC(12,3) NOT NULL,
    quantity        NUMERIC(12,3),
    duty_amount     NUMERIC(12,2) NOT NULL DEFAULT 0.00,
    tax_amount      NUMERIC(12,2) NOT NULL DEFAULT 0.00,
    risk_score      NUMERIC(5,2) NOT NULL DEFAULT 0.00,
    risk_level      VARCHAR(10) NOT NULL DEFAULT 'LOW'
                    CHECK (risk_level IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    is_flagged      BOOLEAN     NOT NULL DEFAULT FALSE,
    inspection_result VARCHAR(20),
    declaration_type VARCHAR(10) NOT NULL DEFAULT 'IMPORT',
    currency_code   CHAR(3)     NOT NULL DEFAULT 'USD',
    created_at      TIMESTAMP   NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP   NOT NULL DEFAULT NOW()
);

COMMIT;
