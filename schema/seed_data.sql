-- =============================================================================
-- Customs Data Warehouse — Synthetic Seed Data
-- =============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- dim_date: populate calendar for 2022-01-01 through 2024-12-31
-- ---------------------------------------------------------------------------
INSERT INTO dim_date (date_key, full_date, year, quarter, month, month_name,
                      week, day_of_month, day_of_week, day_name,
                      is_weekend, is_holiday, fiscal_year, fiscal_quarter)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER                AS date_key,
    d                                               AS full_date,
    EXTRACT(YEAR FROM d)::SMALLINT                  AS year,
    EXTRACT(QUARTER FROM d)::SMALLINT               AS quarter,
    EXTRACT(MONTH FROM d)::SMALLINT                 AS month,
    TO_CHAR(d, 'Month')                             AS month_name,
    EXTRACT(WEEK FROM d)::SMALLINT                  AS week,
    EXTRACT(DAY FROM d)::SMALLINT                   AS day_of_month,
    EXTRACT(ISODOW FROM d)::SMALLINT                AS day_of_week,
    TO_CHAR(d, 'Day')                               AS day_name,
    EXTRACT(ISODOW FROM d) IN (6, 7)               AS is_weekend,
    FALSE                                           AS is_holiday,
    CASE WHEN EXTRACT(MONTH FROM d) >= 10
         THEN EXTRACT(YEAR FROM d)::SMALLINT + 1
         ELSE EXTRACT(YEAR FROM d)::SMALLINT
    END                                             AS fiscal_year,
    CASE WHEN EXTRACT(MONTH FROM d) >= 10
         THEN ((EXTRACT(MONTH FROM d)::INT - 10) / 3) + 1
         ELSE ((EXTRACT(MONTH FROM d)::INT + 2) / 3)
    END::SMALLINT                                   AS fiscal_quarter
FROM generate_series('2022-01-01'::DATE, '2024-12-31'::DATE, '1 day') AS d
ON CONFLICT (date_key) DO NOTHING;

-- ---------------------------------------------------------------------------
-- dim_importer
-- ---------------------------------------------------------------------------
INSERT INTO dim_importer (importer_id, importer_name, tax_id, country_code, country_name, city, registration_date, importer_type) VALUES
    ('IMP-001', 'Caspian Trade LLC',        'TIN-AZ-00101', 'AZ', 'Azerbaijan', 'Baku',       '2018-03-15', 'STANDARD'),
    ('IMP-002', 'Silk Road Imports GmbH',   'TIN-DE-00201', 'DE', 'Germany',    'Hamburg',     '2016-07-22', 'STANDARD'),
    ('IMP-003', 'Anatolian Goods Ltd',      'TIN-TR-00301', 'TR', 'Turkey',     'Istanbul',    '2019-01-10', 'STANDARD'),
    ('IMP-004', 'Black Sea Commerce',       'TIN-GE-00401', 'GE', 'Georgia',    'Tbilisi',     '2020-05-08', 'SMALL'),
    ('IMP-005', 'Meridian Wholesale Inc',   'TIN-US-00501', 'US', 'United States', 'New York', '2015-11-30', 'LARGE'),
    ('IMP-006', 'Dubai General Trading',    'TIN-AE-00601', 'AE', 'UAE',        'Dubai',       '2017-09-14', 'LARGE'),
    ('IMP-007', 'Baltic Supply Chain OY',   'TIN-FI-00701', 'FI', 'Finland',    'Helsinki',    '2021-02-18', 'STANDARD'),
    ('IMP-008', 'Central Asia Logistics',   'TIN-KZ-00801', 'KZ', 'Kazakhstan', 'Almaty',      '2019-08-25', 'STANDARD')
ON CONFLICT (importer_id) DO NOTHING;

-- ---------------------------------------------------------------------------
-- dim_exporter
-- ---------------------------------------------------------------------------
INSERT INTO dim_exporter (exporter_id, exporter_name, country_code, country_name, city, exporter_type) VALUES
    ('EXP-001', 'Shanghai Electronics Co',   'CN', 'China',        'Shanghai',   'LARGE'),
    ('EXP-002', 'Mumbai Textiles Pvt Ltd',   'IN', 'India',        'Mumbai',     'STANDARD'),
    ('EXP-003', 'Izmir Agro Export',         'TR', 'Turkey',       'Izmir',      'STANDARD'),
    ('EXP-004', 'Milan Fashion House SRL',   'IT', 'Italy',        'Milan',      'STANDARD'),
    ('EXP-005', 'Seoul Tech Industries',     'KR', 'South Korea',  'Seoul',      'LARGE'),
    ('EXP-006', 'Sao Paulo Commodities SA',  'BR', 'Brazil',       'Sao Paulo',  'LARGE'),
    ('EXP-007', 'Tokyo Machinery Corp',      'JP', 'Japan',        'Tokyo',      'LARGE'),
    ('EXP-008', 'Rotterdam Chemical BV',     'NL', 'Netherlands',  'Rotterdam',  'STANDARD')
ON CONFLICT (exporter_id) DO NOTHING;

-- ---------------------------------------------------------------------------
-- dim_commodity (sample HS codes)
-- ---------------------------------------------------------------------------
INSERT INTO dim_commodity (hs_code, hs_chapter, hs_heading, hs_subheading, description, chapter_desc, heading_desc, unit_of_measure, duty_rate, is_restricted) VALUES
    ('8471300000', '84', '8471', '847130', 'Portable digital automatic data processing machines',   'Nuclear reactors, boilers, machinery', 'Automatic data processing machines', 'UNIT', 5.00,  FALSE),
    ('6109100000', '61', '6109', '610910', 'T-shirts, singlets and other vests, of cotton',         'Articles of apparel, knitted',          'T-shirts and singlets',              'KG',   12.00, FALSE),
    ('0805100000', '08', '0805', '080510', 'Oranges, fresh',                                        'Edible fruit and nuts',                 'Citrus fruit, fresh or dried',       'KG',   8.00,  FALSE),
    ('2710192100', '27', '2710', '271019', 'Kerosene-type jet fuel',                                'Mineral fuels, oils',                   'Petroleum oils',                     'LTR',  3.50,  TRUE),
    ('7108120000', '71', '7108', '710812', 'Gold in unwrought forms (non-monetary)',                 'Precious metals and stones',            'Gold, unwrought or semi-manufactured','KG',   0.00,  TRUE),
    ('3004900000', '30', '3004', '300490', 'Medicaments, packaged for retail sale',                  'Pharmaceutical products',               'Medicaments in measured doses',       'KG',   2.00,  FALSE),
    ('8703230000', '87', '8703', '870323', 'Motor vehicles, 1500-3000cc',                            'Vehicles other than railway',           'Motor cars and vehicles',             'UNIT', 15.00, FALSE),
    ('9401610000', '94', '9401', '940161', 'Upholstered wooden-framed seats',                        'Furniture, bedding, lamps',             'Seats',                               'UNIT', 6.00,  FALSE)
ON CONFLICT (hs_code) DO NOTHING;

-- ---------------------------------------------------------------------------
-- dim_route
-- ---------------------------------------------------------------------------
INSERT INTO dim_route (origin_country_code, origin_country_name, origin_port, dest_country_code, dest_country_name, dest_port, transport_mode, distance_km, avg_transit_days) VALUES
    ('CN', 'China',       'Shanghai',   'AZ', 'Azerbaijan', 'Baku',      'SEA',   8500, 35),
    ('TR', 'Turkey',      'Istanbul',   'AZ', 'Azerbaijan', 'Baku',      'ROAD',  2200, 5),
    ('DE', 'Germany',     'Hamburg',    'AZ', 'Azerbaijan', 'Baku',      'RAIL',  4800, 14),
    ('IN', 'India',       'Mumbai',     'AZ', 'Azerbaijan', 'Baku',      'SEA',   5200, 21),
    ('IT', 'Italy',       'Milan',      'AZ', 'Azerbaijan', 'Baku',      'AIR',   3400, 1),
    ('KR', 'South Korea', 'Seoul',      'AZ', 'Azerbaijan', 'Baku',      'AIR',   7200, 2),
    ('BR', 'Brazil',      'Santos',     'US', 'United States', 'New York','SEA',   9600, 18),
    ('JP', 'Japan',       'Yokohama',   'FI', 'Finland',    'Helsinki',  'SEA',  14000, 40)
ON CONFLICT (origin_country_code, origin_port, dest_country_code, dest_port, transport_mode) DO NOTHING;

-- ---------------------------------------------------------------------------
-- dim_customs_post
-- ---------------------------------------------------------------------------
INSERT INTO dim_customs_post (post_code, post_name, region, post_type, country_code) VALUES
    ('AZ-BKU-01', 'Baku Main Customs',          'Baku',             'PORT',     'AZ'),
    ('AZ-AST-01', 'Astara Border Customs',       'Lankaran-Astara',  'BORDER',   'AZ'),
    ('AZ-BLK-01', 'Balakan Border Customs',      'Sheki-Zagatala',   'BORDER',   'AZ'),
    ('AZ-GYD-01', 'Heydar Aliyev Airport Post',  'Baku',             'AIRPORT',  'AZ'),
    ('AZ-SMX-01', 'Samur Border Customs',         'Khachmaz',         'BORDER',   'AZ'),
    ('AZ-NAX-01', 'Nakhchivan Customs',           'Nakhchivan',       'BORDER',   'AZ')
ON CONFLICT (post_code) DO NOTHING;

-- ---------------------------------------------------------------------------
-- fact_declarations (sample records)
-- ---------------------------------------------------------------------------
INSERT INTO fact_declarations (
    declaration_no, date_key, importer_key, exporter_key, commodity_key,
    route_key, customs_post_key, declared_value, statistical_value,
    weight_kg, quantity, duty_amount, tax_amount,
    risk_score, risk_level, is_flagged, inspection_result,
    declaration_type, currency_code
) VALUES
    ('DCL-2024-000001', 20240115, 1, 1, 1, 1, 1, 125000.00, 128000.00, 450.000,  20, 6250.00,  22500.00, 12.50, 'LOW',      FALSE, NULL,        'IMPORT', 'USD'),
    ('DCL-2024-000002', 20240118, 2, 4, 7, 3, 1, 285000.00, 290000.00, 1800.000,  3,  42750.00, 51300.00, 25.00, 'LOW',      FALSE, NULL,        'IMPORT', 'EUR'),
    ('DCL-2024-000003', 20240122, 3, 3, 2, 2, 2, 18500.00,  19000.00,  3200.000,  NULL, 2220.00, 3330.00, 45.80, 'MEDIUM',   FALSE, NULL,        'IMPORT', 'USD'),
    ('DCL-2024-000004', 20240125, 1, 1, 1, 1, 1, 8500.00,   8200.00,   380.000,   15, 425.00,   1530.00,  78.90, 'HIGH',     TRUE,  'MISMATCH', 'IMPORT', 'USD'),
    ('DCL-2024-000005', 20240201, 4, 2, 6, 4, 1, 42000.00,  43500.00,  850.000,   NULL, 840.00,  7560.00, 15.20, 'LOW',      FALSE, NULL,        'IMPORT', 'USD'),
    ('DCL-2024-000006', 20240205, 6, 5, 1, 6, 4, 950000.00, 960000.00, 200.000,   50, 47500.00, 171000.00, 88.50, 'CRITICAL', TRUE, 'UNDERVALUE','IMPORT', 'USD'),
    ('DCL-2024-000007', 20240210, 1, 3, 3, 2, 2, 6800.00,   7000.00,   12000.000, NULL, 544.00,  1224.00,  8.00,  'LOW',      FALSE, NULL,        'IMPORT', 'USD'),
    ('DCL-2024-000008', 20240215, 3, 8, 4, 2, 5, 175000.00, 178000.00, 50000.000, NULL, 6125.00, 31500.00, 62.30, 'HIGH',     TRUE,  'RESTRICTED','IMPORT', 'USD'),
    ('DCL-2024-000009', 20240220, 7, 7, 8, 8, 1, 32000.00,  33000.00,  2200.000,  40, 1920.00,  5760.00,  22.10, 'LOW',      FALSE, NULL,        'IMPORT', 'EUR'),
    ('DCL-2024-000010', 20240228, 5, 6, 5, 7, 1, 520000.00, 525000.00, 5.000,     NULL, 0.00,    93600.00, 72.40, 'HIGH',     TRUE,  'DOCS_MISSING','IMPORT','USD')
ON CONFLICT (declaration_no) DO NOTHING;

COMMIT;
