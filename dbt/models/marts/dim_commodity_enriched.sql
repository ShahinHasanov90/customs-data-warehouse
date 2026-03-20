-- =============================================================================
-- Mart: dim_commodity_enriched
-- Enriched commodity dimension with HS hierarchy labels and usage stats.
-- =============================================================================

WITH commodity AS (

    SELECT * FROM {{ source('customs', 'dim_commodity') }}

),

declarations AS (

    SELECT * FROM {{ ref('stg_declarations') }}

),

usage_stats AS (

    SELECT
        commodity_key,
        COUNT(declaration_id)                               AS total_declarations,
        COUNT(DISTINCT importer_key)                        AS distinct_importers,
        COUNT(DISTINCT exporter_key)                        AS distinct_exporters,
        SUM(declared_value)                                 AS total_declared_value,
        SUM(weight_kg)                                      AS total_weight_kg,
        AVG(declared_value / NULLIF(weight_kg, 0))          AS avg_unit_value,
        AVG(risk_score)                                     AS avg_risk_score,
        COUNT(CASE WHEN is_flagged THEN 1 END)              AS flagged_count,
        MIN(date_key)                                       AS first_seen_date_key,
        MAX(date_key)                                       AS last_seen_date_key
    FROM declarations
    GROUP BY commodity_key

),

enriched AS (

    SELECT
        c.commodity_key,
        c.hs_code,

        -- HS hierarchy
        c.hs_chapter,
        c.hs_heading,
        c.hs_subheading,
        c.chapter_desc                                      AS hs_chapter_description,
        c.heading_desc                                      AS hs_heading_description,
        c.description                                       AS commodity_description,

        -- Classification
        CASE
            WHEN c.hs_chapter IN ('01','02','03','04','05','06','07','08','09','10','11','12','13','14','15')
                THEN 'Agricultural & Food Products'
            WHEN c.hs_chapter IN ('25','26','27')
                THEN 'Mineral Products & Fuels'
            WHEN c.hs_chapter IN ('28','29','30','31','32','33','34','35','36','37','38')
                THEN 'Chemical & Pharmaceutical'
            WHEN c.hs_chapter IN ('50','51','52','53','54','55','56','57','58','59','60','61','62','63')
                THEN 'Textiles & Apparel'
            WHEN c.hs_chapter IN ('72','73','74','75','76','78','79','80','81','82','83')
                THEN 'Base Metals'
            WHEN c.hs_chapter IN ('84','85')
                THEN 'Machinery & Electronics'
            WHEN c.hs_chapter IN ('86','87','88','89')
                THEN 'Transport Equipment'
            WHEN c.hs_chapter IN ('71')
                THEN 'Precious Metals & Stones'
            ELSE 'Other'
        END                                                 AS commodity_sector,

        -- Properties
        c.unit_of_measure,
        c.duty_rate,
        c.is_restricted,
        c.is_prohibited,

        -- Usage statistics (from fact table)
        COALESCE(u.total_declarations, 0)                   AS total_declarations,
        COALESCE(u.distinct_importers, 0)                   AS distinct_importers,
        COALESCE(u.distinct_exporters, 0)                   AS distinct_exporters,
        COALESCE(u.total_declared_value, 0)                 AS total_declared_value,
        COALESCE(u.total_weight_kg, 0)                      AS total_weight_kg,
        u.avg_unit_value,
        COALESCE(u.avg_risk_score, 0)                       AS avg_risk_score,
        COALESCE(u.flagged_count, 0)                        AS flagged_count,
        u.first_seen_date_key,
        u.last_seen_date_key

    FROM commodity c
    LEFT JOIN usage_stats u
        ON c.commodity_key = u.commodity_key

)

SELECT * FROM enriched
