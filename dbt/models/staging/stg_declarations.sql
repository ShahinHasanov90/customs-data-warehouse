-- =============================================================================
-- Staging: stg_declarations
-- Cleans and standardises raw declaration data from the source system.
-- =============================================================================

WITH source AS (

    SELECT
        declaration_id,
        declaration_no,
        date_key,
        importer_key,
        exporter_key,
        commodity_key,
        route_key,
        customs_post_key,
        declared_value,
        statistical_value,
        weight_kg,
        quantity,
        duty_amount,
        tax_amount,
        risk_score,
        risk_level,
        is_flagged,
        inspection_result,
        declaration_type,
        currency_code,
        created_at,
        updated_at
    FROM {{ source('customs', 'fact_declarations') }}

),

cleaned AS (

    SELECT
        declaration_id,
        TRIM(declaration_no)                                AS declaration_no,
        date_key,
        importer_key,
        exporter_key,
        commodity_key,
        route_key,
        customs_post_key,

        -- Monetary values: ensure non-negative
        GREATEST(declared_value, 0)                         AS declared_value,
        COALESCE(statistical_value, declared_value)         AS statistical_value,

        -- Weight and quantity
        GREATEST(weight_kg, 0)                              AS weight_kg,
        quantity,

        -- Duty and tax
        GREATEST(duty_amount, 0)                            AS duty_amount,
        GREATEST(tax_amount, 0)                             AS tax_amount,

        -- Risk fields
        ROUND(LEAST(GREATEST(risk_score, 0), 100), 2)      AS risk_score,
        UPPER(TRIM(risk_level))                             AS risk_level,
        COALESCE(is_flagged, FALSE)                         AS is_flagged,
        UPPER(TRIM(inspection_result))                      AS inspection_result,

        -- Metadata
        UPPER(TRIM(declaration_type))                       AS declaration_type,
        UPPER(TRIM(currency_code))                          AS currency_code,
        created_at,
        updated_at

    FROM source

)

SELECT * FROM cleaned
