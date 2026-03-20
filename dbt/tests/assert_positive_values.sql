-- =============================================================================
-- Custom Test: assert_positive_values
-- Ensures declared_value, weight_kg, and duty_amount are non-negative
-- in the staging model. Any rows returned indicate a failure.
-- =============================================================================

SELECT
    declaration_id,
    declaration_no,
    declared_value,
    weight_kg,
    duty_amount
FROM {{ ref('stg_declarations') }}
WHERE declared_value < 0
   OR weight_kg < 0
   OR duty_amount < 0
