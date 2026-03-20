-- =============================================================================
-- Custom Test: assert_valid_risk_scores
-- Ensures risk_score is within the valid range [0, 100] and risk_level
-- matches the expected categories. Any rows returned indicate a failure.
-- =============================================================================

SELECT
    declaration_id,
    declaration_no,
    risk_score,
    risk_level
FROM {{ ref('stg_declarations') }}
WHERE risk_score < 0
   OR risk_score > 100
   OR risk_level NOT IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')
