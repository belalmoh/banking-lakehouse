-- =============================================================================
-- Custom Test: Assert Positive Amounts
-- Ensures all transaction amounts in cleansed models are positive
-- =============================================================================

SELECT
    transaction_id,
    amount
FROM {{ ref('transactions_cleansed') }}
WHERE amount <= 0
