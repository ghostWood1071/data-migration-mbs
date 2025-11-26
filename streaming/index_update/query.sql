WITH daily_index AS (
    SELECT
        IndexCode,
        TotalValuesTraded,
        Last_updated + INTERVAL 7 HOUR AS trading_date
    FROM market_data_index
),
ranked AS (
    SELECT
        IndexCode,
        trading_date,
        TotalValuesTraded,
        ROW_NUMBER() OVER (
            PARTITION BY IndexCode, DATE(trading_date)
            ORDER BY trading_date DESC
        ) AS rn
    FROM daily_index
),
last_per_index AS (
    SELECT
        IndexCode,
        trading_date,
        TotalValuesTraded AS last_total_values_traded
    FROM ranked
    WHERE rn = 1
)
SELECT
    DATE(trading_date) AS C_DATE,
    SUM(last_total_values_traded) AS total_market_value
FROM last_per_index
GROUP BY DATE(trading_date)
ORDER BY DATE(trading_date);