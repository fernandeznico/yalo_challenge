INSERT INTO analytics.iowa_liquor_daily_sales_by_store(
    store_number,
    date,
    variety_of_items,
    bottles_sold,
    liters_sold,
    sale_dollars
)
SELECT
    store_number::integer,
    date,
    count(distinct item_number) AS variety_of_items,
    sum(bottles_sold) AS bottles_sold,
    sum(volume_sold_liters) AS liters_sold,
    round(sum(state_bottle_retail * bottles_sold)::decimal, 4) AS sale_dollars
FROM staging.iowa_liquor_sales
WHERE date = '{{ ds }}'
GROUP BY store_number, date
ON CONFLICT (store_number, date) DO UPDATE
SET (variety_of_items, bottles_sold, liters_sold, sale_dollars) = (
    EXCLUDED.variety_of_items, EXCLUDED.bottles_sold, EXCLUDED.liters_sold, EXCLUDED.sale_dollars
)