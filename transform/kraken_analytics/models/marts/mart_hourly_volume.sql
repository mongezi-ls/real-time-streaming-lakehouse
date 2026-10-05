{{ config(materialized='table') }}

with revenue as (
    select * from {{ ref('stg_gold_revenue_per_minute') }}
)

select
    symbol,
    date_trunc('hour', revenue_minute) as hour,
    round(sum(notional_volume), 2) as hourly_notional_volume,
    sum(trade_count) as hourly_trade_count,
    round(avg(notional_volume), 2) as avg_per_minute_volume
from revenue
group by symbol, date_trunc('hour', revenue_minute)
order by hour desc, symbol