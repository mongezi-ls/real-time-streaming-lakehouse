{{ config(materialized='table') }}

with metrics as (
    select * from {{ ref('stg_gold_symbol_metrics') }}
),

revenue as (
    select
        symbol,
        round(sum(notional_volume), 2) as total_revenue,
        min(revenue_minute) as first_minute,
        max(revenue_minute) as last_minute
    from {{ ref('stg_gold_revenue_per_minute') }}
    group by symbol
)

select
    m.symbol,
    m.trade_count,
    round(m.vwap, 2) as vwap,
    m.buy_count,
    m.sell_count,
    round(m.buy_sell_ratio, 4) as buy_sell_ratio,
    r.total_revenue,
    r.first_minute,
    r.last_minute,
    m.gold_processed_at
from metrics m
left join revenue r on m.symbol = r.symbol