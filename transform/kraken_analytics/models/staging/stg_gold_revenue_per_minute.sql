{{ config(materialized='view') }}

select
    symbol,
    minute as revenue_minute,
    round(cast(notional_volume as double), 2) as notional_volume,
    trade_count,
    round(cast(total_qty as double), 8) as total_qty,
    gold_processed_at
from delta.`/Volumes/workspace/default/kraken_data/gold_revenue_per_minute`