{{ config(materialized='view') }}

select
    symbol,
    trade_count,
    round(cast(notional_volume as double), 2) as notional_volume,
    round(cast(total_qty as double), 8) as total_qty,
    round(cast(vwap as double), 2) as vwap,
    buy_count,
    sell_count,
    round(cast(buy_sell_ratio as double), 4) as buy_sell_ratio,
    gold_processed_at
from delta.`/Volumes/workspace/default/kraken_data/gold_symbol_metrics`