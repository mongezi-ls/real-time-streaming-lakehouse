{{ config(materialized='view') }}

select
    trade_id,
    symbol,
    side,
    round(cast(price as double), 4) as price,
    round(cast(qty as double), 8) as qty,
    ord_type,
    event_ts,
    kafka_ts,
    ingested_at,
    event_date,
    silver_processed_at
from delta.`/Volumes/workspace/default/kraken_data/silver_kraken_trades`
where trade_id is not null