create or replace table RAW.EEM_historical_transformed as
with base as (
    select *
    from RAW.EEM_raw_data
),
daily_calc as (
    select
        *,
        (close_price - lag(close_price) over(partition by ticker order by trade_date)) / lag(close_price) over(partition by ticker order by trade_date) as daily_return
    from base
),
metrics as (
    select
        *,
        -- moving averages
        avg(close_price) over(partition by ticker order by trade_date rows between 19 preceding and current row) as ma_20,
        avg(close_price) over(partition by ticker order by trade_date rows between 49 preceding and current row) as ma_50,

        -- rolling volatility
        stddev(daily_return) over(partition by ticker order by trade_date rows between 19 preceding and current row) as volatility_20,
        stddev(daily_return) over(partition by ticker order by trade_date rows between 49 preceding and current row) as volatility_50,

        -- RSI (14-day)
        avg(case when daily_return > 0 then daily_return else 0 end) over(partition by ticker order by trade_date rows between 13 preceding and current row) as avg_gain,
        avg(case when daily_return < 0 then abs(daily_return) else 0 end) over(partition by ticker order by trade_date rows between 13 preceding and current row) as avg_loss,

        -- High/Low metrics
        (high - low) as daily_range,
        (high - low) / nullif(open, 0) as range_pct,
        (high + low + close_price) / 3 as typical_price
    from daily_calc
)
select *,
    case
        when avg_gain + avg_loss = 0 then 0
        else 100 - (100 / (1 + (avg_gain / avg_loss)))
    end as rsi_14
from metrics;

