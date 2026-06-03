{{ config(materialized='view') }}

with source_data as (
    SELECT *
    FROM {{ source('raw','transactions')}}
    QUALIFY ROW_NUMBER() OVER(
    PARTITION BY tx_hash
    ORDER BY block_timestamp ASC
    ) = 1
)

select *
from source_data
