{{ config(materialized='view', schema='analytics') }}

with raw as (
    select
        AccountID as account_id,
        try_cast(CustomerID as integer) as customer_id,
        coalesce(try_cast(Balance as double), 0) as balance,
        lower(trim(AccountType)) as account_type
    from {{ source('raw', 'accounts') }}
)

select * from raw