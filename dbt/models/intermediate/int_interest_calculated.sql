{{ config(materialized='view', schema='analytics') }}

with base as (
    select
        *,
        case
            when balance is null then 0.0
            when balance < 10000 then 0.01
            when balance >= 10000 and balance < 20000 then 0.015
            else 0.02
        end
        +
        case when has_loan = true then 0.005 else 0 end
        as interest_rate
    from {{ ref('int_accounts_joined') }}
)

select
    account_id,
    customer_id,
    balance as original_balance,
    interest_rate,
    coalesce(balance, 0.0) * interest_rate as interest_amount,
    coalesce(balance, 0.0) + (coalesce(balance,0.0) * interest_rate) as new_balance
from base
