{{ config(materialized='view', schema='analytics') }}

select
    a.account_id,
    a.customer_id,
    a.balance,
    a.account_type,
    coalesce(c.has_loan, false) as has_loan
from {{ ref('stg_accounts') }} a
left join {{ ref('stg_customers') }} c
    on a.customer_id = c.customer_id
where lower(a.account_type) = 'savings'
