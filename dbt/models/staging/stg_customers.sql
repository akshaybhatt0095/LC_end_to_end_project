{{ config(materialized='view', schema='analytics') }}

with raw as (
    select
        try_cast(CustomerID as integer) as customer_id,
        trim(lower(Name)) as name,
        case
            when lower(trim(HasLoan)) in ('yes', 'y', 'true', '1') then true
            when lower(trim(HasLoan)) in ('no', 'n', 'false', '0') then false
            when lower(trim(HasLoan)) in ('none', '') then null
            else null  
        end as has_loan
    from {{ source('raw', 'customers') }}
)

select * from raw
