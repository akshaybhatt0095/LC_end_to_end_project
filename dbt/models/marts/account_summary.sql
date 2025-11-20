{{ config(materialized='table', schema='analytics') }}

select * from {{ ref('int_interest_calculated') }}
