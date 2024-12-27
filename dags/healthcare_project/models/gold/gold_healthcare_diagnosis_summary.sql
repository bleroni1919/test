{{ config(
    materialized='table'
) }}

with silver_data as (
  select *
  from {{ ref('silver_healthcare_data') }}
),
diagnosis_summary as (
  select
    diagnosis,
    round(avg(length_of_stay), 2) as avg_length_of_stay,
    count(patient_name) as patient_count,
    round(avg(billing_amount), 2) as avg_billing_amount,
    round(avg(age), 2) as avg_age
  from silver_data
  group by diagnosis
)
select *
from diagnosis_summary
