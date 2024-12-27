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
    avg(length_of_stay) as avg_length_of_stay,
    count(patient_name) as patient_count,
    avg(Billing_Amount) as avg_billing_amount,
    count(distinct Hospital) as unique_hospitals
  from silver_data
  group by diagnosis
)
select *
from diagnosis_summary
