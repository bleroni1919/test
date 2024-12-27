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
    avg(billing_amount) as avg_billing_amount,
    count(distinct hospital_name) as unique_hospitals,
    first_value(hospital_name) over (partition by diagnosis order by count(*) desc) as most_common_hospital
  from silver_data
  group by diagnosis, hospital_name
)
select
  diagnosis,
  avg_length_of_stay,
  patient_count,
  avg_billing_amount,
  most_common_hospital
from diagnosis_summary
group by diagnosis, avg_length_of_stay, patient_count, avg_billing_amount, most_common_hospital
