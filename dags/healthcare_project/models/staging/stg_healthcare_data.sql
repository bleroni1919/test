{{ config(materialized='ephemeral') }}

with raw_data as (
  select *
  from {{ source('bronze', 'bronze_healthcare_data') }}
),
cleaned_data as (
  select
    patient_id,
    date_of_admission,
    discharge_date,
    trim(lower(patient_name)) as patient_name_clean,
    coalesce(diagnosis, 'Unknown') as diagnosis_clean
  from raw_data
)
select *
from cleaned_data
