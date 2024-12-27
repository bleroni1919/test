{{ config(
    materialized='incremental',
    unique_key='id'
) }}

with raw_data as (
  select *
  from {{ source('bronze', 'bronze_healthcare_data') }}
  {% if is_incremental() %}
    where Date_of_Admission > (select max(Date_of_Admission) from {{ this }})
  {% endif %}
),
cleaned_data as (
  select
    INITCAP(Name) as patient_name,  
    Age,
    case
      when Age <= 12 then 'Child'
      when Age between 13 and 59 then 'Adult'
      else 'Senior'
    end as age_group,
    Gender as gender,
    Blood_Type,
    Medical_Condition as diagnosis,
    Date_of_Admission as date_of_admission,
    year(Date_of_Admission) as admission_year,
    month(Date_of_Admission) as admission_month,
    Discharge_Date as discharge_date,
    datediff(Discharge_Date, Date_of_Admission) as length_of_stay,
    Doctor,
    Hospital,
    Insurance_Provider,
    Billing_Amount,
    avg(Billing_Amount) over() as avg_billing_amount,
    Room_Number,
    Admission_Type,
    Medication,
    Test_Results
  from raw_data
),
unique_medication_count as (
  select
    count(distinct Medication) as unique_medications
  from raw_data
)
select
  cleaned_data.*,
  unique_medications
from cleaned_data
cross join unique_medication_count
