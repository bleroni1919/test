{{ config(
    materialized='table'
) }}

with raw_data as (
  select *
  from {{ source('bronze', 'bronze_healthcare_data') }}
),
cleaned_data as (
  select
    INITCAP(Name) as patient_name,
    Age as age,
    case
      when age <= 12 then 'Child'
      when age between 13 and 59 then 'Adult'
      else 'Senior'
    end as age_group,
    Gender as gender,
    Blood_Type as blood_type,
    Medical_Condition as diagnosis,
    Date_of_Admission as date_of_admission,
    year(Date_of_Admission) as admission_year,
    month(Date_of_Admission) as admission_month,
    Discharge_Date as discharge_date,
    datediff(Discharge_Date, Date_of_Admission) as length_of_stay,
    Doctor as doctor_name,
    Hospital as hospital_name,
    Insurance_Provider as insurance_provider,
    Billing_Amount as billing_amount,
    avg(Billing_Amount) over() as avg_billing_amount,
    Room_Number as room_number,
    Admission_Type as admission,
    Medication as medication,
    Test_Results as test_results
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
