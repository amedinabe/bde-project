{{
    config(
        unique_key='lga_code'
    )
}}

select 
cast(substring(lga_code_2016, 4) as int4) as lga_code,
median_age_persons, median_mortgage_repay_monthly, median_tot_prsnl_inc_weekly, median_rent_weekly, median_tot_fam_inc_weekly, average_num_psns_per_bedroom, median_tot_hhd_inc_weekly, average_household_size
from "postgres"."raw"."census_g02" as g02