create or replace view sandbox.durdapilletadelaparra.nsf_payment_plans as
with nsfs_w_pps as (
    select
        transaction_id
    from
        sandbox.durdapilletadelaparra.nsf_payment_plans1
),
nsfs_wo_pps as (
    select 
        loan_id,
        transaction_id,
        posted_year,
        posted_week,
        posted_date,
        amount,
        resolved,
        resolved_date,
        null as pp_guid,
        null as plan_start,
        null as plan_end,
        null as plan_type,
        null as time_to_payment_plan,
        null as plan_category,
        false as has_pp
    from
        sandbox.durdapilletadelaparra.nsf_report
    where
        first_nsf
        and transaction_id not in (select * from nsfs_w_pps)
)
select
    *,
    true as has_pp
from
    sandbox.durdapilletadelaparra.nsf_payment_plans1
union all 
select * 
from nsfs_wo_pps;