create or replace view sandbox.durdapilletadelaparra.nsf_payment_plans1 as
with payment_plans as (
    select
        loan_guid,
        merchant_guid,
        coalesce(guid, turnkey_guid) as pp_guid,
        plan_type as payment_plan_type,
        start_date::date as payment_plan_start,
        end_date::date as payment_plan_end
    from
        analytics_production.dbt_ario_turnkey.dim_payment_plan
    order by
        start_date desc
)
select * from (
    select
        r.loan_id,
        r.transaction_id,
        r.posted_year,
        r.posted_week,
        r.posted_date,
        r.amount,
        r.resolved,
        r.resolved_date,
        pp.pp_guid,
        pp.payment_plan_start as plan_start,
        pp.payment_plan_end as plan_end,
        pp.payment_plan_type as plan_type,
        datediff(day, r.posted_date, plan_start) as time_to_payment_plan,
        case
            when plan_type = 'deferred' then 'Deferred'
            when plan_type in ('skip_payment','reduction','up_to_50','51_to_80') then 'Reduction and skip'
            when plan_type in ('restructured_plan','frequency_plan') then 'Restructured'
            when plan_type in ('nominal','covid_recovered') then 'Other'
            else null end as plan_category
    from
        (select * from sandbox.durdapilletadelaparra.nsf_report where first_nsf) r
        left join payment_plans pp
            on r.loan_id = pp.loan_guid
            and pp.payment_plan_start >= r.posted_date
            and datediff(day, r.posted_date, pp.payment_plan_start) <= 55
            and (iff(pp.payment_plan_type='deferred', (pp.payment_plan_start < r.resolved_date or r.resolved_date is null), false)
                or
                iff(pp.payment_plan_type in ('skip_payment','reduction','up_to_50','51_to_80','restructured_plan','frequency_plan'),
                    (pp.payment_plan_start>r.resolved_date and r.resolved_date is not null), false)
                or
                pp.payment_plan_type in ('nominal','covid_recovered'))
    where
        pp.pp_guid is not null
    qualify row_number() over(partition by pp.pp_guid order by r.posted_date desc)=1
)
qualify row_number() over(partition by transaction_id order by posted_date desc)=1;