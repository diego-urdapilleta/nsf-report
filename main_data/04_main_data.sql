create or replace dynamic table sandbox.durdapilletadelaparra.nsf_report_dt
target_lag = '1 day'
warehouse = DATASCIENCE_WH
as

with sales as (
    select
        merchant_guid,
        date_trunc('month', created_at)::date as adjudication_month,
        max(offer_context['merchant.average_monthly_sales']::int) as avg_monthly_sales
    from analytics_production.dbt_ario.dim_adjudication
    where target_id = 5
    group by 1,2
)

select

-- Transaction attributes
    lt.transaction_id,
    lt.amount,
    lt.posted_date,
    date_part(week_iso, lt.posted_date) as posted_week,
    date_part(month, lt.posted_date) as posted_month,
    date_part(yearofweekiso, lt.posted_date)::varchar as posted_year,
    lt.pymt_type,
    lt.is_first_nsf as first_nsf,
    lt.resolved as resolved,
    lt.resolved_date as resolved_date,
    datediff(day, lt.posted_date, lt.resolved_date) as time_to_cure,
    lt.pymt_type='NSF' as is_nsf,
    lt.nsf_source,

-- Origination attributes
    bb.merchant_guid,
    lt.loan_id,
    bb.loan_deposited_at,
    bb.repayment_schedule,
    bb.loan_term::int as loan_term_length,
    bb.beacon_score as fico,
    l.principal_amount as loan_amount,
    ref.risk_group_combined::varchar as risk_group_combined,
    s.avg_monthly_sales,
    m.macro_industry as industry,
    m.addresses:legal_business_address[0].province::string AS province,
    case
        when bb.repayment_schedule = 'daily' then 365/12
        when bb.repayment_schedule = 'weekly' then 52/12
        when bb.repayment_schedule = 'bi-weekly' then 26/12
        else null 
        end
        as term_quotient,
    loan_term_length / term_quotient as term_months,
    case
        when term_months < 9  then 'less than 9 months'
        when term_months < 12  then '9-12 months'
        when term_months < 18  then '12-18 months'
        when term_months >= 18  then '18 months or above'
        else null 
        end
        as term_bucket,
    case 
        when fico < 600 then 'below 600'
        when fico < 650 then '600-649'
        when fico < 700 then '650-699'
        when fico < 750 then '700-749'
        when fico >= 750 then '750 or above'
        else null
        end
        as fico_bucket,
    case
        when ref.existing_customer=1 then 'Existing'
        else 'New' end as client_type,
    case
        when s.avg_monthly_sales < 50000 then 'below $50k'
        when s.avg_monthly_sales < 100000 then '$50K-$100k'
        when s.avg_monthly_sales < 200000 then '$100k-$200k'
        when s.avg_monthly_sales <1000000 then '$200k-$1M'
        when s.avg_monthly_sales >= 1000000 then '$1M or above'
        else null
        end
        as sales_bucket,
    case 
        when loan_amount < 25000 then 'below $25k'
        when loan_amount < 50000 then '$25K-$50k'
        when loan_amount < 100000 then '$50K-$100k'
        when loan_amount >= 100000 then '$100k or above'
        else null
        end
        as origination_amount_bucket,
        
-- Amortization attributes
    coalesce(bb.overridden_dpd, bb.final_delinquency) as dpd,
    bb.account_status,
    bb.total_nsfs,
    bb.provision_rate,
    bb.arrears_balance,
    bb.outstanding_balance,
    bb.principal_balance as outstanding_principal,
    1 - (bb.principal_balance / loan_amount) as percentage_paid,
    case 
        when bb.outstanding_balance < 10000 then 'below $10k'
        when bb.outstanding_balance < 20000 then '$10K-$20k'
        when bb.outstanding_balance < 50000 then '$20K-$50k'
        when bb.outstanding_balance >= 50000 then '$50k or above'
        else null
        end
        as balance_bucket,
    datediff(day, bb.loan_deposited_at, lt.posted_date) as loan_age_days,
    case 
        when loan_age_days < 90 then 'less than 3 months'
        when loan_age_days < 180 then '3-6 months'
        when loan_age_days < 365 then '6-12 months'
        when loan_age_days >= 365 then '12 months or above'
        else null
        end
        as age_bucket,
    ps.total_outstanding_balance as portfolio_outstanding

from
    sandbox.durdapilletadelaparra.all_transactions lt
    left join analytics_production.dbt_ario_turnkey.borrowing_base_snapshot bb
        on lt.loan_id = bb.driven_ubl_guid
        and lt.posted_date >= bb.dbt_valid_from
        and lt.posted_date <= bb.dbt_valid_to
    left join analytics_production.dbt_reporting.dim_referential ref
        on lt.loan_id = ref.loan_guid
    left join analytics_production.dbt_ario.dim_merchant m
        on bb.merchant_guid = m.guid
    left join analytics_production.dbt_ario_turnkey.dim_loan l
        on lt.loan_id = l.driven_ubl_guid
    left join sandbox.durdapilletadelaparra.portfolio_snapshot ps
        on lt.posted_date <= ps.as_of_date
        and lt.posted_date > dateadd(week, -1, ps.as_of_date)
    left join sales s
        on bb.merchant_guid = s.merchant_guid
        and abs(datediff('month', date_trunc('month', bb.loan_deposited_at), s.adjudication_month)) < 2
        and s.avg_monthly_sales is not null
        and s.avg_monthly_sales >0 
qualify row_number() over (partition by lt.transaction_id order by lt.posted_date) = 1;