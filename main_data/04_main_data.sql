create or replace dynamic table sandbox.durdapilletadelaparra.nsf_report_dt
target_lag = '1 day'
warehouse = DATASCIENCE_WH
as
with params as (
    select 14 as resolution_threshold
),

all_transactions as (
    select
        *,
        coalesce(tk_transaction_id, ario_transaction_id) as transaction_id
    from sandbox.durdapilletadelaparra.all_transactions order by tk_transaction_id nulls last
),

sales as (
    select
        merchant_guid,
        created_at as adjudication_date,
        max(offer_context['merchant.average_monthly_sales']::int) as avg_monthly_sales
    from
        analytics_production.dbt_ario.dim_adjudication
    where
        target_id = 5
    group by 1,2
),

sales_deduped as (
    select
        lt.transaction_id,
        s.avg_monthly_sales
    from
        all_transactions lt
        left join analytics_production.dbt_ario_turnkey.dim_loan l
            on lt.loan_id = l.driven_ubl_guid
        left join sales s
            on l.merchant_guid = s.merchant_guid
            and s.adjudication_date <= l.loan_deposited_at
    qualify row_number() over (
        partition by lt.transaction_id
        order by s.adjudication_date desc nulls last
    ) = 1
),

loan_snapshot_attributes as (
    select
        lt.transaction_id,
        coalesce(bb.overridden_dpd, bb.final_delinquency) as dpd,
        bb.account_status,
        bb.total_nsfs,
        bb.provision_rate,
        bb.arrears_balance,
        bb.outstanding_balance,
        bb.principal_balance as outstanding_principal,
        1 - (bb.principal_balance / bb.new_cash) as percentage_paid,
        bb.beacon_score as fico,
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
            as age_bucket
    from
        all_transactions lt
        left join analytics_production.dbt_ario_turnkey.borrowing_base_snapshot bb
            on lt.loan_id = bb.driven_ubl_guid
            and lt.posted_date >= bb.dbt_valid_from
            and (lt.posted_date <= bb.dbt_valid_to or bb.dbt_valid_to is null)
    qualify row_number() over (partition by lt.transaction_id order by bb.dbt_updated_at desc) = 1
),

merchant_attributes as (
    select
        lt.transaction_id,
        l.merchant_guid,
        l.loan_deposited_at,
        l.repayment_schedule,
        l.loan_term::int as loan_term_length,
        --bb.beacon_score as fico,
        l.principal_amount as loan_amount,
        ref.risk_group_combined::varchar as risk_group_combined,
        s.avg_monthly_sales,
        m.macro_industry as industry,
        m.addresses:legal_business_address[0].province::string AS province,
        case
            when l.repayment_schedule = 'daily' then 365/12
            when l.repayment_schedule = 'weekly' then 52/12
            when l.repayment_schedule = 'bi-weekly' then 26/12
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
            as origination_amount_bucket
    from
        all_transactions lt
        left join analytics_production.dbt_reporting.dim_referential ref
            on lt.loan_id = ref.loan_guid
        left join analytics_production.dbt_ario_turnkey.dim_loan l
            on lt.loan_id = l.driven_ubl_guid
        left join analytics_production.dbt_ario.dim_merchant m
            on l.merchant_guid = m.guid
        left join sales_deduped s
            using(transaction_id)
),

portfolio_attributes as (
    select
        lt.transaction_id,
        ps.total_outstanding_balance as portfolio_outstanding
    from
        all_transactions lt
        left join sandbox.durdapilletadelaparra.portfolio_snapshot ps
            on lt.posted_date <= ps.as_of_date
            and lt.posted_date > dateadd(week, -1, ps.as_of_date)
    qualify row_number() over (partition by lt.transaction_id order by ps.as_of_date desc) = 1
)

select
    lt.loan_id,
    lt.transaction_id,
    lt.ario_transaction_id,
    lt.tk_transaction_id,
    lt.amount,
    lt.posted_date,
    date_part(week_iso, lt.posted_date) as posted_week,
    date_part(month, lt.posted_date) as posted_month,
    date_part(yearofweekiso, lt.posted_date)::varchar as posted_year,
    lt.pymt_type,
    lt.is_first_nsf as first_nsf,
    lt.platform_resolved,
    lt.resolved_date,
    datediff(day, lt.posted_date, lt.resolved_date) as time_to_cure,
    lt.platform_resolved and time_to_cure <= (select resolution_threshold from params) as resolved, -- RESOLUTION LOGIC
    lt.pymt_type='NSF' as is_nsf,
    lt.nsf_source,

    m.* exclude transaction_id,
    lsa.* exclude transaction_id,
    p.* exclude transaction_id

from
    all_transactions lt
    left join loan_snapshot_attributes lsa
        using(transaction_id)
    left join merchant_attributes m
        using(transaction_id)
    left join portfolio_attributes p
        using(transaction_id)
;