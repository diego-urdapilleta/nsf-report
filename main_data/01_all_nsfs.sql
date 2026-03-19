create or replace view sandbox.durdapilletadelaparra.all_nsfs_1 as
with ario_nsfs as (
    select
        money_transfer_id,
        iff(state='RESOLVED', TRUE, FALSE) as platform_resolved,
        description,
        amount,
        created_at,
        updated_at
    from
        analytics_production.dbt_ario.fct_payment_charge
    where
        created_at > '2023-01-01'
),
lt as (
    select
        money_transfer_id,
        coalesce(loan_guid, correlation_id) as loan_id,
        tk_transaction_guid,
        correlation_guid,
        date_trunc(day,posted_at)::date as posted_date,
        debit_amount as amount
    from
        analytics_production.dbt_ario_turnkey.fct_ledger_transaction
    where
        transaction_type in ('Reversal')
        and request_type = 'normal_payments'
        and posted_at >= '2023-01-01'
        and debit_amount > 0
),
amounts as (
    select
        money_transfer_id,
        sum(amount) as amount
    from
        lt
    group by
        money_transfer_id
),
ario_full as (
    select
        --a.money_transfer_id,
        lt.loan_id,
        lt.correlation_guid as ario_transaction_id,
        tk_transaction_guid as tk_transaction_id,
        lt.posted_date,
        amounts.amount,
        a.platform_resolved,
        a.description,
        iff(a.platform_resolved, a.updated_at, null) as resolved_date,
        'Ario' as nsf_source
    from
        ario_nsfs a
        left join amounts
            on a.money_transfer_id = amounts.money_transfer_id
        left join lt
            on a.money_transfer_id = lt.money_transfer_id
    where
        lt.amount is not null
    qualify row_number() over (partition by ario_transaction_id order by posted_date) = 1
),
tk_full as (
    select
        f.driven_loan_guid as loan_id,
        lt.correlation_guid as ario_transaction_id,
        f.transaction_guid as tk_transaction_id,
        f.effective_date as posted_date,
        f.effective_amount as amount,
        f.is_nsf_resolved as platform_resolved,
        f.description,
        iff(f.is_nsf_resolved, coalesce(t.resolved_date, t.updated_date), null) as resolved_date,
        'Turnkey' as nsf_source
    from 
        analytics_production.dbt_turnkey.fct_payment_charge f
        left join analytics_production.dbt_turnkey.loan_transaction_detail t
            on t.nsf_transaction_guid = f.transaction_guid
        left join analytics_production.dbt_ario_turnkey.fct_ledger_transaction lt
            on f.transaction_guid = lt.tk_transaction_guid
    where
        f.created_at > '2023-01-01'
),
final_nsfs as (
    select * from ario_full
    union all
    select * from tk_full
)
select
    *
from
    final_nsfs;