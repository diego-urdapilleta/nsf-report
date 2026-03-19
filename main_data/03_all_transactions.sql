create or replace view sandbox.durdapilletadelaparra.all_transactions as
select
    *,
    'NSF' as pymt_type 
from
    sandbox.durdapilletadelaparra.all_nsfs
where
    posted_date >= '2024-01-01'
union all
select
    loan_id,
    ario_transaction_id,
    tk_transaction_id,
    posted_date,
    amount,
    null as resolved,
    null as description,
    null as resolved_date,
    null as is_first_nsf,
    null as nsf_source,
    'Payment' as pymt_type
from (
    select
        coalesce(loan_guid, correlation_id) as loan_id,
        max(correlation_guid) as ario_transaction_id,
        max(tk_transaction_guid) as tk_transaction_id,
        sum(debit_amount) as amount,
        date_trunc(day,posted_at)::date as posted_date
    from
        analytics_production.dbt_ario_turnkey.fct_ledger_transaction
    where
        transaction_type in ('Principal Paid', 'Interest Paid')
        and request_type = 'normal_payments'
        and posted_at >= '2024-01-01'
        and debit_amount > 0
    group by 
        loan_id,
        posted_date
);