create or replace view sandbox.durdapilletadelaparra.all_nsfs as
select * exclude (loan_has_tk)
from (
    select
        *,
        max(case when tk_transaction_id is not null then 1 else 0 end) over (partition by loan_id) as loan_has_tk
    from (
        select
            loan_id,
            ario_transaction_id,
            tk_transaction_id,
            posted_date,
            amount,
            platform_resolved,
            description,
            resolved_date,
            is_first_nsf,
            nsf_source
        from (
            select
                a.*,
                lag(resolved_date) over (
                    partition by a.loan_id
                    order by a.posted_date) as prev_resolved_date,
                coalesce(bb_ss.overridden_dpd, bb_ss.final_delinquency) as dpd_at_transaction,
                case
                    when a.posted_date = min(a.posted_date) over (partition by a.loan_id) then true
                    when prev_resolved_date is null then false
                    when prev_resolved_date >= a.posted_date then false -- NSFs are always resolved chronologically
                    when bb_ss.state in ('writtenoff', 'readyforsettlement', 'readyforwriteoff', 'settled','suspended') then false
                    when dpd_at_transaction > 2 then false
                    else true end as is_first_nsf
            from
                sandbox.durdapilletadelaparra.all_nsfs_1 a
                left join analytics_production.dbt_ario_turnkey.borrowing_base_snapshot bb_ss
                    on a.loan_id = bb_ss.driven_ubl_guid
                    and a.posted_date >= bb_ss.dbt_valid_from
                    and a.posted_date <= bb_ss.dbt_valid_to
        )
    )
)
where loan_has_tk = 0 or tk_transaction_id is not null;