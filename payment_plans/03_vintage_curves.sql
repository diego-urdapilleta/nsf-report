with days as (
    select
        seq4() as days_since_nsf
    from
        table(generator(rowcount => 55))
),
denoms as (
    select
        posted_year,
        sum(amount) as year_total_amount,
        count(transaction_id) as year_total_count
    from
        sandbox.durdapilletadelaparra.nsf_report
    where
        first_nsf
        and {{#raw resolved_selec}}
        and posted_month in {{month_selec2}}
    group by
        posted_year
)
select
    d.days_since_nsf,
    pp.posted_year,
    pp.plan_category,
    sum(case when pp.plan_start is not null then pp.amount else 0 end) / min(den.year_total_amount) as amount_pp_rate,
    count_if(pp.plan_start is not null) / min(den.year_total_count) as count_pp_rate
from 
    days d
    left join (select * from sandbox.durdapilletadelaparra.nsf_payment_plans where {{#raw resolved_selec}} and month(POSTED_DATE) in {{month_selec2}}) pp
        on pp.time_to_payment_plan <= d.days_since_nsf
    left join denoms den
        on pp.posted_year = den.posted_year
group by 
    plan_category,
    days_since_nsf,
    pp.posted_year
order by
    days_since_nsf;