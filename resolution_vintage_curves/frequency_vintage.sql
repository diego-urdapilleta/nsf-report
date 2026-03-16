with days as (
  select
    seq4() as days_since_nsf
  from table(generator(rowcount => {{#raw vin_window}}+1))
  where days_since_nsf > 0
),
eligible_transactions as (
  select
    posted_year,
    amount,
    resolved,
    time_to_cure
  from sandbox.durdapilletadelaparra.nsf_report
  where {{#raw delq_selec_vin1}}
    and posted_date < dateadd(days, -{{#raw vin_window}}, current_date())
    and posted_month in {{month_selec1}}
),
denoms as (
  select
    posted_year,
    sum(amount) as year_total_amount,
    count(*) as year_total_count
  from eligible_transactions
  group by posted_year
)
select
  d.days_since_nsf,
  r.posted_year,
  sum(case when r.resolved = true then r.amount else 0 end) / min(den.year_total_amount) as amount_cure_rate,
  count_if(r.resolved = true) / min(den.year_total_count) as count_cure_rate
from days d
left join eligible_transactions r
  on r.time_to_cure <= d.days_since_nsf
left join denoms den
  on r.posted_year = den.posted_year
group by days_since_nsf, r.posted_year
order by days_since_nsf;