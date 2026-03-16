select
    posted_year,
    posted_week,
    sum(case when pymt_type='Payment' then amount else 0 end) as payment_amount,
    sum(case when {{#raw delq_selector1}} then amount else 0 end) as nsf_amount,
    payment_amount + nsf_amount as total_amount,
    nsf_amount / total_amount as nsf_amount_rate,
    min(posted_date) as week_start
from
    sandbox.durdapilletadelaparra.nsf_report
group by
    posted_year,
    posted_week;
