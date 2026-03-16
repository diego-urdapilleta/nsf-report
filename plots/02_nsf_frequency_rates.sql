select
    posted_year,
    posted_week,
    count_if(pymt_type='Payment') as payment_count,
    count_if({{#raw delq_selector1}}) as nsf_count,
    payment_count + nsf_count as total_count,
    nsf_count / total_count as nsf_frequency_rate,
    min(posted_date) as week_start
from
    sandbox.durdapilletadelaparra.nsf_report
group by
    posted_year,
    posted_week;