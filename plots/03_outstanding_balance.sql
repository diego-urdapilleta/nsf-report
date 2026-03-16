select
    posted_week,
    posted_year,
    loan_id,
    min(outstanding_balance) as nsf_amount,
    max(portfolio_outstanding) as week_portfolio_outstanding,
    nsf_amount / week_portfolio_outstanding as nsf_percentage
from
    sandbox.durdapilletadelaparra.nsf_report
where
    {{#raw delq_selector1}}
group by
    posted_year,
    posted_week,
    loan_id;