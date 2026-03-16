from snowflake.snowpark import Session
import os
from dotenv import load_dotenv

load_dotenv()
credentials = {'account': os.getenv('SF_ACCOUNT'),
               'user': os.getenv('SF_USER'),
               'authenticator': 'externalbrowser',
               'role': 'RISK_ROLE',
               'warehouse': 'DATASCIENCE_WH',
               'database': 'ANALYTICS_PRODUCTION'}

session = Session.builder.configs(credentials).create()

with open('01_all_nsfs.sql', 'r') as file:
    query = file.read()
    session.sql(query).collect()
with open('02_first_nsf_indicator.sql', 'r') as file:
    query = file.read()
    session.sql(query).collect()
with open('03_all_transactions.sql', 'r') as file:
    query = file.read()
    session.sql(query).collect()
with open('04_main_data.sql', 'r') as file:
    query = file.read()
    session.sql(query).collect()

# Save as View for Sigma
query = '''create or replace view sandbox.durdapilletadelaparra.nsf_report
as select * from sandbox.durdapilletadelaparra.nsf_report_dt;'''
session.sql(query).collect()