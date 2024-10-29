import os
import psycopg2
import pandas as pd
from tabulate import tabulate

import mytests
# import the data quality checks
from dataqualitychecks import check_for_nulls
from dataqualitychecks import check_for_min_max
from dataqualitychecks import check_for_valid_values
from dataqualitychecks import check_for_duplicates
from dataqualitychecks import run_data_quality_check

# connect to database
pgpassword = "BdimKpnGVfkF5IlrX7MQnzSj"
conn = psycopg2.connect(
		user = "postgres",
	    password = pgpassword,
	    host = "postgres",
	    port = "5432",
	    database = "billingDW")

print("Connected to data warehouse")

#Start of data quality checks
results = []
tests = {key:value for key,value in mytests.__dict__.items() if key.startswith('test')}
for testname,test in tests.items():
    test['conn'] = conn
    results.append(run_data_quality_check(**test))

#print(results)
df=pd.DataFrame(results)
df.index+=1
df.columns = ['Test Name', 'Table','Column','Test Passed']
print(tabulate(df,headers='keys',tablefmt='psql'))
#End of data quality checks
conn.close()
print("Disconnected from data warehouse")
