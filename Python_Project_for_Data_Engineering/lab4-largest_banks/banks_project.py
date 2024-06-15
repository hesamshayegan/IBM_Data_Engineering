import requests
import sqlite3
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

from datetime import datetime

url = 'https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']
log_file = 'code_log.txt'
csv_path = 'Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(timestamp + ':' + message + '\n')

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')

    for row in rows:

        col = row.find_all('td')
        if len(col) != 0:
            data_dict = {
                'Name': col[1].a['title'],
                'MC_USD_Billion': float(col[2].contents[0])
            }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
            

    return df
df = extract(url, table_attribs)
print(df)

# def transform(df):
#     ''' This function accesses the CSV file for exchange rate
#     information, and adds three columns to the data frame, each
#     containing the transformed version of Market Cap column to
#     respective currencies'''
#     market_cap_list = df['MC_USD_Billion'].tolist()

#     market_cap_EUR = [np.round(x * 0.93, 2) for x in  market_cap_list]
#     market_cap_GBP = [np.round(x * 0.8, 2) for x in  market_cap_list]
#     market_cap_INR = [np.round(x * 82.95, 2) for x in  market_cap_list]
#     df.insert(2, "MC_EUR_Billion", market_cap_EUR)
#     df.insert(3, "MC_GBP_Billion", market_cap_GBP)
#     df.insert(4, "MC_INR_Billion", market_cap_INR)

#     return df

# def load_to_csv(df, csv_path):
#     ''' This function saves the final data frame as a CSV file in
#     the provided path. Function returns nothing.'''
#     df.to_csv(csv_path)

# def load_to_db(df, sql_connection, table_name):
#     ''' This function saves the final data frame to a database
#     table with the provided name. Function returns nothing.'''
#     df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

# def run_query(query_statement, sql_connection):
#     ''' This function runs the query on the database table and
#     prints the output on the terminal. Function returns nothing. '''
#     print(query_statement)
#     query_output = pd.read_sql(query_statement, sql_connection)
#     print(query_output)


# # TESTING
# # log the installation of the ETL process
# log_progress('Preliminaries complete. Initiating ETL process.')

# df = extract(url, table_attribs)

# log_progress('Data extraction complete. Initiating Transformation process')

# df = transform(df)

# log_progress('Data transformation complete. Initiating loading process')

# load_to_csv(df, csv_path)

# log_progress('Data saved to CSV file')

# sql_connection = sqlite3.connect(db_name)

# log_progress('SQL Connection initiated.')

# load_to_db(df, sql_connection, table_name)

# log_progress('Data loaded to Database as table. Running the query')

# query_statement = f"SELECT * FROM {table_name}"
# run_query(query_statement, sql_connection)
# query_statement = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
# run_query(query_statement, sql_connection)
# query_statement = f"SELECT Name from {table_name} LIMIT 5"
# run_query(query_statement, sql_connection)

# log_progress('Process Complete.')

# sql_connection.close()