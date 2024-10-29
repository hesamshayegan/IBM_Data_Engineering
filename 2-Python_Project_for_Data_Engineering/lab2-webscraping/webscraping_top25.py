import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

# initilalization

url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = 'Movies_top_25.db'
table_name = 'Top-25'
csv_path = 'top_25_films.csv'
df = pd.DataFrame(columns=['Film', 'Year', "Rotten Tomatoes' Top 100"])
count = 0

html_page = requests.get(url).text
# parse the text in the html format
data = BeautifulSoup(html_page, 'html.parser')

tables = data.find_all('tbody')
# first table
rows = tables[0].find_all('tr')

for row in rows:
    if count < 25:
        col = row.find_all('td')
        if len(col) != 0:
            data_dict = {'Film': str(col[1].contents[0]),
                         'Year': int(col[2].contents[0]),
                         "Rotten Tomatoes' Top 100": col[3].contents[0]
                        }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
            count += 1
            if data_dict['Year'] >= 2000:
                print(data_dict['Film'], data_dict['Year'])
    else:
        break

df.to_csv(csv_path)

connection = sqlite3.connect(db_name)
df.to_sql(table_name, connection, if_exists='replace', index=False)
connection.close()
