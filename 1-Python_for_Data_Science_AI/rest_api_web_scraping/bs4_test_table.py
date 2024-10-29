import requests
from bs4 import BeautifulSoup

url = 'https://en.wikipedia.org/wiki/IBM'

response = requests.get(url)
html_content = response.text

soup = BeautifulSoup(html_content, 'html5lib')


table = soup.find('table', class_='wikitable')

print(table)

# Check if table is found
if table:
    print(type(table))

    # Find all rows in the table
    rows = table.find_all('tr')

    for i, row in enumerate(rows):
        print(f'Row {i}')
        cells = row.find_all(['td', 'th'])

        for j, cell in enumerate(cells):
            print(f'Column {j} cell: {cell.get_text(strip=True)}')
else:
    print("Table not found")
