import requests
from bs4 import BeautifulSoup

url = 'https://en.wikipedia.org/wiki/IBM'

response = requests.get(url)

html_content = response.text

soup = BeautifulSoup(html_content, 'html.parser')


# Find all <a> tags (anchor tags) in the HTML
links = soup.find_all('a')
# Iterate through the list of links and print their text
for link in links:
    print(link.text)