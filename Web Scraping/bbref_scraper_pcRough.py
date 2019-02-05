from selenium import webdriver
from bs4 import BeautifulSoup
import pprint as pp
import json

global CACHE_REQUESTS

try:
    inputfile = "player_dict.json"
    f = open(inputfile, 'r')
    fstr = f.read()
    CACHE_REQUESTS = json.loads(fstr)
    f.close()

except:
    CACHE_REQUESTS = {}

options = webdriver.ChromeOptions()
options.add_argument('--headless')
driver = webdriver.Chrome('/home/mothership/Desktop/InstaPy/InstaPy/assets/chromedriver', chrome_options=options)

#a single game example, split this up later
base_url = "https://www.baseball-reference.com/boxes/KCA/KCA201604030.shtml"
specific_url = ""

def get_baseball_stats(base_url, specific_url):

    search_url = base_url + specific_url

    if search_url in CACHE_REQUESTS.keys():
        print("Sourced from the cache")
        return CACHE_REQUESTS[search_url]

    else:
        driver.get(search_url)
        html = driver.page_source
        soup = BeautifulSoup(html, 'lxml')

        div = soup.findAll('table', {'class': 'sortable stats_table now_sortable'})
        #div = soup.findAll('div', {'class': 'table_outer_container mobile_table'})
        CACHE_REQUESTS[search_url] = str(div)

        with open("player_dict.json", 'w') as outfile:
            json.dump(CACHE_REQUESTS, outfile, indent=4)

        return CACHE_REQUESTS[search_url]

output = get_baseball_stats(base_url, specific_url)
scrape_content = BeautifulSoup(output, 'lxml')

for tbody_data in scrape_content.findAll('tbody'):
    for row in tbody_data.findAll('tr'):
        for cell in row.findAll('td'):
            print(cell)
            break
