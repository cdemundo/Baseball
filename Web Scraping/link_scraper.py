from selenium import webdriver
from bs4 import BeautifulSoup
import pprint as pp
import json

global CACHE_REQUESTS

try:
    inputfile = "link_cache.json"
    f = open(inputfile, 'r')
    fstr = f.read()
    CACHE_REQUESTS = json.loads(fstr)
    f.close()
    print(inputfile +' loaded.')

except:
    CACHE_REQUESTS = {}
    print('No link cache data found, creating cache...')

options = webdriver.ChromeOptions()
options.add_argument('--headless')
driver = webdriver.Chrome('/home/mothership/Desktop/InstaPy/InstaPy/assets/chromedriver', chrome_options=options)

def get_links(search_url):

    if search_url in CACHE_REQUESTS.keys():
        print("Sourced from the cache: " + search_url)
        return CACHE_REQUESTS[search_url]

    else:
        print("Scraping for data: " + search_url)
        driver.get(search_url)
        html = driver.page_source
        soup = BeautifulSoup(html, 'lxml')

        game_stats = soup.findAll('div', {'class': 'section_content'})
        CACHE_REQUESTS[search_url] = str(game_stats)

        with open("link_cache.json", 'w') as outfile:
            json.dump(CACHE_REQUESTS, outfile, indent=4)

        return CACHE_REQUESTS[search_url]

def list_of_links(search_url):

    output = get_links(search_url)

    link_scrape_raw = BeautifulSoup(output, 'lxml')

    game_list = []
    for game in link_scrape_raw.findAll('em'):
        game_list.append(game.find('a')['href'])

    return(game_list)

#scrape major scheduling sites
search_url = "https://www.baseball-reference.com/leagues/MLB/2016-schedule.shtml"
search_url2 = "https://www.baseball-reference.com/leagues/MLB/2017-schedule.shtml"
search_url3 = "https://www.baseball-reference.com/leagues/MLB/2018-schedule.shtml"

#season_2016 = list_of_links(search_url)
#season_2017 = list_of_links(search_url2)
#season_2018 = list_of_links(search_url3)

#print(len(season_2018))
