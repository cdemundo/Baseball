from selenium import webdriver
from bs4 import BeautifulSoup
import pprint as pp
import json
import os

from link_scraper import list_of_links

global CACHE_REQUESTS

try:
    inputfile = "scrape_cache.json"
    f = open(inputfile, 'r')
    fstr = f.read()
    CACHE_REQUESTS = json.loads(fstr)
    f.close()
    print(inputfile + 'Cache loaded.')

except:
    CACHE_REQUESTS = {}
    print('No baseball data cache found, creating cache...')

options = webdriver.ChromeOptions()
options.add_argument('--headless')
driver = webdriver.Chrome('/home/mothership/Desktop/InstaPy/InstaPy/assets/chromedriver', chrome_options=options)

maximum_pages_to_scrape = 3

def get_baseball_stats(base_url, specific_url):

    search_url = base_url + specific_url

    if search_url in CACHE_REQUESTS.keys():
        print("Sourced from the cache")
        return CACHE_REQUESTS[search_url]

    else:
        driver.get(search_url)
        html = driver.page_source
        soup = BeautifulSoup(html, 'lxml')

        player_stats = soup.findAll('table', {'class': 'sortable stats_table now_sortable'})
        game_stats = soup.findAll('div', {'class': 'scorebox_meta'})
        #div = soup.findAll('div', {'class': 'table_outer_container mobile_table'})
        CACHE_REQUESTS[search_url] = [str(player_stats), str(game_stats)]

        with open("scrape_cache.json", 'w') as outfile:
            json.dump(CACHE_REQUESTS, outfile, indent=4)

        return CACHE_REQUESTS[search_url]


# base_url = "https://www.baseball-reference.com/boxes/KCA/KCA201604030.shtml"
base_url = "https://www.baseball-reference.com"

links = list_of_links("https://www.baseball-reference.com/leagues/MLB/2016-schedule.shtml")

# If our cache JSON object does not exist, create it and start it off with a '['
# This is necessary to allow our scraper to access streaming data
if os.path.isfile('./player_data.json') == False:
    with open('player_data.json', 'w') as outfile:
        outfile.write('[')

#########################################################
## Scraping Begins here

x = 0
for link in links:

    if x == maximum_pages_to_scrape: break
    x+=1

    output = get_baseball_stats(base_url, link)

    player_scrape_raw = BeautifulSoup(output[0], 'lxml')
    game_scrape_raw = BeautifulSoup(output[1], 'lxml')

    ###############################################################################
    # Extract GAME level context data from site
    ###########################################

    y = 0
    for div in game_scrape_raw.findAll('div'):

        if y == 1:
            try: date = str(div.contents[0])
            except: date = 'unknown_date'
        if y == 2:
            try: start_time = str(div.contents[0])
            except: start_time = 'unknown_start_time'
        if y == 4:
            try: location = str(div.contents[1])
            except: location = "unknown_location"
        if y == 5:
            try: duration = str(div.contents[1])
            except: duration = "unknown_duration"
        if y == 6:
            try: game_situation = str(div.contents[0])
            except: game_situation = "unknown_game_situation"
        y+=1

    ###############################################################################
    # Extract player level data from site
    #####################################

    for tbody_data in player_scrape_raw.findAll('tbody'):
        print('Accessing player scrape data...')
        for row in tbody_data.findAll('tr'):
            # if row.has_attr("class"): print("CONTINUE")

            # Necessary try/except to skip spacers in webpages (using continue clause)
            try:
                if row.has_attr("class"):
                    #print('HTML Spacer Element Bypassed...')
                    continue
                else:
                    pass
            except:
                pass

            # Each player level stat is initialized with game environmental values
            json_bb_data = {
                'date' : date,
                'start_time' : start_time,
                'location' : location,
                'duration' : duration,
                'game_situation' : game_situation
            }
            player_game_data = row.find('th')

            # Player_ID
            # grandcu01
            json_bb_data['player_id'] = str(player_game_data['data-append-csv'])

            # Player Name
            json_bb_data['player_name'] = str(player_game_data.find('a').contents[0])

            # Player Position
            # try/except is necessary to navigate a nonstandard website structure
            # P2 here means pitcher 2, indicating the pitcher was on the roll but
            # may have not pitched in the game
            try:
                json_bb_data['player_role'] = str(player_game_data.contents[1])
            except:
                json_bb_data['player_role'] = 'P2'

            # Finds all the data-stat objects (game level player stats) and adds them to our dictionary
            for item in row.findAll('td', attrs={'data-stat' : True}):
                json_bb_data[item['data-stat']] = str(item.get_text())

            # Appends each line of player level data scraped in the loop to an
            # individual row in our database.  Adds a comma to preserve correct format
            with open('player_data.json', 'a') as outfile:
                json.dump(json_bb_data, outfile)
                outfile.write(',')

# Hacky way of removing the final comma and closing the JSON list with a ']' to place it in the correct format
with open('player_data.json', 'rb+') as outfile:
    outfile.seek(-1, os.SEEK_END)
    outfile.truncate()

with open('player_data.json', 'a') as outfile:
    outfile.write(']')
