from selenium import webdriver
from bs4 import BeautifulSoup
import pprint as pp
import json
import os

class bbref_scraper(object):
	def __init__(self, chromedriverPath, baseurl = 'https://www.baseball-reference.com'):

		self.CACHE_REQUESTS = ""
		self.base_url = baseurl

		try:
			inputfile = "link_cache.json"
			f = open(inputfile, 'r')
			fstr = f.read()
			self.CACHE_REQUESTS = json.loads(fstr)
			f.close()
			print(inputfile +' loaded.')

		except:
			self.CACHE_REQUESTS = {}
			print('No link cache data found, creating cache...')


		options = webdriver.ChromeOptions()
		options.add_argument('--headless')
		self.driver = webdriver.Chrome(chromedriverPath, chrome_options=options)

	def get_links(self, search_url):
		'''
		For a given baseball ref season page, check to see if it's in the cache already and return the relevant divs if so
		Parameters
		----------
		search_url : str
			baseball ref season page of form: https://www.baseball-reference.com/leagues/MLB/2016-schedule.shtml

		Returns
		----------
		 : str
			A string of the html for all the divs containing links from a baseballref season page
		'''

		if search_url in self.CACHE_REQUESTS.keys():
			print("Sourced from the cache: " + search_url)
			return self.CACHE_REQUESTS[search_url]

		else:
			print("Scraping for data: " + search_url)
			self.driver.get(search_url)
			html = self.driver.page_source
			soup = BeautifulSoup(html, 'lxml')

			game_stats = soup.findAll('div', {'class': 'section_content'})
			self.CACHE_REQUESTS[search_url] = str(game_stats)

			with open("link_cache.json", 'w') as outfile:
				json.dump(self.CACHE_REQUESTS, outfile, indent=4)

			return self.CACHE_REQUESTS[search_url]


	def list_of_links(self, search_url):
		'''
		For a given baseball ref season page, get a list of links for all the games for that season

		Parameters
		----------
		search_url : str
			baseball ref season page of form: https://www.baseball-reference.com/leagues/MLB/2016-schedule.shtml

		Returns
		----------
		list of links : list(str)
			A list of all the links for a given baseball ref season
		'''

		output = self.get_links(search_url)

		link_scrape_raw = BeautifulSoup(output, 'lxml')

		game_list = []
		for game in link_scrape_raw.findAll('em'):
			game_list.append(game.find('a')['href'])

		return(game_list)

	def get_game_html(self, specific_url):
		'''
		For a given baseball ref game page, return the stats for that game.  If it's cached already, return it

		Parameters
		----------
		base_url : str
			default baseball reference page "https://www.baseball-reference.com" as default

		specific_url : str
			part of the link specific to an individual game on baseball reference
			example: boxes/SDN/SDN201803290.shtml

		Returns
		----------
		[player_stats, game_stats] : list
				a list of length 2 with HTML for game and player stats
		'''

		search_url = self.base_url + specific_url

		if search_url in self.CACHE_REQUESTS.keys():
			print("Sourced from the cache")
			return self.CACHE_REQUESTS[search_url]

		else:
			self.driver.get(search_url)
			html = self.driver.page_source
			soup = BeautifulSoup(html, 'lxml')

			player_stats = soup.findAll('table', {'class': 'sortable stats_table now_sortable'})
			game_stats = soup.findAll('div', {'class': 'scorebox_meta'})
			#div = soup.findAll('div', {'class': 'table_outer_container mobile_table'})
			self.CACHE_REQUESTS[search_url] = [str(player_stats), str(game_stats)]

			with open("scrape_cache.json", 'w') as outfile:
				json.dump(self.CACHE_REQUESTS, outfile, indent=4)
				
			
		print('html returned for: ', specific_url)

		return self.CACHE_REQUESTS[search_url]

	def parse_game_html(self, links, max_pages_to_scrape):
		'''
		Takes the output of 'get_game_html' function and parses it to return useful, player level game performance data

		Parameters
		----------
		player_stats : str
			html table from a baseball ref game page

		game stats : str
			html table from a baseball ref game page

		Returns
		----------
		
		'''
		for link in links[0:max_pages_to_scrape]:
			
			output = self.get_game_html(link)

			player_scrape_raw = BeautifulSoup(output[0], 'lxml')
			game_scrape_raw = BeautifulSoup(output[1], 'lxml')

			###############################################################################
			# Extract GAME level context data from site
			###########################################

			print('scraping game content for: ', link)

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

			print('scraping player content for: ', link)

			for tbody_data in player_scrape_raw.findAll('tbody'):
				#print('Accessing player scrape data...')
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
					print(json_bb_data)

				# Hacky way of removing the final comma and closing the JSON list with a ']' to place it in the correct format
				with open('player_data.json', 'rb+') as outfile:
					outfile.seek(-1, os.SEEK_END)
					outfile.truncate()

				with open('player_data.json', 'a') as outfile:
					outfile.write(']')

				print('player content scraped for: ', link)


