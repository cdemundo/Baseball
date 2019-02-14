import scrapy
from scrapy_splash import SplashRequest
from datetime import datetime
from scrapy.crawler import CrawlerProcess
import sys
sys.path.append(r"C:\Users\Chris\Google Drive\Programming\Baseball")
import database_utility
import json
import pandas as pd


class BBRefSpider(scrapy.Spider):
	name = "bbref"    

	#start_urls = ['https://www.baseball-reference.com/leagues/MLB/2017-schedule.shtml',
	#'https://www.baseball-reference.com/leagues/MLB/2015-schedule.shtml',
	#'https://www.baseball-reference.com/leagues/MLB/2016-schedule.shtml',
	#'https://www.baseball-reference.com/leagues/MLB/2018-schedule.shtml']

	start_urls = ['https://www.baseball-reference.com/leagues/MLB/2016-schedule.shtml']

	def start_requests(self):

		#load a list of game links that we have already tried
		try:
			self.cached_links = pd.read_csv('scrape_cache.csv', header=None)
			self.cached_links.columns = ['links']
		except:
			self.cached_links = pd.DataFrame()
			print('No baseball data cache found')

		#we've also already scraped 3000 games without recording the link.  i don't want to rescrape those so we'll check into the database
		dh = database_utility.DatabaseHelper(sql_filepath = 'baseball_data.sqlite', key_joiner_filepath = r"C:\Users\Chris\Google Drive\Programming\Baseball\baseball_key_joiner.csv")

		filepath = r'C:\\Users\\Chris\\Google Drive\\Programming\\Baseball\\Web Scraping\\bbref_scraper\\bbref_scraper\\bbref.jl'
		batting_df, pitching_df = dh.pull_raw_bbref_data(filepath)
		print("Existing game db loaded!")

		#panda series of games we already scraped
		self.games_already_scraped = pitching_df['game_date'].astype(str) + pitching_df['home_team'] + pitching_df['away_team']

		#scrape the individual season pages
		for url in self.start_urls:
			yield SplashRequest(url, self.parse_season_page, args={'wait': 20})

	def parse_season_page(self, response):
		print("PARSING THE SEASON PAGE")
		base_url = "https://www.baseball-reference.com"
		ems = response.css('em')

		links = []
		for em in ems:
			links.append(em.css('a::attr(href)').extract_first())

		for link in links:
			#print("LINK IS: ", link)
			if link in self.cached_links.values:
				#we've already scraped this link
				continue
			else:
				#parse the link
				#we set max timeout high because of Splash getting overloaded with requests in the queue https://splash.readthedocs.io/en/stable/faq.html#i-m-getting-lots-of-504-timeout-errors-please-help
				yield SplashRequest(base_url+link, self.parse_game_page, args={'wait': 20, 'timeout' : 90}, meta={'link_to_cache' : link})



	def parse_game_page(self, response):

		#get team names
		names = response.css('div.section_heading').css('h2::text').extract()
		away_team = names[0]
		home_team = names[1]

		#hunt through layered divs to get to individual game meta stats
		game_stats = response.css('div.scorebox_meta').css('div').css('div::text').extract()

		try:
			game_date = str(game_stats[1])
		except:
			game_date = 'unknown_date'

		#need to format gamedate to check against db
		try:
			temp_date = game_date.split(',',1)[1]
			temp_date = pd.to_datetime(temp_date).strftime('%Y-%m-%d')
		except IndexError:
			temp_date = 'unknown_date'

		#if this game is already in the database, we can stop scraping and return
		temp_id = temp_date + home_team + away_team

		#games already scraped is a pandas series, so lets look in the values
		if temp_id in self.games_already_scraped.values:
			print("already in the db!!")
			#store the link in the cache
			with open('scrape_cache.csv','a') as fd:
				fd.write(response.meta['link_to_cache']+'\n')
			return ""
		else:
			print("ok lets do this!")
			try:
				start_time = str(game_stats[3])
			except:
				start_time = 'unknown_start_time'

			try:
				attendance = str(game_stats[5]).split(': ')[1]
			except:
				attendance = 'unknown_attendance'

			try:
				location = str(game_stats[7]).split(': ')[1]
			except:
				location = 'unknown_location'

			try:
				game_situation = str(game_stats[11])
			except:
				game_situation = 'unknown_game_situation'

			print("GOT GAME STATS")

			#parse individual player performance

			player_stats = response.css('div.table_outer_container')

			#this returns 6 divs.  the first is away batting, second is home batting. third is away pitching, fourth is home pitching

			#batting info
			away_batting_rows = player_stats[0].css('tbody').css('tr')
			home_batting_rows = player_stats[1].css('tbody').css('tr')

			away_pitching_rows = player_stats[2].css('tbody').css('tr')
			home_pitching_rows = player_stats[3].css('tbody').css('tr')

			away_batter_stats = self.parse_stats_rows(away_batting_rows)
			home_batter_stats = self.parse_stats_rows(home_batting_rows)

			away_pitching_stats = self.parse_stats_rows(away_pitching_rows)
			home_pitching_stats = self.parse_stats_rows(home_pitching_rows)

			yield{
				'game_date' : game_date,
				'home_team' : home_team,
				'away_team' : away_team,
				'start_time' : start_time,
				'location' : location, 
				'attendance' : attendance,
				'game_situation' : game_situation,
				'away_batter_stats' : away_batter_stats, 
				'home_batter_stats' : home_batter_stats,
				'away_pitching_stats' : away_pitching_stats,
				'home_pitching_stats' : home_pitching_stats
			}

			#store the link in the cache
			with open('scrape_cache.csv','a') as fd:
				fd.write(response.meta['link_to_cache']+'\n')

	def parse_stats_rows(self, rows):
		'''
		Takes in the tr associated with a batter stats table.  Parses it to return a dictionary of
		stats for individual batters in the table.

		Parameters
		--------------
		rows : Scrapy selector object

		Returns
		---------------
		batter_stats : dict
			Dictionary where key is player_id, value is dict {stat_name:value}
		'''

		batter_stats = {}
		for row in rows:
			#check to make sure tds are associated with the row
			tds = row.css('td')

			if len(tds) == 0:
				continue
			else:
				#get the player id
				player_id = row.css('th::attr(data-append-csv)').extract_first()
				player_position = row.css('th::text').extract_first()

				if player_id in batter_stats:
					#row duplication, this player info is already stored, go to the next row
					continue
				else:
					stats = {}
					for td in tds:
						#data-stat is the name of the stat
						#text is the value
						stat_name = td.css('td::attr(data-stat)').extract_first()
						value = td.css('td::text').extract()
						if len(value) > 0:
							value = value[0]
						else:
							value = 0
						stats[stat_name] = value

					stats['position'] = player_position 

					batter_stats[player_id] = stats

		return batter_stats

	