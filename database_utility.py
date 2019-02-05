import sqlite3
import pandas as pd
import datetime

class DatabaseHelper(object):
	def __init__(self, filepath):
		self.filepath = filepath

	def pull_raw_statcast_data(self, start_date = "", end_date = "", table_name = 'pitch_data'):
		"""
		Utility function to get raw pitch data from the sqlite file, using the filepath passed 
		on instantiation.  

		Can specify dates to pull, get all data in the database if not.  Converts to a pandas dataframe, 
		does some cleaning and returns

		Parameters
		----------
		start_date : str
			Start date in format 'YYYY-MM-DD'

		end_date : str
			End date in format 'YYYY-MM-DD'
		
		Returns
		----------
		dataframe
			A pandas dataframe of pitch data for the specified dates
		"""

		#check for the right date formats if provided
		if len(start_date) > 0:
			try:
				datetime.datetime.strptime(start_date, '%Y-%m-%d')
			except ValueError:
				print("Start date should be formatted YYYY-MM-DD")
				return
		else:
			#the first date in the database as of 1/19/2019
			start_date = '2015-04-05'

		if len(end_date) > 0:
			try:
				datetime.datetime.strptime(end_date	, '%Y-%m-%d')
			except ValueError:
				print("End date should be formatted YYYY-MM-DD")
				return
		else:
			#the last date in the database as of 1/19/2019
			end_date = '2018-10-28'

		#open a connection to the sqlite file
		conn = sqlite3.connect(self.filepath)
		c = conn.cursor()

		column = 'game_date'

		#get all data from between the given start and end date
		c.execute('SELECT * FROM {tn} WHERE {cn} >= "{sd}" AND {cn} <= "{ed}"'.\
			format(tn=table_name, cn=column, sd=start_date, ed=end_date))

		#we want to get the column names to map them to the pandas df
		names = [description[0] for description in c.description]

		raw_data = pd.DataFrame(c.fetchall(), columns=names)

		raw_data.drop(columns=['index', '<html>'], inplace=True)

		return raw_data

	def pull_raw_bbref_data(self, filepath):
		"""
		Utility function to get raw game data from bbref file generated from scraping the baseball reference website.

		Since this isn't stored in a database, we will just pull, clean, and return the bbref data - filtering can happen in pandas

		Parameters
		----------
		filepath : str
			location of the bbref.jl file
		
		Returns
		----------
		dataframe
			A pandas dataframe of pitch data for the specified dates
		"""
		raw_df = pd.read_json(filepath, lines=True)

		
		#first, we want to get information about what the columns will be from the raw dataframe
		batter_stats = raw_df['away_batter_stats'][0]
		pitcher_stats = raw_df['away_pitching_stats'][0]

		#these are the 'game information' columns. the rest are stat specific
		meta_info_cols = ['away_team', 'home_team', 'game_date', 'location', 'start_time', 'attendance', 'game_situation', 'player', 'team']

		#get the first batter name so we can use it as a key
		batter_name = next(iter(raw_df.iloc[0]['away_batter_stats']))
		pitcher_name = next(iter(raw_df.iloc[0]['away_pitching_stats']))

		#each entry in batter_stats and pitcher_stats is a dictionary representing a player - use specific players to get the keys (columns)
		batting_stat_cols = meta_info_cols + list(batter_stats[batter_name].keys()) 
		pitching_stat_cols = meta_info_cols + list(pitcher_stats[pitcher_name].keys())

		#finally, parse the dictionaries in the raw dataframe to return a new dataframe where every row is the performance of an individual player in a single game
		#create a separate batting and pitching df to make it easier to analyze
		batting_df = pd.DataFrame(self.parse_bbref_batter_df(raw_df), columns=batting_stat_cols)
		pitching_df = pd.DataFrame(self.parse_bbref_pitcher_df(raw_df), columns=pitching_stat_cols)

		return batting_df, pitching_df

	#some utility functions for cleaning up the raw BBRef data.  They could be nicer, but they work for now.
	def parse_bbref_batter_df(self, df):
		all_batter_list = []
		for i in range(0, len(df)):
			row = df.iloc[i]
			meta_info_list = []
			meta_info_list.append(row['away_team'])
			meta_info_list.append(row['home_team'])
			meta_info_list.append(row['game_date'])
			meta_info_list.append(row['location'])
			meta_info_list.append(row['start_time'])
			meta_info_list.append(row['attendance'])
			meta_info_list.append(row['game_situation'])

			away_batter_stats = row['away_batter_stats']

			for player in away_batter_stats.keys():
				temp_list = []
				temp_list.append(player)
				temp_list.append("away")
				for value in away_batter_stats[player].values():
					temp_list.append(value)
				all_batter_list.append(meta_info_list + temp_list)

			home_batter_stats = row['home_batter_stats']

			for player in home_batter_stats.keys():
				temp_list = []
				temp_list.append(player)
				temp_list.append('home')
				for value in home_batter_stats[player].values():
					temp_list.append(value)
				all_batter_list.append(meta_info_list + temp_list)

		return all_batter_list

	def parse_bbref_pitcher_df(self, df):
		all_pitcher_list = []
		for i in range(0, len(df)):
			row = df.iloc[i]
			meta_info_list = []
			meta_info_list.append(row['away_team'])
			meta_info_list.append(row['home_team'])
			meta_info_list.append(row['game_date'])
			meta_info_list.append(row['location'])
			meta_info_list.append(row['start_time'])
			meta_info_list.append(row['attendance'])
			meta_info_list.append(row['game_situation'])

			away_pitcher_stats = row['away_pitching_stats']

			for player in away_pitcher_stats.keys():
				temp_list = []
				temp_list.append(player)
				temp_list.append("away")
				for value in away_pitcher_stats[player].values():
					temp_list.append(value)
				all_pitcher_list.append(meta_info_list + temp_list)

			home_pitcher_stats = row['home_pitching_stats']

			for player in home_pitcher_stats.keys():
				temp_list = []
				temp_list.append(player)
				temp_list.append('home')
				for value in home_pitcher_stats[player].values():
					temp_list.append(value)
				all_pitcher_list.append(meta_info_list + temp_list)
			
		return all_pitcher_list           




	def create_player_lookup_csv(self):
		"""
		Use pybaseball's lookup feature to generate a csv of all players names and ids in 
		the raw database
			
		"""
		raw_data = self.dh.pull_raw_data()

		batter_ids = set(raw_data.batter.unique())
		pitcher_ids = set(raw_data.pitcher.unique())
		player_ids = batter_ids.union(pitcher_ids)

		pybaseball.playerid_reverse_lookup(list(player_ids)).to_csv('player_lookup.csv')

	def load_player_lookup_df(self, filepath='player_lookup.csv'):
		try:
			return pd.read_csv('player_lookup.csv')
		except FileNotFoundError:
			print("Couldn't find player_lookup.csv in the same directory.  Try using the 'create_player_lookup' function to generate it")