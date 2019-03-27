import sqlite3
import pandas as pd
import numpy as np
import datetime
import math

from pybaseball import playerid_reverse_lookup

class DatabaseHelper(object):
	def __init__(self, sql_filepath, key_joiner_filepath):
		self.filepath = sql_filepath
		self.key_join_path = key_joiner_filepath

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

	def load_data(self, preload=True, path2018="", path2017="", write_csv=False, verbose=False):
		'''
		Utility function - load data from CSV if CSVs exist. If not, get raw data from .jl files.
		'''

		if preload:
			print("Trying to load raw csvs!")

			try:
				batting_df = pd.read_csv('batting_df_master.csv')
				pitching_df = pd.read_csv('pitching_df_master.csv')

				print("CSV's Loaded!! Returning batting and pitching df")
			except FileNotFoundError:
				print("Couldn't find the files raw_batting_df.csv and raw_pitching_df.csv in the current directory.")
				return ""
		else:
			if (len(path2017) == 0) or (len(path2018) == 0):
				print("To load raw data, you must pass the path to bbref.jl and bbref_2018.jl")
				return ""
			else:
				batting_df, pitching_df = self.combine_scraped_data(path2017=path2017, path2018=path2018, write_csv = write_csv, verbose=verbose)

		return batting_df, pitching_df


	def pull_raw_bbref_data(self, filepath):
		"""
		Utility function to get raw game data from bbref file generated from scraping the baseball reference website.

		Since this isn't stored in a database, we will just pull, clean, and return the bbref data - filtering can happen in pandas

		Parameters
		----------
		filepath : str
			location of the bbref.jl file
		preload : bool
			if true, look for csv's in same folder and load those instead of pulling raw data
		write_csv : bool
			if true, write a csv of batting_df and pitching_df after pulling raw data

		Returns
		----------
		dataframe
			A pandas dataframe of pitch data for the specified dates
		"""
		print("Reading in data..")
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
		print("Parsing raw dictionaries to get stats...")
		batting_df = pd.DataFrame(self.parse_bbref_batter_df(raw_df), columns=batting_stat_cols)
		pitching_df = pd.DataFrame(self.parse_bbref_pitcher_df(raw_df), columns=pitching_stat_cols)

		#DO DATA CLEANING BEFORE RETURNING
		print("Cleaning dates...")
		pitching_df = self.clean_up_dates(pitching_df)
		batting_df = self.clean_up_dates(batting_df)

		#ADD UNIQUE ID COLUMNS
		id_col = pitching_df['game_date'].astype(str) + pitching_df['stadium'] + pitching_df['player']
		pitching_df.insert(loc=0, column='game_id', value=id_col)

		id_col = batting_df['game_date'].astype(str) + batting_df['stadium'] + batting_df['player']
		batting_df.insert(loc=0, column='game_id', value=id_col)

		print("Converting stats columns to numeric...")
		#convert numeric columns to numeric - this will make it easier later on to select the appropriate columns for transformations
		batting_cols = batting_df.columns.drop(['game_id', 'away_team', 'home_team', 'game_date', 'start_time', 'game_situation', 'player', 'team', 'PO', 'A', 'details', 'position', 'day_of_week', 'year', 'attendance', 'stadium'])
		batting_df[batting_cols] = batting_df[batting_cols].apply(pd.to_numeric, errors='coerce', axis=1)

		#same for pitching
		pitching_cols = pitching_df.columns.drop(['game_id', 'away_team', 'home_team', 'game_date', 'start_time', 'game_situation', 'player', 'attendance', 'team', 'position', 'day_of_week', 'year', 'stadium'])
		pitching_df[pitching_cols] = pitching_df[pitching_cols].apply(pd.to_numeric, errors='coerce', axis=1)

		#we havent cleaned up the raw data yet - so drop duplicates
		batting_df.drop_duplicates(inplace=True)
		pitching_df.drop_duplicates(inplace=True)

		#Drop values where "At Bats" == 0 - backup pitchers!
		batting_df = batting_df[ (batting_df['AB'] != 0.0) ]

		#finally we want to remove any data from the validation period
		#pitching_df = pitching_df[pitching_df['game_date'] < '2018-08-01']
		#batting_df = batting_df[batting_df['game_date'] < '2018-08-01']

		print("Raw data returned!")
		return batting_df, pitching_df

	def calc_pitching_fd_score(self, preload = True, path2017="", path2018="", write_csv=False):
		'''
		We need to add in some columns and do some cleaning to calculate the Pitching specific FD score
		'''
		batting_df, pitching_df = self.load_data(preload, path2017, path2018, write_csv)

		print("Calculating FanDuel pitching score...")

		#DATA CLEANUP
		#if ER is above 15, set it equal to 0 as it's likely a mistake
		pitching_df['ER'] = pd.to_numeric(pitching_df['ER'])
		pitching_df.loc[pitching_df['ER'] > 14,'ER'] = 0

		#we will do the same for innings pitched - assuming it should be a max of 9, which is a complete game
		#this induces some NA's which we will drop at the end
		pitching_df['IP'] = pd.to_numeric(pitching_df['IP'], errors='coerce')
		pitching_df.loc[pitching_df['IP'] > 9, 'IP'] = 0.0

		#same for strikeouts
		pitching_df['SO'] = pd.to_numeric(pitching_df['SO'], errors='coerce')

		#a win is indicated in a weird column - it's pulled from a text scrape on Baseball Reference thats in the same spot as the position for batters, which is why the column is called 'position'
		#if the string in this column contains a 'W', we can say this pitcher got a win

		#first convert NaNs to string so we can search using str.contains
		pitching_df['position'] = pitching_df['position'].fillna("NA")
		#then set a flag it was a win
		pitching_df.loc[pitching_df['position'].str.contains('W'), 'win_recorded'] = 1
		#fill the non wins with 0
		pitching_df['win_recorded'] = pitching_df['win_recorded'].fillna(0)

		#Fanduels uses a metric called 'quality starts' where points are given if a starting pitcher has a good game.  We need to calculate this

		#first group and find the first pitcher of each game
		qual_start_df = pitching_df.groupby(['game_date', 'stadium', 'start_time', 'team'])['player'].first().reset_index()

		#then create a new id column we will join back to the original df on.  we want an ID for the game and team, not specific to the player
		qual_start_df['first_pitch_id'] = qual_start_df['game_date'].astype(str) + qual_start_df['stadium'] + qual_start_df['start_time'] + qual_start_df['team']
		#we need to create this ID in the other df too so we can merge
		pitching_df['first_pitch_id'] = pitching_df['game_date'].astype(str) + pitching_df['stadium'] + pitching_df['start_time'] + pitching_df['team']

		#take the subset of just the two columns we need - rename player so it doesn't duplicate
		qual_start_df = qual_start_df[['first_pitch_id', 'player']]
		qual_start_df.columns = ['first_pitch_id', 'first_pitcher']

		#join it back to the original df
		pitching_df = pd.merge(pitching_df, qual_start_df, on='first_pitch_id')

		#create a bool column to indicate if a row contains a starting pitcher
		pitching_df.loc[pitching_df['player'] == pitching_df['first_pitcher'], 'is_first_pitcher'] = 1
		pitching_df.loc[pitching_df['player'] != pitching_df['first_pitcher'], 'is_first_pitcher'] = 0

		#lets clean up the dataframe and get rid of unneeded columns
		pitching_df.drop(['first_pitcher', 'first_pitch_id'], axis=1, inplace=True)

		#find the quality starts using boolean filters
		qual_start_df = pitching_df[(pitching_df['ER'] <= 3) & (pitching_df['is_first_pitcher'] == 1) & (pitching_df['IP'] >= 6)]

		#qual_start_df['quality_start'] = 1
		qual_start_df.insert(0, 'quality_start', 1)
		#take only the columns we need
		qual_start_df = qual_start_df[['game_id', 'quality_start']]

		#merge back in to the original df, left join to keep all the original rows
		pitching_df = pd.merge(pitching_df, qual_start_df, on='game_id', how='left')
		pitching_df['quality_start'] = pitching_df['quality_start'].fillna(0)

		#clean up memory
		del qual_start_df

		#finally, for each row in the df - calculate what the FD score should be
		pitching_df['fd_score'] = pitching_df.apply(self.fd_pitching_score, axis=1)

		#remove columns we don't need
		pitching_df.drop(['quality_start'], axis=1, inplace=True)

		return pitching_df

	def fd_pitching_score(self, row):
		#IP are worth 3 points per completed inning

		#so if ip is 4.2, this will split and do:
		#4*3 + 2 = 14 points
		ip = str(row['IP']).split('.')
		#first value is completed innings
		ip_pts = int(ip[0])*3
		#add the fractional value which should be 0,1,2
		ip_pts += int(ip[1])

		#SO are a straight 3 points per SO
		so_pts = row['SO']*3

		#ER are negative 3 points per 1 ER
		er_pts = -(row['ER']*3)

		#a win is worth 6 points
		win_pts = row['win_recorded']*6

		#and a quality start is worth 4 points
		qual_start_pts = row['quality_start']*4

		return ip_pts+so_pts+er_pts+win_pts+qual_start_pts

	def calc_fd_scores_roto(self, start_date='2015-04-01', end_date='2018-07-19', preload=True, write_csv=False, path2017="", path2018=""):
		#the dates in rotoguru are in a weird format, need to clean them

		# PART 1 - pull in bbref data and store as a df to be merge later
		batting_df, pitching_df = self.load_data(preload=True, write_csv=False, path2017="", path2018="")
		#Our batting and pitching df need to match on this new game_id
		batting_df['roto_game_id'] = batting_df['game_date'] + batting_df['player']
		pitching_df['roto_game_id'] = pitching_df['game_date'] + pitching_df['player']

		print("Loading rotoguru data..")

		try:
		    rotoguru = pd.read_csv("roto_data_2015-2018.csv")
		except FileNotFoundError:
		    print("Couldn't find the rotoguru csv!")

		#match rotoguru to baseball reference with different keys
		print("Getting bbref key to merge rotoguru and bbref data")
		unique_players = list(rotoguru['MLB_ID'].unique())
		lookup = playerid_reverse_lookup(unique_players)

		rotoguru = pd.merge(rotoguru, lookup[['key_mlbam', 'key_bbref']], left_on='MLB_ID', right_on='key_mlbam')

		print("Cleaning up rotoguru dates..")
		#we need to make the rotoguru dates match the bbref date format - use a helper function
		rotoguru['game_date'] = rotoguru.apply(self.clean_rotoguru_dates, axis=1)
		rotoguru.drop('Date', axis=1, inplace=True)

		print("Merging bbref and rotoguru data to get FD scores")
		#create a unique id to merge rotoguru and bbref data
		rotoguru['roto_game_id'] = rotoguru['game_date'] + rotoguru['key_bbref']

		print("Batting df pre merge: ", batting_df.shape)
		rotoguru = rotoguru[['roto_game_id', 'FD_points', 'Pos']]
		batting_df = pd.merge(batting_df, rotoguru, on='roto_game_id')
		print("Batting df post merge: ", batting_df.shape)

		print("Pitching df pre merge: ", pitching_df.shape)
		rotoguru = rotoguru[['roto_game_id', 'FD_points', 'Pos']]
		pitching_df = pd.merge(pitching_df, rotoguru, on='roto_game_id')
		print("Pitching df post merge: ", pitching_df.shape)

		#we want to remove pitchers - we're not going to include them in the batting model, we have a separate df for pitching
		batting_df = batting_df[batting_df['Pos'] != 'P']
		batting_df.dropna(inplace=True)

		return batting_df, pitching_df

	def calc_batting_fd_score(self, start_date='2015-04-01', end_date='2018-07-19', preload=True, write_csv=False, path2017="", path2018=""):
		# PART 1 - get bbref data
		# Inputs:
		# start_date - beginning of time to pull statcast data
		# stop_date - time to cease pulling statcast data
		# Filepath1 - include path to filepath to bbref .jl file from scraper
		# Filepath2 - include path to baseball_name_translator

		# Outputs:
		# DF of batting stats merged from both bbref and statcast sources

		# Note:
		# There are no dates for bbref data bcause the scraping system has what it has
		# As we scrape more and icnrease our dataset we can hone in on specific seasons using these date variables

		# PART 1 - pull in bbref data and store as a df to be merge later
		batting_df, pitching_df = self.load_data(path2017=path2017, path2018=path2018, preload=preload, write_csv=write_csv)

		# PART 2 - get statcast data
		try:
			print("Accessing statcast_cache...")
			print("If dates are missing try rebuilding cache...")
			statcast_input_frame = pd.read_csv('statcast_cache.csv')
		except:
			print("Getting raw statcast data...")
			statcast_input_frame = self.pull_raw_statcast_data(start_date=start_date, end_date=end_date)
			statcast_input_frame.to_csv('statcast_cache.csv')

		events_worth_points = ['single', 'double', 'triple', 'walk', 'hit_by_pitch', 'home_run', 'hit_by_pitch']
		statcast_df = statcast_input_frame[ statcast_input_frame['events'].isin(events_worth_points) ]

		# Gets a list of batter keys, (unique list prevents repeat occurances)
		player_list = list(statcast_df['batter'].unique().astype(int))

		# Lookup keys to get each player's various keys (mlb, bbref, etc.)
		player_id_values = playerid_reverse_lookup(player_list, key_type='mlbam')

		# Merge player keys to batter df based on key
		cols_to_merge = ['name_last', 'name_first', 'key_mlbam', 'key_bbref', 'key_fangraphs', 'key_retro']
		statcast_df_2 = statcast_df.merge(player_id_values[cols_to_merge], how='inner', left_on='batter', right_on='key_mlbam')

		# Bring in stadium codes to to use with "home team" to determine the "stadium" where the game took place
		try:
			stadium_codes = pd.read_csv(self.key_join_path)

		except FileNotFoundError:
			print("Couldn't find baseball_key_joiner.csv in the same directory.")

		# Merge stadium codes onto existing statcast DF, merge on home team name
		statcast_df_3 = statcast_df_2.merge(stadium_codes, how='left', left_on='home_team', right_on='team_abbr')

		# Ad hoc basic key generation - can extract this to a function later if necessary
		# Matches on game_id since no 'start_time' value is available for statcast
		statcast_df_3['game_date'] = pd.to_datetime(statcast_df_3['game_date'])
		statcast_df_3['game_date'] = statcast_df_3['game_date'].astype(str)
		statcast_df_3['stadium'] = statcast_df_3['stadium'].astype(str)

		statcast_df_3['game_id'] = statcast_df_3['game_date'] + \
									statcast_df_3['stadium'] + \
									statcast_df_3['key_bbref'].astype(str)
		print("Aggregating data...")
		# Counts and groups events by game_id and event type, then unpacks events via unstack into their own columns
		batter_agg = statcast_df_3.groupby(['batter', 'home_team', 'game_date', 'game_id', 'events']).size() \
									.unstack(fill_value=0)
		batter_agg2 = batter_agg.reset_index()

		# Aggregates fan duel values
		batter_agg3 = batter_agg2.groupby(['batter', 'home_team', 'game_date', 'game_id']) \
											.agg({ 'hit_by_pitch' : 'sum', \
											'home_run' : 'sum', \
											'single' : 'sum', \
											'double' : 'sum', \
											'triple' : 'sum', \
											'walk' : 'sum', \
											'hit_by_pitch' : 'sum'})
		statcast_data = batter_agg3.reset_index()

		print("Merging bbref and statcast data...")
		# Merge statcast and bbref databases, dropna (there are a lot b/c statcast has +1 year of data with no bbref values)
		batter_dataframe_final = batting_df.merge(statcast_data, how='left', left_on='game_id', right_on='game_id')
		batter_dataframe_final.drop(columns=['home_team_y', 'game_date_y'], inplace=True)
		batter_dataframe_final.rename(columns={"home_team_x": "home_team", "game_date_x" : "game_date"}, inplace=True)
		#batter_dataframe_final = batter_dataframe_final.dropna()

		# Score game performance
		batter_dataframe_final['fd_score'] = batter_dataframe_final.apply(self.fd_batting_score, axis=1)

		# NAN values after the bbref-statcast join a lack of value for an in game event.  A player
		# who has at lease 1 AT BAT in a game, but fails to generate a FD scoring event, returns a NAN
		# This NAN should be a zero, since the player scored zero FD points
		batter_dataframe_final[['hit_by_pitch', 'home_run', 'single', 'double', 'triple']] = batter_dataframe_final[['hit_by_pitch', 'home_run', 'single', 'double', 'triple']].fillna(value=0)

		batter_dataframe_final = batter_dataframe_final[ (batter_dataframe_final['game_date'] < end_date) & (batter_dataframe_final['game_date'] > start_date)]

		# Take walk values from BB if 'walk' value is not filled due to failure to join between
		# statcast and bbref
		batter_dataframe_final['walk'] = np.where(batter_dataframe_final['walk'].isnull(), batter_dataframe_final['BB'], batter_dataframe_final['walk'])

		batter_dataframe_final['roto_game_id'] = batter_dataframe_final['game_date'].astype(str) + batter_dataframe_final['player'].astype(str)

		print("Batting FD Score calculated! Returning data..")
		return batter_dataframe_final

	def fd_batting_score(self, row):

		# Null batter indicates a misalignment with bbref and statcast
		if math.isnan(row['batter']):
			# This returns 0 for AT BATs that result in strikeouts, flyouts, and nonscoring events
			if (row['H'] == 0) and (row['RBI'] == 0) and row['BB'] == 0:
				return 0
			# This returns scoring values under specific conditions that dont not involving batting
			else:
				y = 0
				y = y + float(row['RBI']) * 3.5
				y = y + float(row['R']) * 3.2
				y = y + float(row['BB']) * 3
				return y

		x = 0
		# Does data capture the following scenario:
		# 1 x HR = 12pts + 3.2pts <- is this how fanduel works?
		x = x + float(row['RBI']) * 3.5
		x = x + float(row['R']) * 3.2
		x = x + float(row['BB']) * 3
		x = x + row['single'] * 3
		x = x + row['double'] * 6
		x = x + row['triple'] * 9
		x = x + row['home_run'] * 12
		x = x + row['hit_by_pitch'] * 3

		return x

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


	def clean_up_dates(self, df):
		'''
			General cleanup of bbref data before returning it.  Things like formatting dates, location, etc
		'''

		df['day_of_week'], df['game_date'] = df['game_date'].str.split(',', 1).str
		df['game_date'] = pd.to_datetime(df['game_date'], infer_datetime_format=True)
		df['year'] = df['game_date'].map(lambda x : x.year)
		#pitching_df['year'] = pitching_df['year'].astype('int')

		#clean up start time strings
		clean_times = []
		for time in df['start_time'].str.split(': '):
			try:
				clean_times.append(time[1].split(' L')[0])
			except IndexError:
				clean_times.append("unknown_start_time")

		df['start_time'] = clean_times

		#clean up locations
		try:
			location_df = pd.read_csv(self.key_join_path)

			location_df.rename(columns={'team_name': 'home_team'}, inplace=True)
			df = pd.merge(df, location_df[['home_team', 'stadium']], on='home_team')
			df = df.drop('location', 1)
		except FileNotFoundError:
			print("Couldn't find the baseball stadium csv - should be called 'baseball_key_joiner.csv")

		return df

	def clean_rotoguru_dates(self, row):
		'''Helper function to clean up rotoguru dates, which are in the format 20150127.0 (as a float)'''
		date = str(row['Date'])

		y, md = date[:4], date[4:]

		m, d = md[:2], md[2:4]

		clean_date = y + "-" + m + "-" + d

		return clean_date

	def combine_scraped_data(self, path2018, path2017, write_csv=False, verbose=False):
		'''This function is a utility function that combines the multiple scraped data files we have.  We have saved the datasets to a CSV, but if we ever need to recreate them'''
		#get the separate datasets from hardcoded locations
		batting_df_2017, pitching_df_2017 = self.pull_raw_bbref_data(filepath=path2017)

		if verbose:
			print("Merges for combine_scraped_data")
			print("--------------------------------")
			print("Batting DF 2015-2017 shape: ", batting_df_2017.shape)
			print("Pitching DF 2015-2017 shape: ", pitching_df_2017.shape)

		batting_df_2018, pitching_df_2018 = self.pull_raw_bbref_data(filepath=path2018)

		if verbose:
			print("Batting DF 2018 shape: ", batting_df_2018.shape)
			print("Pitching DF 2018 shape: ", pitching_df_2018.shape)

		#2018 data contained some old data from other years, vice versa for 2017.  make sure each is cleaned to the appropriate year
		batting_df_2018 = batting_df_2018[batting_df_2018['year'] == 2018.0]
		pitching_df_2018 = pitching_df_2018[pitching_df_2018['year'] == 2018.0]
		batting_df_2017 = batting_df_2017[batting_df_2017['year'] < 2018.0]
		pitching_df_2017 = pitching_df_2017[pitching_df_2017['year'] < 2018.0]

		batting_df = pd.concat([batting_df_2017, batting_df_2018])
		pitching_df = pd.concat([pitching_df_2017, pitching_df_2018])

		if verbose:
			print("Batting DF after concat: \n")
			print(batting_df.year.value_counts())
			print("Pitching DF after concat: \n")
			print(pitching_df.year.value_counts())

		if write_csv:
			batting_df.to_csv("batting_df_master.csv", index=False, header=True)
			pitching_df.to_csv("pitching_df_master.csv", index=False, header=True)

		return batting_df, pitching_df

	def create_player_lookup_csv(self):
		"""
		Use pybaseball's lookup feature to generate a csv of all players names and ids in
		the raw database

		"""
		raw_data = self.pull_raw_data()

		batter_ids = set(raw_data.batter.unique())
		pitcher_ids = set(raw_data.pitcher.unique())
		player_ids = batter_ids.union(pitcher_ids)

		pybaseball.playerid_reverse_lookup(list(player_ids)).to_csv('player_lookup.csv')

	def load_player_lookup_df(self, filepath='player_lookup.csv'):
		try:
			return pd.read_csv('player_lookup.csv')
		except FileNotFoundError:
			print("Couldn't find player_lookup.csv in the same directory.  Try using the 'create_player_lookup' function to generate it")
