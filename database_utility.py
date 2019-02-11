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

		#DO DATA CLEANING BEFORE RETURNING
		pitching_df = self.clean_up_dates(pitching_df)
		batting_df = self.clean_up_dates(batting_df)

		#ADD UNIQUE ID COLUMNS
		id_col = pitching_df['game_date'].astype(str) + pitching_df['stadium'] + pitching_df['start_time'] + pitching_df['player']
		pitching_df.insert(loc=0, column='game_id', value=id_col)

		id_col = batting_df['game_date'].astype(str) + batting_df['stadium'] + batting_df['start_time'] + batting_df['player']
		batting_df.insert(loc=0, column='game_id', value=id_col)

		return batting_df, pitching_df

	def calc_pitching_fd_score(self, filepath):
		'''
		We need to add in some columns and do some cleaning to calculate the Pitching specific FD score
		'''

		batting_df, pitching_df = self.pull_raw_bbref_data(filepath)

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

		pitching_df.drop_duplicates(inplace=True)

		#Fanduels uses a metric called 'quality starts' where points are given if a starting pitcher has a good game.  We need to calculate this

		#first group and find the first pitcher of each game
		qual_start_df = pitching_df.groupby(['game_date', 'stadium', 'start_time', 'team'])['player'].first().reset_index()

		#then create a new id column we will join back to the original df on.  we want an ID for the game and team, not specific to the player
		qual_start_df['first_pitch_id'] = qual_start_df['game_date'].astype(str) + qual_start_df['stadium'] + qual_start_df['start_time'] + qual_start_df['team']
		#we need to create this ID in the other df too so we can merge
		pitching_df['first_pitch_id'] = pitching_df['game_date'].astype(str) + pitching_df['stadium'] + pitching_df['start_time'] + qual_start_df['team']

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
		qual_start_df['quality_start'] = 1
		#take only the columns we need
		qual_start_df = qual_start_df[['game_id', 'quality_start']]

		#merge back in to the original df, left join to keep all the original rows
		pitching_df = pd.merge(pitching_df, qual_start_df, on='game_id', how='left')
		pitching_df['quality_start'] = pitching_df['quality_start'].fillna(0)

		#clean up memory
		del qual_start_df

		#finally, for each row in the df - calculate what the FD score should be
		pitching_df['fd_score'] = pitching_df.apply(self.fd_pitching_score, axis=1)

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

	def calc_batting_fd_score(self, pitching_df, filepath):
		batting_df, pitching_df = self.pull_raw_bbref_data(filepath)


		

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

		df['Day of Week'], df['game_date'] = df['game_date'].str.split(',', 1).str
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
			location_df = pd.read_csv('baseball_key_joiner.csv')

			location_df.rename(columns={'team_name': 'home_team'}, inplace=True)
			df = pd.merge(df, location_df[['home_team', 'stadium']], on='home_team')
			df = df.drop('location', 1)
		except FileNotFoundError:
			print("Couldn't find the baseball stadium csv - should be called 'baseball_key_joiner.csv")

		return df

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