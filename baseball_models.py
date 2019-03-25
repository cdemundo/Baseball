import pandas as pd
import numpy as np
import shap
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error
import time
import datetime

from pybaseball import playerid_reverse_lookup

class CrossValidator(object):

	def train_test_split(self, X, y, num_splits = 2):
		'''
			This function should be called on any data we are using to split it before we do anything into training, development and test data.

			We will ALWAYS reserve the last 2 months of data (Aug-Sept of 2018) as final test data

			We will apply 'walk-along' cross-fold validation for the rest of the data

		Parameters
		--------------
		df : dataframe
			dataframe of features and a column called ['fd_score']

		Returns
		--------------
		X : pandas dataframe
			dataframe of features
		y : pandas Series
			target to predict
		tscv : sklearn.model_selection._split.TimeSeriesSplit
			Provides the indices to use for walk-along cross-fold validation on X and y
		'''

		#check to make sure input is the right type
		assert isinstance(X, pd.DataFrame), 'X should be a Pandas DataFrame'

		if num_splits == 0:
			num_splits = len(X) - 1

		#has to be at least 2 splits for TimeSeriesSplit to work
		if num_splits < 2:
			num_splits = 2

		tscv = TimeSeriesSplit(n_splits=num_splits)

		return X, y, tscv


	def clean_for_model(self, X, new_features=[], batting=False, pitching=False):
		'''
		The goal of this function is to clean model input - reduce the number of features to only ones that we have identified
		are good

		I am manually maintaining this list as it is the result of feature engineering.  We have logged a history of all model training for posterity

		Parameters
		-----------------
			X : pandas dataframe
				observations in each row and features as columns
			new_features : list
				list of new features we are evaluating
			batting : bool
				flag for working on batting model
			pitching: bool
				flag for working on pitching model

		Returns
		-------------------
			X : pandas dataframe
				dataframe limited to known good features and new columns
		'''
		if batting == True:
			#note - game_id isn't a feature, but we remove it in the cross-validation stage
			known_good_batting_features = ['game_id','PA_ytdavg', 'home_run_ytdavg', 'PA_14dayavg', 'RBI_ytdavg', 'slugging_perc_7dayavg', 'slugging_perc_14dayavg', 'pitches_ytdavg', 'single_ytdavg', 'PA_21dayavg', 'strikes_total_ytdavg']
			if len(new_features) > 0:
				features_to_keep = list(set(known_good_batting_features).union(set(new_features)))
				return X[features_to_keep]
			else:
				return X[known_good_batting_features]

		elif pitching == True:
			#we dont know good features yet so just return X
			#known_good_pitching_features = ['game_id']
			if len(new_features) > 0:
				features_to_keep = list(set(known_good_pitching_features).union(set(new_features)))
				return X[features_to_keep]
			else:
				return X

		else:
			print("Either batting or pitching flag must be true!")
			return ''

	def cross_validate(self, X, y, tscv, model, fillna=False):
		i = 0
		self.test_maes = []
		self.train_maes = []
		for train_index, test_index in tscv.split(X):
			start = time.time()
			print("Running iter: " + str(i+1))
			self.X_train, self.X_test = X.iloc[train_index], X.iloc[test_index]
			self.y_train, self.y_test = y.iloc[train_index], y.iloc[test_index]

			#some models, like XGBoost handle nas, others don't.  Fill NAs if flag is set
			if fillna == True:
				X_train.fillna(0, inplace=True)
				X_test.fillna(0, inplace=True)

			#remove the ids so we can link them back up to predictions later
			self.X_train_ids = self.X_train['game_id']
			self.X_train = self.X_train.drop('game_id', axis=1)

			#train the model
			self.model = model.fit(self.X_train, self.y_train)

			#same procedure - remove test ids
			self.X_test_ids = self.X_test['game_id']
			self.X_test = self.X_test.drop('game_id', axis=1)

			#run predictions using trained model
			self.train_preds = self.model.predict(self.X_train)
			self.test_preds = self.model.predict(self.X_test)

			#record the results
			self.train_maes.append(mean_absolute_error(self.y_train, self.train_preds))
			self.test_maes.append(mean_absolute_error(self.y_test, self.test_preds))
			print("Iter " + str(i+1) + " took " + str(time.time()-start) + " seconds.")
			i += 1
		#if we are at the last iteration, lets return the predicted values from the test set so we can
		#put them into the optimizer
		self.id_preds = list(zip(self.test_preds, self.X_test_ids))

	def log_model(self, notes=""):
		'''
		The goal of this is to log a model stored in the current instance of the object
		'''
		#calculate SHAP values to report most important features
		explainer = shap.TreeExplainer(self.model)
		shap_values = explainer.shap_values(self.X_train)

		#sum magnitude of SHAP values along columns (features)
		shap_sums = pd.DataFrame(np.abs(shap_values), columns = self.X_train.columns).sum(axis=0).sort_values(ascending=False)
		shap_sums = list(shap_sums.index[0:10].values)

		#create a dataframe to write results to csv
		results = pd.DataFrame([np.mean(self.train_maes)], columns = ['avg_train_MAE'])

		results['avg_test_MAE'] = np.mean(self.test_maes)
		results['params'] = str(self.model.get_params())
		results['features'] = str(list(self.X_train.columns.values))
		results['top_SHAP'] = str(shap_sums)

		results['min_train_date'] = self.X_train_ids.sort_values().iloc[0]
		results['max_train_date'] = self.X_train_ids.sort_values().iloc[len(self.X_train_ids)-1]

		#allow capturing descriptive notes
		if len(notes)>0:
			results['notes'] = notes

		results['date_trained'] = str(datetime.datetime.now().strftime("%Y-%m-%d"))

		results.to_csv("logged_results.csv", mode='a', header=False, index=False)

class MovingAverageModel(object):
	#def __init__(self):

	def fit(self, X, y, num_days=5):

		self.X = X
		self.y = y

		self.X['fd_score'] = y

		#calculate moving average for every game in X
		mov_df  = self.X.groupby(['player', 'game_date', 'game_id'])['fd_score'].min().reset_index()
		mov_df['moving_avg'] = mov_df.groupby('player')['fd_score'].apply(lambda x: x.rolling(num_days, 1).mean())

		#combine it back with the original feature set
		self.fitted_X = pd.merge(self.X, mov_df[['game_id', 'moving_avg']], on = 'game_id')

		self.fitted_X = self.fitted_X[['player', 'game_date', 'game_id', 'moving_avg']]

		#sort it by player and game.  Predict function will look at a date and get the moving avg from the most recent game before it
		self.fitted_X.sort_values(['player', 'game_date', 'game_id'], inplace=True)

		#get rid of the target column in the feature df
		X.drop('fd_score', inplace=True, axis=1)

		del mov_df

	def predict(self, X):
		'''

		'''
		#for every row in X, predict the fanduel score based on moving average of preceding games
		predictions = X.apply(self.get_moving_avg, axis = 1)

		return predictions


	def get_moving_avg(self, row):
		'''
			Helper function that looks up the moving average in fitted X values based on the player name and the game date

			Parameters
			--------------
			row : Pandas series
				Expected to contain keys 'player' and 'game_date'

			Returns
			--------------
			float
				the predicted value for the given player and game_date
				returns mean of all values if not enough rows to calculate
		'''

		#since fitted_x is sorted by player and date, we can simply look up the game
		#right before the one we want to predict and get the moving average from it

		#we can use game date here because we aren't concerned about double headers.  We always want a game from a preceding day
		filtered_df = self.fitted_X[(self.fitted_X['player'] == row['player']) & (self.fitted_X['game_date'] < row['game_date'])]

		#we filtered out all games > prediction date, so the most recent moving average is the
		#last row in the filtered df - return it as the prediction
		val = filtered_df.tail(1)['moving_avg'].values

		if np.isnan(val):
			return self.y.mean()
		else:
			return val

class FeatureEngineer(object):

	def __init__(self,df):
		if len(df) == 0:
			print("Need to pass a dataframe to engineer on initializing!")
			return ""
		#select only the numeric cols, excluding the year
		self.num_cols = list(df.select_dtypes(include=np.number).drop('year', axis=1).columns.values)

		#for calculating averages, we want a df sorted by player and date
		self.avg_df = df.sort_values(['player', 'game_date'])

	def calc_lifetime_avg(self):

		return_df = self.avg_df.copy()

		new_cols = ['game_id']

		for col in self.num_cols:
			if col != 'fd_score':

				newcol = col+'_lifeavg'

				#we group by player and date and calc expanded mean. we shift down by one to make sure no current day info is included
				return_df[newcol] = return_df.sort_values(['player', 'game_date']).groupby(['player'])[col].expanding().mean().shift().values
				#set the first value for each player to NaN since we wouldnt have data for it
				return_df.loc[return_df.groupby(['player'])[newcol].head(1).index, newcol] = np.NaN

				new_cols.append(newcol)

		#we return just the game_id and the new columns created.  These can be merged with other features created before going to the model
		return return_df[new_cols]

	def calc_rolling_avg(self):

		return_df = self.avg_df.copy()

		#we need to figure out if these are pitching averages or batting averages - different timeframes are relevant
		try:
			return_df['IP']
			avgs = [2,3,5,10,15]
		except KeyError:
			#1-4 week and 6 week
			avgs = [7,14,21,28,42]

		new_cols = ['game_id']

		for col in self.num_cols:
			if col != 'fd_score':
				for avg in avgs:

					newcol = col + '_' + str(avg) + 'dayavg'

					#we group by player and date and calc expanded mean. we shift down by one to make sure no current day info is included
					return_df[newcol] = return_df.sort_values(['player', 'game_date']).groupby(['player'])[col].rolling(avg,avg).mean().shift().values
					#set the first value for each player to NaN since we wouldnt have data for it
					return_df.loc[return_df.groupby(['player'])[newcol].head(1).index, newcol] = np.NaN

					new_cols.append(newcol)

		#we return just the game_id and the new columns created.  These can be merged with other features created before going to the model
		return return_df[new_cols]

	def calc_ytd_avgs(self):

		return_df = self.avg_df.copy()

		#since it's YTD, we want to make sure we sort by year as well
		return_df = return_df.sort_values(['player', 'year', 'game_date'])

		new_cols = ['game_id']

		for col in self.num_cols:
			if col != 'fd_score':
				newcol = col+'_ytdavg'

				#we group by player and year and calc expanded mean. we shift down by one to make sure no current day info is included
				return_df[newcol] = return_df.groupby(['player', 'year'])[[col]].expanding().mean().shift().values
				#set the first value for each player to NaN since we wouldnt have data for it
				return_df.loc[return_df.groupby(['player'])[newcol].head(1).index, newcol] = np.NaN

				new_cols.append(newcol)

		#we return just the game_id and the new columns created.  These can be merged with other features created before going to the model
		return return_df[new_cols]

	# For the first time, include the path to the statcast cache, follow on running can include preload=True
	def stadium_batter_avg(self, switch_cutoff=0.05, preload=False, filepath_statcast_cache = '../statcast_cache.csv'):

		batting_df = self.avg_df.copy()

		batting_column = 'batting_hand_' + str(switch_cutoff)

		if not preload:
			try:
				statcast_frame_raw = pd.read_csv(filepath_statcast_cache)
			except:
				print("Could not locate statcast_cache.csv, please check filepath_statcast_cache value")
				return ""

			left_right_hand = statcast_frame_raw.groupby(['batter', 'stand']).size()

			left_right_hand = left_right_hand.reset_index()
			left_right_hand.rename(columns={0:"bat_count"}, inplace=True)

			# Convert to long format with columns titled L / R (Left or Right handed pitches received)
			lr_pivot = left_right_hand.pivot(columns='stand', index="batter", values='bat_count')
			lr_pivot = lr_pivot.reset_index()
			lr_pivot = lr_pivot.fillna(0)

			# Creates helpyer columns to calculate how many pitches each player received
			# for each hand, and calculate the % of switch hitting each engaged in
			lr_pivot['primary_hand'] = np.where(lr_pivot['L'] > lr_pivot['R'], 'L', 'R')
			lr_pivot['major_count'] = np.where(lr_pivot['L'] > lr_pivot['R'], lr_pivot['L'], lr_pivot['R'])
			lr_pivot['minor_count'] = np.where(lr_pivot['L'] > lr_pivot['R'], lr_pivot['R'], lr_pivot['L'])
			lr_pivot['switch_perc'] = (lr_pivot['minor_count'] / lr_pivot['major_count'])
			lr_pivot = lr_pivot.replace(np.inf, np.nan).fillna(0)

			# if a player performs more than 5% of their bats using the other hand, they're classified as a switch hitter
			lr_pivot[batting_column] = np.where(lr_pivot['switch_perc'] < switch_cutoff, lr_pivot['primary_hand'], "S")

			lr_pivot.sort_values(by='switch_perc', ascending=False)

			left_right = lr_pivot[['batter', batting_column]]

			# Lookup 'batter keys' to mlb keys
			player_list = left_right['batter'].tolist()

			# Lookup keys to get each player's various keys (mlb, bbref, etc.)
			player_id_values = playerid_reverse_lookup(player_list, key_type='mlbam')

			# Merge player keys to batter df based on key
			cols_to_merge = ['name_last', 'name_first', 'key_mlbam', 'key_bbref', 'key_fangraphs', 'key_retro']
			left_right_with_keys = left_right.merge(player_id_values[cols_to_merge], how='inner', left_on='batter', right_on='key_mlbam')

			# Cache
			print('Creating cache "batter_hand.csv" in current directory...')
			left_right_with_keys.to_csv('batter_hand.csv', index=False)

		# Load the cache
		left_right = pd.read_csv('batter_hand.csv')

		batting_df2 = batting_df.merge(left_right[[batting_column, 'key_bbref']], how="left", left_on="player", right_on="key_bbref")
		batting_df2.drop('key_bbref', 1, inplace=True)

		# Grouyp by stadium and batting hand.  Possible future expansion here based on date, maybe not tho
		stadium_hand_averages = batting_df2.groupby(['stadium', batting_column])['batting_avg'].mean()
		stadium_hand_averages = stadium_hand_averages.reset_index()

		stadium_hand_averages.rename({"batting_avg": 'stadium_batting_avg_' + str(switch_cutoff) }, axis=1, inplace=True)

		# Bring in the stadium averages to our normal DF (so we link back up with game_id)
		batting_df3 = batting_df2.merge(stadium_hand_averages, how="left", left_on=["stadium", batting_column], right_on=["stadium", batting_column])

		return_frame = batting_df3[[ 'game_id', 'stadium_batting_avg_' + str(switch_cutoff) ]]

		return return_frame

	def stadium_dummies(self):

		batting_df = self.avg_df.copy()

		orig_columns = batting_df.columns.tolist()
		orig_columns.remove('game_id')

		cols_to_convert = ['game_situation', 'team', 'day_of_week', 'stadium']

		# Remove values fropped during get_dummies call from list to be used to drop all old values
		for value in cols_to_convert:
		    orig_columns.remove(value)

		loc_time_day_frame = pd.get_dummies(batting_df, columns=cols_to_convert, drop_first=True)
		loc_time_day_frame.drop(orig_columns, axis=1, inplace=True)

		return loc_time_day_frame

	def stadium_stats(self, ballpark_stat_path='ballpark_handed_stats.csv', ballpark_key_path='../baseball_key_joiner.csv'):
		# ballpark_stat_path is the link to the new file uploaded to github "ballpark_handed_stats.csv"
		# ballpark_key_path is the link to the original baseball_key_joiner.csv file

		batting_df = self.avg_df.copy()

		ballpark_hand = pd.read_csv(ballpark_stat_path)
		ballpark_keys = pd.read_csv(ballpark_key_path)

		ballpark_hand = ballpark_hand.merge(ballpark_keys[['stadium','team_abbr']], how="left", left_on="stadium_abbr", right_on="team_abbr")

		# Label incoming columns
		#ballpark_hand = ballpark_hand.add_prefix('stadium_')

		# add "stadium_stadium" and "stadium_year" to verify dates and locations
		stad_cols = ['game_id', 'stadium_BA', 'stadium_OBP', 'stadium_SLG', 'stadium_OPS', 'stadium_BAbip']

		# Roll stadium data points back 1 year, so bbref 2017 is joined with stadium data from 2016
		ballpark_hand['stadium_Year'] = ballpark_hand['stadium_Year'] - 1

		stadium_features = batting_df.merge(ballpark_hand,
		                                   how="left",
		                                   left_on=["stadium", "year"],
		                                   right_on=["stadium_stadium", "stadium_Year"])

		stadium_features = stadium_features[stad_cols]
		return stadium_features
