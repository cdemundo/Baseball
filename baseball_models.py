import pandas as pd
from sklearn.model_selection import TimeSeriesSplit

class BaseballModelSelection(object):
	'''
		This class has functions that are similar to custom versions of sklearn.model_selection functions
	'''
	#def __init__(self):


	def train_test_split(self, df, num_splits = 0):
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
		assert isinstance(df, pd.DataFrame), 'Input should be a Pandas DataFrame'

		#drop any data in the holdout timeframe
		valid_df = df[df['game_date'] < '2018-08-01']

		assert valid_df['game_date'].max().strftime('%Y-%m-%d') < '2018-08-01'

		try: 
			y = valid_df['fd_score']
			X = valid_df.drop('fd_score', axis=1)
		except KeyError:
			print('fd_score column does not exist!')
			return ""

		if num_splits == 0:
			num_splits = len(X) - 1

		#has to be at least 2 splits for TimeSeriesSplit to work
		if num_splits < 2:
			num_splits = 2

		tscv = TimeSeriesSplit(n_splits=num_splits)

		return X, y, tscv

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
		'''

		#since fitted_x is sorted by player and date, we can simply look up the game
		#right before the one we want to predict and get the moving average from it

		#we can use game date here because we aren't concerned about double headers.  We always want a game from a preceding day
		filtered_df = self.fitted_X[(self.fitted_X['player'] == row['player']) & (self.fitted_X['game_date'] < row['game_date'])]

		#we filtered out all games > prediction date, so the most recent moving average is the
		#last row in the filtered df - return it as the prediction
		return filtered_df.tail(1)['moving_avg'].values

