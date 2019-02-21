import pandas as pd
import numpy as np
from sklearn.model_selection import TimeSeriesSplit

class BaseballPipeline(object):
	'''
		This class has functions that are similar to custom versions of sklearn.model_selection functions
	'''
	#def __init__(self):


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


import time

class FeatureEngineering(object):
    '''This is a class to consolidate feature engineering functions in one spot.'''
    
    #def __init__(self):
            
    def offset_avgs(self, df, moving=True, num_days=5, offset=1, ytd=True, lifetime=False, verbose=False):
        '''
        Returns the offset moving average for numeric columns in a dataframe.  Assumes that all numeric columns passed can be turned into an average!

        Parameters
        ------------
        df : dataframe 
            A pandas dataframe with at least one numeric column
        num_dats : int
            The window size to use for the rolling average
        offset : int
            The number of days from the current to offset the rolling average.  Minimum has to be 1 - we do not want to include the current day in the average

        Returns
        -------------
        df : the original dataframe with the new rows added
        '''
                    
        #there is a way to speed this up I think - for when I have time to consider.  applying rolling to entire groupby object, not just columns
        #https://stackoverflow.com/questions/49590013/speed-up-rolling-window-in-pandas
        ytd_time = 0
        moving_time = 0

        #select only the numeric cols, excluding the year
        num_cols = list(df.select_dtypes(include=np.number).drop('year', axis=1).columns.values)

        if len(num_cols) > 1:
            #groupby including the game date so we are sorted by game order
            try:
                avg_df = df.groupby(['player', 'year', 'game_date', 'game_id'])[num_cols].max().reset_index()
            except KeyError:
                print("Expected to find ['player', 'year', 'game_date', 'game_id'] as columns in the dataframe.")
                return ""

            groups = avg_df.groupby(['year', 'player'])

            #for every numeric column, calculate the offset moving average
            new_cols = []
            print("Calculating averages!")
            for col in num_cols:
            	if col != 'fd_score':
	                if moving:
	                    start = time.time()
	                    if verbose:
	                    	print("Calculating moving avg for " + str(col))
	                    col_name = col + '_' + str(num_days) + 'dayavg'
	                    new_cols.append(col_name)

	                    avg_df[col_name] = groups[col].apply(lambda x: x.rolling(num_days, 1).mean().shift().bfill()).reset_index()[col]

	                    #the first value for each player should not contain information about the current day.  reset it to
	                    avg_df.loc[avg_df2.groupby('player')[col_name].head(1).index, col_name] = avg_df[col_name].mean()
	                
	                    moving_time += time.time()-start
	                
	                if ytd:
	                    start = time.time()
	                    
	                    if verbose: 
	                    	print("Calculating ytd avg for " + str(col))
	                    col_name = col + '_ytdavg'
	                    new_cols.append(col_name)
	                    
	                    #expanding applies an infinite window size of length n, where n is the position of the current row in the groupby object
	                    #shift moves all the results up by 1.  so for a given row, this value will be "the YTD avg from the preceding day"
	                    #if it is the first game of the season, YTD will equal = YTD from previous year
	                    avg_df[col_name] = groups[col].expanding().mean().shift().values

	                    #same logic as above - first day should not contain information about the current day
	                    avg_df.loc[avg_df2.groupby('player')[col_name].head(1).index, col_name] = avg_df[col_name].mean()
	                    
	                    ytd_time += time.time()-start
	                    
	                ## FIGURE OUT LIFETIME AVG  !!!!!                  
	                if lifetime: 
	                    print("Calculating ytd avg for " + str(col))
	                    col_name = col + '_ytdavg'
	                    avg_df[col_name] = avg_df.groupby(['player', 'year'])[col].expanding().mean().values

            #the columns we will merge back on
            new_cols.append('game_id')
            
            if verbose: 
            	print("YTD TIME: ", ytd_time)
            	print("MOV TIME: ", moving_time)

            print("Done calculating averages!")
            #we just return the moving averages - we don't want any of the other info as features as it's all information about the same day
            return avg_df[new_cols], avg_df['fd_score']

        else:
            print("No numeric columns were found in the dataframe!")
            return ""
    

