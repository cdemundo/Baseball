**Moneyball**

**Abstract**

Fantasy sports creates a unique arena for applied machine learning and algorithmic problem solving.  Fantasy sports betting creates an environment where players of various levels of sophistication can come together and compete inside a system of set rules.  By using new explorative prediction methods, given the existing set of constraints of an online platform, it was our goal to explore the predictive power and limitations inherent in predicting sports outcomes on a fantasy sports platform.

**Problem Statement**

The rules of various online betting platforms differ, so it was necessary to pick a specific game and a specific online platform to focus the analysis.  The intuition is that the principles that permit superior prediction in one sport would not necessarily apply to another.  For this reason we chose baseball as our sport, and FanDuel as the fantasy sports betting platform.  

Baseball has many characteristics that make it the perfect sport for a machine learning based prediction project.  First, baseball is played sequentially, each action in the game has a clear demarkation that allows an analyst to differentiate where one ‘game event’ stops and the next begins.  In baseball the fundamental event is a pitch.  The pitcher throws a ball and the batter either hits it or misses it.  The game advances by pitch, one pitch at a time, and the outcome of each pitch can be evaluated and recorded.  The characteristics of the game (I.e. Player positions, score, time, etc.) can be recorded for each pitch which provides information about the conditions of the game as it progresses.  Additionally, baseball has many individual characteristics for a team sport.  The performance of each individual player’s contribution can be measured against every other player on his team.  Contrast this with a sport like soccer, where each player seamlessly interacts with team mates in a way that makes an individual contribution difficult to measure.  

For those unfamiliar with fantasy sports, the outcome of any particular game is often of no consequence, and baseball is no exception.  For example, a FanDuel user designs a fantasy sports team made up of baseball players.  Each player’s ‘value’ to the team is measured by the performance of that player in the game he plays, and is independent of his actual team’s success or failure in the real world.  Thus, accurately predicting the performance of an individual player is critical to creating a competitive fantasy sports team.  But that is only the first step in the process.

The first task is to predict the performance of baseball players on any given day using only the information that would be available on the day of prediction.  This means that when considering the performance of a player at time t, the only information that may be used for prediction is data that would have existed on day t-1 or before.  

The second task is to take the predictions from the first stage of this project, and optimize player performance given the constraints of the online betting platform.  Constraints exist that prevent a user from selecting the 10 best players for any one team, otherwise a large number of teams would be structurally similar with predictable, undifferentiated outcomes.  This constraint forces a user to make tradeoffs when selecting players for their own team, increasing the uncertainty and competitiveness of the game. Maximizing the performance of a team across the cast of active players is crucial to creating a competitive fantasy sports team.

**Data Sources**

*Statcast* - a pitch by pitch database that includes many technical metrics that describe the movement of the ball with precision.  Statcast includes values that represent the in game conditions, like which position a player is in, or the score of the game.

*Baseball-Reference.com (“bbref”)* - a resource that includes a litany of sabermetric data of various levels of usefulness.  Statistics that relate to player performance by stadium are available from baseball-reference.com, as were individual player performance data that related to events not captured by Statcast (I.e. Base on ball, Runs-batted-in, and others).

*RotoGuru* - a sports statistic aggregator that sells data regarding player and team performance.  Noteworthy amongst the information they sell is the historical outcome for each player for various fantasy sports platforms.  This provided very useful data for testing our models.

*Pybaseball* - a python based module created  for the purpose of looking up and analyzing sabermetric data.  It includes player and team level aggregations, and contains the necessary keys for combining databases from Statcast and baseball-reference.com, which have different player keys.

**Designing the Feature Engineering Pipeline**

1. First it was necessary to establish a base dataset to use as the source for feature engineering.  Initially this was our Statcast aggregation joined to baseball-reference, later we used the rotoguru dataset.  This is the dataset calibrated at the level of time-player keys, so each value was representative of the performance of an individual on a given day.  All feature engineering was based off this dataset, and once calculated the new features were merged back onto the original data frame as extra columns.  This allowed for both team members to test and experiment individually but always return to a data frame of the same dimensionality.  By creating a python class that initialized the baseline data frame, each team member could create a class method to form the experimental features back onto the baseline data frame.  

2. Using pandas to filter in the level of an individual player, it was possible to sort by date to get a data frame comprised of an individual’s performance by day.  Filtering by player was required to calculate all types of rolling and expanding averages, which are comprised of a window of values for a single player.  Once the appropriate filter/sort logic was established, we looped across all players to provide the necessary features for our data.

3.  Additional features were calculated on a case by case basis and merged back onto the base data frame using the appropriate key.  For example, performance metrics for individual baseball stadiums were joined by stadium.  For any features of this kind, we took care to prevent the possibility that future data would leak into these values we used the values from the previous year. An example is that of 2017 stadium yearly averages leaking into all games that took place in 2017.  This would be inappropriate since the values used to calculate the average would include values from the future, thereby invalidating the feature.   The details of specific features explored in this experiment are included in the “Features” section later in this essay.

**Feature Creation**

Baseball is notorious for having a large number of stats that relate to player performance.  The study of baseball statistics has its own name and is called sabermetrics.  These metrics were available to us for analysis and using them in their raw form, in addition to creating our own features, fueled our exploration.  A detailed list of sophisticated baseball values can be found at the following link:

http://hosted.stats.com/mlb/stats.asp?file=glossary   

*Lifetime Averages* - average of a specific metric over the lifetime of a player’s career.  This metric only includes the lifetime values from the previous year to avoid creating an average that is composed of values from the current year.  Using values from the end of the current year as a predictive element for a score in the beginning of the year creates a situation where data leakage occurs.

*Year to Date Averages* - year to date average of a specific metric, for example batting average or average points per game.  This value takes in all previous values in a given year as inputs to calculate a YTD average.  

*Multi-day moving averages (2,3,4,5…n day average)* - average over a set period of time to indicate some element of performance over time.  Multi-day moving averages were calculated for all available metrics (RBI %, batting average, etc...) and include values that apply to both batting and pitching performance.  Moving averages differ from year to date averages in that moving averages have a time limit beyond which values are no longer considered.  For example, in a 10 game moving average, values from the 11th previous game are ignored, and only the previous 10 games are used to calculate the average.   

*Stadium Metrics* These values represent metrics of player performance, aggregated at the level of the stadium in which the game was played.  The goal is to identify advantages that certain players may have in a specific stadium, based on the geometry or characteristics of the stadium.  This is important in baseball, stadiums are not always symmetrical and can create advantages for left or right handed batters.  Stadiums with unfavorable geometry may expose a player to a low angle setting sun for longer, presumably affecting his accuracy.

*Batting Order* This value represents the order in which a player bats in a game.  This is important because a player that bats second or third in a lineup has a higher chance of achieving an RBI, since a precondition of an RBI is having a player on base.  To capture this potential advantage, the batting order of a player was added to the base data frame as a feature.

*Weather Data* Data relating to the observed wind conditions during gameplay were included as a feature on the assumption that favorable (or unfavorable) winds could influence the distance a ball would fly, once hit.

*Pitching Matchup* This value represents the performance of the batter against the opponent team starting pitcher.  The lineups for both teams in a baseball game will be posted the night before a game is played, so we could measure the effectiveness of each batter against the opposing pitcher.  This value would allow our algorithm to take into account the impact of a particularly strong or weak starting pitcher, which could impact the ability of the opposing team to score.  

**XGBoost**

We used the legendary XGBoost algorithm to make the predictions, using a time-sequenced data split, which is a recommended best practice when dealing with time sequenced data.  Hyper-parameter tuning was completed after the feature engineering phase was exhausted.  

**Optimization**

Once our player predictions were made, the next step was to select a high performance team that could win a fantasy baseball game.  We were able to complete this task using a python module called pydfs-lineup-optimizer.  This module was designed to take active baseball players for a given day and create the team that will provide the greatest number of points.  

Fantasy sports websites provide rosters of players to their users. The idea is to help users choose lineups, and the roster includes some basic information around player performance. In our case the provided roster included the historical average fantasy points of each player to help users estimate future performance and make decisions about which players to select for their lineup.  Additionally, each player has a corresponding “salary” value that a user must “pay” to have that player on his team, which is also listed.  
A user has a set amount of fictional capital to spend on a lineup, this his or her options are limited.  Users make tradeoffs between players when designing their lineup.  The total salary cap for a team is set, so users need to decide which players will be most valuable, greatly increasing the uncertainty of the game.  This is where pydfs-lineup-optimizer comes in.

The pydfs-lineup-optimizer module is based in PuLP (Python Linear Programming) and uses a series of constraints to maximize the point value of a lineup based on historic fantasy points scoring.  Constraints include things like the salary cap for a lineup, the positions a player can play, and how many players from the same team can be on a given fantasy team.  Inside of these constraints, pydfs-lineup-optimizer will seek to maximize the fantasy points for a lineup and return the output.  By replacing the historical fantasy sports scores with our predicted scores, we were able to use this system to maximize fantasy sports performance based on our predicted values instead of the historical values.

**Useful Resources**

*Neural Networks and LSTM for Time Series Forecasting*

https://machinelearningmastery.com/how-to-develop-lstm-models-for-time-series-forecasting/

https://datascience.stackexchange.com/questions/24403/prediction-interval-around-lstm-time-series-forecast

https://tht.fangraphs.com/using-recurrent-neural-networks-to-predict-player-performance/

https://iamtrask.github.io/2015/11/15/anyone-can-code-lstm/

https://www.altumintelligence.com/articles/a/Time-Series-Prediction-Using-LSTM-Deep-Neural-Networks

https://machinelearningmastery.com/backtest-machine-learning-models-time-series-forecasting/

*Optimization*

https://pydfs-lineup-optimizer.readthedocs.io/en/latest/usage.html#base-usage

*Useful Insights on Calculating Rolling Averages in Pandas*

https://stackoverflow.com/questions/54622062/pandas-calculate-rolling-average-of-group-excluding-current-row
