from pydfs_lineup_optimizer import get_optimizer, Site, Sport
import pandas as pd
import numpy as np

def prep_optimization_actual_performance(game_date="2015-04-25", data_csv_path='mlb-dbd-2015.csv'):

    # Inputs:
    # 1. Date of game to be optimized
    # 2. data_csv_path is the input CSV to be used in case of historical input (i.e. 2015 Roto data)

    # Outputs:
    # 1. Creates a file called "baseball_optimization_actual_performance.csv" that is fed into optimization code

    # Sample Code (make sure you have a copy of mlb-dbd-2015.csv)
    # import moneyball_optimizer as mo
    # mo.prep_optimization_actual_performance(game_date="2015-04-25", data_csv_path='mlb-dbd-2015.csv')
    # mo.optimize_by_date()

    columns = ['Id', 'Position', 'First Name', 'Nickname', 'Last Name', 'FPPG', 'Played', 'Salary', 'Game', 'Team', 'Opponent', 'Injury Indicator', 'Injury Details', 'Tier']

    # Historical Game Analysis (The best we could have done if we optimized on FD metrics)
    import_frame = pd.read_csv(data_csv_path, sep=":", index_col=False)
    import_frame.columns = ['GID', 'MLB_ID','Name_Last_First','Name_First_Last',' P/H','Hand','Date','Team','Oppt','H/A','Game#','Game_ID','Gametime_ET','Team_score','Oppt_score','Home_Ump','Temp','Condition','W_speed','W_dir','ADI','prior_ADI','GS','GP','Pos','Order','Oppt_pitch_hand','Oppt_pich_GID','Oppt_pitch_MLB_ID','Oppt_pitch_Name','PA','wOBA_num','IP','W/L/S','QS','FD_points','DK_points','DD_points','SF_points','FD_salary','DK_salary','DD_salary','SF_salary','FD_pos','DK_pos','DD_pos','SF_pos']
    import_frame.drop(import_frame.tail(1).index,inplace=True)
    pd.set_option('display.max_columns', 500)

    # Format Dates
    import_frame['Date'] = import_frame['Date'].astype(str).str[:-2]
    import_frame['Date'] = pd.to_datetime(import_frame['Date'])

    #Rename the columns we can use
    import_frame.rename(columns={'Pos': 'Position',
                                'MLB_ID' : 'Id',
                                'Name_First_Last' : 'Nickname',
                                'FD_salary': 'Salary',
                                'Team' : 'Team',
                                'Oppt' : 'Opponent'}, inplace=True)

    # Create Last Name and First Name variables to match FD
    import_frame['Last Name'], import_frame['First Name'] = zip(*import_frame['Name_Last_First'].apply(lambda x: x.split(',')))

    # Calculate average FPPG (Fantasy points per game) using expanding mean for each player (the Chris Method)
    fppg = import_frame.groupby(['Nickname','Date'])[['FD_points']].max()
    fppg['FPPG'] = fppg.groupby('Nickname')["FD_points"].expanding().mean().values
    import_frame = import_frame.merge(fppg, how="inner", left_on=["Nickname", "Date"], right_on=["Nickname", "Date"])

    # Calculate "Played" variable (count # of games) and MERGE frame
    game_counter = import_frame.groupby(['Nickname','Date']).size().reset_index(name='counts')
    game_counter['Played'] = game_counter.groupby('Nickname')['counts'].cumsum()

    import_frame = import_frame.merge(game_counter, how="inner", left_on=["Nickname", "Date"], right_on=["Nickname", "Date"])

    # Create the remaining columns to align the import scraped DF (Roto, Year 2015)
    import_frame['Game'] = import_frame['Team'].str.upper() + "@" + import_frame['Opponent'].str.upper()
    import_frame['Injury Indicator'] = np.nan
    import_frame['Injury Details' ] = np.nan
    import_frame['Tier'] = np.nan
    import_frame['Salary'] = import_frame['Salary'].fillna(0).astype(int)

    import_frame.sort_values(by=["Nickname", "Date"], ascending=True)

    # This is a list of all available positions in our dataset and includes the raw form permutations of 'Position'
    positions = import_frame['Position'].unique().tolist()

    # Scrubs the "Position" values to remove/replace LF, CF, RF (just use OF)
    import_frame['Positions_Replaced'] = import_frame['Position'].str.replace('LF', 'OF')
    import_frame['Positions_Replaced'] = import_frame['Positions_Replaced'].str.replace('RF', 'OF')
    import_frame['Positions_Replaced'] = import_frame['Positions_Replaced'].str.replace('CF', 'OF')
    #import_frame['Positions_Replaced'] = import_frame['Positions_Replaced'].str.replace('DH', '')
    import_frame['Positions_Reduced'] = import_frame['Positions_Replaced'].str.split('-')

    #reduce number of combinations by casting each row as a set
    import_frame['Positions_Reduced'] = import_frame.apply(lambda row: set(row['Positions_Reduced']), axis=1)

    #sort the list in each row to align values for out of order strings (i.e. "1B-2B" != "2B-1B")
    import_frame['Positions_Reduced'] = import_frame['Positions_Reduced'].sort_values().apply(lambda x: sorted(x))

    # Select the first position only (simplifying mechanism for troubleshooting, keep commented out for now)
    #import_frame['Positions_Reduced'] = import_frame['Positions_Reduced'].apply(lambda x: x[0])

    #make position value back into string value (necessary input into optimization function)
    import_frame['Positions_Reduced'] = import_frame['Positions_Reduced'].apply(lambda x: '-'.join(map(str, x)))

    #Take newly formed position values (ordered, reduced, etc.) and place in 'Position' row
    import_frame['Position'] = import_frame['Positions_Reduced']

    import_frame2 = import_frame[ import_frame["Date"] == game_date ]

    import_frame_output = import_frame2[columns]
    import_frame_output.to_csv('baseball_optimization_actual_performance.csv')

def optimize_by_date(optimization_input='baseball_optimization_actual_performance.csv'):
    # Inputs:
    # 1. location of the .CSV to be optimized

    # Outputs:
    # 1. For now this function prints the result.  May be able to enhance this later on

    optimizer = get_optimizer(Site.FANDUEL, Sport.BASEBALL)

    optimizer.load_players_from_csv(optimization_input)
    lineup_generator = optimizer.optimize(20)

    for lineup in lineup_generator:
        print(lineup)
