import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import requests
import time
from datetime import datetime, timedelta
import pytz

API_KEY = '39c094dc26939f21aab4314415efcd6f'
MASS_BOOKS = ['draftkings', 'fanduel', 'betmgm', 'williamhill_us', 'espnbet', 'ballybet']
ODDS_KEYS = ['basketball_nba', 'americanfootball_nfl']
ODDS_KEYS2 = ['basketball_nba', 'basketball_ncaab','icehockey_nhl','tennis_atp_us_open', 'tennis_wta_us_open']
REGIONS = ['us', 'us2']
S3_PATH = 's3://moneygans-data/'

class OddsAPI():
    def __init__(self, sport):
        self.data = self.format_df(self.get_raw_odds(sport))
    
    def get_raw_odds(self, sport):
        response_dfs = []
        for region in REGIONS:
            odds_url = f'https://api.the-odds-api.com/v4/sports/{sport}/odds/?apiKey={API_KEY}&regions={region}&markets=h2h&oddsFormat=american&dateFormat=unix'
            response_dfs.append(pd.json_normalize(requests.get(odds_url).json()))
        response_df = pd.concat([response_dfs[0], response_dfs[1]])
        
        #Remove events that are not from today
        response_df = response_df[response_df['commence_time'].apply(ts_to_iso).apply(get_date) == get_date(ts_to_iso(time.time()))]
        return response_df
    
    def format_df(self, response_df):
        """This Function takes in the API response and returns a formatted dataframe to later be processed.
        
        Args:
            response_df: (pd.DataFrame): Formatted json dataframe returned by api call
        
        Returns:
            pd.DataFrame: formatted dataframe to be later used for analysis

        """
        new_df = pd.DataFrame()
        #Iterates through bookmakers to extract odds
        for i, row in enumerate(response_df.iterrows()):
            index, data = row  # Unpack the tuple
            for book in data['bookmakers']:
                if book['key'] in MASS_BOOKS:
                    new_row = data.drop('bookmakers')
                    new_row['book'] = book['key']
                    if book['markets'][0]['outcomes'][0]['name'] == new_row['home_team']:
                        new_row['home_money'] = book['markets'][0]['outcomes'][0]['price']
                        new_row['away_money'] = book['markets'][0]['outcomes'][1]['price']
                    else:
                        new_row['home_money'] = book['markets'][0]['outcomes'][1]['price']
                        new_row['away_money'] = book['markets'][0]['outcomes'][0]['price']
                    
                    #Sets positive and negative moneyline variables
                    if new_row['home_money'] > 0 and new_row['away_money'] < 0:
                        new_row['pos_money'] = new_row['home_money']
                        new_row['neg_money'] = new_row['away_money']
                    elif new_row['home_money'] < 0 and new_row['away_money'] > 0:
                        new_row['pos_money'] = new_row['away_money']
                        new_row['neg_money'] = new_row['home_money']
                    #If both moneylines are negative, postive moneyline will be the less negative of the two
                    elif new_row['home_money'] < 0 and new_row['away_money'] < 0:
                        new_row['pos_money'] = max(new_row['home_money'],new_row['away_money'])
                        new_row['neg_money'] = min(new_row['home_money'],new_row['away_money'])



                    #Adds column representing the time before the game starts in iso format
                    #Time to game represents the time in 
                    new_row['commence_time_pst'] = ts_to_iso(new_row['commence_time'])
                    new_row['ts'] = time.time()
                    new_row['game_time_ms'] =  new_row['ts'] - new_row['commence_time']
                    new_row['game_time_hrs'] = convert_to_hours(new_row['game_time_ms'])
                    new_row['last_update'] = book['markets'][0]['last_update']
                    new_row['last_update_pst'] = ts_to_iso(new_row['last_update'])
                    new_row['true_game_time_ms'] = new_row['last_update'] - new_row['commence_time']
                    new_df = pd.concat([new_df, new_row], axis=1)
            # print(new_row)
        
        return new_df.T.reset_index(drop=True)
    
    def save_live_odds(self, date, time, sport):
        s3_path = S3_PATH+f'{sport}/{date}/live_odds/{sport}_{date}_{time}.parquet'
        if not self.data.empty:
            self.data.to_parquet(s3_path, index=False)

#Converts UNIX timestamp to ISO format for readability
def ts_to_iso(ts):  
    tz = pytz.timezone('America/Los_Angeles')
    return datetime.fromtimestamp(ts, tz).isoformat()

def extract_time(iso_timestamp):
    # Parse the ISO timestamp
    dt = datetime.fromisoformat(iso_timestamp)
    # Format only hours and minutes
    return dt.strftime("%H:%M")

#Gets Date from ISO Timestamp
def get_date(s):
    return s.split('T')[0] 

# Converts hours to seconds
def convert_to_hours(seconds):
    hours = seconds / 3600
    return hours

def sleep_until_out_of_interval(start_time: str, end_time: str):
    """
    Sleeps until the current time is outside the given time range.

    :param start_time: Start time in 'HH:MM' 24-hour format (e.g., '06:00').
    :param end_time: End time in 'HH:MM' 24-hour format (e.g., '01:00').
    """
    # Use Eastern Time zone
    tz = pytz.timezone('America/Los_Angeles')
    now = datetime.now(tz)
    current_time = now.time()

    # Convert start_time and end_time to datetime objects (today's date)
    start = datetime.combine(now.date(), datetime.strptime(start_time, '%H:%M').time())
    end = datetime.combine(now.date(), datetime.strptime(end_time, '%H:%M').time())

    # Localize start and end times to the Eastern Time zone
    start = tz.localize(start)
    end = tz.localize(end)

    # If the end time is before the start time, it means the interval spans midnight
    if end < start:
        end += timedelta(days=1)  # Adjust the end time to the next day if it spans midnight

    # Check if the current time is in the interval
    if start <= now <= end:
        # Calculate how long to sleep until the end of the interval
        sleep_duration = (end - now).total_seconds()
        print(f"Current time is within the interval {start_time} - {end_time} PST, sleeping for {sleep_duration} seconds.")
        time.sleep(sleep_duration)
    #else:
        #print("Current time is outside the specified interval, no sleep.")

def get_current_date_time_est():
    # Define the Pacific Standard Time zone
    pst_timezone = pytz.timezone('America/Los_Angeles')
    
    # Get the current time in UTC
    utc_time = datetime.now(pytz.utc)
    
    # Convert the current UTC time to PST time
    pst_time = utc_time.astimezone(pst_timezone)
    
    return pst_time

def execute_interval(interval_seconds):
    tz = pytz.timezone('America/Los_Angeles')

    # Align to the next interval
    est_time = datetime.now(tz)
    seconds = est_time.second
    delay = interval_seconds - (seconds % interval_seconds)
    if delay == interval_seconds:
        delay = 0  # Already aligned
    time.sleep(delay)  # Align with the next interval

    # Set next_run to the current time (already aligned)
    next_run = datetime.now(tz).replace(microsecond=0)

    while True:
        # Perform the data scraping and saving process
        odds_api = OddsAPI(ODDS_KEYS[0])
        data = odds_api.data
        first_start_time = data[data['commence_time'] == data['commence_time'].min()].iloc[0]['commence_time_pst']
        start_time_lag = (datetime.fromisoformat(first_start_time) - timedelta(minutes=5)).strftime('%H:%M')

        # Sleep during the specified interval
        sleep_until_out_of_interval('00:00', start_time_lag)

        est_time = datetime.now(tz)
        curr_date = est_time.strftime('%Y-%m-%d')
        curr_time = est_time.strftime('%H:%M:%S')

        print(f"Save executed at {curr_date} {curr_time}")

        # Execute the save function
        odds_api.save_live_odds(curr_date, curr_time, ODDS_KEYS[0])

        # Update next_run for the following interval
        next_run += timedelta(seconds=interval_seconds)

        # Calculate time remaining until the next interval
        now = datetime.now(tz)
        time_to_wait = (next_run - now).total_seconds()

        if time_to_wait < 0:
            # If we're behind schedule, adjust next_run
            missed_intervals = int(abs(time_to_wait) // interval_seconds) + 1
            next_run += timedelta(seconds=interval_seconds * missed_intervals)
            time_to_wait = (next_run - datetime.now(tz)).total_seconds()

        time.sleep(max(time_to_wait, 0))  # Wait precisely until the next interval





if __name__ == '__main__':
    execute_interval(15)
