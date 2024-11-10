import pandas as pd
import time
from datetime import datetime, timedelta
import pytz

from plot_opening_lines import list_filenames, plot_moneylines

STARTING_MONEY_ALL_PATH = 's3://moneygans-data/basketball_nba/starting_money/starting_money_agg/starting_money_all.parquet'


def combine_opening_money(df_new):
    df_all = pd.read_parquet(STARTING_MONEY_ALL_PATH, engine='pyarrow')
    df_new['date'] = df_new['commence_time_pst'].apply(lambda x: x.split('T')[0]).unique()[0]
    df_new_combined = pd.concat([df_all, df_new], axis=0)
    df_new_combined.to_parquet(STARTING_MONEY_ALL_PATH, engine='pyarrow')

def get_current_date_time_pst():
    # Define the Pacific Standard Time zone
    pst_timezone = pytz.timezone('America/Los_Angeles')
    
    # Get the current time in UTC
    utc_time = datetime.now(pytz.utc)
    
    # Convert the current UTC time to PST time
    pst_time = utc_time.astimezone(pst_timezone)
    
    return pst_time

def main():
    bucket_name = 'moneygans-data'  # Replace with your S3 bucket name
    prev_date = (get_current_date_time_pst() - timedelta(days=1)).strftime('%Y-%m-%d')
    directory_prefix = f'basketball_nba/starting_money/{prev_date}_starting/data/'
    prev_day_starting_path = list_filenames(bucket_name, directory_prefix)[0]
    df_prev_day = pd.read_parquet(f's3://moneygans-data/{prev_day_starting_path}', engine='pyarrow')
    combine_opening_money(df_prev_day)

    df_combined_new = pd.read_parquet(STARTING_MONEY_ALL_PATH, engine='pyarrow')
    plot_moneylines(df_combined_new, prev_date, plot_agg=True)


if __name__ == '__main__':
    # main()
    while True:
        try:
            # Get current time in PST
            tz = pytz.timezone('America/Los_Angeles')
            now = datetime.now(tz)

            # Calculate next 2 AM PST
            next_run = now.replace(hour=2, minute=30, second=0, microsecond=0)
            if now >= next_run:
                # If it's already past 2 AM today, schedule for next day
                next_run += timedelta(days=1)

            # Calculate time difference
            time_to_sleep = (next_run - now).total_seconds()
            print(f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            print(f"Next run scheduled at: {next_run.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            print(f"Sleeping for {time_to_sleep} seconds ({time_to_sleep / 3600:.2f} hours)")

            # Sleep until next run time
            time.sleep(time_to_sleep)

            # Execute main function
            print(f"Starting execution at {datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")
            main()
            print(f"Execution finished at {datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")

        except Exception as e:
            print(f"An error occurred: {e}")

        # After execution, loop back to schedule next run