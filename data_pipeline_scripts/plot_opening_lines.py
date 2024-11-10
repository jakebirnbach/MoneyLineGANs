import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import boto3
import pytz
from datetime import datetime, timedelta
import seaborn as sns
import os
import time

MASS_BOOKS = ['draftkings', 'fanduel', 'betmgm', 'williamhill_us', 'espnbet', 'ballybet'] #List of Mass sportsbooks

def list_filenames(bucket_name, directory_prefix):
    """Returns a list of filenames within a particular directory in AWS S3

    Args:
        bucket_name (str): name of bucket with directory
        directory_prefix (str): path of directory to get the files.
    """
    # Create a boto3 client for S3
    s3 = boto3.client('s3')

    # Initialize a paginator to handle multiple pages of S3 objects (if necessary)
    paginator = s3.get_paginator('list_objects_v2')

    # Create a list to hold the filenames
    filenames = []

    # Use the paginator to fetch all objects within the specified directory
    for page in paginator.paginate(Bucket=bucket_name, Prefix=directory_prefix):
        # Access the 'Contents' from each page which lists the objects
        for obj in page.get('Contents', []):  # Check if 'Contents' key is in page
            filenames.append(obj['Key'])

    return filenames

def get_start_lines(df):
    """Gets the starting moneylines for each game. 

    This function takes in the aggregated data for a particular day and extracts the rows representing
    the moneylines at the start of each event. It does this by finding the data that was collected
    at the time closest to the start time.

    Returns:
        pd.DataFrame: Containing all the starting lines for a particula day for all the books.
    """
    ret_df = pd.DataFrame()
    #Iterates through individual games
    for game_id in df['id'].unique():
        #Iterates through sportsbooks
        for book in MASS_BOOKS:
            game_df = df[(df['id'] == game_id) & (df['book'] == book)] 
            min_line_idx = np.argmin(game_df['true_game_time_ms'].abs())
            start_line = game_df.iloc[min_line_idx].to_frame().T
            ret_df = pd.concat([ret_df, start_line], axis=0)
    return ret_df.reset_index(drop=True)

def plot_moneylines(df, date, plot_agg=False):

    # Create a PdfPages object to save plots to a PDF
    pdf_filename = f'{date}_starting_moneyline_plots.pdf'
    pdf = PdfPages(pdf_filename)

    # Plot 1: Scatter Plot
    plt.figure(figsize=(10, 6))
    plt.scatter(df['pos_money'], df['neg_money'], alpha=0.7)
    plt.title('Scatter Plot of pos_money vs neg_money')
    plt.xlabel('pos_money')
    plt.ylabel('neg_money')
    plt.grid(True)
    pdf.savefig()
    plt.close()

    # Plot 2: 2D Density Plot
    plt.figure(figsize=(10, 6))
    sns.kdeplot(
        x=df['pos_money'],
        y=df['neg_money'],
        cmap="Blues",
        shade=True,
        thresh=0.05,
        bw_adjust=1
    )
    plt.title('2D Density Plot of pos_money vs neg_money')
    plt.xlabel('pos_money')
    plt.ylabel('neg_money')
    pdf.savefig()
    plt.close()

    # Plot 3: Hexbin Plot
    plt.figure(figsize=(10, 6))
    plt.hexbin(df['pos_money'], df['neg_money'], gridsize=20, cmap='Blues', mincnt=1)
    plt.title('Hexbin Plot of pos_money vs neg_money')
    plt.xlabel('pos_money')
    plt.ylabel('neg_money')
    cb = plt.colorbar()
    cb.set_label('Count in bin')
    pdf.savefig()
    plt.close()

    # Plot 4: Joint Plot
    joint_plot = sns.jointplot(
        x='pos_money',
        y='neg_money',
        data=df,
        kind='scatter',
        height=8,
        marginal_kws=dict(bins=20, fill=True)
    )
    joint_plot.fig.suptitle('Joint Plot of pos_money vs neg_money', y=1.03)
    pdf.savefig(joint_plot.fig)
    plt.close(joint_plot.fig)

    # Plot 5: Regression Plot
    sns.set_style('whitegrid')
    plt.figure(figsize=(10, 6))
    sns.regplot(
        x='pos_money',
        y='neg_money',
        data=df,
        line_kws={'color': 'red'}
    )
    plt.title('Regression Plot of pos_money vs neg_money')
    plt.xlabel('pos_money')
    plt.ylabel('neg_money')
    pdf.savefig()
    plt.close()

    # Close the PDF object
    pdf.close()

    # AWS S3 configuration
    s3_bucket_name = 'moneygans-data' 
    
    if plot_agg:
        s3_file_key = f'basketball_nba/starting_money/starting_money_agg/plots/{pdf_filename}'
    else:
        s3_file_key = f'basketball_nba/starting_money/{date}_starting/plots/{pdf_filename}'       # Replace with the desired S3 object key

    # Create an S3 client
    s3_client = boto3.client('s3')

    # Upload the file
    try:
        s3_client.upload_file(pdf_filename, s3_bucket_name, s3_file_key)
        print(f"File '{pdf_filename}' uploaded to 's3://{s3_bucket_name}/{s3_file_key}' successfully.")
    except Exception as e:
        print(f"Error uploading file: {e}")

    # Optionally, remove the local PDF file after uploading
    os.remove(pdf_filename)

def main():
    tz = pytz.timezone('America/Los_Angeles')
    pst_time = datetime.now(tz)
    prev_date = (pst_time - timedelta(days=1)).strftime('%Y-%m-%d')

    bucket_name = 'moneygans-data'
    directory_prefix = f'basketball_nba/aggregated_data/{prev_date}_all/'

    prev_day_paths = list_filenames(bucket_name, directory_prefix)
    if not prev_day_paths:
        print(f"No files found in {directory_prefix}")
        return

    prev_day_path = prev_day_paths[0]
    df = pd.read_parquet(f's3://{bucket_name}/{prev_day_path}', engine='pyarrow')
    df['true_game_time_ms'] = df['last_update'] - df['commence_time']

    start_line_df = get_start_lines(df)
    money_lines = start_line_df[['pos_money', 'neg_money']].astype('int64')

    plot_moneylines(money_lines, prev_date)

    s3_bucket_name = 'moneygans-data' 
    s3_file_key = f'basketball_nba/starting_money/{prev_date}_starting/data/starting_moneylines.parquet'
    start_line_df.to_parquet(f's3://{s3_bucket_name}/{s3_file_key}', engine='pyarrow')

if __name__ == '__main__':
    while True:
        try:
            # Get current time in PST
            tz = pytz.timezone('America/Los_Angeles')
            now = datetime.now(tz)

            # Calculate next 2 AM PST
            next_run = now.replace(hour=2, minute=0, second=0, microsecond=0)
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