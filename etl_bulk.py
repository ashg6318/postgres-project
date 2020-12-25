import os
import glob
import psycopg2
import pandas as pd
import time
from sql_queries_bulk import *
import configparser

config = configparser.ConfigParser()
config.read("postgres.cfg")

host=config.get("creds", "host")
username=config.get("creds", "username")
password=config.get("creds", "password")


def process_song_file(cur, song_files):
    """
    - Pulls all song data from data/song_data into 1 local csv file
    - Ingests all song data into a temp staging table
    - Inserts records into songs and artists tables from the temp table
    """
    song_csv_path = ''.join([os.getcwd(), "/song_data.csv"])
    df_song = pd.read_json(song_files[0] , lines=True)
    for i in range(1, len(song_files)):
        df_song = pd.concat([df_song, pd.read_json(song_files[i] , lines=True)], ignore_index=True)

    df_song = df_song.reindex(sorted(df_song.columns), axis=1)
    df_song.to_csv(song_csv_path, index=False, header=True)

    # create song temp table and insert  records
    cur.execute(song_data_temp_table_create)
    cur.execute(copy_query_song, (song_csv_path, ))
            
    # insert song records
    cur.execute(song_table_insert)
    
    # insert artist records
    cur.execute(artist_table_insert)


def process_log_file(cur, log_files):
    """
    - Pulls all log data from data/log_data into 1 local csv file
    - Ingests all log data into a temp staging table
    - Inserts records into songplays, users and time tables
    """
    log_csv_path = ''.join([os.getcwd(), "/log_data.csv"])
    df_log = pd.read_json(log_files[0] , lines=True)
    for i in range(1, len(log_files)):
        df_log = pd.concat([df_log, pd.read_json(log_files[i] , lines=True)], ignore_index=True)

    df_log = df_log.reindex(sorted(df_log.columns), axis=1)
    df_log.to_csv(log_csv_path, index=False, header=True)
    
    # create song temp table and insert  records
    cur.execute(log_data_temp_table_create)
    cur.execute(copy_query_log, (log_csv_path, ))
            
    # insert user records
    cur.execute(user_table_insert)
    
    # insert time records
    cur.execute(time_table_insert)
    
    # insert songplays records
    cur.execute(songplay_table_insert)


def process_data(cur, conn, filepath, func):
    """
    - Run for all song and log files
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))
    
    func(cur, all_files)
    conn.commit()


def main():
    """
    - Execute data extraction from json files and insertion into tables
    """
    print("ETL process started")
    st = time.time()
    conn = psycopg2.connect("host="+host+" dbname=sparkifydb user="+username+" password="+password)
    cur = conn.cursor()
    
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    
    # drop temp tables
    cur.execute(song_data_temp_table_drop)
    cur.execute(log_data_temp_table_drop)
    conn.commit()
    
    conn.close()
    print("ETL process finished")
    print("It took {} seconds".format(time.time() - st))

if __name__ == "__main__":
    main()