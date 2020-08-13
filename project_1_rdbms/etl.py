import os
import time
import logging
import glob
import psycopg2
from functools import partial
import pandas as pd
from sql_queries import *
from data_quality import check_log_data_quality
from data_utils import single_insert_songplay_records, bulk_insert_songplay_records

logger = logging.getLogger(__name__)

def process_song_file(cur, filepath: str):
    """
    Process a single song file containing a single json record of a song.
    Records are inserted into both the artists and songs tables
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    

    # we are inserting the artists first because songs have an FK dependecny on artists
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 
                      'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    

def process_log_file(cur, filepath: str, bulk=False):
    """
    Process a single log file containing multiple json records.
    Records are inserted into the time, users and songplays tables
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]
    
    check_log_data_quality(filepath, df)

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit="ms")
    
    # for database storage: convert nanonseconds to seconds since epoch time, which is a more universally known standard
    df["timestamp"] = (t.astype(int)/10**9).astype(int)
    
    # insert time data records
    time_data = (
        df["timestamp"],
        t.dt.hour,
        t.dt.day,
        t.dt.weekofyear,
        t.dt.month,
        t.dt.year,
        t.dt.dayofweek
    )
    column_labels = (
        'timestamp',
        'hour',
        'day',
        'week',
        'month',
        'year',
        'weekday'
    )
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]
    
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
        
    if bulk:
        bulk_insert_songplay_records(cur, df)
    else:
        single_insert_songplay_records(cur, df)


def process_data(cur, conn, filepath: str, func):
    """
    Extract the json files from the given filepath
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    logger.info('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        logger.info('{}/{} files processed.'.format(i, num_files))

        
def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    start_time = time.time()
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=partial(process_log_file, bulk=True))
    
    logger.info('Adding FK constraints and updating table statistics')
    cur.execute(songplay_table_alter)
    cur.execute(song_table_alter)
    cur.execute(db_analyze)
    end_time = time.time()
    
    logger.info('Total Runtime for ETL: {:,d} seconds'.format(int((end_time - start_time))))

    conn.close()


if __name__ == "__main__":
    logging.basicConfig(format='[ %(asctime)s ] %(filename)s(%(lineno)d) %(levelname)s - %(message)s', level=logging.INFO)
    main()