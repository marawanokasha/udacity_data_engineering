import csv
import psycopg2
import pandas as pd
from io import StringIO
from typing import List, Tuple
from sql_queries import song_select, songplay_table_insert, songplay_insert_columns

        
def _prepare_songplay_record(cur: psycopg2.extensions.cursor, row: pd.Series) -> Tuple:
    """
    prepare a songplay row read from the json file to be inserted into the DB
    """
    
    # get songid and artistid from song and artist tables
    cur.execute(song_select, (row.song, row.artist, row.length))
    results = cur.fetchone()

    if results:
        songid, artistid = results
    else:
        songid, artistid = None, None

    # insert songplay record
    songplay_data = (
        int(row.timestamp),
        row.userId,
        row.level,
        songid,
        artistid,
        row.sessionId,
        row.location,
        row.userAgent,
    )

    return songplay_data


def single_insert_songplay_records(cur: psycopg2.extensions.cursor, df: pd.DataFrame):
    """
    insert the songplay table rows row-by-row into the DB
    """
    # insert songplay records
    for index, row in df.iterrows():
        songplay_data = _prepare_songplay_record(cur, row)
        cur.execute(songplay_table_insert, songplay_data)


def bulk_insert_songplay_records(cur: psycopg2.extensions.cursor, df: pd.DataFrame):
    """
    Bulk insert into the DB for the songplays records using the COPY FROM command
    instead of sending each row separately
    """
    records = []
    # insert songplay records
    for index, row in df.iterrows():
        songplay_data = _prepare_songplay_record(cur, row)
        records.append(songplay_data)
    
    # create a file like object containing csv-formatted data that can then be passed to the DB as a file
    file_like = StringIO()
    csv_writer = csv.writer(file_like, delimiter='\t', quotechar="'")
    csv_writer.writerows(records)
    file_like.seek(0)
    
    cur.copy_from(file_like, "songplays", sep="\t", null='', columns=songplay_insert_columns)
