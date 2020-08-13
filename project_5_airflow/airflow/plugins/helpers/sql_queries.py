class Constants:
    USER_LEVELS = set(["paid", "free"])
    
    
class SqlQueries:
    
    songplay_table_insert = ("""
        SELECT
                md5(events.start_time || events.sessionid) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(dayofweek from start_time), 
        extract(week from start_time), extract(month from start_time), extract(year from start_time)
        FROM songplays
    """)
    
    data_quality_checks = [
        
        ## Songplays
        # Size Checks
        ("SELECT COUNT(*) FROM songplays", lambda x: x[0][0] > 0),
        # Validity Checks
        # we actually have a lot of valid songplays where the songid and artistid are null because we are doing a left join
        # when we are inserting into the songplays table
#         ("SELECT COUNT(*) FROM songplays WHERE songid is NULL", lambda x: x[0][0] == 0), 
#         ("SELECT COUNT(*) FROM songplays WHERE artistid is NULL", lambda x: x[0][0] == 0),
        
        ## Users
        # Size Checks
        ("SELECT COUNT(*) FROM users", lambda x: x[0][0] > 0),
        # Validity Checks
        ("SELECT COUNT(*) FROM users WHERE userid is NULL", lambda x: x[0][0] == 0),
        ("SELECT COUNT(*) FROM users WHERE first_name is NULL", lambda x: x[0][0] == 0),
        ("SELECT COUNT(*) FROM users WHERE last_name is NULL", lambda x: x[0][0] == 0),
        ("SELECT DISTINCT level FROM users", lambda x: len(set([y[0] for y in x]).difference(Constants.USER_LEVELS)) == 0 ),
        
        ## Artists
        # Size Checks
        ("SELECT COUNT(*) FROM artists", lambda x: x[0][0] > 0),
        # Validity Checks
        ("SELECT COUNT(*) FROM artists WHERE name is NULL", lambda x: x[0][0] == 0),
        
        ## Songs
        # Size Checks
        ("SELECT COUNT(*) FROM songs", lambda x: x[0][0] > 0),
        # Validity Checks
        ("SELECT COUNT(*) FROM songs WHERE title is NULL", lambda x: x[0][0] == 0),
        ("SELECT COUNT(*) FROM songs WHERE artistid is NULL", lambda x: x[0][0] == 0),
        ("SELECT MIN(duration), MAX(duration) FROM songs", lambda x: x[0][0] > 0 and x[0][1] < 10000),
        
        ## Time
        # Size Checks
        ("SELECT COUNT(*) FROM time", lambda x: x[0][0] > 0),
        # Validity Checks
        ("SELECT COUNT(*) FROM time WHERE hour is NULL", lambda x: x[0][0] == 0),
        ("SELECT COUNT(*) FROM time WHERE day is NULL", lambda x: x[0][0] == 0),
        ("SELECT COUNT(*) FROM time WHERE week is NULL", lambda x: x[0][0] == 0),
        ("SELECT COUNT(*) FROM time WHERE month is NULL", lambda x: x[0][0] == 0),
        ("SELECT COUNT(*) FROM time WHERE year is NULL", lambda x: x[0][0] == 0),
        ("SELECT COUNT(*) FROM time WHERE dayofweek is NULL", lambda x: x[0][0] == 0),
        
        ("SELECT DISTINCT hour FROM time", lambda x: len(set([y[0] for y in x]).difference(set(range(0,24)))) == 0 ),
        ("SELECT DISTINCT day FROM time", lambda x: len(set([y[0] for y in x]).difference(set(range(1,32)))) == 0 ),
        ("SELECT DISTINCT dayofweek FROM time", lambda x: len(set([y[0] for y in x]).difference(set(range(0,7)))) == 0 ),
        ("SELECT DISTINCT week FROM time", lambda x: len(set([y[0] for y in x]).difference(set(range(1,53)))) == 0 ),
        ("SELECT DISTINCT month FROM time", lambda x: len(set([y[0] for y in x]).difference(set(range(1,13)))) == 0 ),
        ("SELECT MAX(year) FROM time", lambda x: x[0][0] < 2030 and x[0][0] > 1900),
        
    ]