import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS event_staging"
staging_songs_table_drop = "DROP TABLE IF EXISTS song_staging"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS \"user\""
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES


## Staging Tables create
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS event_staging(
    id INT IDENTITY(0,1),
    
    -- user-related attributes
    userId INT,
    gender CHAR,
    auth VARCHAR NOT NULL,
    level VARCHAR NOT NULL,
    firstName VARCHAR NULL,
    lastName VARCHAR NULL,
    location VARCHAR NULL,
    
    sessionId INT NOT NULL,
    itemInSession INT NOT NULL,

    artist VARCHAR NULL,
    
    song VARCHAR NULL,
    length FLOAT NULL,
    
    page VARCHAR NOT NULL,
    
    method VARCHAR NOT NULL,
    registration VARCHAR NULL,
    status INT NOT NULL,
    ts BIGINT NOT NULL,
    userAgent VARCHAR,
    
    PRIMARY KEY(id)
)
DISTKEY(song)
 -- we could distribute on either song or artist. We're gonna have a shuffle when joining anyway,
 -- better not to have it for both fields
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS song_staging(
    id INT IDENTITY(0,1),
    num_songs INT,
    artist_id VARCHAR NOT NULL,
    artist_latitude FLOAT NULL,
    artist_longitude FLOAT NULL,
    artist_location VARCHAR,
    artist_name VARCHAR NOT NULL,
    song_id VARCHAR NOT NULL,
    title VARCHAR NOT NULL,
    duration FLOAT,
    year INT,
    
    PRIMARY KEY(id)
)
DISTKEY(title)
""")

## Final Tables create

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay (
    songplay_id INT IDENTITY(0,1), 
    start_time BIGINT, 
    user_id VARCHAR, 
    level VARCHAR, 
    song_id VARCHAR, 
    artist_id VARCHAR, 
    session_id INT, 
    location VARCHAR, 
    user_agent VARCHAR,
    
    PRIMARY KEY(songplay_id)
) DISTKEY(song_id)
-- Distribute both the songplay and song tables using the song_id column so 
-- we can eliminate shuffling when joining them, which is prevalent in our queries
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS \"user\" (
    user_id INT, 
    first_name VARCHAR, 
    last_name VARCHAR, 
    gender CHAR, 
    level VARCHAR, 
    
    PRIMARY KEY(user_id)
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song (
    song_id VARCHAR, 
    title VARCHAR, 
    artist_id VARCHAR, 
    year INT, 
    duration FLOAT, 
    
    PRIMARY KEY(song_id)
)  DISTKEY(song_id)
-- Distribute both the songplay and song tables using the song_id column so 
-- we can eliminate shuffling when joining them, which is prevalent in our queries
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist (
    artist_id VARCHAR, 
    name VARCHAR, 
    location VARCHAR, 
    latitude FLOAT, 
    longitude FLOAT, 
    
    PRIMARY KEY(artist_id)
) DISTSTYLE ALL
 -- in a realistic DB there are probably not as many artists as songs or users, so we may want to copy it across 
 -- all partitions for better querying performance
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time INT, 
    hour INT, 
    day INT, 
    week INT, 
    month INT, 
    year INT,
    weekday INT, 
    
    PRIMARY KEY(start_time)
)
-- too many timestamps to be able to do a DISTSTYLE All
""")

# STAGING TABLES

staging_events_copy = ("""
COPY event_staging(
    artist,auth,firstName,gender,itemInSession,lastName,length,level,location,
    method,page,registration,sessionId,song,status,ts,userAgent,userId)
FROM '{s3_bucket}'
IAM_ROLE '{iam_role}'
JSON {json_paths_file}
""").format(
    s3_bucket=config.get('S3','LOG_DATA'),
    iam_role=config.get('IAM_ROLE','ARN'),
    json_paths_file=config.get('S3','LOG_JSONPATH')
)

staging_songs_copy = ("""
COPY song_staging
FROM {s3_bucket}
IAM_ROLE '{iam_role}'
JSON 'auto'
""").format(
    s3_bucket=config.get('S3','SONG_DATA'),
    iam_role=config.get('IAM_ROLE','ARN')
)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent
) 
SELECT  ts/1000,   -- convert milliseconds to seconds (epoch), more understandable and leads to a lower number of rows in the `time` table
        userId, level, so_st.song_id, so_st.artist_id,
        sessionId, location, userAgent
FROM    event_staging ev_st
JOIN    song_staging so_st ON ( 
    ev_st.song = so_st.title AND
    ev_st.artist = so_st.artist_name
)
WHERE
page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO "user" (
    user_id, 
    first_name,
    last_name,
    gender,
    level
) 
SELECT  DISTINCT  
        userId, firstName, lastName, gender, level
FROM    event_staging
WHERE
        userId IS NOT NULL
-- not supported by Redshift, so we use a SELECT DISTINCT
-- ON CONFLICT (user_id)
-- DO NOTHING
""")

song_table_insert = ("""
INSERT INTO song (
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
) 
SELECT  DISTINCT
        song_id, title, artist_id, year, duration
FROM    song_staging
WHERE
        song_id IS NOT NULL
-- not supported by Redshift, so we use a SELECT DISTINCT
-- ON CONFLICT (song_id)
-- DO NOTHING
""")

artist_table_insert = ("""
INSERT INTO artist (
    artist_id, 
    name, 
    location, 
    latitude, 
    longitude
)
SELECT  DISTINCT 
        artist_id, artist_name, 
        artist_location, artist_latitude, artist_longitude
FROM    song_staging
WHERE
        artist_id IS NOT NULL
-- not supported by Redshift, so we use a SELECT DISTINCT
-- ON CONFLICT (artist_id)
-- DO NOTHING
""")

time_table_insert = ("""
INSERT INTO time (
    start_time, 
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT  DISTINCT
        ts/1000,  -- convert milliseconds to seconds (epoch), more understandable
        EXTRACT(hour from (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')),
        EXTRACT(day from (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')),
        EXTRACT(week from (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')),
        EXTRACT(month from (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')),
        EXTRACT(year from (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')),
        EXTRACT(dayofweek from (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second'))
FROM    event_staging
-- not supported by Redshift, so we use a SELECT DISTINCT
-- ON CONFLICT (start_time)
-- DO NOTHING
""")
# to_timestamp for converting from unix epoch to DB timestamp is unfortunately not supported by redshift, so we had to do this TIMESTAMP 'epoch' thing

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
