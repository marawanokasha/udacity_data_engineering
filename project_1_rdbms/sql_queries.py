# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES


# We use BigSerial here because it's not inconceivable that we'd have more than 2B song plays during the lifetime of the application
songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id BIGSERIAL,  
        start_time int NOT NULL,
        user_id int NOT NULL,
        level varchar NOT NULL,
        song_id varchar,
        artist_id varchar,
        session_id int NOT NULL,
        location varchar,
        user_agent varchar,
        
        CONSTRAINT songplays_pk PRIMARY KEY (songplay_id)
    )
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id int,
        first_name varchar NOT NULL,
        last_name varchar NOT NULL,
        gender char NOT NULL,
        level varchar NOT NULL,
        
        CONSTRAINT users_pk PRIMARY KEY (user_id)
    )
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id varchar,
        title varchar NOT NULL,
        artist_id varchar NOT NULL,
        year int,
        duration decimal, -- we use decimal here so we can do exact floating-point comparisons, even though when trying, float also worked
        
        CONSTRAINT songs_pk PRIMARY KEY (song_id)
    )
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id varchar,
        name varchar NOT NULL,
        location varchar,
        latitude float,
        longitude float,
        
        CONSTRAINT artists_pk PRIMARY KEY (artist_id)
    )
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time int,
        hour smallint NOT NULL,
        day smallint NOT NULL,
        week smallint NOT NULL,
        month smallint NOT NULL,
        year smallint NOT NULL,
        weekday smallint NOT NULL,
        
        CONSTRAINT time_pk PRIMARY KEY (start_time)
    )
""")



# ALTER TABLE to add constraints, we could have also added them during table creation, but
# having them here allows us the flexibility to execute them later even after the ETL
# so the ETL is faster

songplay_table_alter = ("""
    ALTER TABLE songplays
        ADD CONSTRAINT users_fk  FOREIGN KEY (user_id) REFERENCES users (user_id),
        ADD CONSTRAINT time_fk FOREIGN KEY (start_time) REFERENCES time (start_time),
        ADD CONSTRAINT songs_fk FOREIGN KEY (song_id) REFERENCES songs (song_id),
        ADD CONSTRAINT artists_fk FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
""")


song_table_alter = ("""
    ALTER TABLE songs
        ADD FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
"""
)



# ANALYZE all the db tables to update the table statitistics so query planner makes optimized query execution plans

db_analyze = ("""
    ANALYZE
""");



# INSERT RECORDS

songplay_insert_columns = ["start_time", "user_id", "level", "song_id", "artist_id", "session_id", "location", "user_agent"]

songplay_table_insert = (f"""
    INSERT INTO songplays ({", ".join(songplay_insert_columns)})
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id)
    DO UPDATE
    SET first_name = EXCLUDED.first_name,
        last_name= EXCLUDED.last_name,
        gender = EXCLUDED.gender,
        level = EXCLUDED.level
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id)
    DO UPDATE
    SET name = EXCLUDED.name,
        location= EXCLUDED.location,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude
""")

# we specifically don't update in case of a conflict because this table is about timestamps
# so by definition if the timestamp is equal, then all the other components will be equal
# and no need to incur the overhead of an update
time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time)
    DO NOTHING
""")


# FIND SONGS

song_select = ("""
    SELECT songs.song_id, songs.artist_id 
    FROM songs 
    JOIN artists ON (songs.artist_id = artists.artist_id)
    WHERE 
        songs.title = %s AND
        artists.name = %s AND
        songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]