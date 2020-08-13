## Data Modeling with Cassandra

This project reads data about song plays recorded in CSV files and uses them to fill different Cassandra tables to enable analysis on the song playing behavior of users.

## Database Schema

We create the following Cassandra tables:

- session_library: to answer queries regarding specific user sessions, the schema is as follows:
    - `session_id` int -> **PK**, **Partitioning Key**
    - `item_in_session` int -> **PK**
    - `artist_name` text
    - `song_name` text
    - `song_length` float
    
  Example Query:

      ```
      SELECT artist_name, song_name, song_length 
      FROM session_library
      WHERE 
        session_id = 338 
        AND item_in_session = 4
      ```

- user_library: to answer queries regarding user behaviour, the schema is as follows:
    - `user_id` int -> **PK**, **Partitioning Key**
    - `session_id` int -> **PK**
    - `item_in_session` int -> **PK**
    - `artist_name` text
    - `song_name` text
    - `user_first_name` text
    - `user_last_name` text

  Example Query:
  
      ```
      SELECT artist_name, song_name, user_first_name, user_last_name 
      FROM user_library
      WHERE 
        user_id = 10 
        AND session_id = 182
      ```

- song_library: to answer queries regarding specific songs, the schema is as follows:
    - `song_name` text -> **PK** **Partitioning Key**
    - `user_id` int -> **PK**
    - `user_first_name` text
    - `user_last_name` text
      
  Example Query:
  
      ```
      SELECT user_first_name, user_last_name 
      FROM song_library
      WHERE 
        song_name = 'All Hands Against His Own'
      ```

  For this table, if a user listens to a song multiple times, it only counts once, so the number of rows here should be less than the other tables. This is desirable because we don't want to double count our users across their different sessions. We could have also used `SELECT DISTINCT` but then that would have led to removing users with the same first and last names of other users who listened to the same song.
 
 ## How to run the project
 

 Use the [attached jupyter notebook](./Project_1B_Project_Template.ipynb) to create the Cassandra keyspace and tables and run the ETL job.