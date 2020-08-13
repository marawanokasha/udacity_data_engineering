## ETL with S3 + Redshift

This project reads data about song plays recorded in JSON files on S3, stages them on Redshift and then creates a dimensional model in Redshift to be enable analysis on the song playing behavior of users.

Dumping the data from S3 into Redshift staging table allows us to:
1. Use the fast Amazon-internal network to transfer huge amounts of data from S3 to Redshift quickly, instead of first downloading it locally and then uploading it again
1. We are able to use the parallelization capabilities of Redshift's MPP engine to read concurrently from multiple files in an S3 bucket
1. Give Redshift the advantage of having the data local on the cluster when massaging the data into the target dimensional model, allowing it to again load the data in parallel from the staging tables to the final tables

## Instructions

1. Create the infrastructure

        python create_infra.py

   This creates the redshift cluster and its associated IAM role for accessing S3.

1. Create the tables

        python create_tables.py

   This creates the staging and final tables.

   The staging tables are:
    - `event_staging`: contains the data from the event data files in the S3 bucket
    - `song_staging`: contains the data from the song data files in the S3 bucket 

   The dimensional model consists of 5 tables, the fact table is `songplay`, containing, while the dimension tables are:
    - `user`: contains data about the user playing the song
    - `artist`: contains data about the artist for the song
    - `song`: contains data about the song played
    - `time`: contains data about the time a specific song play occurred at

2. Conduct the ETL to read from S3 and populate the staging and final Redshift tables

        python etl.py
        
   This copies the data from JSON files in the S3 bucket, puts them in the staging tables. It then creates a star-schema dimensional model from these staging tables. 

3. Conduct the analysis

4. When you are done, delete the infrastructure we created to save costs

        python delete_infra.py

## Examples of Analysis to run

- Find out how many users we have

    ```
    SELECT COUNT(*)
    FROM "user"
    ```

- Find out the top listened-to songs

    ```
    SELECT s.song_id, s.title, COUNT(*) as num_listens
    FROM song AS s
    JOIN songplay AS sp ON s.song_id = sp.song_id
    GROUP BY s.song_id, s.title
    ORDER BY num_listens DESC
    LIMIT 10
    ```
    
- Find out the top listened-to artists

    ```
    SELECT a.artist_id, a.name, COUNT(*) as num_listens
    FROM artist AS a
    JOIN songplay AS sp ON a.artist_id = sp.artist_id
    GROUP BY a.artist_id, a.name
    ORDER BY num_listens DESC
    LIMIT 10
    ```
    

- Find out how many song listens we had in the months of 2018

    ```
    SELECT t.year, t.month, COUNT(*) as num_listens
    FROM time AS t
    JOIN songplay AS sp ON t.start_time = sp.start_time
    WHERE t.year = 2018
    GROUP BY t.year, t.month
    ORDER BY num_listens DESC
    LIMIT 10
    ```