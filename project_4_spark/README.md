# ETL with Spark + S3

This project reads data about song plays recorded in JSON files on S3, stages them with Spark and then creates a dimensional model and writes that model back into S3 where it can be analysed using Spark jobs or Athena using schema-on-read.


## Project structure
- `data` -> contains sample raw json files for testing locally
- `output_data` -> contains the output of the ETL process when executed locally for testing
- `dl.cfg` -> contains configuration for the AWS secrets used to access the S3 buckets for running the analysis on the full dataset
- `etl.py` -> contains the logic for the copying data from and to S3 and creating the dimensional model
- `Test ETL.ipynb` -> testing for the functionality of the etl pipeline on a sample of files
- `Test Queries.ipynb` -> testing for querying the dimensional model created


## Instructions

1. Set the AWS credentials in `dl.cfg`
1. Run the ETL script which does the copying from and to S3

        python etl.py [--local]
    
   You can use the optional `--local` flag to run the ETL locally on some test data.

   This creates the the following tables in the dimensional model as parquet files:

   The dimensional model consists of 5 tables, the fact table is `songplay`, containing, while the dimension tables are:
    - `songplays`: fact table containing data about song plays
    - `users`: dimension table containing data about the user playing the song
    - `artists`: dimension table containing data about the artist for the song
    - `songs`: dimension table containing about the song played
    - `time`: dimension table containing data about the time a specific song play occurred at

3. Conduct your analysis by either spinning up an EMR cluster and using a notebook or by directly using Athena:
    - Create AWS Glue Crawler: 
        - Set the data store to be the output S3 bucket: `s3://udacity-lesson3-project-bucket`
        - Create an IAM Role with access to this bucket
        - Create a new database `sparkify_analysis` to contains the tables
    - Run the Crawler and wait till it detects the 5 tables
    - Go to Athena and select the `sparkify_analysis` database


## Examples of Analysis to run

You can use those for Spark SQL or Athena

- Find out how many users we have

    ```
    SELECT COUNT(*)
    FROM users_table_parquet
    ```

- Find out the top listened-to songs

    ```
    SELECT s.song_id, s.title, COUNT(*) as num_listens
    FROM songplays_table_parquet AS sp
    JOIN songs_table_parquet AS s ON s.song_id = sp.song_id
    GROUP BY s.song_id, s.title
    ORDER BY num_listens DESC
    LIMIT 10
    ```
    
- Find out the top listened-to artists

    ```
    SELECT a.artist_id, a.name, COUNT(*) as num_listens
    FROM songplays_table_parquet AS sp 
    JOIN artists_table_parquet AS a ON a.artist_id = sp.artist_id
    GROUP BY a.artist_id, a.name
    ORDER BY num_listens DESC
    LIMIT 10
    ```
    

- Find out how many song listens we had in the months of 2018

    ```
    SELECT t.year, t.month, COUNT(*) as num_listens
    FROM songplays_table_parquet AS sp
    JOIN time_table_parquet AS t ON t.start_time = sp.start_time
    WHERE t.year = 2018
    GROUP BY t.year, t.month
    ORDER BY num_listens DESC
    LIMIT 10
    ```