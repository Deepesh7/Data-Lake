### Project Description

Sparkify is a music streaming startup. Their data resides in S3, in a directory of JSON logs. The task of the project is to extract transform and load songs data and event logs and create analytical tables and store it parquet format in Amazon S3 using Elastic Map Reduce with Spark.

## How to run

_To run this project_, Fill the following details in `dl.cfg` file in the root of this project with the following data:

```
KEY=YOUR_AWS_ACCESS_KEY
SECRET=YOUR_AWS_SECRET_KEY
```
Create an S3 Bucket named  `songplay-analysis`  where output results will be stored.

Finally, run the following command:

`python etl.py`

_To run on an Jupyter Notebook powered by an EMR cluster_, Run the notebok etl.ipynb on EMR cluster.

## Project Structure

- etl.py : Python script that performs ETL process on source data and stores in the destination S3 bucket.
- dl.cfg : Configuration file that consist of AWS Credentials.
- etl.ipynb : Python notebook to run on the EMR cluster to perform ETL process.
- README.md : Current file, follow this to understand the project.

## ETL Pipeline

1.  Load credentials
    
2.  Read data from S3
    -   Song data:  `s3://udacity-dend/song_data`
    -   Log data:  `s3://udacity-dend/log_data`
    
    The script reads song_data and load_data from S3.
    
3.  Process data using spark
    
    Transforms them to create Dimension Tables and Fact Table. The tables are designed to support analysis. Schema design is shown in the schema description section below.
    
4.  Load it back to S3
    
    Writes them to parquet files in table directories on S3.
    

## Source Data

-   **Song datasets**: all json files are nested in subdirectories under  _s3a://udacity-dend/song_data_. A sample of this files is:

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

-   **Log datasets**: all json files are nested in subdirectories under  _s3a://udacity-dend/log_data_. A sample of a single row of each files is:

```
{"artist":"Slipknot","auth":"Logged In","firstName":"Aiden","gender":"M","itemInSession":0,"lastName":"Ramirez","leng
```

## Schema Design
**Dimensional Tables**
- users
	user_id (Integer): ID of user
	first_name (String): user's first name
	last_name (String): user's last name
	gender (String): user's gender
	level (String): paid level or free level

- songs 
	song_id (String): ID of song
	title (String): title of song
	artist_id (String): ID of artist of the song
	year (Integer): Year of song release
	duration (Double): Song duration in milliseconds

- artists
	artist_id (String): ID of artist
	name (String): Name of artist
	location (String): Location of artist
	latitude (Double): Latitude location of artist
	longitude (Double): Longitude location of artist

- time
	start_time (Timestamp): start time (timestamp) of user 	activity
	hour (Integer): Hour extracted from timestamp
	day (Integer): Day extracted from timestamp
	week (Integer): Week extracted from timestamp
	month (Integer): Month extracted from timestamp
	year (Integer): Year extracted from timestamp
	weekday (String): Weekday extracted from timestamp

**Fact Tables**
- songplays: records in log data associated with song plays i.e. records with page NextSong

	songplay_id (Integer): ID for each songplay record
	start_time (Timestamp): start time (timestamp) of user activity
	user_id (Integer) : ID of user 
	level (String): paid level or free level
	song_id (String): ID of song
	artist_id (String): ID of artist
	session_id (String): ID of user session
	location (String): User location
	user_agent (String): Agent used by the user to access Sparkify Platform.

    



