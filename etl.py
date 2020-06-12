import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_CREDS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates the spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Reads all the json files from s3 bucket located in s3a://udacity-dend/song_data/
    Transforms the input and creates the songs and artists dimension table
    Stores the result in parquet format in 
    s3a://songplay-analysis/dim_songs and s3a://songplay-analysis/dim_artists
    
    input to the function:
        spark : SparkSession object
        input_data : Path to the source data
        output data : Path to store output data
    """
    song_data = input_data + "song_data/*/*/*/*.json"
    
    df = spark.read.json(song_data)

    songs_table = df.select(
        'song_id', 
        'title', 
        'artist_id',
        'year', 
        'duration'
        ).distinct()
    
    songs_table.write.parquet(output_data + "dim_songs", mode = 'overwrite')
    
    artists_table = artists_table = (
        df.select(
            'artist_id',
            col('artist_name').alias('name'),
            col('artist_location').alias('location'),
            col('artist_latitude').alias('latitude'),
            col('artist_longitude').alias('longitude'),
        ).distinct()
    )
    
    artists_table.write.parquet(output_data + "dim_artists", mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Reads all the json files from s3 bucket located in s3a://udacity-dend/log_data/
    Transforms the input and creates the users and time dimension table and songplays fact table
    Stores the result in parquet format in 
    s3a://songplay-analysis/dim_songs and s3a://songplay-analysis/dim_artists 
    and s3a://songplay-analysis/fact_sonplays.
    
    input to the function:
        spark : SparkSession object
        input_data : Path to the source data
        output data : Path to store output data
    """
    log_data =  input_data + 'log_data/*/*/*.json'

    df = spark.read.json(log_data)
    df = df.where(df.page == 'NextSong')
   
    users_table = (
        df.select(
            col('userId').alias('user_id'),
            col('firstName').alias('first_name'),
            col('lastName').alias('last_name'),
            col('gender').alias('gender'),
            col('level').alias('level')
        ).distinct()
    )
    
    users_table.write.parquet(output_data + "dim_users", mode="overwrite")

    df = df.withColumn(
        "start_time",
        F.to_timestamp(F.from_unixtime((col("ts") / 1000) , 'yyyy-MM-dd HH:mm:ss.SSS')).cast("Timestamp")
    ) 
    
    time_table = df.select("start_time").dropDuplicates() \
                       .withColumn("hour", hour(col("start_time"))) \
                       .withColumn("day", dayofmonth(col("start_time"))) \
                       .withColumn("week", weekofyear(col("start_time"))) \
                       .withColumn("month", month(col("start_time"))) \
                       .withColumn("year", year(col("start_time"))) \
                       .withColumn("weekday", date_format(col("start_time"), 'E'))
     
    
    time_table.write.parquet(output_data + "dim_time", mode="overwrite")

    song_df = spark.read.parquet(output_data + "dim_songs")

    songplays_table = (
        df.withColumn("songplay_id", F.monotonically_increasing_id())
          .join(song_df, song_df.title == df.song)
          .select(
            "songplay_id",
            col("start_time"),
            col("userId").alias("user_id"),
            "level",
            "song_id",
            "artist_id",
            col("sessionId").alias("session_id"),
            "location",
            col("userAgent").alias("user_agent")
          )
    )

    songplays_table.write.parquet(output_data + "fact_songplays", mode="overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://songplay-analysis/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
