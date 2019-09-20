import os
import boto3
import configparser

from datetime import date

config = configparser.ConfigParser()
config.read('config.cfg')

s3_bucket=config['S3']['S3_BUCKET']
s3_base_staging_dir=config['S3']['S3_BASE_STAGING_DIR']

local_i94_dir = config['LOCAL_DATA_PATH']['I94_DATA_DIR']
local_iso_location_file = config['LOCAL_DATA_PATH']['ISO_LOC_FILE']
local_us_demography_file = config['LOCAL_DATA_PATH']['US_DEMO_FILE']


def create_staging_dir(s3):
    """ This function creates a new directoty inside staging directory for current day.
  
        Parameters: 
            s3: boto 3 client 
          
        Returns: 
            Nothing
     """
    
    s3_today_load_dir="date="+str(date.today())
    
    #s3.put_object(Bucket=s3_bucket, Key=("{}/{}/".format(s3_base_staging_dir, s3_today_load_dir)))
    s3_staging_dir = "s3://{}/{}/{}".format(s3_bucket, s3_base_staging_dir, s3_today_load_dir)
    print("Created a new dir under Staging for today: {}".format(s3_staging_dir))


def stage_file(s3, local_name, target_name):
    bucketName = s3_bucket
Key = "data/IP2LOCATION_ISO3166.csv"
outPutname = "{}/{}/{}".format(s3_base_staging_dir, s3_today_load_dir, "ISO_location_code_lookup.csv")

s3.upload_file(Key, bucketName, outPutname)
    
def create_spark_session():
     """ The function creates a new Spark session object. 
  
        Parameters: Nothing
          
        Returns: spark session object
     """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_data(spark, input_data, output_data):
     """ The function extracts data from S3 and does processing to create Fact and Dim tables. 
         The processed output is presisted back in S3 buckets as Parquet files. 
  
        Parameters: 
            spark: Spark session object
            input_data: Input S3 Bucket
            output_data: Output S3 Bucket
            
        Returns: Nothing
     """
    # FILE PATHS: song and log data files
    song_data = input_data + "song-data/*/*/*/*.json"
    log_data  = input_data + "log_data/*/*/*.json"
    
    # READ: song data file
    df_input_song = spark.read.json(song_data)
    
    # READ: log data file and filter by actions for song plays
    df_input_logs = spark.read.json(log_data)
    df_input_logs = df_input_logs.filter(df_input_logs.page == "NextSong")
   
    
    # FACT TABLE: fact_songplays 
    songs = df_input_song.alias("songs")
    logs  = df_input_logs.alias("logs")
    df_songplays = logs.join(songs, (logs.artist == songs.artist_name) & (logs.song == songs.title)) \
                    .withColumn("songplay_id", monotonically_increasing_id()) \
                    .withColumn("start_time", to_timestamp((logs.ts/1000).cast('timestamp'), "yyyy-MM-dd hh:mm:ss")) \
                    .selectExpr( "songplay_id", \
                                 "start_time", \
                                 "logs.userId AS user_id", \
                                 "logs.level",\
                                 "songs.song_id", \
                                 "songs.artist_id", \
                                 "logs.sessionId AS session_id", \
                                 "songs.artist_location AS location", \
                                 "logs.userId AS user_agent", \
                                 "year(CAST(start_time AS date)) AS year", \
                                 "month(CAST(start_time AS date)) AS month")   # year & month column used for partitioning
    df_songplays.drop_duplicates()
    
    # DIM TABLE 1: dim_songs
    df_songs = df_input_song.selectExpr("song_id",\
                                        "title", \
                                        "artist_id", \
                                        "year", \
                                        "duration")
    df_songs.drop_duplicates(subset=['song_id'])

    # DIM TABLE 2: dim_artists
    df_artists = df_input_song.filter("artist_id is not null and artist_id != ''") \
                               .selectExpr("artist_id",\
                                           "artist_name AS name",\
                                           "artist_location AS location",\
                                           "artist_latitude AS latitude",\
                                           "artist_longitude AS longitude")
    df_artists.drop_duplicates(subset=['artist_id'])
    
    # DIM TABLE 3: dim_users
    df_users = df_input_logs.filter("userId is not null and userId != ''") \
                            .selectExpr("userId AS user_id", \
                                        "firstName AS first_name", \
                                        "lastName AS last_name", \
                                        "gender", \
                                        "level")
    df_users = df_users.drop_duplicates(subset=['user_id'])
    
     # DIM TABLE 4: dim_time
    df_time = df_songplays.withColumn("hour" , hour(df_songplays.start_time)) \
                           .withColumn("day", dayofmonth(df_songplays.start_time)) \
                           .withColumn("week", weekofyear(df_songplays.start_time)) \
                           .withColumn("month", month(df_songplays.start_time)) \
                           .withColumn("year", year(df_songplays.start_time)) \
                           .withColumn("weekday", date_format(df_songplays.start_time, 'EEEE')) \
                           .select('start_time', 'hour', 'week', 'month', 'year', 'weekday')
    df_time = df_time.drop_duplicates()
    
    # write: as Parquet files
    #df_users.write.parquet("output/dim_users.parquet")
    #df_artists.write.parquet("output/dim_artists.parquet")
    #df_songs.write.partitionBy('year','artist_id').parquet("output/dim_songs.parquet")
    #df_time.write.partitionBy('year','month').parquet("output/dim_time.parquet")
    #df_songplays.write.partitionBy('year','month').parquet("output/fact_songplays.parquet")   
    df_songplays.write.mode('overwrite').partitionBy('year','month').parquet(output_data + "fact_songplays.parquet")
    df_users.write.mode('overwrite').parquet(output_data + "dim_users.parquet")
    df_artists.write.mode('overwrite').parquet(output_data + "dim_artists.parquet")
    df_songs.write.mode('overwrite').partitionBy('year','artist_id').parquet(output_data + "dim_songs.parquet")
    df_time.write.mode('overwrite').partitionBy('year','month').parquet(output_data + "dim_time.parquet")
   
    
def main():
    os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEYS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_ACCESS_KEYS']['AWS_SECRET_ACCESS_KEY']
    
    s3 = boto3.client('s3')
    
    create_staging_dir(s3)
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-output-parquet/"
    
    process_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
