from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import os
import configparser
import logging

# Setup of variables
logging.info("SETTING UP variables...")
config = configparser.ConfigParser()
with open("dl.cfg", "r") as f:
    config.read_file(f)

song_data_path = os.path.join(config["DATA"]["input_path"], "song_data/*/*/*/*.json")
log_data_path = os.path.join(config["DATA"]["input_path"], "log_data/*.json")
output_path = config["DATA"]["output_path"]
tables = ["songs", "artists", "users", "time", "songplays"]

# Schema for log_data and song_data
schema = {
    "log_data": T.StructType() \
                    .add("artist", T.StringType())
                    .add("auth", T.StringType())
                    .add("firstName", T.StringType())
                    .add("gender", T.StringType())
                    .add("itemInSession", T.IntegerType())
                    .add("lastName", T.StringType())
                    .add("length", T.FloatType())
                    .add("level", T.StringType())
                    .add("location", T.StringType())
                    .add("method", T.StringType())
                    .add("page", T.StringType())
                    .add("registration", T.FloatType())
                    .add("sessionId", T.IntegerType())
                    .add("song", T.StringType())
                    .add("status", T.IntegerType())
                    .add("ts", T.StringType())
                    .add("userAgent", T.StringType())
                    .add("userId", T.StringType()),
    "song_data": T.StructType() \
                    .add("artist_id", T.StringType())
                    .add("artist_latitude", T.FloatType())
                    .add("artist_location", T.StringType())
                    .add("artist_longitude", T.FloatType())
                    .add("artist_name", T.StringType())
                    .add("duration", T.FloatType())
                    .add("num_songs", T.IntegerType())
                    .add("song_id", T.StringType())
                    .add("title", T.StringType())
                    .add("year", T.IntegerType())
}

def initialize_SparkSession():
    """
    Initializes a SparkSession if none is currently running and returns the SparkSession object. 
    """
    logging.info("INITIALIZING SparkSession...")
    spark = SparkSession \
                .builder \
                .appName("Sparkify_Data_Lake") \
                .getOrCreate()

    return spark

def process_log_data(spark, schema, input, output):
    """
    Processes log_data and creating users, time, and songplays tables.

    Params:
        spark(spark.SparkSession): A SparkSession object
        schema(spark.sql.types.StructType): A StructType object that represents the schema of the table
        input(str): Input path
        output(str): Output path
    """
    logging.info("PROCESSING log_data...")
    # Read log_data
    df = (spark.read.json(input, schema = schema, mode = "PERMISSIVE")) \
                .where(F.col("page") == "NextSong")

    # # Create users table and write as parquet
    logging.info("CREATING users...")
    users_table = df.selectExpr(["userId as user_id", "firstName as first_name",
                                 "lastName as last_name", "gender", "level"]) \
                    .dropDuplicates()
    users_table.write \
               .partitionBy("last_name") \
               .parquet(os.path.join(output, "users"), mode = "overwrite")

    # Create time table and write as parquet
    logging.info("CREATING time...")
    df = df.withColumn("start_time", F.to_timestamp(F.from_unixtime(df["ts"]/1000), format = "YYYY-MM-dd HH:mm:ss"))
    time_table = df.select(["start_time", F.hour("start_time").alias("hour"),
                            F.dayofmonth("start_time").alias("day"), F.weekofyear("start_time").alias("week"),
                            F.month("start_time").alias("month"), F.year("start_time").alias("year"), 
                            F.dayofweek("start_time").alias("weekday")]) \
                   .dropDuplicates()
    time_table.write \
              .partitionBy(["year", "month"]) \
              .parquet(os.path.join(output, "time"), mode = "overwrite")

    # Read songs table 
    df_songs = spark.read.parquet(os.path.join(output, "songs"))
    df = df.join(df_songs, df.song == df_songs.title, how = "inner")
    df = df.withColumn("songplay_id", F.monotonically_increasing_id()+1)

    # Create songsplays table and write as parquet
    logging.info("CREATING songplays...")
    songplays_table = df.select(["songplay_id", "start_time",
                                 F.col("userId").alias("user_id"), "level",
                                 df_songs.song_id.alias("song_id"), "artist_id",
                                 F.col("sessionId").alias("session_id"), "location",
                                 F.col("userAgent").alias("user_agent")])
    
    songplays_table.write \
                   .parquet(os.path.join(output, "songplays"), mode = "overwrite")    

    return logging.info("PROCESSED log_data.")

def process_song_data(spark, schema, input, output):
    """
    Processes song_data and creating songs and artists tables.

    Params:
        spark(spark.SparkSession): A SparkSession
        schema(spark.sql.types.StructType): A StructType object that represents the schema of the table
        input(str): Input path
        output(str): Output path
    """
    logging.info("PROCESSING song_data...")

    # Read song_data
    df = spark.read.json(input, schema = schema, mode = "PERMISSIVE")

    # Create songs table and write as parquet
    logging.info("CREATING songs...")
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).dropDuplicates()
    songs_table.write \
               .partitionBy(["year", "artist_id"]) \
               .parquet(os.path.join(output, "songs"), mode = "overwrite")

    # Create artists table and write as parquet
    logging.info("CREATING artists...")
    artists_table = df.selectExpr(["artist_id", "artist_name as name", 
                                   "artist_location as location", "artist_latitude as latitude",
                                   "artist_longitude as longitude"]) \
                      .dropDuplicates()
    artists_table.write \
                 .parquet(os.path.join(output, "artists"), mode = "overwrite")

    return logging.info("PROCESSED song_data.")

def main():
    """
    Function used to create an ETL pipeline that loads data from an S3 bucket,
    performs transformations using Spark and then loads the output into an S3 bucket.
    Usage: python.exe etl.py Windows
           python     etl.py Linux/Mac 
    """
    spark = initialize_SparkSession()
    process_song_data(spark, schema.get("song_data"), song_data_path, output_path)
    spark.catalog.clearCache()
    process_log_data(spark, schema.get("log_data"), log_data_path, output_path)

    # Count number of rows for each table
    for table in tables:
        try:
            df = spark.read.parquet(os.path.join(output_path, table))
            logging.info(f"{table}: ROWS = {df.count()}")
        except Exception as e:
            logging.error(e)

    spark.stop()

if __name__ == "__main__":
    main()