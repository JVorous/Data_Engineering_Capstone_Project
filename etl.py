import configparser
import os
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Description: Instantiates connection to Spark session

        Arguments:
            None

        Returns:
            SparkSession object
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    return spark


def process_files_data(spark, input_data, output_data):
    """
        Description: Transforms raw song JSON files into analytics tables and stores them on S3

        Arguments:
            spark: reference to current SparkSession
            input_data: Starting folder for data
            output_data: output folder for analytical tables

        Returns:
            None
    """
    # get filepath to census data file
    census_data = input_data + 'New_York_City_Population_by_Borough__1950_-_2040.json'

    song_schema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),
        StructField("num_songs", IntegerType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("year", IntegerType()),
    ])

    # read song data file with supplied schema
    df = spark.read.json(song_data, schema=song_schema)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(
        output_data + 'songs_table.parquet',
        partitionBy=['year', 'artist_id'],
        mode='overwrite'
    )

    # extract columns to create artists table
    artists_table = df.select(
        'artist_id',
        F.col('artist_name').alias('name'),
        F.col('artist_location').alias('location'),
        F.col('artist_latitude').alias('latitude'),
        F.col('artist_longitude').alias('longitude')
    )

    # write artists table to parquet files
    artists_table.write.parquet(
        output_data + 'artists_table.parquet',
        mode='overwrite'
    )


def main():
    """
        Description: Main driver function. Creates spark session and then calls for data processing.

        Arguments:
            None

        Returns:
            None
    """
    spark = create_spark_session()

    input_data = config['S3']['IN_BUCKET']
    output_data = config['S3']['OUT_BUCKET']

    process_files_data(spark, input_data, output_data)

    spark.stop()


if __name__ == "__main__":
    main()

