import sys
assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
from pyspark.sql import SparkSession, functions, types
from pyspark.sql import SparkSession
import datetime


reddit_submissions_path = '/Users/sukhi/Desktop/cmpt353/reddit/2021_out/submissions'
output1 = '/Users/sukhi/Desktop/cmpt353/reddit/training'

def extract_hour(timestamp):
    utc_datetime = datetime.datetime.utcfromtimestamp(int(timestamp))
    return utc_datetime.hour

def convert_to_day_number(timestamp):
    utc_datetime = datetime.datetime.utcfromtimestamp(int(timestamp))
    day_number = utc_datetime.weekday() + 1  # Monday is 1, Sunday is 7
    if day_number >= 7:
        day_number = 7  # Ensure Sunday remains as 7
    return day_number

submissions_schema = types.StructType([
    #types.StructField('archived', types.BooleanType()),
    types.StructField('author', types.StringType()),
    #types.StructField('created', types.LongType()),
    types.StructField('created_utc', types.StringType()),
    #types.StructField('distinguished', types.StringType()),
    #types.StructField('domain', types.StringType()),
    #types.StructField('downs', types.LongType()),
    #types.StructField('edited', types.BooleanType()),
    #types.StructField('from', types.StringType()),
    #types.StructField('gilded', types.LongType()),
    #types.StructField('id', types.StringType()),
    #types.StructField('is_self', types.BooleanType()),
    #types.StructField('link_flair_css_class', types.StringType()),
    #types.StructField('link_flair_text', types.StringType()),
    #types.StructField('media', types.StringType()),
    #types.StructField('name', types.StringType()),
    types.StructField('num_comments', types.LongType()),
    #types.StructField('over_18', types.BooleanType()),
    #types.StructField('permalink', types.StringType()),
    #types.StructField('quarantine', types.BooleanType()),
    #types.StructField('retrieved_on', types.LongType()),
    #types.StructField('saved', types.BooleanType()),
    types.StructField('score', types.LongType()),
    #types.StructField('secure_media', types.StringType()),
    #types.StructField('selftext', types.StringType()),
    #types.StructField('stickied', types.BooleanType()),
    types.StructField('subreddit', types.StringType()),
    #types.StructField('subreddit_id', types.StringType()),
    #types.StructField('thumbnail', types.StringType()),
    #types.StructField('title', types.StringType()),
    #types.StructField('ups', types.LongType()),
    #types.StructField('url', types.StringType()),
    #types.StructField('year', types.IntegerType()),
    types.StructField('month', types.IntegerType()),
])

def main(outputa):
   extract_hour_udf = functions.udf(extract_hour, types.IntegerType())
   convert_to_day_number_udf =functions.udf(convert_to_day_number, types.IntegerType())
   #read the submissions and comments files
   submissions = spark.read.json(reddit_submissions_path, schema = submissions_schema)
   submissions = submissions.withColumn('hour', extract_hour_udf('created_utc'))
   submissions  = submissions.withColumn('day', convert_to_day_number_udf('created_utc')).drop('created_utc')
   submissions.coalesce(1).write.csv(output1, mode='append', header=True)

if __name__ == '__main__':
    outputa = sys.argv[1]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.4' # make sure we have Sprk 3.4+
    spark.sparkContext.setLogLevel('WARN')
   

    main(outputa)