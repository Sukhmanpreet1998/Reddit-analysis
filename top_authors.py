from pyclbr import Function
import sys
from scipy import stats
from pyspark.sql import SparkSession, functions, types 

spark = SparkSession.builder.appName('reddit extracter').getOrCreate()

reddit_submissions_path = 'submissions_merged.json'
reddit_comments_path = 'comments_merged.json'

comments_schema = types.StructType([
    types.StructField('archived', types.BooleanType()),
    types.StructField('author', types.StringType()),
    types.StructField('body', types.StringType()),
    types.StructField('controversiality', types.LongType()),
    types.StructField('created_utc', types.StringType()),
    types.StructField('distinguished', types.StringType()),
    types.StructField('downs', types.LongType()),
    types.StructField('edited', types.StringType()),
    types.StructField('gilded', types.LongType()),
    types.StructField('id', types.StringType()),
    types.StructField('name', types.StringType()),
    types.StructField('parent_id', types.StringType()),
    types.StructField('retrieved_on', types.LongType()),
    types.StructField('score', types.LongType()),
    types.StructField('score_hidden', types.BooleanType()),
    types.StructField('subreddit', types.StringType()),
    types.StructField('subreddit_id', types.StringType()),
    types.StructField('ups', types.LongType()),
    types.StructField('year', types.IntegerType()),
    types.StructField('month', types.IntegerType()),
])

submissions_schema = types.StructType([
    types.StructField('archived', types.BooleanType()),
    types.StructField('author', types.StringType()),
    types.StructField('created', types.LongType()),
    types.StructField('created_utc', types.StringType()),
    types.StructField('distinguished', types.StringType()),
    types.StructField('domain', types.StringType()),
    types.StructField('downs', types.LongType()),
    types.StructField('edited', types.BooleanType()),
    types.StructField('from', types.StringType()),
    types.StructField('gilded', types.LongType()),
    types.StructField('id', types.StringType()),
    types.StructField('is_self', types.BooleanType()),
    types.StructField('link_flair_css_class', types.StringType()),
    types.StructField('link_flair_text', types.StringType()),
    types.StructField('media', types.StringType()),
    types.StructField('name', types.StringType()),
    types.StructField('num_comments', types.LongType()),
    types.StructField('over_18', types.BooleanType()),
    types.StructField('permalink', types.StringType()),
    types.StructField('quarantine', types.BooleanType()),
    types.StructField('retrieved_on', types.LongType()),
    types.StructField('saved', types.BooleanType()),
    types.StructField('score', types.LongType()),
    types.StructField('secure_media', types.StringType()),
    types.StructField('selftext', types.StringType()),
    types.StructField('stickied', types.BooleanType()),
    types.StructField('subreddit', types.StringType()),
    types.StructField('subreddit_id', types.StringType()),
    types.StructField('thumbnail', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('ups', types.LongType()),
    types.StructField('url', types.StringType()),
    types.StructField('year', types.IntegerType()),
    types.StructField('month', types.IntegerType()),
])

def main():

    #Read files from folders
    reddit_comments = spark.read.json(reddit_comments_path, schema = comments_schema)
    reddit_submissions = spark.read.json(reddit_submissions_path, schema = submissions_schema)
    
    #Calculate Mean of scores and gilded for comments
    mean_values_comments = reddit_comments.agg(
    functions.mean('score').alias('mean_score'),
    functions.mean('gilded').alias('mean_gilded')
    ).collect()[0]


    #Filter comments on basis of score and gilded means of comments
    filtered_authors_comments = reddit_comments.filter(
    (reddit_comments['score'] > mean_values_comments['mean_score']) &
    (reddit_comments['gilded'] > mean_values_comments['mean_gilded']) 
    )

    #Calculate Mean of scores and gilded for submissions
    mean_values_submission = reddit_submissions.agg(
    functions.mean('score').alias('mean_score'),
    functions.mean('gilded').alias('mean_gilded')
    ).collect()[0]

     #Filter submissions on basis of score and gilded means of submissions
    filtered_authors_submissions = reddit_submissions.filter(
    (reddit_submissions['score'] > mean_values_submission['mean_score']) &
    (reddit_submissions['gilded'] > mean_values_submission['mean_gilded']) 
    ) 

    # write the results, we are looking for details of authors
    filtered_authors_comments.select('author', 'score', 'gilded').coalesce(1).write.json("Top_authors_comments.json", mode='overwrite')
    filtered_authors_submissions.select('author', 'score', 'gilded').coalesce(1).write.json("Top_authors_submissions.json", mode='overwrite')


main()
