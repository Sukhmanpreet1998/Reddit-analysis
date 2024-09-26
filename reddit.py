import sys
assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
from pyspark.sql import SparkSession, functions, types
from pyspark.sql import SparkSession
import datetime
from pyspark.ml.stat import ChiSquareTest


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

comments_schema = types.StructType([
    types.StructField('archived', types.BooleanType()),
    types.StructField('author', types.StringType()),
    types.StructField('author_flair_css_class', types.StringType()),
    types.StructField('author_flair_text', types.StringType()),
    types.StructField('body', types.StringType()),
    types.StructField('controversiality', types.LongType()),
    types.StructField('created_utc', types.StringType()),
    types.StructField('distinguished', types.StringType()),
    types.StructField('downs', types.LongType()),
    types.StructField('edited', types.StringType()),
    types.StructField('gilded', types.LongType()),
    types.StructField('id', types.StringType()),
    types.StructField('link_id', types.StringType()),
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

def main(inputs, output):
    submissions=spark.read.json(f"{inputs}/submissions", schema=submissions_schema)
    comments = spark.read.json(f"{inputs}/comments", schema=comments_schema)

# To analyse author and comments for year/month
    comments = comments.filter(comments['year'].isNotNull())
    comments=comments.filter(comments['author']!='[deleted]') #[deleted] author means that post has been deleted
    grouped=comments.groupBy('year','author')
    comm_count= grouped.agg(functions.count("*"))
    comm_count=comm_count.filter(comm_count['count(1)']>100) # filtering the comment count to be greater than 0
    comm_count=comm_count.orderBy(functions.desc('count(1)'))

    #Group by author to get the top 20 commenters over the period 2016-2021
    grouped2=comm_count.groupBy('author')
    top_list=grouped2.agg(functions.sum('count(1)'))
    top_list=top_list.orderBy(functions.desc('sum(count(1))'))
    top_list=top_list.select('author')
    top_list=top_list.limit(10)
    top_list.write.csv(output,mode='overwrite')

    #Filter the comments data for top 20 commenters to verify the trend in comments over time  (Below concept has been used in Jupyter notebook for plotting)

    comments=comments.select(comments['month'],comments['author'])
    grouped3 = comments.groupBy('month', 'author')
    comm_count_top = grouped3.agg(functions.count("*"))
    comm_count_top = comm_count_top.filter(comm_count_top['author'].isin(top_list['author']))
   
# To analyse if the is_self posts make any difference in score
    sub_grouped=submissions.groupBy("is_self")
    sub_group=sub_grouped.agg(functions.count('*'), functions.sum('score'))

    true_Set=sub_group.filter(sub_group['is_self']=='true')
    true_Set_l=true_Set.collect()
    false_Set = sub_group.filter(sub_group['is_self'] == 'false')
    false_Set_l = false_Set.collect()

    av_score_is_Self = true_Set_l[0]['sum(score)']/true_Set_l[0]['count(1)']
    av_score_not_Self = false_Set_l[0]['sum(score)'] / false_Set_l[0]['count(1)']

    print(av_score_is_Self)
    print(av_score_not_Self)






if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.4' # make sure we have Sprk 3.4+
    spark.sparkContext.setLogLevel('WARN')
   

    main(inputs, output)