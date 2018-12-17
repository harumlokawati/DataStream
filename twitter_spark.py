from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import pandas as pd
import sys
import json
import requests

def aggregate_tags_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

def inv_agg(new_values, total_sum):
  return sum(new_values) - (total_sum or 0)

def get_sql_context_instance(spark_context):
  if ('sqlContextSingletonInstance' not in globals()):
    globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
  return globals()['sqlContextSingletonInstance']

def process_rdd_bad_word(time, rdd):
  try:
    sql_context = get_sql_context_instance(rdd.context)
    # convert the RDD to Row RDD
    row_rdd = rdd.map(lambda w: Row(badword=w[0], counter=w[1]))
    # create a DF from the Row RDD
    hashtags_df = sql_context.createDataFrame(row_rdd)
    # Register the dataframe as table
    hashtags_df.registerTempTable("bad_words")
    # get the top 10 hashtags from the table using SQL and print them
    hashtag_counts_df = sql_context.sql("select badword, counter from bad_words order by counter desc limit 10")
    count_distinct = sql_context.sql('select distinct(badword) from bad_words').count()
    print("distinct bad word : " + str(count_distinct))
    hashtag_counts_df.show()
    # call this method to prepare top 10 hashtags DF and send them
    # send_df_to_dashboard(hashtag_counts_df)
  except:
    e = sys.exc_info()[0]
    print("Error: %s" % e)

def process_rdd_word(time, rdd):
  try:
    sql_context = get_sql_context_instance(rdd.context)
    # convert the RDD to Row RDD
    row_rdd = rdd.map(lambda w: Row(word=w[0], counter=w[1]))
    # create a DF from the Row RDD
    hashtags_df = sql_context.createDataFrame(row_rdd)
    # Register the dataframe as table
    hashtags_df.registerTempTable("words")
    # get the top 10 hashtags from the table using SQL and print them
    hashtag_counts_df = sql_context.sql("select word, counter from words order by counter desc limit 10")
    count_distinct = sql_context.sql('select distinct(word) from words').count()
    print("distinct word : " + str(count_distinct))
    hashtag_counts_df.show()
    # call this method to prepare top 10 hashtags DF and send them
    # send_df_to_dashboard(hashtag_counts_df)
  except:
    e = sys.exc_info()[0]
    print("Error: %s" % e)

def process_rdd_user(time, rdd):
  try:
    sql_context = get_sql_context_instance(rdd.context)
    # convert the RDD to Row RDD
    row_rdd = rdd.map(lambda w: Row(bad_user=w[0], counter=w[1]))
    # create a DF from the Row RDD
    hashtags_df = sql_context.createDataFrame(row_rdd)
    # Register the dataframe as table
    hashtags_df.registerTempTable("badusers")
    # get the top 10 hashtags from the table using SQL and print them
    hashtag_counts_df = sql_context.sql("select bad_user, counter from badusers order by counter desc limit 10")
    count_distinct = sql_context.sql('select distinct(bad_user) from badusers').count()
    print("distinct user : " + str(count_distinct))
    hashtag_counts_df.show()
    # call this method to prepare top 10 hashtags DF and send them
    # send_df_to_dashboard(hashtag_counts_df)
  except:
    e = sys.exc_info()[0]
    print("Error: %s" % e)

def process_rdd(time, rdd):
  print("----------- %s -----------" % str(time))
  try:
    sql_context = get_sql_context_instance(rdd.context)
    # convert the RDD to Row RDD
    row_rdd = rdd.map(lambda w: Row(category=w[0], counter=w[1]))
    # create a DF from the Row RDD
    hashtags_df = sql_context.createDataFrame(row_rdd)
    # Register the dataframe as table
    hashtags_df.registerTempTable("categories")
    # get the top 10 hashtags from the table using SQL and print them
    hashtag_counts_df = sql_context.sql("select category, counter from categories order by counter desc limit 10")
    hashtag_counts_df.show()
    # call this method to prepare top 10 hashtags DF and send them
    # send_df_to_dashboard(hashtag_counts_df)
  except:
    e = sys.exc_info()[0]
    print("Error: %s" % e)

def send_df_to_dashboard(df):
	# extract the hashtags from dataframe and convert them into array
	top_tags = [str(t.hashtag) for t in df.select("hashtag").collect()]
	# extract the counts from dataframe and convert them into array
	tags_count = [p.hashtag_count for p in df.select("hashtag_count").collect()]
	# initialize and send the data through REST API
	url = 'http://localhost:5001/updateData'
	request_data = {'label': str(top_tags), 'data': str(tags_count)}
	response = requests.post(url, data=request_data)

def sampling_func(rdd):
  return rdd.sample(False,0.5,10)

def filter_harsh_word(tweet, data):
  tweet += "|||"
  bad = False
  for word in data:
    w = " " + word[0] + " "
    if w.lower() in tweet.lower():
      bad = True
      tweet += word[0] + ","
  if bad:
    return tweet + "|||!FILTEREDBAD"
  return tweet + "-|||!FILTEREDNOTBAD"

def extract_value(tweet):
  values = tweet.split("|||")
  return values

def get_bad_words(tweet):
  return tweet[4].split(',')[:-1]

def get_words(tweet):
  return tweet[3].lower().split(' ')

# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 2)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream("localhost",9009)
data = pd.read_csv("bad.csv").values.tolist()
print(data)

sampled = dataStream.transform(sampling_func)

# trial = sampled.map(lambda s : extract(s))

bad_filtered = sampled.map(lambda s: filter_harsh_word(s, data))
# split each tweet into words
extracted = bad_filtered.map(lambda x: extract_value(x)).filter(lambda x: len(x)>5)

# words = extracted.flatMap(lambda line: line.split(" "))
categories = extracted.map(lambda x : (x[5],1))
tags_totals = categories.updateStateByKey(aggregate_tags_count)
# do processing for each RDD generated in each interval
tags_totals.foreachRDD(process_rdd)
bad_words = extracted.flatMap(lambda x: get_bad_words(x))
bad_Words_count = bad_words.map(lambda x: (x, 1)).updateStateByKey(aggregate_tags_count)
bad_Words_count.foreachRDD(process_rdd_bad_word)
users = extracted.filter(lambda x: x[5] == '!FILTEREDBAD').map(lambda x : (x[0],1)).updateStateByKey(aggregate_tags_count)
users.foreachRDD(process_rdd_user)
# total_user = users.updateStateByKey(aggregate_tags_count)
# total_user.foreachRDD(process_rdd)

words = extracted.filter(lambda x: x[5] == '!FILTEREDBAD').flatMap(lambda x: get_words(x)).map(lambda x: (x,1))
agg_words = words.updateStateByKey(aggregate_tags_count)
agg_words.foreachRDD(process_rdd_word)


# filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
# hashtags = words.filter(lambda w: "!FILTERED" in w).map(lambda x: (x, 1))
# adding the count of each hashtag to its last count
# window_val = categories.reduceByKeyAndWindow(aggregate_tags_count, inv_agg, 8, 4)
# window_val.foreachRDD(process_rdd)

# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()