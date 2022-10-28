from datetime import datetime
import sys
import seaborn as sns

import numpy as np


import re
import pytz

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover

import findspark
findspark.init()

appName = 'Trending Twitter topics'
spark = SparkSession.builder.appName(appName).master("local[*]").getOrCreate()


def getDate(x):
    if x is not None:
        return str(datetime.strptime(x,'%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC).strftime("%Y-%m-%d %H:%M:%S"))
    else:
        return None

def clean_tweet(tweet):    
    r = tweet.lower()
    r = re.sub('rt', '', r)  #removing the retweet notice
    r = re.sub("@[A-Za-z0-9_]+","", r) #removing the at user notice
    r = re.sub("#" ,"", r) #removing hashtag sign, the hashtag itself can stay. However there is a separate entity in twitter object, hashtags
    r = re.sub(r'http\S+', '', r) #removing links
    r = re.sub('[()!?]', ' ', r)
    r = re.sub('\[.*?\]',' ', r)
    r = re.sub("[^a-z0-9]"," ", r)
    r = re.sub(r"\b\d+\b", "", r) #removing anomalies, single digits, won't be useful in an analysis
    r = re.sub('\s*\\b([a-z]|[a-z]{2})\\b', '', r) #removing single characters
    r = r.split()    
    r = " ".join(word for word in r)
    return r

#to eliminate non-Dutch hashtags
def clean_hashtags(word):
    word=re.sub('[^a-zA-Z0-9]', '', word)
    return word



def main():
    if len(sys.argv) != 2:
        print('You must specify the path to the file')        
    else:
        op_mode = int(input('Please enter the operating mode: 1 - top 5 topics per day/ 2 - top N topics per selected date range: '))
        if op_mode == 1:

            df = spark.read.json(sys.argv[1]) #reading the data into Spark DataFrame

            #selecting only the neccesary columns    
            tweets = df.select("created_at", "full_text", col("entities.hashtags.text").alias('hashtags')) 

            date_fn = udf(getDate, StringType()) #converting the functon into Spark user defined function
            udf_clean_tweet = udf(clean_tweet, StringType()) 
            
            tweets = tweets.withColumn("created_at", to_utc_timestamp(date_fn("created_at"),"UTC")) #actual conversion
            tweets = tweets.withColumn('full_text', udf_clean_tweet('full_text')) #clean tweets from media, links and mentions

            #split the strings of text, i.e. tokens
            tokenizer = Tokenizer(inputCol="full_text", outputCol="tokenized_text")
            tokenized_tweets = tokenizer.transform(tweets)
            
            

            #removing the stop words.
            stp_words = StopWordsRemover.loadDefaultStopWords('dutch')
            swr = StopWordsRemover(inputCol="tokenized_text", outputCol="filtered_text", stopWords=stp_words)

            tweets_sw = swr.transform(tokenized_tweets)

            #This is the go to method. However, due to the tweets being in Dutch, it limited the abilities to operate on the data. 
            #For example, there is no easy way to identify the nouns in Dutch, to try and calculate the occurence in tweets to identify the trensds.
            #The problem is with the stopwords list- however extended, couldn't clean the data completely. Plus the verbs and lack of knowledge of the language.
            #Instead, the chosen approach - count the frequency of the occuring hashtags. Twitter actually parses them and keeps it in entities array.

            #result = tweets_sw.withColumn('topic', explode('filtered_text')).groupBy('topic').count().sort('count', ascending=False)

            result = tweets_sw.withColumn('topic', explode('hashtags')).groupBy('topic').count().sort('count', ascending=False)

            #Since we got some non-Dutch hashtags, we'll eliminate them. Transforming Spark Dataframe to Pandas for convenience and speed

            pd_res = result.toPandas()

            pd_res['topic'] = pd_res['topic'].apply(clean_hashtags)
            pd_res['topic'] = pd_res['topic'].replace('', np.nan)
            pd_res = pd_res.dropna().reset_index(drop=True)

            #Getting top topics
            sl_pd_res = pd_res.loc[0:4]

            sl_pd_res.to_csv('trending_topics.csv', index= False)         

            #Plotting the countplot and saving it to the current directory
            img_plot = sns.barplot(data=sl_pd_res, x = 'topic', y = 'count')
            fig = img_plot.get_figure()
            fig.savefig("trending_topics.png")
            
        elif op_mode == 2:
            begin_date = (input('Please, enter the start timestamp for the trending topics interval: '))
            end_date = (input('Please, enter the end timestamp for the trending topics interval: ')  )          
            topic_nmbr = int(input('Please enter the number of trending topics you would like to see in the desried interval: '))

            dates = (begin_date, end_date)
            date_from, date_to = [to_date(lit(s)).cast(TimestampType()) for s in dates]

            df = spark.read.json(sys.argv[1]) #reading the data into Spark DataFrame   

            #getting only neccessary columns
            tweets = df.select("created_at", "full_text", col("entities.hashtags.text").alias('hashtags')) 

            #slice the DataFrame to the selected timestamp range
            tweets = tweets.where(col('created_at').between(*dates))

            date_fn = udf(getDate, StringType()) #converting the functon into Spark user defined function
            udf_clean_tweet = udf(clean_tweet, StringType()) 
            
            tweets = tweets.withColumn("created_at", to_utc_timestamp(date_fn("created_at"),"UTC")) #actual conversion
            tweets = tweets.withColumn('full_text', udf_clean_tweet('full_text')) #clean tweets from media, links and mentions            


            #split the strings of text, i.e. tokens
            tokenizer = Tokenizer(inputCol="full_text", outputCol="tokenized_text")
            tokenized_tweets = tokenizer.transform(tweets)
            
            

            #removing the stop words.
            stp_words = StopWordsRemover.loadDefaultStopWords('dutch')
            swr = StopWordsRemover(inputCol="tokenized_text", outputCol="filtered_text", stopWords=stp_words)

            

            #This is the go to method. However, due to the tweets being in Dutch, it limited the abilities to operate on the data. 
            #For example, there is no easy way to identify the nouns in Dutch, to try and calculate the occurence in tweets to identify the trensds.
            #The problem is with the stopwords list- however extended, couldn't clean the data completely. Plus the verbs and lack of knowledge of the language.
            #Instead, the chosen approach - count the frequency of the occuring hashtags. Twitter actually parses them and keeps it in entities array.

            #result = tweets_sw.withColumn('topic', explode('filtered_text')).groupBy('topic').count().sort('count', ascending=False)

            result = tweets_sw.withColumn('topic', explode('hashtags')).groupBy('topic').count().sort('count', ascending=False)

            #Since we got some non-Dutch hashtags, we'll eliminate them. Transforming Spark Dataframe to Pandas for convenience and speed

            pd_res = result.toPandas()

            pd_res['topic'] = pd_res['topic'].apply(clean_hashtags)
            pd_res['topic'] = pd_res['topic'].replace('', np.nan)
            pd_res = pd_res.dropna().reset_index(drop=True)

            #Getting top topics
            sl_pd_res = pd_res.loc[0:topic_nmbr-1]

            sl_pd_res.to_csv('trending_topics.csv', index = False)
            
            #Plotting the countplot and saving it to the current directory
            img_plot = sns.barplot(data=sl_pd_res, x = 'topic', y = 'count')
            fig = img_plot.get_figure()
            fig.savefig("trending_topics.png")           
        else:
            print ('Error! Only 2 operating modes currently available. Restart the program and select either 1 or 2')




if __name__ == '__main__':
    main()