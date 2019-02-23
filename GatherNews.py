import pandas as pd
from pandas.io.json import json_normalize
import requests
from pprint import pprint
import json
import os
from datetime import datetime, timedelta
import glob
import boto3
import psycopg2
from sqlalchemy import create_engine, MetaData
import sqlalchemy
import Config



class GatherNews:
    api_key = '63bac41779514dca80fe7eb25971361a'
    today = datetime.today().date()

    def __init__(self, country, category):
        self.country = country
        self.category = category
        self.endpoint = 'https://newsapi.org/v2/top-headlines?country={}&apiKey={}&category={}&pageSize=100'.format(
            self.country, self.api_key, self.category)

    def pullNews(self):

        # Pull todays data
        r = requests.get(self.endpoint)
        data = json.loads(r.content)
        data = data['articles']

        # Get hour of pull
        timeOfPull = str(datetime.now())
        timeOfPull = timeOfPull[:-7].replace(':', '')

        # Dump this pulls data
        df = json_normalize(data)

        return df

    def cleanCSVFile(self, df):
        # Remove unnecessary columns
        df.drop(labels=['source.id'], axis=1, inplace=True)

        # Convert dates to datetimes
        df['publishedAt'] = pd.to_datetime(df['publishedAt'], format='%Y-%m-%d %H:%M:%S')
        df['publishedAt'] = df['publishedAt'].dt.strftime(date_format='%Y-%m-%d %H:%M:%S')

        # Remove the .com from the source
        df['source.name'] = df['source.name'].str.replace('.com', '')

        # Lowercase and remove the punctuation from the string columns
        stringColumns = ['author', 'content',
                         'description', 'source.name', 'title']
        for col in stringColumns:
            df[col] = df[col].str.lower()
            df[col] = df[col].str.replace('[^\w\s]', '')

        df.columns = ['author', 'content', 'description', 'published', 'source',
                    'title', 'url', 'url_image']

        # Connect to DB to check what URLs are in the database already
        credentials = Config.read_credentials()
        conn = psycopg2.connect(dbname='sportsnews',
                                user='shawn87411',
                                host='sportsnewsdbinstance.cf8tqavognjx.us-east-1.rds.amazonaws.com',
                                password=credentials['postgres']['password'])

        cur = conn.cursor()

        # Query for recent URLs in database 
        sqlQuery = open('queries/checkForDuplicates.sql', 'r').read()
        sqlQuery = (sqlQuery.replace('TWO DAYS AGO', str(self.today-timedelta(days=2)))
                            .replace('TOMORROW', str(self.today+timedelta(days=1))))
        urlsInDatabase = pd.read_sql(sqlQuery, conn)

        # Remove rows already in database from our new data
        df = df[(~df['title'].isin(urlsInDatabase['title'])) & (~df['url'].isin(urlsInDatabase['url']))]

        return df

    def pushDataToPostgres(self, df):
        # Connect to database
        credentials = Config.read_credentials()
        engine = create_engine(r'postgresql://{}:{}@sportsnewsdbinstance.cf8tqavognjx.us-east-1.rds.amazonaws.com/sportsnews'.format(credentials['postgres']['user'], credentials['postgres']['password']))
        
        # If there are no new rows to add return None
        if df.empty:
            print ("No new rows to add to database")
            return None

        # If we do have new data to pass
        else:
            df.to_sql('news_first_clean', engine, if_exists='append', index=False)
            print ('{} rows added to database!'.format(len(df)))
            return None


if __name__ == '__main__':
    GN = GatherNews('us', 'sports')
    df = GN.pullNews()
    df2 = GN.cleanCSVFile(df)
    GN.pushDataToPostgres(df2)
