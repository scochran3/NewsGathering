import luigi
from luigi.contrib.s3 import S3Target, S3Client
from luigi.contrib.postgres import PostgresTarget
from GatherNews import GatherNews
import s3fs
import pandas as pd
import boto3
import Config
from datetime import datetime
import psycopg2


class PullNews(luigi.Task):

    # AWS Credentials
    credentials = Config.read_credentials()
    client = S3Client(aws_access_key_id=credentials['aws']['aws_access_key_id'], 
                        aws_secret_access_key = credentials['aws']['aws_secret_access_key'])

    # Get time of run
    timeOfPull = str(datetime.now())
    timeOfPull = timeOfPull[:-7].replace(':', '')

    def output(self):
        
        return S3Target('s3://sports-news-data/data/rawData/{}.csv'.format(self.timeOfPull), client=self.client)


    def run(self):
        GN = GatherNews('us', 'sports')
        result = GN.pullNews()

        f = self.output().open('w')
        result.to_csv(f, index=None, encoding='utf8')
        f.close()


class ProcessRawData(luigi.Task):

    credentials = Config.read_credentials()
    client = S3Client(aws_access_key_id=credentials['aws']['aws_access_key_id'], 
                      aws_secret_access_key = credentials['aws']['aws_secret_access_key'])

    def requires(self):
        return PullNews()

    def output(self):
        return S3Target('s3://sports-news-data/data/processedData/{}.csv'.format(self.input().path[-21:-4])
                , client=self.client)
        
    def run(self):
        df = pd.read_csv(self.input().open('r'), sep=',')
        GN = GatherNews('us', 'sports')
        result = GN.cleanCSVFile(df)
        f = self.output().open('w')
        result.to_csv(f, index=None)
        f.close()

class PushNewsToDatabase(luigi.Task):

    credentials = Config.read_credentials()

    def requires(self):
        return ProcessRawData()

    def run(self):
        df = pd.read_csv(self.input().open('r'), sep=',')
        GN = GatherNews('us', 'sports')
        GN.pushDataToPostgres(df)

if __name__ == '__main__':
    luigi.run(["--local-scheduler"], main_task_cls=PushNewsToDatabase)
