import os

from utils import *

# # Configure Logging
import logging

logger = logging.getLogger()
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))


# Environment Variables
REGION = os.getenv('AWS_REGION', default='us-east-2')
ES_ENDPOINT = os.getenv('ES_ENDPOINT')


def lambda_handler(event, context):
    reader = S3Reader()
    es_idx = ESIndexer(region=REGION, endpoint=ES_ENDPOINT)

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        logger.info('bucket: {}, key: {}'.format(bucket, key))
        items = [item for item in reader.read(bucket, key)]

        parser = TweetParser().parse_statuses(items)
        tweets = parser.get_tweets()
        places = parser.get_places()
        users = parser.get_users()

        es_idx.index(tweets, _index='tweets', _type='tweet_type')
        es_idx.index(places, _index='places', _type='place_type')
        es_idx.index(users, _index='users', _type='user_type')

    return {
        'status': 'success'
    }