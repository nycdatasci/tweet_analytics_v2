# Import  libraries
import json
import os
import time

import tweepy
from utils import *

import logging

# Configure Logging
logger = logging.getLogger()
logger.setLevel(getattr(logging, os.getenv('log_level', 'INFO')))

# Environment Variables
# twitter credentials
access_token = os.getenv('access_token')
access_token_secret = os.getenv('access_token_secret')
consumer_key = os.getenv('consumer_key')
consumer_secret = os.getenv('consumer_secret')

# track - a comma-separated list of phrases which will be used to determine what Tweets will be delivered on the stream.
track = os.getenv('track', 'datascience, dataengineering')
# timeout - time in seconds to listen to the stream
timeout = int(os.getenv('timeout', '20'))
# firehose delivery stream name
stream_name = os.getenv('stream_name', '')


def lambda_handler(event, context):
    logger.info('Initializing writer')
    if stream_name:
        logger.info('Using FirehoseWriter (stream: {})'.format(stream_name))
        writer = FirehoseWriter(DeliveryStreamName=stream_name)
    else:
        logger.info('Using SimpleWriter')
        writer = SimpleWriter()

    logger.info('Initializing stream listener')
    listener = TweetStreamListener(writer=writer)
    api = twitter_authorizer(consumer_key,
                             consumer_secret,
                             access_token,
                             access_token_secret)
    streamer = tweepy.Stream(auth=api.auth,
                             listener=listener,
                             retry_count=3)
    logger.info('Start streaming tweets containing one or more words in `{}`'.format(track))
    streamer.filter(track=[track], languages=['en'], is_async=True)
    time.sleep(int(timeout))
    logger.info('Disconnecting twitter API...')
    streamer.disconnect()
    logger.info('Total tweets received in {} seconds: {}'.format(timeout, listener.counter))
    return event