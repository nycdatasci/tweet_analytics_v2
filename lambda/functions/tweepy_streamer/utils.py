# Import built-in libraries
import os
import json
import boto3

# Import installed libraries
import tweepy

# # Configure Logging
import logging
logger = logging.getLogger()
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))


def twitter_authorizer(consumer_key,
                       consumer_secret,
                       access_token,
                       access_token_secret):
    """Twitter API authentication.

    :param kwargs: Twitter API credentials.
    :return: Twitter API wrapper.
    """
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    return tweepy.API(auth)


class TweetStreamListener(tweepy.StreamListener):
    """
    A simple Twitter Streaming API wrapper
    """
    def __init__(self, writer):
        super().__init__()
        self.counter = 0
        self.writer = writer

    def on_status(self, status):
        data = status._json
        self.writer.write(data=data)
        # tweet counter increased by 1
        self.counter += 1

    def on_error(self, status_code):
        logger.error('Connection error: {}'.format(status_code))
        # Return True to restart
        return True


class SimpleWriter(object):
    """
    A simple Writer
    """
    def __init__(self):
        self.queue = list()

    def write(self, data):
        logger.info('created_at: {}\ttext: {}'.format(data['created_at'], data['text']))
        self.queue.append(data)

    def read(self):
        return self.queue


class FirehoseWriter(object):
    """
    A simple Kinesis Firehose wrapper
    """
    def __init__(self, DeliveryStreamName):
        self.firehose = boto3.client('firehose')
        if DeliveryStreamName in self.firehose.list_delivery_streams().get('DeliveryStreamNames', []):
            self.streamName = DeliveryStreamName
        else:
            raise ValueError('DeliveryStreamName not available')

    def write(self, data):
        try:
            tweet = json.dumps(data) + '\n'
        except json.JSONDecodeError as e:
            logger.error(e)
        else:
            response = self.firehose.put_record(
                DeliveryStreamName=self.streamName,
                Record={'Data': tweet},
            )
            logger.info('status: {}\ttext: {}'.format(response['ResponseMetadata']['HTTPStatusCode'], data['text']))