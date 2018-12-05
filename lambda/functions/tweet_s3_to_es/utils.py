import os
import json
from functools import partialmethod

import tweepy

import boto3
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.helpers import bulk

# # Configure Logging
import logging

logger = logging.getLogger()
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))


class S3Reader(object):

    def __init__(self):
        self.s3 = boto3.client(service_name='s3')

    def read(self, bucket, key):
        for t in self.s3.get_object(Bucket=bucket, Key=key)['Body'].read().strip().split(b'\n'):
            try:
                item = json.loads(t)
            except json.JSONDecodeError as e:
                logger.error(e)
            else:
                yield item


class TweetParser(object):
    tweet_attributes = ['created_at', 'text', 'source', 'in_reply_to_status_id', 'in_reply_to_user_id',
                        'quote_count', 'reply_count', 'retweet_count', 'favorite_count',
                        'coordinates']

    user_attributes = ['name', 'screen_name', 'location', 'url', 'description',
                       'followers_count', 'friends_count', 'listed_count', 'favourites_count',
                       'statuses_count', 'created_at', 'lang', 'profile_image_url']

    place_attributes = ['place_type', 'full_name', 'country_code', 'url']

    def __init__(self):
        self.api = tweepy.API()
        self.statuses = []

    def parse_statuses(self, items):
        self.statuses = tweepy.Status().parse_list(api=self.api, json_list=items)
        return self

    def get_tweets(self):
        for status in self.statuses:
            dict_ = self._to_dict(status, self.tweet_attributes)

            for obj in ('user', 'place'):
                if hasattr(status, obj) and getattr(status, obj):
                    dict_[obj + '_id'] = getattr(status, obj).id

            if hasattr(status, 'entities'):
                dict_['entities'] = self._parse_entities(status.entities)
            if hasattr(status, 'extended_entities'):
                dict_['extented_entities'] = self._parse_extented_entities(status.extended_entities)

            yield dict_

    def _parse_entities(self, entities):
        hashtags = [hashtag['text'] for hashtag in entities.get('hashtags', [])]
        user_mentions = [{'screen_name': mention['screen_name'],
                          'id': mention['id']} for mention in entities.get('user_mentions', [])]
        media = [media['media_url'] for media in entities.get('media', [])]
        urls = [url['expanded_url'] for url in entities.get('urls', [])]
        return {'hashtags': hashtags,
                'user_mentions': user_mentions,
                'media': media,
                'urls': urls}

    def _parse_extented_entities(self, extented_entities):
        media = [{'media_url': media['media_url'],
                  'type': media['type']} for media in extented_entities.get('media', [])]
        return {'media': media}

    def _get_objects(self, obj, attributes):
        for status in self.statuses:
            if hasattr(status, obj) and getattr(status, obj):
                yield self._to_dict(getattr(status, obj), attributes)

    def _to_dict(self, tweet_obj, attributes):
        dict_ = dict()
        if hasattr(tweet_obj, 'id_str'):
            dict_['id'] = getattr(tweet_obj, 'id_str')
        elif hasattr(tweet_obj, 'id'):
            dict_['id'] = getattr(tweet_obj, 'id')
        for attr in attributes:
            if hasattr(tweet_obj, attr) and getattr(tweet_obj, attr) is not None:
                dict_[attr] = getattr(tweet_obj, attr)
        return dict_

    get_users = partialmethod(_get_objects, obj='user', attributes=user_attributes)
    get_places = partialmethod(_get_objects, obj='place', attributes=place_attributes)


class ESIndexer(object):

    def __init__(self, region, endpoint):
        session_var = boto3.session.Session()
        credentials = session_var.get_credentials()
        awsauth = AWS4Auth(credentials.access_key, credentials.secret_key,
                           region, 'es',
                           session_token=credentials.token)
        self.es = Elasticsearch(host=endpoint,
                                port=443,
                                connection_class=RequestsHttpConnection,
                                http_auth=awsauth,
                                use_ssl=True,
                                verify_ssl=True)
        logger.info(self.es.info())

    def index(self, objects, _index, _type):
        counter = 0

        def data_generator():
            nonlocal counter
            for obj in objects:
                bulk_doc = {
                    '_type': _type,
                    '_id': obj['id'],
                    '_source': obj
                }
                counter += 1
                yield bulk_doc

        success, _ = bulk(self.es, data_generator(), index=_index)
        logger.info('ElasticSearch indexed {}/{} documents to {}'.format(success, counter, _index))
