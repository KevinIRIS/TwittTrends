from . import configure
from elasticsearch import Elasticsearch
import json
maxTweets = 1000000


class eservice():
    def __init__(self):
        self.index = 0
        __domain = configure.aws_es_domain
        self.__es = Elasticsearch(hosts=[{'host': __domain, 'port': 80, 'use_ssl': False}])
        __body = {"mappings": {"sentimenttwitter": {"properties": {"username": {"type": "string"}, "timestamp": {"type": "date"},
                                                        "location": {"type": "geo_point"},
                                                        "content": {"type": "string"},
                                                        "sentiment": {"type": "string"}
                                                        }}}}
        if self.__es.indices.exists(index="sentimenttwitter") is True:
            self.__es.indices.create(index="sentimenttwitter", ignore=400, body = __body)

    def send_to_es(self,json_meesage_from_sns):
        self.index = self.index % maxTweets + 1
        message = json.loads(json_meesage_from_sns)
        print(message)
        if message is not None:
            long = message["long"]["StringValue"]
            lat = message["lat"]["StringValue"]
            username = message["username"]["StringValue"]
            timestamp = message["timestamp_ms"]["StringValue"]
            content = message["content"]
            sentiment = message["sentiment"]
            dictionary = {"location": {"lat": lat, "lon": long}, "username": username, "timestamp": timestamp,
                          "content": content, "sentiment": sentiment}
            self.__es.index(index="sentimenttwitter", doc_type='sentimenttwitter', id=self.index, body=json.dumps(dictionary))
