import boto3
from . import configure
import json

class sns_service:

    def __init__(self):
        self.sns = boto3.resource("sns", aws_access_key_id = configure.aws_access_key_id, \
               aws_secret_access_key = configure.aws_secret_access_key)
        self.sns_topic = self.sns.Topic(configure.aws_sns_arn)

    def get_topic(self, topic):
        return self.sns.create_topic(Name=topic)

    def subscribe_topic(self,topic, endpoint):
        return self.sns_topic.subscribe(Protocol='http', Endpoint=endpoint)

