from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import boto3
from . import configure


class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def __init__(self):
        sqs = boto3.resource("sqs",aws_access_key_id = configure.aws_access_key_id,\
                             aws_secret_access_key = configure.aws_secret_access_key)
        self.__queue = sqs.get_queue_by_name( QueueName = "twitt")
        print(self.__queue)

    def on_data(self, data):
        if data is not None:
            __count = 0
            json_data = json.loads(data)
            print(json_data)
            if json_data.get("user") is not None \
                    and json_data.get("user").get("name") is not None \
                    and json_data.get("user").get("lang") == "en":
                __username = json_data.get("user").get("name")
                __count += 1

            if json_data.get("coordinates") is not None:
                __geo = json_data.get("coordinates").get("coordinates")
                __long = __geo[0]
                __lan = __geo[1]
                __count += 1
            elif json_data.get("place") is not None:
                __geo = json_data.get("place").get("bounding_box").get("coordinates")[0]
                __long = __geo[0][0]
                __lan = __geo[0][1]
                __count += 1
            if json_data.get("text") is not None:
                __text = json_data.get("text")
                __count += 1
            if json_data.get("timestamp_ms") is not None:
                __time = json_data.get("timestamp_ms")
                __count += 1
            if __count == 4:
                dictionary = {"location": {"lat": __lan, "lon": __long}, "username": __username,"timestamp": __time, "text": __text}
                message = json.dumps(dictionary)
                print(message)
                self.__queue.send_message(MessageBody = message)



    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(configure.consumer_key, configure.consumer_secret)
    auth.set_access_token(configure.access_token, configure.access_token_secret)

    stream = Stream(auth, l)
    stream.filter(locations=[-180,-90,180,90], stall_warnings = True)

