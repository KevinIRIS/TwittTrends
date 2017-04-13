from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import boto3
import configure


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
            #print(json_data)
            if json_data.get("lang") == "en":
                if json_data.get("user") is not None \
                        and json_data.get("user").get("name") is not None:
                    __username = json_data.get("user").get("name")
                    __count += 1
                if json_data.get("coordinates") is not None:
                    __geo = json_data.get("coordinates").get("coordinates")
                    print(json_data.get("coordinates").get("coordinates"))
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
                    #dictionary = {"location": {"lat": __lan, "lon": __long}, "username": __username,"timestamp": __time, "text": __text}
                    attributes = {"username":{"StringValue":__username, "DataType":"String"},"timestamp_ms":{"StringValue":str(__time), "DataType":"String"}, "lat":{"StringValue": str(__lan), "DataType": "String"}, "long" :{"StringValue": str(__long), "DataType": "String"}}
                    #message = json.dumps(dictionary)
                    #print(message)
                    self.__queue.send_message(MessageBody = __text, MessageAttributes = attributes)



    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(configure.consumer_key, configure.consumer_secret)
    auth.set_access_token(configure.access_token, configure.access_token_secret)

    stream = Stream(auth, l)
    stream.filter(locations=[-180,-90,180,90], stall_warnings = True)

    {'place': {'place_type': 'city', 'country_code': 'BR', 'country': 'Brazil', 'id': '68e019afec7d0ba5',
               'name': 'Sao Paulo', 'attributes': {}, 'full_name': 'Sao Paulo, Brazil',
               'bounding_box': {'type': 'Polygon', 'coordinates': [
                   [[-46.826039, -24.008814], [-46.826039, -23.356792], [-46.365052, -23.356792],
                    [-46.365052, -24.008814]]]}, 'url': 'https://api.twitter.com/1.1/geo/id/68e019afec7d0ba5.json'},
     'favorited': False, 'user': {'profile_use_background_image': True, 'notifications': None, 'utc_offset': None,
                                  'profile_text_color': '333333', 'listed_count': 255,
                                  'profile_image_url': 'http://pbs.twimg.com/profile_images/592088809782845440/y9iSmma7_normal.png',
                                  'name': '大空お天気bot2', 'follow_request_sent': None, 'contributors_enabled': False,
                                  'url': 'http://oozora-otenki.tumblr.com/usage', 'profile_background_tile': False,
                                  'profile_background_image_url': 'http://abs.twimg.com/images/themes/theme1/bg.png',
                                  'followers_count': 2826, 'profile_sidebar_border_color': 'C0DEED',
                                  'is_translator': False, 'screen_name': 'oozora_otenki', 'protected': False,
                                  'profile_sidebar_fill_color': 'DDEEF6', 'following': None, 'statuses_count': 33202,
                                  'default_profile': True, 'location': None,
                                  'profile_background_image_url_https': 'https://abs.twimg.com/images/themes/theme1/bg.png',
                                  'default_profile_image': False, 'lang': 'en', 'id_str': '3207467247',
                                  'created_at': 'Sat Apr 25 21:51:02 +0000 2015', 'time_zone': None, 'friends_count': 0,
                                  'verified': False, 'id': 3207467247, 'profile_link_color': '1DA1F2',
                                  'profile_banner_url': 'https://pbs.twimg.com/profile_banners/3207467247/1430014445',
                                  'description': 'GeoNames + Yahoo Weather API + Google/Flickr images + ImageMagick. @akari_oozora',
                                  'profile_image_url_https': 'https://pbs.twimg.com/profile_images/592088809782845440/y9iSmma7_normal.png',
                                  'profile_background_color': 'C0DEED', 'geo_enabled': True, 'favourites_count': 0},
     'timestamp_ms': '1492114213497', 'retweeted': False, 'contributors': None, 'lang': 'pt', 'filter_level': 'low',
     'truncated': False, 'is_quote_status': False, 'entities': {'media': [
        {'indices': [83, 106], 'display_url': 'pic.twitter.com/EniuCk8CD6', 'type': 'photo',
         'media_url_https': 'https://pbs.twimg.com/media/C9UYwRwXcAAlehG.jpg',
         'media_url': 'http://pbs.twimg.com/media/C9UYwRwXcAAlehG.jpg',
         'expanded_url': 'https://twitter.com/oozora_otenki/status/852614924585115650/photo/1',
         'id': 852614922135629824, 'id_str': '852614922135629824',
         'sizes': {'small': {'resize': 'fit', 'w': 680, 'h': 340}, 'medium': {'resize': 'fit', 'w': 960, 'h': 480},
                   'thumb': {'resize': 'crop', 'w': 150, 'h': 150}, 'large': {'resize': 'fit', 'w': 960, 'h': 480}},
         'url': 'https://t.co/EniuCk8CD6'}], 'hashtags': [], 'user_mentions': [
        {'indices': [0, 8], 'name': 'Brazilian Banyana', 'id_str': '35392456', 'screen_name': 'Konryuu',
         'id': 35392456}], 'symbols': [], 'urls': []}, 'in_reply_to_status_id_str': '852614778522611714',
     'in_reply_to_status_id': 852614778522611714, 'id': 852614924585115650, 'in_reply_to_user_id': 35392456,
     'extended_entities': {'media': [
         {'indices': [83, 106], 'display_url': 'pic.twitter.com/EniuCk8CD6', 'type': 'photo',
          'media_url_https': 'https://pbs.twimg.com/media/C9UYwRwXcAAlehG.jpg',
          'media_url': 'http://pbs.twimg.com/media/C9UYwRwXcAAlehG.jpg',
          'expanded_url': 'https://twitter.com/oozora_otenki/status/852614924585115650/photo/1',
          'id': 852614922135629824, 'id_str': '852614922135629824',
          'sizes': {'small': {'resize': 'fit', 'w': 680, 'h': 340}, 'medium': {'resize': 'fit', 'w': 960, 'h': 480},
                    'thumb': {'resize': 'crop', 'w': 150, 'h': 150}, 'large': {'resize': 'fit', 'w': 960, 'h': 480}},
          'url': 'https://t.co/EniuCk8CD6'}]},
     'source': '<a href="http://twitter.com/akari_oozora" rel="nofollow">大空お天気</a>', 'possibly_sensitive': False,
     'in_reply_to_screen_name': 'Konryuu', 'display_text_range': [9, 82], 'id_str': '852614924585115650',
     'favorite_count': 0, 'retweet_count': 0, 'created_at': 'Thu Apr 13 20:10:13 +0000 2017',
     'text': '@Konryuu São Paulo\nSão Paulo, Brazil\n20.4°C | 68.8°F\nHumidity: 77%\nOvercast Clouds https://t.co/EniuCk8CD6',
     'in_reply_to_user_id_str': '35392456', 'coordinates': {'type': 'Point', 'coordinates': [-46.63611, -23.5475]},
     'geo': {'type': 'Point', 'coordinates': [-23.5475, -46.63611]}}
