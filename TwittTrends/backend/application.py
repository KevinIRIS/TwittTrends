import json

from django.views.decorators.csrf import csrf_exempt

from .eservice import  eservice
from django.http import HttpResponse
from elasticsearch import Elasticsearch
import re
from . import configure
import os.path
BASE = os.path.dirname(os.path.abspath(__file__))

message_queue = []
elastic = eservice()

@csrf_exempt
def sns_request(request):
    # message_queue.append("xxxxxxxx")
    # types = json.loads(request.body)["Type"]
    # if types == 'SubscriptionConfirmation':
    #     print("sub")
    # elif types == 'Notification':
    #     message_queue.append("xxxx")
    #     received_json_data = json.loads(request.body)
    #     message = received_json_data['Message']
    #
    #     data_json = json.dumps(message)  # to string
    #     # message_queue.append(data_json)
    #     parse_data(data_json)  # save to elastic search
    type = request.META.get('HTTP_X_AMZ_SNS_MESSAGE_TYPE')
    message_queue.append("add")
    if type == 'SubscriptionConfirmation':
        received_json_data = json.loads(request.body)
        url = received_json_data['SubscribeURL']
        print(url)
    elif type == 'Notification':
        received_json_data = json.loads(request.body.decode('utf8'))
        message = received_json_data['Message']
        message_queue.append(message)
        elastic.send_to_es(message)
    {"Type": "Notification", "MessageId": "798e552d-25f1-506a-97f8-04598ce40d05",
     "TopicArn": "arn:aws:sns:us-east-1:000916848138:twittertrend",
     "Message": "{\"content\": \"@Natures_Voice hello we saw this today over Penzance, is it a buzzard? https://t.co/8OqoFFOin2\", \"sentiment\": \"positive\", \"lat\": {\"DataType\": \"String\", \"StringValue\": \"49.882472\"}, \"username\": {\"DataType\": \"String\", \"StringValue\": \"Budgies Roadtrip\"}, \"long\": {\"DataType\": \"String\", \"StringValue\": \"-6.368504\"}, \"timestamp_ms\": {\"DataType\": \"String\", \"StringValue\": \"1492716914082\"}}",
     "Timestamp": "2017-04-22T00:41:57.094Z", "SignatureVersion": "1",
     "Signature": "RyJfUIZ9eDdXGR6j9L0o/Lw9Dph607sqX8Slw1YmwnmqLO8eAc143j8uV7ft82T8r12mtE+LfG8s1bpbnNp6yOz/ZgYgfBBv+gY6n4AT9OCX4gOR0Gu+XnSyErG/CoExDmI2+FxrQN5xIuq9VSNu/+NhptwHGt15uKV40Hu6Zm3HMQbiAfll1x5JQrhkyhomQ6xqz31kN0yhthmvsRjk34aOiNvKm8+37zGM5IMvmGRjMR4l397oFIqvLExOt/Lt12yBQRzct5EsznHytVic7j6TENnPJpUxyIskt/M47udqA3PlOa9JzbT+kRbkbPFKmHxgrMn24qdAt9k4yIN+nQ==",
     "SigningCertURL": "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-b95095beb82e8f6a046b3aafc7f4149a.pem",
     "UnsubscribeURL": "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:000916848138:twittertrend:7bf21d9c-9117-4fbd-bf88-170a876480e1"}

    {"Type": "SubscriptionConfirmation", "MessageId": "b6772f74-3f8f-4c21-81f0-8603d297768a",
     "Token": "2336412f37fb687f5d51e6e241d59b68c9f41a4238fa20855bc65d052e9e1dd060566935c2af8848c349f6b5255125ed8888d203d38d35ae61efac1737fce1b074d6d3371ca5f9ac41cefdf0876b46f1e2dcb0f6c507c79e7a3be504b245055c877f8d2ab2ecfff3191ab63ec67ee754",
     "TopicArn": "arn:aws:sns:us-east-1:000916848138:twittertrend",
     "Message": "You have chosen to subscribe to the topic arn:aws:sns:us-east-1:000916848138:twittertrend.\nTo confirm the subscription, visit the SubscribeURL included in this message.",
     "SubscribeURL": "https://sns.us-east-1.amazonaws.com/?Action=ConfirmSubscription&TopicArn=arn:aws:sns:us-east-1:000916848138:twittertrend&Token=2336412f37fb687f5d51e6e241d59b68c9f41a4238fa20855bc65d052e9e1dd060566935c2af8848c349f6b5255125ed8888d203d38d35ae61efac1737fce1b074d6d3371ca5f9ac41cefdf0876b46f1e2dcb0f6c507c79e7a3be504b245055c877f8d2ab2ecfff3191ab63ec67ee754",
     "Timestamp": "2017-04-22T00:40:13.026Z", "SignatureVersion": "1",
     "Signature": "DpaKy74tKmhVanpvokVwPuYvqENoRpPljHXKAnnLvEXNKFhpHDtjdnj2rQHajH0NvteC9s4y9vPb8xMlF9ToyH7aSRKzYpu6N03j7plbJMSB18IZ2cxMhsmegV32nT3Y0ZQ8nnmtMa0Ofe/HFSlWZnQtagbY7ODhRgoyIChfHoWsCvs0oTAzYIcGv5dlyYNzuaJ8Bk88TBrp0I7kq1/vdXoAjLar0HI6aQIqa4h+nmJO3Gx9R1EB/3250YlcSkKqSPJS6NDMM8HE1oRrZFzvKEL1DwouozVCquntu87Xq7QZU2Fdt2zORWQggOpldSbS2LGCYrkwxpkAQqIdihLBWg==",
     "SigningCertURL": "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-b95095beb82e8f6a046b3aafc7f4149a.pem"}
def get_query(request, keyword):
    es = Elasticsearch([{'host': configure.aws_es_domain, 'port': 80,'use_ssl': False}])
    keyword = re.sub('[^A-Za-z0-9]+', '', keyword)
    data = es.search(index="sentimenttwitter", body={"sort": [{"timestamp": {"order": "desc"}}], "query": {"match": {"content": keyword}}, "size": 100})['hits']
    response = HttpResponse(json.dumps(data), content_type="application/json")
    return response
def get_geo(request, lat, long):
    es = Elasticsearch([{'host': configure.aws_es_domain, 'port': 80,'use_ssl': False}])
    data = es.search(index= "sentimenttwitter" , body={ "query":
      { "bool" :
            { "must" : { "match_all" : {} },
              "filter" :
                  { "geo_distance" :
                        { "distance" : "1000km", "location" : str(lat) + "," + str(long) }
                    }
              }
        }
    })['hits']
    response = HttpResponse(json.dumps(data), content_type="application/json")
    return response

def display(request):
    html = "<html><body>Failed to load tweetmap.</body></html>"
    with open(os.path.join(BASE, './../../templates/index.html'), 'r') as myfile:
        html = myfile.read()
        myfile.close()
    return HttpResponse(html)
def update(request):
    if message_queue:
        return HttpResponse(message_queue.pop(-1))
    else:
        return HttpResponse('hello')