from concurrent.futures import ThreadPoolExecutor
from time import sleep

from watson_developer_cloud import NaturalLanguageUnderstandingV1
import watson_developer_cloud.natural_language_understanding.features.v1 as \
    features
import boto3
import configure
import json


sqs = boto3.resource("sqs",aws_access_key_id = configure.aws_access_key_id, \
                             aws_secret_access_key = configure.aws_secret_access_key)
queue = sqs.get_queue_by_name(QueueName="twitt")
sns = boto3.resource("sns", aws_access_key_id = configure.aws_access_key_id, \
               aws_secret_access_key = configure.aws_secret_access_key)
sns_topic = sns.Topic(configure.aws_sns_arn)


def analyse(queue,sns_topic):
    messages = queue.receive_messages(MessageAttributeNames=['All'], VisibilityTimeout=30, MaxNumberOfMessages=10)
    for message in messages:
        if message.body is not None and message.message_attributes is not None:
            #print(message.body)
            sns_message = {}
            nlp = NaturalLanguageUnderstandingV1(version='2017-02-27',\
                url = "https://gateway.watsonplatform.net/natural-language-understanding/api",\
                username = configure.ibm_username,\
                password = configure.ibm_password)
            response = nlp.analyze(text = message.body, features=[features.Sentiment()])
            print(response["sentiment"]["document"]["label"])
            if response["sentiment"]["document"]["label"] is not None:
                username = message.message_attributes["username"]
                sentiment = response["sentiment"]["document"]["label"]
                lat = message.message_attributes["lat"]
                long = message.message_attributes["long"]
                timestamp_ms = message.message_attributes["timestamp_ms"]
                sns_message["username"] = username
                sns_message["content"] = message.body
                sns_message["lat"] = lat
                sns_message["long"] = long
                sns_message["sentiment"] = sentiment
                sns_message["timestamp_ms"] = timestamp_ms
                message_for_send = json.dumps(sns_message)
                response1 = sns_topic.publish(Message = message_for_send)
                print(response1)
            else:
                print("sentiment analyse error!")
            #{'language': 'en', 'sentiment': {'document': {'score': 0.0, 'label': 'neutral'}}}


if __name__ == '__main__':
    thread_pool = ThreadPoolExecutor(max_workers=10)
    while True:
        thread_pool.submit(analyse,queue,sns_topic)
        sleep(1)

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

