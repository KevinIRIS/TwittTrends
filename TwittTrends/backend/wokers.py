from concurrent.futures import ThreadPoolExecutor
from time import sleep

from  watson_developer_cloud import natural_language_understanding_v1
import watson_developer_cloud.natural_language_understanding.features.v1 as \
    features
import boto3
import configure
import json


sqs = boto3.resource("sqs",aws_access_key_id = configure.aws_access_key_id, \
                             aws_secret_access_key = configure.aws_secret_access_key)
queue = sqs.get_queue_by_name(QueueName="twitt")

def analyse(queue):
    messages = queue.receive_messages(MessageAttributeNames=['All'], VisibilityTimeout=30, MaxNumberOfMessages=10)
    for message in messages:
        if message.body is not None and message.message_attributes is not None:
            print(message.body)
            nlp = natural_language_understanding_v1(version='2017-02-27',\
                username= configure.ibm_username,\
                password= configure.ibm_password)
            print(nlp)
            response = nlp.analyze(text = message.body, features = [features.Entities(), features.Keywords()])
            print("sss")
            print(response)



if __name__ == '__main__':
    thread_pool = ThreadPoolExecutor(max_workers=10)
    while True:
        thread_pool.submit(analyse,queue)
        sleep(1)