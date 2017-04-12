from concurrent.futures import ThreadPoolExecutor

import watson_developer_cloud
from . import configure
import boto3

sqs = boto3.resource("sqs",aws_access_key_id = configure.aws_access_key_id,\
                             aws_secret_access_key = configure.aws_secret_access_key)
queue = sqs.get_queue_by_name( QueueName = "twitt")


def analyse(queue):
    # TO DO
    pass



if __name__ == '__main__':
    thread_pool = ThreadPoolExecutor(max_workers=10)
    thread_pool.submit(analyse,queue)