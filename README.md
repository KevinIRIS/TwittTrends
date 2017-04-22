Idea:

There is a thread running in background and using Twitter API to fetch posts. Send the data to SQS, then use a thread pool to assign thread to extract message from SQS and use IBM API analyse the sentiment. publish the message to sns, sns notifies the endpoint to store the data to ES.Based on Django framework, build a webpage and send request to ElasticSearch to get data.

How to use:

fill in your owen Twitter key and Google Map key,

start serverend.py, which can make sure you can get posts in real time.

Open this webPage: http://custom-env-1.pmspppavfq.us-east-1.elasticbeanstalk.com/

Select keyword and then move your mouse to the marker, you will see what they said about this keyword.
