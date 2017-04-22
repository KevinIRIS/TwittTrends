from elasticsearch import Elasticsearch

from TwittTrends.backend.eservice import eservice
from TwittTrends.backend.sns_service import sns_service

#print(sns_service().subscribe_topic("sss",""))

x = "{\"content\": \"@Natures_Voice hello we saw this today over Penzance, is it a buzzard? https://t.co/8OqoFFOin2\", \"sentiment\": \"positive\", \"lat\": {\"DataType\": \"String\", \"StringValue\": \"49.882472\"}, \"username\": {\"DataType\": \"String\", \"StringValue\": \"Budgies Roadtrip\"}, \"long\": {\"DataType\": \"String\", \"StringValue\": \"-6.368504\"}, \"timestamp_ms\": {\"DataType\": \"String\", \"StringValue\": \"1492716914082\"}}"
eservice().send_to_es(x)
