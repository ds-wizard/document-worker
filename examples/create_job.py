import datetime
import pymongo
import pika
import uuid
import json

db_job = {
    'uuid': str(uuid.uuid4()),
    'name': 'Testing document',
    'state': 'QueuedDocumentState',
    'format': 'html',
    'templateUuid': '43a3fdd1-8535-42e0-81a7-5edbff296e65',
    'createdAt': datetime.datetime.utcnow()
}

with open('example-rabbitmq.json', mode='r') as fp:
    queue_job = json.load(fp)
queue_job['documentUuid'] = db_job['uuid']

mongo_client = pymongo.MongoClient(host='localhost', port=27017)
db = mongo_client['test_db']
collection = db['documents']
job_id = str(collection.insert_one(db_job).inserted_id)
print(f'Created job with document UUID {db_job["uuid"]} ({job_id})')

mq = pika.BlockingConnection(
        parameters=pika.ConnectionParameters(host='localhost', port=5672)
    )
channel = mq.channel()
channel.queue_declare(queue='test_queue', durable=True)
channel.basic_publish(exchange='', routing_key='test_queue', body=json.dumps(queue_job).encode('utf-8'))
print(f'Published to queue')
mq.close()
