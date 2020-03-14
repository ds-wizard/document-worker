import datetime
import pymongo
import pika
import uuid
import json

JSON_UUID = 'd3e98eb6-344d-481f-8e37-6a67b6cd1ad2'
HTML_UUID = 'a9293d08-59a4-4e6b-ae62-7a6a570b031c'
PDF_UUID = '68c26e34-5e77-4e15-9bf7-06ff92582257'
LATEX_UUID = 'dbc94579-40d7-42c3-975c-71e30d07778b'
DOCX_UUID = 'f4bd941a-dfbe-4226-a1fc-200fb5269311'

db_job = {
    'uuid': str(uuid.uuid4()),
    'name': 'Testing document',
    'state': 'QueuedDocumentState',
    'formatUuid': DOCX_UUID,
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
