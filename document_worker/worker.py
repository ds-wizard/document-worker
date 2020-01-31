import datetime
import functools
import gridfs
import json
import logging
import pika
import pymongo
import sys
import uuid

from document_worker.builder import DocumentBuilder
from document_worker.config import DocumentWorkerConfig
from document_worker.conversions import FormatConvertor
from document_worker.formats import Formats
from document_worker.templates import TemplateRegistry


class JobException(Exception):

    def __init__(self, job_id: str, message: str):
        self.job_id = job_id
        self.message = message


class DocumentState:
    QUEUED = 'QueuedDocumentState'
    PROCESSING = 'InProgressDocumentState'
    FAILED = 'ErrorDocumentState'
    FINISHED = 'DoneDocumentState'


class DocumentField:
    UUID = 'uuid'
    STATE = 'state'
    TEMPLATE = 'templateUuid'
    FORMAT = 'format'
    RETRIEVED = 'retrievedAt'
    FINISHED = 'finishedAt'
    METADATA = 'metadata'
    METADATA_CONTENT_TYPE = 'contentType'
    METADATA_FILENAME = 'fileName'


class JobDataField:
    DOCUMENT_UUID = 'documentUuid'
    DOCUMENT_CONTEXT = 'documentContext'


def handle_job_step(message):
    def decorator(func):
        @functools.wraps(func)
        def handled_step(job, *args, **kwargs):
            try:
                return func(job, *args, **kwargs)
            except JobException as e:
                raise e
            except Exception as e:
                job._raise_exc(f'{message}: [{type(e).__name__}] {e}')
        return handled_step
    return decorator


class Job:

    DOCUMENT_FIELDS = [
        DocumentField.STATE,
        DocumentField.TEMPLATE,
        DocumentField.FORMAT,
    ]

    def __init__(self, config: DocumentWorkerConfig, document_builder: DocumentBuilder):
        self.config = config
        self.document_builder = document_builder
        self.mongo_client = pymongo.MongoClient(**config.mongo.mongo_client_kwargs)
        self.mongo_db = self.mongo_client[config.mongo.database]
        self.mongo_collection = self.mongo_db[config.mongo.collection]
        self.mongo_fs = gridfs.GridFS(self.mongo_db, config.mongo.fs_collection)

        self.doc_uuid = 'unknown'
        self.doc_context = dict()
        self.doc_filter = None
        self.doc = None
        self.base_file = None
        self.final_file = None
        self.target_format = None

    def _raise_exc(self, message: str):
        raise JobException(self.doc_uuid, message)

    def _modify_doc(self, modification: dict):
        return self.mongo_collection.find_one_and_update(
            self.doc_filter, {'$set': modification},
            return_document=pymongo.ReturnDocument.AFTER
        )

    @handle_job_step('Failed to process job body')
    def process_body(self, body):
        data = json.loads(body.decode('utf-8'))
        if JobDataField.DOCUMENT_UUID not in data.keys():
            self._raise_exc('Job data in body does not contain document UUID')
        self.doc_uuid = data[JobDataField.DOCUMENT_UUID]
        self.doc_filter = {DocumentField.UUID: self.doc_uuid}
        self.doc_context = data.get(JobDataField.DOCUMENT_CONTEXT, self.doc_context)

    @handle_job_step('Failed to connect to Mongo database')
    def connect_mongo(self):
        # TODO: retry
        host = self.config.mongo.host
        port = self.config.mongo.port
        db = self.config.mongo.database
        collection = self.config.mongo.collection

        logging.info(f'Connecting to Mongo DB @ {host}:{port}/{db}')
        collections = self.mongo_db.list_collection_names()
        if collection not in collections:
            self._raise_exc(f'Collection "{collection}" not found in Mongo database')

    @handle_job_step('Failed to get job details from Mongo DB')
    def get_job(self):
        logging.info(f'Getting the document "{self.doc_uuid}" details from Mongo DB')
        self.doc = self._modify_doc({DocumentField.RETRIEVED: datetime.datetime.utcnow()})
        if self.doc is None:
            self._raise_exc(f'Document "{self.doc_uuid}" not found')
        logging.info(f'Job "{self.doc_uuid}" details received')

    @handle_job_step('Failed to verify job details')
    def verify_job(self):
        logging.info(f'Verifying the received job "{self.doc_uuid}" details')
        # verify fields
        for field in self.DOCUMENT_FIELDS:
            if field not in self.doc.keys():
                self._raise_exc(f'Missing field "{field}" in the job details')
        # verify state
        state = self.doc[DocumentField.STATE]
        logging.info(f'Original state of job is {state}')
        if state == DocumentState.FINISHED:
            self._raise_exc(f'Job is already finished')
        # verify template
        template_uuid = uuid.UUID(self.doc[DocumentField.TEMPLATE])
        if not self.document_builder.template_registry.has_template(template_uuid):
            self._raise_exc(f'Template {template_uuid} not found')
        # verify format and conversion
        target_format_name = self.doc[DocumentField.FORMAT].lower()
        self.target_format = Formats.get(target_format_name)
        if self.target_format is None:
            self._raise_exc(f'Unknown target format {target_format_name}')
        if self.target_format != Formats.JSON:
            source_format = self.document_builder.template_registry[template_uuid].output_format
            if not self.document_builder.format_convertor.can_convert(source_format, self.target_format):
                self._raise_exc(f'Cannot convert {source_format} to {target_format_name}')

    @handle_job_step('Failed to build final document')
    def build_document(self):
        logging.info(f'Building document by rendering template with context')
        template_uuid = uuid.UUID(self.doc[DocumentField.TEMPLATE])
        self.final_file = self.document_builder.build_document(
            template_uuid, self.doc_context, self.target_format
        )

    @handle_job_step('Failed to store document in GridFS')
    def store_document(self):
        # TODO: retry
        host = self.config.mongo.host
        port = self.config.mongo.port
        db = self.config.mongo.database

        logging.info(f'Storing file to GridFS @ {host}:{port}/{db}')
        document_uuid = self.doc[DocumentField.UUID]

        file_id = self.mongo_fs.put(
            self.final_file.content,
            filename=document_uuid
        )
        logging.info(f'File {document_uuid} stored with id {file_id}')

    def finalize(self):
        document_uuid = self.doc[DocumentField.UUID]
        self._modify_doc({
            DocumentField.FINISHED: datetime.datetime.utcnow(),
            DocumentField.STATE: DocumentState.FINISHED,
            DocumentField.METADATA: {
                DocumentField.METADATA_CONTENT_TYPE: self.final_file.format.content_type,
                DocumentField.METADATA_FILENAME: self.final_file.filename(document_uuid)
            }
        })
        logging.info(f'Document {document_uuid} record finalized')

    def set_job_state(self, state: str):
        return self._modify_doc({DocumentField.STATE: state})

    def try_set_job_state(self, state: str) -> bool:
        try:
            result = self.set_job_state(state)[DocumentField.STATE]
            return result == DocumentState.FAILED
        except Exception as e:
            logging.warning(f'Tried to set state of {self.doc_uuid} to {state} but failed: {e}')
            return False


class DocumentWorker:

    def __init__(self, config: DocumentWorkerConfig, templates_dir):
        self.config = config
        self.document_builder = DocumentBuilder(
            TemplateRegistry(templates_dir),
            FormatConvertor()  # TODO: pass configs
        )
        self._prepare_logging()
        self.document_builder.template_registry.load_templates()

    def _prepare_logging(self):
        logging.basicConfig(
            stream=sys.stdout,
            level=self.config.logging.level,
            format=self.config.logging.message_format
        )

    def run(self):
        # TODO: retry
        queue = self.config.mqueue.queue
        logging.info(f'Connecting to MQ @ {self.config.mqueue.host}:{self.config.mqueue.port}')
        mq = pika.BlockingConnection(
            parameters=self.config.mqueue.connection_parameters
        )
        channel = mq.channel()
        channel.basic_qos(prefetch_count=1)
        logging.info(f'Waiting for messages in queue "{queue}"')
        channel.queue_declare(queue=queue, durable=True)

        channel.basic_consume(queue=queue, on_message_callback=self._callback, auto_ack=False)
        channel.start_consuming()
        logging.info(f'Consuming stopped')

    def _callback(self, ch, method, properties, body):
        logging.info(f'Received a job')
        job = Job(self.config, self.document_builder)
        try:
            job.process_body(body)
            job.connect_mongo()
            job.get_job()
            job.verify_job()
            job.set_job_state(DocumentState.PROCESSING)
            job.build_document()
            job.store_document()
            job.finalize()
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logging.info('Job ACKed')
        except JobException as e:
            logging.error(f'({e.job_id}) {e.message}')
            if job.try_set_job_state(DocumentState.FAILED):
                logging.info(f'({e.job_id}) Set state to {DocumentState.FAILED}')
                logging.info('Job ACKed')
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                logging.warning(f'({e.job_id}) Could not set state to {DocumentState.FAILED}')
        except Exception as e:
            logging.error(f'Job failed with error: {e}')
            if job.try_set_job_state(DocumentState.FAILED):
                logging.info(f'Set state to {DocumentState.FAILED}')
                ch.basic_ack(delivery_tag=method.delivery_tag)
                logging.info('Job ACKed')
            else:
                logging.warning(f'Could not set state to {DocumentState.FAILED}')
        finally:
            logging.info(f'Job processing finished')
