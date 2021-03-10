import datetime
import functools
import gridfs
import json
import logging
import pika
import pymongo
import sys
import uuid
import tenacity

from typing import Optional

from document_worker.config import DocumentWorkerConfig
from document_worker.consts import JobDataField, DocumentField, DocumentState
from document_worker.documents import DocumentFile, DocumentNameGiver
from document_worker.logging import DocWorkerLogger, DocWorkerLoggerWrapper, DocWorkerLogFilter
from document_worker.templates import TemplateRegistry

RETRY_MONGO_MULTIPLIER = 0.5
RETRY_MONGO_TRIES = 3
RETRY_MQ_MULTIPLIER = 0.5
RETRY_MQ_TRIES = 10


class JobException(Exception):

    def __init__(self, job_id: str, message: str):
        self.job_id = job_id
        self.message = message


def handle_job_step(message):
    def decorator(func):
        @functools.wraps(func)
        def handled_step(job, *args, **kwargs):
            try:
                return func(job, *args, **kwargs)
            except JobException as e:
                job.log.debug('Handling job exception', exc_info=True)
                raise e
            except Exception as e:
                job.log.debug('Handling exception', exc_info=True)
                job.raise_exc(f'{message}: [{type(e).__name__}] {e}')
        return handled_step
    return decorator


class JobContext:

    def __init__(self, config: DocumentWorkerConfig, template_registry: TemplateRegistry,
                 name_giver: DocumentNameGiver):
        self.config = config
        self.template_registry = template_registry
        self.name_giver = name_giver


class Job:

    DOCUMENT_FIELDS = [
        DocumentField.STATE,
        DocumentField.TEMPLATE,
        DocumentField.FORMAT,
    ]

    def __init__(self, ctx: JobContext):
        self.config = ctx.config
        self.template_registry = ctx.template_registry
        self.name_giver = ctx.name_giver
        self.mongo_client = pymongo.MongoClient(**ctx.config.mongo.mongo_client_kwargs)
        self.mongo_db = self.mongo_client[ctx.config.mongo.database]
        self.mongo_collection = self.mongo_db[ctx.config.mongo.collection]
        self.mongo_fs = gridfs.GridFS(self.mongo_db, ctx.config.mongo.fs_collection)

        self.template = None
        self.trace_uuid = str(uuid.uuid4())
        self.doc_uuid = 'unknown'
        self.doc_context = dict()
        self.doc_filter = None  # type: Optional[dict]
        self.doc = None  # type: Optional[dict]
        self.final_file = None  # type: Optional[DocumentFile]

        self.log = DocWorkerLoggerWrapper(
            trace_id=self.trace_uuid,
            document_id=self.doc_uuid,
        )

    def raise_exc(self, message: str):
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
            self.raise_exc('Job data in body does not contain document UUID')
        self.doc_uuid = data[JobDataField.DOCUMENT_UUID]
        self.log.document_id = self.doc_uuid
        self.doc_filter = {DocumentField.UUID: self.doc_uuid}
        self.doc_context = data.get(JobDataField.DOCUMENT_CONTEXT, self.doc_context)

    @handle_job_step('Failed to connect to Mongo database')
    @tenacity.retry(
        reraise=True,
        wait=tenacity.wait_exponential(multiplier=RETRY_MONGO_MULTIPLIER),
        stop=tenacity.stop_after_attempt(RETRY_MONGO_TRIES),
        before=tenacity.before_log(logging.getLogger(), logging.INFO),
        after=tenacity.after_log(logging.getLogger(), logging.INFO),
    )
    def connect_mongo(self):
        host = self.config.mongo.host
        port = self.config.mongo.port
        db = self.config.mongo.database
        collection = self.config.mongo.collection

        self.log.info(f'Connecting to Mongo DB @ {host}:{port}/{db}')
        collections = self.mongo_db.list_collection_names()
        if collection not in collections:
            self.raise_exc(f'Collection "{collection}" not found in Mongo database')

    @handle_job_step('Failed to get job details from Mongo DB')
    def get_job(self):
        self.log.info(f'Getting the document "{self.doc_uuid}" details from Mongo DB')
        self.doc = self._modify_doc({DocumentField.RETRIEVED: datetime.datetime.utcnow()})
        if self.doc is None:
            self.raise_exc(f'Document "{self.doc_uuid}" not found')
        self.log.info(f'Job "{self.doc_uuid}" details received')

    @handle_job_step('Failed to prepare job')
    def prepare_job(self):
        self.log.info(f'Verifying the received job "{self.doc_uuid}" details')
        # verify fields
        for field in self.DOCUMENT_FIELDS:
            if field not in self.doc.keys():
                self.raise_exc(f'Missing field "{field}" in the job details')
        # verify state
        state = self.doc[DocumentField.STATE]
        self.log.info(f'Original state of job is {state}')
        if state == DocumentState.FINISHED:
            self.raise_exc(f'Job is already finished')
        # prepare template
        template_id = self.doc[DocumentField.TEMPLATE]
        self.template = self.template_registry.get_template(template_id)
        if self.template is None:
            self.raise_exc(f'Template {template_id} not found')
        # prepare format
        format_uuid = uuid.UUID(self.doc[DocumentField.FORMAT])
        if not self.template.prepare_format(format_uuid):
            self.raise_exc(f'Format {format_uuid} (in template {template_id}) not found')

    @handle_job_step('Failed to build final document')
    def build_document(self):
        self.log.info(f'Building document by rendering template with context')
        format_uuid = uuid.UUID(self.doc[DocumentField.FORMAT])
        self.final_file = self.template.render(
            format_uuid, self.doc_context
        )

    @handle_job_step('Failed to store document in GridFS')
    @tenacity.retry(
        reraise=True,
        wait=tenacity.wait_exponential(multiplier=RETRY_MONGO_MULTIPLIER),
        stop=tenacity.stop_after_attempt(RETRY_MONGO_TRIES),
        before=tenacity.before_log(logging.getLogger(), logging.INFO),
        after=tenacity.after_log(logging.getLogger(), logging.INFO),
    )
    def store_document(self):
        host = self.config.mongo.host
        port = self.config.mongo.port
        db = self.config.mongo.database

        self.log.info(f'Storing file to GridFS @ {host}:{port}/{db}')
        document_uuid = self.doc[DocumentField.UUID]

        file_id = self.mongo_fs.put(
            self.final_file.content,
            filename=document_uuid
        )
        self.log.info(f'File {document_uuid} stored with id {file_id}')

    def finalize(self):
        document_uuid = self.doc[DocumentField.UUID]
        filename = self.name_giver.name_document(self.doc, self.final_file)
        self._modify_doc({
            DocumentField.FINISHED: datetime.datetime.utcnow(),
            DocumentField.STATE: DocumentState.FINISHED,
            DocumentField.METADATA: {
                DocumentField.METADATA_CONTENT_TYPE: self.final_file.content_type,
                DocumentField.METADATA_FILENAME: filename
            }
        })
        self.log.info(f'Document {document_uuid} record finalized')

    def set_job_state(self, state: str):
        return self._modify_doc({DocumentField.STATE: state})

    def try_set_job_state(self, state: str) -> bool:
        try:
            result = self.set_job_state(state)[DocumentField.STATE]
            return result == DocumentState.FAILED
        except Exception as e:
            self.log.warning(f'Tried to set state of {self.doc_uuid} to {state} but failed: {e}')
            return False


class DocumentWorker:

    def __init__(self, config: DocumentWorkerConfig, workdir: str):
        self.config = config
        self._prepare_logging()
        self.template_registry = TemplateRegistry(config, workdir)
        self.job_context = JobContext(
            config=self.config,
            template_registry=self.template_registry,
            name_giver=DocumentNameGiver(self.config)
        )

    def _prepare_logging(self):
        logging.basicConfig(
            stream=sys.stdout,
            level=self.config.logging.level,
            format=self.config.logging.message_format
        )
        log_filter = DocWorkerLogFilter()
        logging.getLogger().addFilter(filter=log_filter)
        for logger in (logging.getLogger(n) for n in logging.root.manager.loggerDict.keys()):
            logger.addFilter(filter=log_filter)
        logging.setLoggerClass(DocWorkerLogger)

    @tenacity.retry(
        reraise=True,
        wait=tenacity.wait_exponential(multiplier=RETRY_MQ_MULTIPLIER),
        stop=tenacity.stop_after_attempt(RETRY_MQ_TRIES),
        before=tenacity.before_log(logging.getLogger(), logging.INFO),
        after=tenacity.after_log(logging.getLogger(), logging.INFO),
    )
    def run(self):
        queue = self.config.mq.queue
        logging.info(f'Connecting to MQ @ {self.config.mq.host}:{self.config.mq.port}/{self.config.mq.vhost}')
        mq = pika.BlockingConnection(
            parameters=self.config.mq.connection_parameters
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
        job = Job(self.job_context)
        try:
            job.process_body(body)
            job.connect_mongo()
            job.get_job()
            job.prepare_job()
            job.set_job_state(DocumentState.PROCESSING)
            job.build_document()
            job.store_document()
            job.finalize()
        except JobException as e:
            job.log.error(e.message)
            if job.try_set_job_state(DocumentState.FAILED):
                job.log.info(f'Set state to {DocumentState.FAILED}')
            else:
                job.log.warning(f'Could not set state to {DocumentState.FAILED}')
        except Exception as e:
            logging.error(f'Job failed with error: {e}')
            if job.try_set_job_state(DocumentState.FAILED):
                job.log.info(f'Set state to {DocumentState.FAILED}')
            else:
                job.log.warning(f'Could not set state to {DocumentState.FAILED}')
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            job.log.info(f'Job processing finished (ACK sent)')
