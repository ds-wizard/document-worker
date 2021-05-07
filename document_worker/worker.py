import functools
import logging
import select
import signal
import sys
import uuid
import tenacity

from typing import Optional

from document_worker.config import DocumentWorkerConfig
from document_worker.connection.database import Database, DBJob
from document_worker.consts import DocumentField, DocumentState
from document_worker.context import AppContext, JobContext, Context
from document_worker.documents import DocumentFile, DocumentNameGiver
from document_worker.logging import DocWorkerLogger, DocWorkerLoggerWrapper, DocWorkerLogFilter
from document_worker.templates import TemplateRegistry

RETRY_QUERY_MULTIPLIER = 0.5
RETRY_QUERY_TRIES = 3
RETRY_QUEUE_MULTIPLIER = 0.5
RETRY_QUEUE_TRIES = 10

INTERRUPTED = False


def signal_handler(recv_signal, frame):
    logging.info(f'Received interrupt signal: {recv_signal}')
    global INTERRUPTED
    INTERRUPTED = True


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGABRT, signal_handler)


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


class Job:

    DOCUMENT_FIELDS = [
        DocumentField.STATE,
        DocumentField.TEMPLATE,
        DocumentField.FORMAT,
    ]

    def __init__(self, ctx: Context, logger: DocWorkerLoggerWrapper):
        self.ctx = ctx

        self.template = None
        self.doc_uuid = 'unknown'
        self.doc_context = dict()
        self.doc = None  # type: Optional[dict]
        self.final_file = None  # type: Optional[DocumentFile]

        self.log = logger

    def raise_exc(self, message: str):
        raise JobException(self.doc_uuid, message)

    @handle_job_step('Failed to process job body')
    def process_body(self, db_job: DBJob):
        self.doc_uuid = db_job.document_uuid
        self.doc_context = db_job.document_context

    @handle_job_step('Failed to get document from DB')
    @tenacity.retry(
        reraise=True,
        wait=tenacity.wait_exponential(multiplier=RETRY_QUERY_MULTIPLIER),
        stop=tenacity.stop_after_attempt(RETRY_QUERY_TRIES),
        before=tenacity.before_log(logging.getLogger(), logging.INFO),
        after=tenacity.after_log(logging.getLogger(), logging.INFO),
    )
    def get_document(self, cursor):
        self.log.info(f'Getting the document "{self.doc_uuid}" details from DB')
        # TODO: get retrieved timestamp
        # TODO: update retrieved timestamp
        # self.doc = self._modify_doc({DocumentField.RETRIEVED: datetime.datetime.utcnow()})
        # if self.doc is None:
        #     self.raise_exc(f'Document "{self.doc_uuid}" not found')
        self.log.info(f'Job "{self.doc_uuid}" details received')

    @handle_job_step('Failed to prepare job')
    def prepare_job(self):
        # TODO: prepare job
        self.log.info(f'Verifying the received job "{self.doc_uuid}" details')
        # # verify fields
        # for field in self.DOCUMENT_FIELDS:
        #     if field not in self.doc.keys():
        #         self.raise_exc(f'Missing field "{field}" in the job details')
        # # verify state
        # state = self.doc[DocumentField.STATE]
        # self.log.info(f'Original state of job is {state}')
        # if state == DocumentState.FINISHED:
        #     self.raise_exc(f'Job is already finished')
        # # prepare template
        # template_id = self.doc[DocumentField.TEMPLATE]
        # self.template = self.ctx.app.template_registry.get_template(template_id)
        # if self.template is None:
        #     self.raise_exc(f'Template {template_id} not found')
        # # prepare format
        # format_uuid = uuid.UUID(self.doc[DocumentField.FORMAT])
        # if not self.template.prepare_format(format_uuid):
        #     self.raise_exc(f'Format {format_uuid} (in template {template_id}) not found')

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
        wait=tenacity.wait_exponential(multiplier=RETRY_QUEUE_MULTIPLIER),
        stop=tenacity.stop_after_attempt(RETRY_QUEUE_TRIES),
        before=tenacity.before_log(logging.getLogger(), logging.INFO),
        after=tenacity.after_log(logging.getLogger(), logging.INFO),
    )
    def store_document(self):
        ...
        # TODO: store document in S3
        # host = self.config.mongo.host
        # port = self.config.mongo.port
        # db = self.config.mongo.database
        #
        # self.log.info(f'Storing file to GridFS @ {host}:{port}/{db}')
        # document_uuid = self.doc[DocumentField.UUID]
        #
        # file_id = self.mongo_fs.put(
        #     self.final_file.content,
        #     filename=document_uuid
        # )
        # self.log.info(f'File {document_uuid} stored with id {file_id}')

    def finalize(self):
        ...
        # TODO: update document record in DB
        # document_uuid = self.doc[DocumentField.UUID]
        # filename = self.name_giver.name_document(self.doc, self.final_file)
        # self._modify_doc({
        #     DocumentField.FINISHED: datetime.datetime.utcnow(),
        #     DocumentField.STATE: DocumentState.FINISHED,
        #     DocumentField.METADATA: {
        #         DocumentField.METADATA_CONTENT_TYPE: self.final_file.content_type,
        #         DocumentField.METADATA_FILENAME: filename
        #     }
        # })
        # self.log.info(f'Document {document_uuid} record finalized')

    def set_job_state(self, state: str, cursor) -> int:
        cursor.execute(
            query=Database.UPDATE_DOCUMENT,
            vars=(state, self.doc_uuid),
        )
        return cursor.rowcount

    def try_set_job_state(self, state: str, cursor) -> bool:
        try:
            result = self.set_job_state(state, cursor)
            return result == 1
        except Exception as e:
            self.log.warning(f'Tried to set state of {self.doc_uuid} to {state} but failed: {e}')
            return False


class DocumentWorker:

    def __init__(self, config: DocumentWorkerConfig, workdir: str):
        self.config = config
        self.ctx = Context(
            app=AppContext(
                cfg=config,
                template_registry=TemplateRegistry(config, workdir),
                name_giver=DocumentNameGiver(self.config),
            ),
            job=JobContext(
                trace_id='none',
            )
        )
        self._prepare_logging()

    def _prepare_logging(self):
        logging.basicConfig(
            stream=sys.stdout,
            level=self.config.log.level,
            format=self.config.log.message_format
        )
        log_filter = DocWorkerLogFilter()
        logging.getLogger().addFilter(filter=log_filter)
        for logger in (logging.getLogger(n) for n in logging.root.manager.loggerDict.keys()):
            logger.addFilter(filter=log_filter)
        logging.setLoggerClass(DocWorkerLogger)

    @tenacity.retry(
        reraise=True,
        wait=tenacity.wait_exponential(multiplier=RETRY_QUEUE_MULTIPLIER),
        stop=tenacity.stop_after_attempt(RETRY_QUEUE_TRIES),
        before=tenacity.before_log(logging.getLogger(), logging.INFO),
        after=tenacity.after_log(logging.getLogger(), logging.INFO),
    )
    def run(self):
        logging.info(f'Preparing for listening on {Database.CHANNEL}')
        cursor = self.ctx.app.db.conn_queue.new_cursor()
        cursor.execute(Database.LISTEN)
        logging.info(f'Listening on Postgres channel {Database.CHANNEL}')

        notifications = list()
        timeout = self.ctx.app.cfg.db.queue_timout
        while True:
            query_next_job = True
            while query_next_job:
                query_next_job = self._callback()

            logging.info(f'Waiting for new notifications on {Database.CHANNEL}')
            notifications.clear()
            if select.select([self.ctx.app.db.conn_queue], [], [], timeout) == ([], [], []):
                logging.debug(f'Nothing received in this cycle (timeouted after {timeout} seconds.')
            else:
                self.ctx.app.db.conn_queue.connection.poll()
                while self.ctx.app.db.conn_queue.connection.notifies:
                    notifications.append(self.ctx.app.db.conn_queue.connection.notifies.pop())
                logging.info(f'Notifications received ({len(notifications)} in total): {notifications}')

            if INTERRUPTED:  # Interrupt signal received
                break

        cursor.close()

    @tenacity.retry(
        reraise=True,
        wait=tenacity.wait_exponential(multiplier=RETRY_QUERY_MULTIPLIER),
        stop=tenacity.stop_after_attempt(RETRY_QUERY_TRIES),
        before=tenacity.before_log(logging.getLogger(), logging.INFO),
        after=tenacity.after_log(logging.getLogger(), logging.INFO),
    )
    def _callback(self):
        self.ctx.job.trace_id = str(uuid.uuid4())
        logger = DocWorkerLoggerWrapper(
            trace_id=self.ctx.job.trace_id,
            document_id='unknown',
        )
        logger.debug(f'Trying to fetch a new job')
        cursor = self.ctx.app.db.conn_query.new_cursor()
        cursor.execute(Database.SELECT_JOB)
        result = cursor.fetchall()
        if len(result) != 1:
            logging.debug(f'Fetched {len(result)} jobs')
            return False
        db_job = Database.get_as_job(result[0])
        logger.document_id = db_job.document_uuid
        logger.info(f'Fetched job #{db_job.id}')
        job = Job(self.ctx, logger)
        try:
            job.process_body(job)
            # job.connect_mongo()
            # job.get_job()
            # job.prepare_job()
            job.set_job_state(DocumentState.FAILED, cursor)
            # job.build_document()
            # job.store_document()
            # job.finalize()
        except JobException as e:
            job.log.error(e.message)
            if job.try_set_job_state(DocumentState.FAILED, cursor):
                job.log.info(f'Set state to {DocumentState.FAILED}')
            else:
                job.log.warning(f'Could not set state to {DocumentState.FAILED}')
        except Exception as e:
            logging.error(f'Job failed with error: {e}')
            if job.try_set_job_state(DocumentState.FAILED, cursor):
                job.log.info(f'Set state to {DocumentState.FAILED}')
            else:
                job.log.warning(f'Could not set state to {DocumentState.FAILED}')
        finally:
            logger.debug(f'Working done, deleting job from queue')
            cursor.execute(
                query=Database.DELETE_JOB,
                vars=(db_job.id,)
            )
            logger.info(f'Committing transaction')
            self.ctx.app.db.conn_query.connection.commit()
            cursor.close()
            job.log.info(f'Job processing finished')
        return True
