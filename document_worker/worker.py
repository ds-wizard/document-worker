import datetime
import functools
import logging
import pathlib
import select
import signal
import sys
import uuid
import tenacity

from typing import Optional

from document_worker.config import DocumentWorkerConfig
from document_worker.connection.database import Database, DBJob, DBDocument
from document_worker.connection.s3storage import S3Storage
from document_worker.consts import DocumentState, DocumentField
from document_worker.context import Context
from document_worker.documents import DocumentFile, DocumentNameGiver
from document_worker.logging import DocWorkerLogger, DocWorkerLogFilter
from document_worker.templates import prepare_template

RETRY_QUERY_MULTIPLIER = 0.5
RETRY_QUERY_TRIES = 3
RETRY_QUEUE_MULTIPLIER = 0.5
RETRY_QUEUE_TRIES = 5

INTERRUPTED = False


def signal_handler(recv_signal, frame):
    Context.logger.info(f'Received interrupt signal: {recv_signal}')
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

    def __init__(self, db_job: DBJob):
        self.ctx = Context.get()
        self.log = Context.logger
        self.template = None
        self.doc_uuid = db_job.document_uuid
        self.doc_context = db_job.document_context
        self.doc = None  # type: Optional[DBDocument]
        self.final_file = None  # type: Optional[DocumentFile]

    def raise_exc(self, message: str):
        raise JobException(self.doc_uuid, message)

    @handle_job_step('Failed to get document from DB')
    def get_document(self):
        self.log.info(f'Getting the document "{self.doc_uuid}" details from DB')
        self.doc = self.ctx.app.db.fetch_document(document_uuid=self.doc_uuid)
        if self.doc is None:
            self.raise_exc(f'Document "{self.doc_uuid}" not found')
        self.doc.retrieved_at = datetime.datetime.now()
        self.log.info(f'Job "{self.doc_uuid}" details received')
        # verify state
        state = self.doc.state
        self.log.info(f'Original state of job is {state}')
        if state == DocumentState.FINISHED:
            self.raise_exc(f'Job is already finished')
        self.ctx.app.db.update_document_retrieved(
            retrieved_at=self.doc.retrieved_at,
            document_uuid=self.doc_uuid,
        )

    @handle_job_step('Failed to prepare template')
    def prepare_template(self):
        template_id = self.doc.template_id
        format_uuid = self.doc.format_uuid
        self.log.info(f'Document uses template {template_id} with format {format_uuid}')
        # prepare template
        db_template = self.ctx.app.db.fetch_template(template_id=template_id)
        if db_template is None:
            self.raise_exc(f'Template {template_id} not found')
        # prepare template files
        db_files = self.ctx.app.db.fetch_template_files(template_id=template_id)
        db_assets = self.ctx.app.db.fetch_template_assets(template_id=template_id)
        # prepare template
        self.template = prepare_template(
            template=db_template,
            files=db_files,
            assets=db_assets,
        )
        # prepare format
        if not self.template.prepare_format(format_uuid):
            self.raise_exc(f'Format {format_uuid} (in template {template_id}) not found')

    @handle_job_step('Failed to build final document')
    def build_document(self):
        self.log.info(f'Building document by rendering template with context')
        self.final_file = self.template.render(
            format_uuid=self.doc.format_uuid,
            context=self.doc_context,
        )

    @handle_job_step('Failed to store document in S3')
    def store_document(self):
        s3_id = self.ctx.app.s3.identification
        self.log.info(f'Preparing S3 bucket {s3_id}')
        self.ctx.app.s3.ensure_bucket()
        self.log.info(f'Storing document to S3 bucket {s3_id}')
        self.ctx.app.s3.store_document(
            file_name=self.doc_uuid,
            content_type=self.final_file.content_type,
            data=self.final_file.content,
        )
        self.log.info(f'Document {self.doc_uuid} stored in S3 bucket {s3_id}')

    def finalize(self):
        file_name = DocumentNameGiver.name_document(self.doc, self.final_file)
        self.doc.finished_at = datetime.datetime.now()
        self.doc.metadata = {
            DocumentField.METADATA_CONTENT_TYPE: self.final_file.content_type,
            DocumentField.METADATA_FILENAME: file_name,
        }
        self.ctx.app.db.update_document_finished(
            finished_at=self.doc.finished_at,
            metadata=self.doc.metadata,
            document_uuid=self.doc_uuid,
        )
        self.log.info(f'Document {self.doc_uuid} record finalized')

    def set_job_state(self, state: str) -> bool:
        return self.ctx.app.db.update_document_state(
            document_uuid=self.doc_uuid,
            state=state,
        )

    def try_set_job_state(self, state: str) -> bool:
        try:
            return self.set_job_state(state)
        except Exception as e:
            self.log.warning(f'Tried to set state of {self.doc_uuid} to {state} but failed: {e}')
            return False

    def run(self):
        try:
            self.get_document()
            self.prepare_template()
            self.set_job_state(DocumentState.FAILED)
            self.build_document()
            self.store_document()
            self.finalize()
        except JobException as e:
            self.log.error(e.message)
            if self.try_set_job_state(DocumentState.FAILED):
                self.log.info(f'Set state to {DocumentState.FAILED}')
            else:
                self.log.warning(f'Could not set state to {DocumentState.FAILED}')
        except Exception as e:
            Context.logger.error(f'Job failed with error: {e}')
            if self.try_set_job_state(DocumentState.FAILED):
                self.log.info(f'Set state to {DocumentState.FAILED}')
            else:
                self.log.warning(f'Could not set state to {DocumentState.FAILED}')


class DocumentWorker:

    def __init__(self, config: DocumentWorkerConfig, workdir: pathlib.Path):
        self.config = config
        self._prepare_logging()
        self._init_context(workdir=workdir)

    def _init_context(self, workdir: pathlib.Path):
        Context.initialize(
            config=self.config,
            workdir=workdir,
            db=Database(cfg=self.config.db),
            s3=S3Storage(cfg=self.config.s3)
        )

    def _prepare_logging(self):
        logging.basicConfig(
            stream=sys.stdout,
            level=self.config.log.global_level,
            format=self.config.log.message_format
        )
        Context.logger.set_level(self.config.log.level)
        log_filter = DocWorkerLogFilter()
        logging.getLogger().addFilter(filter=log_filter)
        loggers = (logging.getLogger(n)
                   for n in logging.root.manager.loggerDict.keys())
        for logger in loggers:
            logger.addFilter(filter=log_filter)
        logging.setLoggerClass(DocWorkerLogger)

    @tenacity.retry(
        reraise=True,
        wait=tenacity.wait_exponential(multiplier=RETRY_QUEUE_MULTIPLIER),
        stop=tenacity.stop_after_attempt(RETRY_QUEUE_TRIES),
        before=tenacity.before_log(Context.logger, logging.INFO),
        after=tenacity.after_log(Context.logger, logging.INFO),
    )
    def run(self):
        ctx = Context.get()
        Context.logger.info(f'Preparing to listen for document jobs')
        queue_conn = ctx.app.db.conn_queue
        with queue_conn.new_cursor() as cursor:
            cursor.execute(Database.LISTEN)
            queue_conn.listening = True
            Context.logger.info(f'Listening on document job queue')

            notifications = list()
            timeout = ctx.app.cfg.db.queue_timout

            Context.logger.info(f'Entering working cycle, waiting for notifications')
            while True:
                while self._work():
                    pass

                Context.logger.debug(f'Waiting for new notifications')
                notifications.clear()
                if not queue_conn.listening:
                    cursor.execute(Database.LISTEN)
                    queue_conn.listening = True

                w = select.select([queue_conn.connection], [], [], timeout)
                if w == ([], [], []):
                    Context.logger.debug(f'Nothing received in this cycle '
                                         f'(timeouted after {timeout} seconds.')
                else:
                    queue_conn.connection.poll()
                    while queue_conn.connection.notifies:
                        notifications.append(queue_conn.connection.notifies.pop())
                    Context.logger.info(f'Notifications received ({len(notifications)})')
                    Context.logger.debug(f'Notifications: {notifications}')

                if INTERRUPTED:
                    Context.logger.debug(f'Interrupt signal received, ending...')
                    break

    @tenacity.retry(
        reraise=True,
        wait=tenacity.wait_exponential(multiplier=RETRY_QUERY_MULTIPLIER),
        stop=tenacity.stop_after_attempt(RETRY_QUERY_TRIES),
        before=tenacity.before_log(Context.logger, logging.DEBUG),
        after=tenacity.after_log(Context.logger, logging.DEBUG),
    )
    def _work(self):
        Context.update_trace_id(str(uuid.uuid4()))
        ctx = Context.get()
        Context.logger.debug(f'Trying to fetch a new job')
        cursor = ctx.app.db.conn_query.new_cursor()
        cursor.execute(Database.SELECT_JOB)
        result = cursor.fetchall()
        if len(result) != 1:
            Context.logger.debug(f'Fetched {len(result)} jobs')
            return False
        db_job = Database.get_as_job(result[0])
        Context.update_document_id(db_job.document_uuid)
        Context.logger.info(f'Fetched job #{db_job.id}')
        job = Job(db_job=db_job)
        job.run()
        Context.logger.debug(f'Working done, deleting job from queue')
        cursor.execute(
            query=Database.DELETE_JOB,
            vars=(db_job.id,)
        )
        Context.logger.info(f'Committing transaction')
        ctx.app.db.conn_query.connection.commit()
        cursor.close()
        job.log.info(f'Job processing finished')
        return True
