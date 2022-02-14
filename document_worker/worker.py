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
from document_worker.connection.database import Database, DBJob,\
    DBDocument, DBAppConfig, DBAppLimits
from document_worker.connection.s3storage import S3Storage
from document_worker.consts import DocumentState, NULL_UUID
from document_worker.context import Context
from document_worker.documents import DocumentFile, DocumentNameGiver
from document_worker.exceptions import create_job_exception, JobException
from document_worker.limits import LimitsEnforcer
from document_worker.logging import DocWorkerLogger, DocWorkerLogFilter
from document_worker.templates import prepare_template
from document_worker.utils import timeout, JobTimeoutError,\
    PdfWaterMarker, byte_size_format

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


def handle_job_step(message):
    def decorator(func):
        @functools.wraps(func)
        def handled_step(job, *args, **kwargs):
            try:
                return func(job, *args, **kwargs)
            except JobTimeoutError as e:
                raise e  # re-raise (need to be cached by context manager)
            except Exception as e:
                job.log.debug('Handling exception', exc_info=True)
                raise create_job_exception(
                    job_id=job.doc_uuid,
                    message=message,
                    exc=e,
                )
        return handled_step
    return decorator


class Job:

    def __init__(self, db_job: DBJob):
        self.ctx = Context.get()
        self.log = Context.logger
        self.template = None
        self.format = None
        self.app_uuid = db_job.app_uuid
        self.doc_uuid = db_job.document_uuid
        self.doc_context = db_job.document_context
        self.doc = None  # type: Optional[DBDocument]
        self.final_file = None  # type: Optional[DocumentFile]
        self.app_config = None  # type: Optional[DBAppConfig]
        self.app_limits = None  # type: Optional[DBAppLimits]

    @handle_job_step('Failed to get document from DB')
    def get_document(self):
        if self.app_uuid != NULL_UUID:
            self.log.info(f'Limiting to app with UUID: {self.app_uuid}')
        self.log.info(f'Getting the document "{self.doc_uuid}" details from DB')
        self.doc = self.ctx.app.db.fetch_document(
            document_uuid=self.doc_uuid,
            app_uuid=self.app_uuid,
        )
        if self.doc is None:
            raise create_job_exception(
                job_id=self.doc_uuid,
                message='Document record not found in database',
            )
        self.doc.retrieved_at = datetime.datetime.now()
        self.log.info(f'Job "{self.doc_uuid}" details received')
        # verify state
        state = self.doc.state
        self.log.info(f'Original state of job is {state}')
        if state == DocumentState.FINISHED:
            raise create_job_exception(
                job_id=self.doc_uuid,
                message='Document is already marked as finished',
            )
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
        query_args = dict(
            template_id=template_id,
            app_uuid=self.app_uuid,
        )
        db_template = self.ctx.app.db.fetch_template(**query_args)
        if db_template is None:
            raise create_job_exception(
                job_id=self.doc_uuid,
                message=f'Template {template_id} not found in database',
            )
        # prepare template files
        db_files = self.ctx.app.db.fetch_template_files(**query_args)
        db_assets = self.ctx.app.db.fetch_template_assets(**query_args)
        # prepare template
        self.template = prepare_template(
            app_uuid=self.app_uuid,
            template=db_template,
            files=db_files,
            assets=db_assets,
        )
        # prepare format
        self.template.prepare_format(format_uuid)
        self.format = self.template.formats.get(format_uuid)
        # check limits (PDF-only)
        self.app_config = self.ctx.app.db.fetch_app_config(app_uuid=self.app_uuid)
        self.app_limits = self.ctx.app.db.fetch_app_limits(app_uuid=self.app_uuid)
        LimitsEnforcer.check_format(
            job_id=self.doc_uuid,
            doc_format=self.format,
            app_config=self.app_config,
        )

    @handle_job_step('Failed to build final document')
    def build_document(self):
        self.log.info('Building document by rendering template with context')
        self.final_file = self.template.render(
            format_uuid=self.doc.format_uuid,
            context=self.doc_context,
        )
        # Check limits
        LimitsEnforcer.check_doc_size(
            job_id=self.doc_uuid,
            doc_size=self.final_file.byte_size,
        )
        limit_size = None if self.app_limits is None else self.app_limits.storage
        used_size = self.ctx.app.db.get_currently_used_size(app_uuid=self.app_uuid)
        LimitsEnforcer.check_size_usage(
            job_id=self.doc_uuid,
            doc_size=self.final_file.byte_size,
            used_size=used_size,
            limit_size=limit_size,
        )
        # Watermark
        if self.format.is_pdf:
            self.final_file.content = LimitsEnforcer.make_watermark(
                doc_pdf=self.final_file.content,
                app_config=self.app_config,
            )

    @handle_job_step('Failed to store document in S3')
    def store_document(self):
        s3_id = self.ctx.app.s3.identification
        self.log.info(f'Preparing S3 bucket {s3_id}')
        self.ctx.app.s3.ensure_bucket()
        self.log.info(f'Storing document to S3 bucket {s3_id}')
        self.ctx.app.s3.store_document(
            app_uuid=self.app_uuid,
            file_name=self.doc_uuid,
            content_type=self.final_file.content_type,
            data=self.final_file.content,
        )
        self.log.info(f'Document {self.doc_uuid} stored in S3 bucket {s3_id}')

    @handle_job_step('Failed to finalize document generation')
    def finalize(self):
        file_name = DocumentNameGiver.name_document(self.doc, self.final_file)
        self.doc.finished_at = datetime.datetime.now()
        self.doc.file_name = file_name
        self.doc.content_type = self.final_file.content_type
        self.doc.file_size = self.final_file.byte_size
        self.ctx.app.db.update_document_finished(
            finished_at=self.doc.finished_at,
            file_name=self.doc.file_name,
            content_type=self.doc.content_type,
            file_size=self.doc.file_size,
            worker_log=(
                f'Document "{file_name}" generated successfully '
                f'({byte_size_format(self.doc.file_size)}).'
            ),
            document_uuid=self.doc_uuid,
        )
        self.log.info(f'Document {self.doc_uuid} record finalized')

    def set_job_state(self, state: str, message: str) -> bool:
        return self.ctx.app.db.update_document_state(
            document_uuid=self.doc_uuid,
            worker_log=message,
            state=state,
        )

    def try_set_job_state(self, state: str, message: str) -> bool:
        try:
            return self.set_job_state(state, message)
        except Exception as e:
            self.log.warning(f'Tried to set state of {self.doc_uuid} to {state} but failed: {e}')
            return False

    def _run(self):
        self.get_document()
        try:
            with timeout(Context.get().app.cfg.experimental.job_timeout):
                self.prepare_template()
                self.build_document()
                self.store_document()
        except TimeoutError:
            LimitsEnforcer.timeout_exceeded(
                job_id=self.doc_uuid,
            )
        self.finalize()

    def run(self):
        try:
            self._run()
        except JobException as e:
            self.log.error(e.log_message())
            if self.try_set_job_state(DocumentState.FAILED, e.db_message()):
                self.log.info(f'Set state to {DocumentState.FAILED}')
            else:
                self.log.warning(f'Could not set state to {DocumentState.FAILED}')
        except Exception as e:
            job_exc = create_job_exception(
                job_id=self.doc_uuid,
                message='Failed with unexpected error',
                exc=e,
            )
            Context.logger.error(job_exc.log_message())
            if self.try_set_job_state(DocumentState.FAILED, job_exc.db_message()):
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
        PdfWaterMarker.initialize(
            watermark_filename=self.config.experimental.pdf_watermark,
            watermark_top=self.config.experimental.pdf_watermark_top,
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
        Context.logger.info('Preparing to listen for document jobs')
        queue_conn = ctx.app.db.conn_queue
        with queue_conn.new_cursor() as cursor:
            cursor.execute(Database.LISTEN)
            queue_conn.listening = True
            Context.logger.info('Listening on document job queue')

            notifications = list()
            timeout = ctx.app.cfg.db.queue_timout

            Context.logger.info('Entering working cycle, waiting for notifications')
            while True:
                while self._work():
                    pass

                Context.logger.debug('Waiting for new notifications')
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
                    Context.logger.debug('Interrupt signal received, ending...')
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
        Context.logger.debug('Trying to fetch a new job')
        cursor = ctx.app.db.conn_query.new_cursor(use_dict=True)
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
        Context.logger.debug('Working done, deleting job from queue')
        cursor.execute(
            query=Database.DELETE_JOB,
            vars=(db_job.id,)
        )
        Context.logger.info('Committing transaction')
        ctx.app.db.conn_query.connection.commit()
        cursor.close()
        job.log.info('Job processing finished')
        return True
