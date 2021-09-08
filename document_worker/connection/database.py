import dataclasses
import datetime
import logging
import psycopg2  # type: ignore
import psycopg2.extensions  # type: ignore
import psycopg2.extras  # type: ignore
import tenacity

from document_worker.config import DatabaseConfig
from document_worker.consts import DocumentState
from document_worker.context import Context

from typing import List, Optional

ISOLATION_DEFAULT = psycopg2.extensions.ISOLATION_LEVEL_DEFAULT
ISOLATION_AUTOCOMMIT = psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT

RETRY_QUERY_MULTIPLIER = 0.5
RETRY_QUERY_TRIES = 3

RETRY_CONNECT_MULTIPLIER = 0.2
RETRY_CONNECT_TRIES = 10


@dataclasses.dataclass
class DBJob:
    id: int
    document_uuid: str
    document_context: dict
    created_by: Optional[str]
    created_at: datetime.datetime


@dataclasses.dataclass
class DBDocument:
    uuid: str
    name: str
    state: str
    durability: str
    questionnaire_uuid: str
    questionnaire_event_uuid: str
    questionnaire_replies_hash: str
    template_id: str
    format_uuid: str
    file_name: str
    content_type: str
    worker_log: str
    creator_uuid: str
    retrieved_at: Optional[datetime.datetime]
    finished_at: Optional[datetime.datetime]
    created_at: datetime.datetime


@dataclasses.dataclass
class DBTemplate:
    id: str
    name: str
    organization_id: str
    template_id: str
    version: str
    metamodel_version: int
    description: str
    readme: str
    license: str
    allowed_packages: dict
    recommended_package_id: str
    formats: dict
    created_at: datetime.datetime


@dataclasses.dataclass
class DBTemplateFile:
    template_id: str
    uuid: str
    file_name: str
    content: str


@dataclasses.dataclass
class DBTemplateAsset:
    template_id: str
    uuid: str
    file_name: str
    content_type: str


def wrap_json_data(data: dict):
    return psycopg2.extras.Json(data)


class Database:

    LISTEN = 'LISTEN document_queue_channel;'
    SELECT_JOB = 'SELECT * FROM document_queue LIMIT 1 FOR UPDATE SKIP LOCKED;'
    DELETE_JOB = 'DELETE FROM document_queue WHERE id = %s;'
    SELECT_DOCUMENT = 'SELECT * FROM document WHERE uuid = %s LIMIT 1;'
    UPDATE_DOCUMENT_STATE = 'UPDATE document SET state = %s, worker_log = %s WHERE uuid = %s;'
    UPDATE_DOCUMENT_RETRIEVED = 'UPDATE document SET retrieved_at = %s, state = %s WHERE uuid = %s;'
    UPDATE_DOCUMENT_FINISHED = 'UPDATE document SET finished_at = %s, state = %s, ' \
                               'file_name = %s, content_type = %s, worker_log = %s WHERE uuid = %s;'
    SELECT_TEMPLATE = 'SELECT * FROM template WHERE id = %s LIMIT 1;'
    SELECT_TEMPLATE_FILES = 'SELECT * FROM template_file WHERE template_id = %s;'
    SELECT_TEMPLATE_ASSETS = 'SELECT * FROM template_asset WHERE template_id = %s;'

    def __init__(self, cfg: DatabaseConfig):
        self.cfg = cfg
        Context.logger.info('Preparing PostgreSQL connection for QUERY')
        self.conn_query = PostgresConnection(
            name='query',
            dsn=self.cfg.connection_string,
            timeout=self.cfg.connection_timeout,
            autocommit=False,
        )
        self.conn_query.connect()
        Context.logger.info('Preparing PostgreSQL connection for QUEUE')
        self.conn_queue = PostgresConnection(
            name='queue',
            dsn=self.cfg.connection_string,
            timeout=self.cfg.connection_timeout,
            autocommit=True,
        )
        self.conn_queue.connect()

    @classmethod
    def get_as_job(cls, result) -> DBJob:
        _id, document_uuid, document_context, created_by, created_at = result
        return DBJob(
            id=_id,
            document_uuid=document_uuid,
            document_context=document_context,
            created_by=created_by,
            created_at=created_at,
        )

    @classmethod
    def get_as_document(cls, result) -> DBDocument:
        return DBDocument(
            uuid=result[0],
            name=result[1],
            state=result[2],
            durability=result[3],
            questionnaire_uuid=result[4],
            questionnaire_event_uuid=result[5],
            questionnaire_replies_hash=result[6],
            template_id=result[7],
            format_uuid=result[8],
            creator_uuid=result[9],
            retrieved_at=result[10],
            finished_at=result[11],
            created_at=result[12],
            file_name=result[13],
            content_type=result[14],
            worker_log=result[15],
        )

    @classmethod
    def get_as_template(cls, result) -> DBTemplate:
        return DBTemplate(
            id=result[0],
            name=result[1],
            organization_id=result[2],
            template_id=result[3],
            version=result[4],
            metamodel_version=result[5],
            description=result[6],
            readme=result[7],
            license=result[8],
            allowed_packages=result[9],
            recommended_package_id=result[10],
            formats=result[11],
            created_at=result[12],
        )

    @classmethod
    def get_as_template_file(cls, result) -> DBTemplateFile:
        return DBTemplateFile(
            template_id=result[0],
            uuid=result[1],
            file_name=result[2],
            content=result[3],
        )

    @classmethod
    def get_as_template_asset(cls, result) -> DBTemplateAsset:
        return DBTemplateAsset(
            template_id=result[0],
            uuid=result[1],
            file_name=result[2],
            content_type=result[3],
        )

    @tenacity.retry(
        reraise=True,
        wait=tenacity.wait_exponential(multiplier=RETRY_QUERY_MULTIPLIER),
        stop=tenacity.stop_after_attempt(RETRY_QUERY_TRIES),
        before=tenacity.before_log(Context.logger, logging.DEBUG),
        after=tenacity.after_log(Context.logger, logging.DEBUG),
    )
    def fetch_document(self, document_uuid: str) -> Optional[DBDocument]:
        with self.conn_query.new_cursor() as cursor:
            cursor.execute(
                query=self.SELECT_DOCUMENT,
                vars=(document_uuid,),
            )
            result = cursor.fetchall()
            if len(result) != 1:
                return None
            return self.get_as_document(result[0])

    @tenacity.retry(
        reraise=True,
        wait=tenacity.wait_exponential(multiplier=RETRY_QUERY_MULTIPLIER),
        stop=tenacity.stop_after_attempt(RETRY_QUERY_TRIES),
        before=tenacity.before_log(Context.logger, logging.DEBUG),
        after=tenacity.after_log(Context.logger, logging.DEBUG),
    )
    def fetch_template(self, template_id: str) -> Optional[DBTemplate]:
        with self.conn_query.new_cursor() as cursor:
            cursor.execute(
                query=self.SELECT_TEMPLATE,
                vars=(template_id,),
            )
            result = cursor.fetchall()
            if len(result) != 1:
                return None
            return self.get_as_template(result[0])

    @tenacity.retry(
        reraise=True,
        wait=tenacity.wait_exponential(multiplier=RETRY_QUERY_MULTIPLIER),
        stop=tenacity.stop_after_attempt(RETRY_QUERY_TRIES),
        before=tenacity.before_log(Context.logger, logging.DEBUG),
        after=tenacity.after_log(Context.logger, logging.DEBUG),
    )
    def fetch_template_files(self, template_id: str) -> List[DBTemplateFile]:
        with self.conn_query.new_cursor() as cursor:
            cursor.execute(
                query=self.SELECT_TEMPLATE_FILES,
                vars=(template_id,),
            )
            return [self.get_as_template_file(x) for x in cursor.fetchall()]

    @tenacity.retry(
        reraise=True,
        wait=tenacity.wait_exponential(multiplier=RETRY_QUERY_MULTIPLIER),
        stop=tenacity.stop_after_attempt(RETRY_QUERY_TRIES),
        before=tenacity.before_log(Context.logger, logging.DEBUG),
        after=tenacity.after_log(Context.logger, logging.DEBUG),
    )
    def fetch_template_assets(self, template_id: str) -> List[DBTemplateAsset]:
        with self.conn_query.new_cursor() as cursor:
            cursor.execute(
                query=self.SELECT_TEMPLATE_ASSETS,
                vars=(template_id,),
            )
            return [self.get_as_template_asset(x) for x in cursor.fetchall()]

    @tenacity.retry(
        reraise=True,
        wait=tenacity.wait_exponential(multiplier=RETRY_QUERY_MULTIPLIER),
        stop=tenacity.stop_after_attempt(RETRY_QUERY_TRIES),
        before=tenacity.before_log(Context.logger, logging.DEBUG),
        after=tenacity.after_log(Context.logger, logging.DEBUG),
    )
    def update_document_state(self, document_uuid: str, worker_log: str, state: str) -> bool:
        with self.conn_query.new_cursor() as cursor:
            cursor.execute(
                query=self.UPDATE_DOCUMENT_STATE,
                vars=(state, worker_log, document_uuid),
            )
            return cursor.rowcount == 1

    @tenacity.retry(
        reraise=True,
        wait=tenacity.wait_exponential(multiplier=RETRY_QUERY_MULTIPLIER),
        stop=tenacity.stop_after_attempt(RETRY_QUERY_TRIES),
        before=tenacity.before_log(Context.logger, logging.DEBUG),
        after=tenacity.after_log(Context.logger, logging.DEBUG),
    )
    def update_document_retrieved(self, retrieved_at: datetime.datetime,
                                  document_uuid: str) -> bool:
        with self.conn_queue.new_cursor() as cursor:
            cursor.execute(
                query=self.UPDATE_DOCUMENT_RETRIEVED,
                vars=(
                    retrieved_at,
                    DocumentState.PROCESSING,
                    document_uuid,
                ),
            )
            return cursor.rowcount == 1

    @tenacity.retry(
        reraise=True,
        wait=tenacity.wait_exponential(multiplier=RETRY_QUERY_MULTIPLIER),
        stop=tenacity.stop_after_attempt(RETRY_QUERY_TRIES),
        before=tenacity.before_log(Context.logger, logging.DEBUG),
        after=tenacity.after_log(Context.logger, logging.DEBUG),
    )
    def update_document_finished(
            self, finished_at: datetime.datetime, file_name: str,
            content_type: str,  worker_log: str, document_uuid: str
    ) -> bool:
        with self.conn_query.new_cursor() as cursor:
            cursor.execute(
                query=self.UPDATE_DOCUMENT_FINISHED,
                vars=(
                    finished_at,
                    DocumentState.FINISHED,
                    file_name,
                    content_type,
                    worker_log,
                    document_uuid,
                ),
            )
            return cursor.rowcount == 1


class PostgresConnection:

    def __init__(self, name: str, dsn: str, timeout: int = 30000, autocommit: bool = False):
        self.name = name
        self.listening = False
        self.dsn = psycopg2.extensions.make_dsn(dsn, connect_timeout=timeout)
        self.isolation = ISOLATION_AUTOCOMMIT if autocommit else ISOLATION_DEFAULT
        self._connection = None

    @tenacity.retry(
        reraise=True,
        wait=tenacity.wait_exponential(multiplier=RETRY_CONNECT_MULTIPLIER),
        stop=tenacity.stop_after_attempt(RETRY_CONNECT_TRIES),
        before=tenacity.before_log(Context.logger, logging.DEBUG),
        after=tenacity.after_log(Context.logger, logging.DEBUG),
    )
    def _connect_db(self):
        Context.logger.info(f'Creating connection to PostgreSQL database "{self.name}"')
        connection = psycopg2.connect(dsn=self.dsn)
        connection.set_isolation_level(self.isolation)
        # test connection
        cursor = connection.cursor()
        cursor.execute(query='SELECT * FROM document_queue;')
        result = cursor.fetchall()
        Context.logger.debug(f'Jobs in queue: {result}')
        cursor.close()
        connection.commit()
        self._connection = connection
        self.listening = False

    def connect(self):
        if not self._connection or self._connection.closed != 0:
            self._connect_db()

    @property
    def connection(self):
        self.connect()
        return self._connection

    def new_cursor(self):
        return self.connection.cursor()

    def reset(self):
        self.close()
        self.connect()

    def close(self):
        if self._connection:
            Context.logger.info(f'Closing connection to PostgreSQL database "{self.name}"')
            self._connection.close()
        self._connection = None
