import dataclasses
import datetime
import logging
import psycopg2
import psycopg2.extensions

from document_worker.config import DatabaseConfig

from typing import Optional

ISOLATION_DEFAULT = psycopg2.extensions.ISOLATION_LEVEL_DEFAULT
ISOLATION_AUTOCOMMIT = psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT


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
    metadata: dict
    creator_uuid: str
    created_at: datetime.datetime


class Database:

    CHANNEL = 'document_queue_channel'
    TABLE_JOB = 'document_queue'
    TABLE_DOC = 'document'

    LISTEN = f'LISTEN {CHANNEL};'
    SELECT_JOB = f'SELECT * FROM {TABLE_JOB} LIMIT 1 FOR UPDATE SKIP LOCKED;'
    DELETE_JOB = f'DELETE FROM {TABLE_JOB} WHERE id = %s;'
    UPDATE_DOCUMENT = f"""UPDATE {TABLE_DOC}
                SET state = %s
                WHERE uuid = %s"""

    def __init__(self, cfg: DatabaseConfig):
        self.cfg = cfg
        logging.info('Preparing PostgreSQL connection for QUERY')
        self.conn_query = PostgresConnection(
            connection_string=self.cfg.connection_string,
            timeout=self.cfg.connection_timeout,
            autocommit=False,
        )
        self.conn_query.connect()
        logging.info('Preparing PostgreSQL connection for QUEUE')
        self.conn_queue = PostgresConnection(
            connection_string=self.cfg.connection_string,
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
            metadata=result[9],
            creator_uuid=result[10],
            created_at=result[11],
        )


class PostgresConnection:

    def __init__(self, connection_string: str, timeout: int = 30000, autocommit: bool = False):
        self.dsn = psycopg2.extensions.make_dsn(connection_string, connect_timeout=timeout)
        self.isolation = ISOLATION_AUTOCOMMIT if autocommit else ISOLATION_DEFAULT
        self._connection = None

    def connect(self):
        if not self._connection:
            self._connection = self._conn = psycopg2.connect(dsn=self.dsn)
            self._connection.set_isolation_level(self.isolation)

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
            self._connection.close()
        self._connection = None
