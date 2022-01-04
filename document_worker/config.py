import shlex
import yaml
from typing import List, Optional

from document_worker.consts import DocumentNamingStrategy


class MissingConfigurationError(Exception):

    def __init__(self, missing: List[str]):
        self.missing = missing


class DatabaseConfig:

    def __init__(self, connection_string: str, connection_timeout: int, queue_timout: int):
        self.connection_string = connection_string
        self.connection_timeout = connection_timeout
        self.queue_timout = queue_timout

    def __str__(self):
        return f'DatabaseConfig\n' \
               f'- connection_string = {self.connection_string} ({type(self.connection_string)})\n' \
               f'- connection_timeout = {self.connection_timeout} ({type(self.connection_timeout)})\n' \
               f'- queue_timout = {self.queue_timout} ({type(self.queue_timout)})\n'


class S3Config:

    def __init__(self, url: str, username: str, password: str,
                 bucket: str, region: str):
        self.url = url
        self.username = username
        self.password = password
        self.bucket = bucket
        self.region = region

    def __str__(self):
        return f'S3Config\n' \
               f'- url = {self.url} ({type(self.url)})\n' \
               f'- username = {self.username} ({type(self.username)})\n' \
               f'- password = {self.password} ({type(self.password)})\n' \
               f'- bucket = {self.bucket} ({type(self.bucket)})\n'


class LoggingConfig:

    def __init__(self, level: str, global_level: str, message_format: str):
        self.level = level
        self.global_level = global_level
        self.message_format = message_format

    def __str__(self):
        return f'LoggingConfig\n' \
               f'- level = {self.level} ({type(self.level)})\n' \
               f'- message_format = {self.message_format} ({type(self.message_format)})\n'


class DocumentsConfig:

    def __init__(self, naming_strategy: str):
        self.naming_strategy = DocumentNamingStrategy.get(naming_strategy)

    def __str__(self):
        return f'DocumentsConfig\n' \
               f'- naming_strategy = {self.naming_strategy}\n'


class ExperimentalConfig:

    def __init__(self, more_apps_enabled: bool):
        self.more_apps_enabled = more_apps_enabled

    def __str__(self):
        return f'ExperimentalConfig\n' \
               f'- more_apps_enabled = {self.more_apps_enabled}\n'


class CommandConfig:

    def __init__(self, executable: str, args: str, timeout: float):
        self.executable = executable
        self.args = args
        self.timeout = timeout

    @property
    def command(self) -> List[str]:
        return [self.executable] + shlex.split(self.args)

    def __str__(self):
        return f'CommandConfig\n' \
               f'- executable = {self.executable} ({type(self.executable)})\n' \
               f'- args = {self.args} ({type(self.args)})\n' \
               f'- timeout = {self.timeout} ({type(self.timeout)})\n'


class TemplateRequestsConfig:

    def __init__(self, enabled: bool, limit: int, timeout: int):
        self.enabled = enabled
        self.limit = limit
        self.timeout = timeout

    @staticmethod
    def load(data: dict):
        return TemplateRequestsConfig(
            enabled=data.get('enabled', False),
            limit=data.get('limit', 100),
            timeout=data.get('timeout', 1),
        )


class TemplateConfig:

    def __init__(self, ids: List[str], requests: TemplateRequestsConfig,
                 secrets: dict[str, str]):
        self.ids = ids
        self.requests = requests
        self.secrets = secrets

    @staticmethod
    def load(data: dict):
        print(data)
        return TemplateConfig(
            ids=data.get('ids', []),
            requests=TemplateRequestsConfig.load(
                data.get('requests', {}),
            ),
            secrets=data.get('secrets', {}),
        )


class TemplatesConfig:

    def __init__(self, templates: List[TemplateConfig]):
        self.templates = templates

    def get_config(self, template_id: str) -> Optional[TemplateConfig]:
        for template in self.templates:
            if any((template_id.startswith(prefix)
                    for prefix in template.ids)):
                return template
        return None


class DocumentWorkerConfig:

    def __init__(self, db: DatabaseConfig, s3: S3Config, log: LoggingConfig,
                 doc: DocumentsConfig, pandoc: CommandConfig, wkhtmltopdf: CommandConfig,
                 templates: TemplatesConfig, experimental: ExperimentalConfig):
        self.db = db
        self.s3 = s3
        self.log = log
        self.doc = doc
        self.pandoc = pandoc
        self.wkhtmltopdf = wkhtmltopdf
        self.templates = templates
        self.experimental = experimental

    def __str__(self):
        return f'DocumentWorkerConfig\n' \
               f'====================\n' \
               f'{self.db}' \
               f'{self.s3}' \
               f'{self.log}' \
               f'{self.doc}' \
               f'{self.experimental}' \
               f'Pandoc: {self.pandoc}' \
               f'WkHtmlToPdf: {self.wkhtmltopdf}' \
               f'====================\n'


class DocumentWorkerConfigParser:

    DB_SECTION = 'database'
    S3_SECTION = 's3'
    LOGGING_SECTION = 'logging'
    DOCS_SECTION = 'documents'
    DOCS_NAMING_SUBSECTION = 'naming'
    EXTERNAL_SECTION = 'externals'
    PANDOC_SUBSECTION = 'pandoc'
    WKHTMLTOPDF_SUBSECTION = 'wkhtmltopdf'
    TEMPLATES_SECTION = 'templates'
    EXPERIMENTAL_SECTION = 'experimental'

    DEFAULTS = {
        DB_SECTION: {
            'connectionString': 'postgresql://postgres:postgres@postgres:5432/engine-wizard',
            'connectionTimeout': 30000,
            'queueTimeout': 120,
        },
        S3_SECTION: {
            'url': 'http://minio:9000',
            'vhost': 'minio',
            'queue': 'minio',
            'bucket': 'engine-wizard',
            'region': 'eu-central-1',
        },
        LOGGING_SECTION: {
            'level': 'INFO',
            'globalLevel': 'WARNING',
            'format': '%(asctime)s | %(levelname)8s | %(name)s: [T:%(traceId)s] %(message)s',
        },
        DOCS_SECTION: {
            DOCS_NAMING_SUBSECTION: {
                'strategy': 'sanitize'
            }
        },
        EXTERNAL_SECTION: {
            PANDOC_SUBSECTION: {
                'executable': 'pandoc',
                'args': '--standalone',
                'timeout': None,
            },
            WKHTMLTOPDF_SUBSECTION: {
                'executable': 'wkhtmltopdf',
                'args': '',
                'timeout': None,
            },
        },
        TEMPLATES_SECTION: [],
        EXPERIMENTAL_SECTION: {
            'moreAppsEnabled': False,
        },
    }

    REQUIRED = []  # type: list[str]

    def __init__(self):
        self.cfg = dict()

    @staticmethod
    def can_read(content):
        try:
            yaml.load(content, Loader=yaml.FullLoader)
            return True
        except Exception:
            return False

    def read_file(self, fp):
        self.cfg = yaml.load(fp, Loader=yaml.FullLoader)

    def read_string(self, content):
        self.cfg = yaml.load(content, Loader=yaml.FullLoader)

    def has(self, *path):
        x = self.cfg
        for p in path:
            if not hasattr(x, 'keys') or p not in x.keys():
                return False
            x = x[p]
        return True

    def _get_default(self, *path):
        x = self.DEFAULTS
        for p in path:
            x = x[p]
        return x

    def get_or_default(self, *path):
        x = self.cfg
        for p in path:
            if not hasattr(x, 'keys') or p not in x.keys():
                return self._get_default(*path)
            x = x[p]
        return x

    def validate(self):
        missing = []
        for path in self.REQUIRED:
            if not self.has(*path):
                missing.append('.'.join(path))
        if len(missing) > 0:
            raise MissingConfigurationError(missing)

    @property
    def db(self) -> DatabaseConfig:
        return DatabaseConfig(
            connection_string=self.get_or_default(self.DB_SECTION, 'connectionString'),
            connection_timeout=self.get_or_default(self.DB_SECTION, 'connectionTimeout'),
            queue_timout=self.get_or_default(self.DB_SECTION, 'queueTimeout'),
        )

    @property
    def s3(self) -> S3Config:
        return S3Config(
            url=self.get_or_default(self.S3_SECTION, 'url'),
            username=self.get_or_default(self.S3_SECTION, 'username'),
            password=self.get_or_default(self.S3_SECTION, 'password'),
            bucket=self.get_or_default(self.S3_SECTION, 'bucket'),
            region=self.get_or_default(self.S3_SECTION, 'region'),
        )

    @property
    def logging(self) -> LoggingConfig:
        return LoggingConfig(
            level=self.get_or_default(self.LOGGING_SECTION, 'level'),
            global_level=self.get_or_default(self.LOGGING_SECTION, 'globalLevel'),
            message_format=self.get_or_default(self.LOGGING_SECTION, 'format'),
        )

    @property
    def documents(self) -> DocumentsConfig:
        return DocumentsConfig(
            naming_strategy=self.get_or_default(self.DOCS_SECTION, self.DOCS_NAMING_SUBSECTION, 'strategy')
        )

    def _command_config(self, *path: str) -> CommandConfig:
        return CommandConfig(
            executable=self.get_or_default(*path, 'executable'),
            args=self.get_or_default(*path, 'args'),
            timeout=self.get_or_default(*path, 'timeout'),
        )

    @property
    def pandoc(self) -> CommandConfig:
        return self._command_config(self.EXTERNAL_SECTION, self.PANDOC_SUBSECTION)

    @property
    def wkhtmltopdf(self) -> CommandConfig:
        return self._command_config(self.EXTERNAL_SECTION, self.WKHTMLTOPDF_SUBSECTION)

    @property
    def templates(self) -> TemplatesConfig:
        templates_data = self.get_or_default(self.TEMPLATES_SECTION)
        templates = [TemplateConfig.load(data) for data in templates_data]
        return TemplatesConfig(
            templates=templates,
        )

    @property
    def experimental(self) -> ExperimentalConfig:
        return ExperimentalConfig(
            more_apps_enabled=self.get_or_default(self.EXPERIMENTAL_SECTION, 'moreAppsEnabled'),
        )

    @property
    def config(self) -> DocumentWorkerConfig:
        return DocumentWorkerConfig(
            db=self.db,
            s3=self.s3,
            log=self.logging,
            doc=self.documents,
            pandoc=self.pandoc,
            wkhtmltopdf=self.wkhtmltopdf,
            templates=self.templates,
            experimental=self.experimental,
        )
