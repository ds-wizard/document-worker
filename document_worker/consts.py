DEFAULT_ENCODING = 'utf-8'
EXIT_SUCCESS = 0
VERSION = '2.14.0'
PROG_NAME = 'docworker'


class DocumentState:
    QUEUED = 'QueuedDocumentState'
    PROCESSING = 'InProgressDocumentState'
    FAILED = 'ErrorDocumentState'
    FINISHED = 'DoneDocumentState'


class DocumentField:
    UUID = 'uuid'
    NAME = 'name'
    STATE = 'state'
    TEMPLATE = 'templateId'
    FORMAT = 'formatUuid'
    RETRIEVED = 'retrievedAt'
    FINISHED = 'finishedAt'
    METADATA = 'metadata'
    METADATA_CONTENT_TYPE = 'contentType'
    METADATA_FILENAME = 'fileName'


class TemplateFileField:
    FILENAME = 'fileName'
    CONTENT = 'content'


class TemplateAssetField:
    UUID = 'uuid'
    FILENAME = 'fileName'
    CONTENT_TYPE = 'contentType'


class FormatField:
    UUID = 'uuid'
    NAME = 'name'
    STEPS = 'steps'


class StepField:
    NAME = 'name'
    OPTIONS = 'options'


class TemplateField:
    ID = 'id'
    NAME = 'name'
    METAMODEL_VERSION = 'metamodelVersion'
    FILES = 'files'
    FORMATS = 'formats'
    ASSETS = 'assets'


class JobDataField:
    DOCUMENT_UUID = 'documentUuid'
    DOCUMENT_CONTEXT = 'documentContext'


class DocumentNamingStrategy:
    UUID = 'uuid'
    SANITIZE = 'sanitize'
    SLUGIFY = 'slugify'

    _DEFAULT = SANITIZE
    _NAMES = {
        'uuid': UUID,
        'sanitize': SANITIZE,
        'slugify': SLUGIFY,
    }

    @classmethod
    def get(cls, name: str):
        return cls._NAMES.get(name.lower(), cls._DEFAULT)
