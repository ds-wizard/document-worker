DEFAULT_ENCODING = 'utf-8'
EXIT_SUCCESS = 0


class DocumentState:
    QUEUED = 'QueuedDocumentState'
    PROCESSING = 'InProgressDocumentState'
    FAILED = 'ErrorDocumentState'
    FINISHED = 'DoneDocumentState'


class DocumentField:
    UUID = 'uuid'
    NAME = 'name'
    STATE = 'state'
    TEMPLATE = 'templateUuid'
    FORMAT = 'formatUuid'
    RETRIEVED = 'retrievedAt'
    FINISHED = 'finishedAt'
    METADATA = 'metadata'
    METADATA_CONTENT_TYPE = 'contentType'
    METADATA_FILENAME = 'fileName'


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
