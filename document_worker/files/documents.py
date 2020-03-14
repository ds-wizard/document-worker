from document_worker.files import FileFormat
from document_worker.consts import DEFAULT_ENCODING


class DocumentFile:

    def __init__(self, file_format: FileFormat, content: bytes, encoding: str = DEFAULT_ENCODING):
        self.file_format = file_format
        self.content = content
        self.encoding = encoding

    @property
    def content_type(self) -> str:
        return self.file_format.content_type

    def filename(self, name: str) -> str:
        return f'{name}.{self.file_format.file_extension}'

    def store(self, name: str):
        with open(self.filename(name), mode='bw') as f:
            f.write(self.content)
