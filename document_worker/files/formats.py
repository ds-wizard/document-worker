
class FileFormat:

    def __init__(self, name: str, content_type: str, file_extension: str):
        self.name = name
        self.content_type = content_type
        self.file_extension = file_extension

    def __eq__(self, other):
        return isinstance(other, FileFormat) and other.name == self.name

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        return self.name

    def __repr__(self):
        return f'Format[{self.name}]'


class FileFormats:
    JSON = FileFormat('json', 'application/json', 'json')
    HTML = FileFormat('html', 'text/html', 'html')
    PDF = FileFormat('pdf', 'application/pdf', 'pdf')
    DOCX = FileFormat('docx', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document', 'docx')
    Markdown = FileFormat('markdown', 'text/markdown', 'md')
    ODT = FileFormat('odt', 'application/vnd.oasis.opendocument.text', 'odt')
    RST = FileFormat('rst', 'text/x-rst', 'rst')
    LaTeX = FileFormat('latex', 'application/x-tex', 'tex')
    EPUB = FileFormat('epub', 'application/epub+zip', 'epub')
    DocBook4 = FileFormat('docbook4', 'application/docbook+xml', 'dbk')
    DocBook5 = FileFormat('docbook5', 'application/docbook+xml', 'dbk')
    PPTX = FileFormat('pptx', 'application/vnd.openxmlformats-officedocument.presentationml.presentation', 'pptx')
    RTF = FileFormat('rtf', 'application/rtf', 'rtf')
    ADoc = FileFormat('asciidoc', 'text/asciidoc', 'adoc')

    @staticmethod
    def get(name: str):
        known_formats = {
            'html': FileFormats.HTML,
            'pdf': FileFormats.PDF,
            'docx': FileFormats.DOCX,
            'markdown': FileFormats.Markdown,
            'odt': FileFormats.ODT,
            'rst': FileFormats.RST,
            'latex': FileFormats.LaTeX,
            'json': FileFormats.JSON,
            'epub': FileFormats.EPUB,
            'docbook4': FileFormats.DocBook4,
            'docbook5': FileFormats.DocBook5,
            'pptx': FileFormats.PPTX,
            'rtf': FileFormats.RTF,
            'asciidoc': FileFormats.ADoc,
        }
        return known_formats.get(name, None)
