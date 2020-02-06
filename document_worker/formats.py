class Format:

    def __init__(self, name: str, content_type: str, file_extension: str):
        self.name = name
        self.content_type = content_type
        self.file_extension = file_extension

    def __eq__(self, other):
        return isinstance(other, Format) and other.name == self.name

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        return self.name

    def __repr__(self):
        return f'Format[{self.name}]'


class Formats:
    JSON = Format('json', 'application/json', 'json')
    HTML = Format('html', 'text/html', 'html')
    PDF = Format('pdf', 'application/pdf', 'pdf')
    DOCX = Format('docx', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document', 'docx')
    Markdown = Format('markdown', 'text/markdown', 'md')
    ODT = Format('odt', 'application/vnd.oasis.opendocument.text', 'odt')
    RST = Format('rst', 'text/x-rst', 'rst')
    LaTeX = Format('latex', 'application/x-tex', 'tex')

    @staticmethod
    def get(name: str):
        known_formats = {
            'html': Formats.HTML,
            'pdf': Formats.PDF,
            'docx': Formats.DOCX,
            'markdown': Formats.Markdown,
            'odt': Formats.ODT,
            'rst': Formats.RST,
            'latex': Formats.LaTeX,
            'json': Formats.JSON
        }
        return known_formats.get(name, None)

