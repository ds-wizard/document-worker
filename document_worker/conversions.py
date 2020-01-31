import itertools
import subprocess

from document_worker.formats import Format, Formats

# TODO: unify encoding across modules
DEFAULT_ENCODING = 'utf-8'


class WkHtmlToPdf:
    # TODO: config

    SOURCE_FORMATS = [Formats.HTML]
    TARGET_FORMATS = [Formats.PDF]

    def __call__(self, source_format: Format, target_format: Format,
                data: str, metadata: dict) -> bytes:
        # TODO: detect and handle fails
        p = subprocess.Popen(['wkhtmltopdf',
                              '--quiet', '--encoding', DEFAULT_ENCODING, '-', '-'],
                             stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        stdout, stderr = p.communicate(input=data)
        return stdout


class Pandoc:
    # TODO: config

    SOURCE_FORMATS = [Formats.HTML]
    TARGET_FORMATS = [Formats.LaTeX, Formats.RST, Formats.ODT,
                      Formats.DOCX, Formats.Markdown]

    def __call__(self, source_format: Format, target_format: Format,
                data: bytes, metadata: dict) -> bytes:
        # TODO: detect and handle fails
        p = subprocess.Popen(['pandoc', '-s', '-f', 'html', '-t',
                              target_format.name, '-o', '-'],
                             stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        stdout, stderr = p.communicate(input=data)
        return stdout


class FormatConvertor:

    CONVERTORS = [WkHtmlToPdf(), Pandoc()]

    def __init__(self):
        # TODO: config
        self.convertors = dict()
        for c in self.CONVERTORS:
            for conv in itertools.product(c.SOURCE_FORMATS, c.TARGET_FORMATS):
                self.convertors[conv] = c

    def can_convert(self, source_format: Format, target_format: Format):
        return source_format == target_format or (source_format, target_format) in self.convertors

    def convert(self, source_format: Format, target_format: Format,
                data: bytes, metadata: dict) -> bytes:
        if source_format == target_format:
            return data
        # TODO: detect and handle fails
        convertor = self.convertors[source_format, target_format]
        return convertor(source_format, target_format, data, metadata)
