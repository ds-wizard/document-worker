import jinja2  # type: ignore
import json

from typing import Optional

from document_worker.consts import DEFAULT_ENCODING
from document_worker.context import Context
from document_worker.conversions import Pandoc, WkHtmlToPdf, RdfLibConvert
from document_worker.documents import DocumentFile, FileFormat, FileFormats


class FormatStepException(Exception):

    def __init__(self, message):
        self.message = message


class Step:

    def __init__(self, template, options: dict):
        self.template = template
        self.options = options

    def execute_first(self, context: dict) -> Optional[DocumentFile]:
        return self.raise_exc('Called execute_follow on Step class')

    def execute_follow(self, document: DocumentFile) -> Optional[DocumentFile]:
        return self.raise_exc('Called execute_follow on Step class')

    def raise_exc(self, message: str):
        raise FormatStepException(message)


class JSONStep(Step):
    NAME = 'json'
    OUTPUT_FORMAT = FileFormats.JSON

    def execute_first(self, context: dict) -> DocumentFile:
        return DocumentFile(
            self.OUTPUT_FORMAT,
            json.dumps(context, indent=2, sort_keys=True).encode(DEFAULT_ENCODING),
            DEFAULT_ENCODING
        )

    def execute_follow(self, document: DocumentFile) -> Optional[DocumentFile]:
        return self.raise_exc(f'Step "{self.NAME}" cannot process other files')


class Jinja2Step(Step):
    NAME = 'jinja'
    DEFAULT_FORMAT = FileFormats.HTML

    OPTION_ROOT_FILE = 'template'
    OPTION_CONTENT_TYPE = 'content-type'
    OPTION_EXTENSION = 'extension'

    def __init__(self, template, options: dict):
        super().__init__(template, options)
        self.root_file = self.options[self.OPTION_ROOT_FILE]
        self.content_type = self.options.get(self.OPTION_CONTENT_TYPE, self.DEFAULT_FORMAT.content_type)
        self.extension = self.options.get(self.OPTION_EXTENSION, self.DEFAULT_FORMAT.file_extension)

        self.output_format = FileFormat(self.extension, self.content_type, self.extension)
        try:
            self.j2_env = jinja2.Environment(
                loader=jinja2.FileSystemLoader(searchpath=template.template_dir),
                extensions=['jinja2.ext.do'],
            )
            self._add_j2_enhancements()
            self.j2_root_template = self.j2_env.get_template(self.root_file)
        except Exception as e:
            self.raise_exc(f'Failed loading Jinja2 template: {e}')

    def _add_j2_enhancements(self):
        from document_worker.templates.filters import filters
        from document_worker.templates.tests import tests
        self.j2_env.filters.update(filters)
        self.j2_env.tests.update(tests)

    def execute_first(self, context: dict) -> DocumentFile:
        def asset_fetcher(file_name):
            return self.template.fetch_asset(file_name)

        def asset_path(file_name):
            return self.template.asset_path(file_name)

        return DocumentFile(
            self.output_format,
            self.j2_root_template.render(
                ctx=context,
                assets=asset_fetcher,
                asset_path=asset_path,
            ).encode(DEFAULT_ENCODING),
            DEFAULT_ENCODING
        )

    def execute_follow(self, document: DocumentFile) -> Optional[DocumentFile]:
        return self.raise_exc(f'Step "{self.NAME}" cannot process other files')


class WkHtmlToPdfStep(Step):
    NAME = 'wkhtmltopdf'
    INPUT_FORMAT = FileFormats.HTML
    OUTPUT_FORMAT = FileFormats.PDF

    def __init__(self, template, options: dict):
        super().__init__(template, options)
        self.wkhtmltopdf = WkHtmlToPdf(config=Context.get().app.cfg)

    def execute_first(self, context: dict) -> Optional[DocumentFile]:
        return self.raise_exc(f'Step "{self.NAME}" cannot be first')

    def execute_follow(self, document: DocumentFile) -> DocumentFile:
        if document.file_format != FileFormats.HTML:
            self.raise_exc(f'WkHtmlToPdf does not support {document.file_format.name} format as input')
        data = self.wkhtmltopdf(
            source_format=self.INPUT_FORMAT,
            target_format=self.OUTPUT_FORMAT,
            data=document.content,
            metadata=self.options,
            workdir=str(self.template.template_dir),
        )
        return DocumentFile(self.OUTPUT_FORMAT, data)


class PandocStep(Step):
    NAME = 'pandoc'

    INPUT_FORMATS = frozenset([
        FileFormats.DOCX,
        FileFormats.EPUB,
        FileFormats.HTML,
        FileFormats.LaTeX,
        FileFormats.Markdown,
        FileFormats.ODT,
        FileFormats.RST,
    ])
    OUTPUT_FORMATS = frozenset([
        FileFormats.ADoc,
        FileFormats.DocBook4,
        FileFormats.DocBook5,
        FileFormats.DOCX,
        FileFormats.EPUB,
        FileFormats.HTML,
        FileFormats.LaTeX,
        FileFormats.Markdown,
        FileFormats.ODT,
        FileFormats.RST,
        FileFormats.RTF,
    ])

    OPTION_FROM = 'from'
    OPTION_TO = 'to'

    def __init__(self, template, options: dict):
        super().__init__(template, options)
        self.pandoc = Pandoc(config=Context.get().app.cfg)
        self.input_format = FileFormats.get(options[self.OPTION_FROM])
        self.output_format = FileFormats.get(options[self.OPTION_TO])
        if self.input_format not in self.INPUT_FORMATS:
            self.raise_exc(f'Unknown input format "{self.input_format.name}"')
        if self.output_format not in self.OUTPUT_FORMATS:
            self.raise_exc(f'Unknown output format "{self.output_format.name}"')

    def execute_first(self, context: dict) -> Optional[DocumentFile]:
        return self.raise_exc(f'Step "{self.NAME}" cannot be first')

    def execute_follow(self, document: DocumentFile) -> DocumentFile:
        if document.file_format != self.input_format:
            self.raise_exc(f'Unexpected input {document.file_format.name} as input for pandoc')
        data = self.pandoc(
            source_format=self.input_format,
            target_format=self.output_format,
            data=document.content,
            metadata=self.options,
            workdir=str(self.template.template_dir),
        )
        return DocumentFile(self.output_format, data)


class RdfLibConvertStep(Step):
    NAME = 'rdflib-convert'

    INPUT_FORMATS = [
        FileFormats.RDF_XML,
        FileFormats.N3,
        FileFormats.NTRIPLES,
        FileFormats.TURTLE,
        FileFormats.TRIG,
        FileFormats.JSONLD,
    ]

    OUTPUT_FORMATS = INPUT_FORMATS

    OPTION_FROM = 'from'
    OPTION_TO = 'to'

    def __init__(self, template, options: dict):
        super().__init__(template, options)
        self.rdflib_convert = RdfLibConvert(config=Context.get().app.cfg)
        self.input_format = FileFormats.get(options[self.OPTION_FROM])
        self.output_format = FileFormats.get(options[self.OPTION_TO])
        if self.input_format not in self.INPUT_FORMATS:
            self.raise_exc(f'Unknown input format "{self.input_format.name}"')
        if self.output_format not in self.OUTPUT_FORMATS:
            self.raise_exc(f'Unknown output format "{self.output_format.name}"')

    def execute_first(self, context: dict) -> Optional[DocumentFile]:
        return self.raise_exc(f'Step "{self.NAME}" cannot be first')

    def execute_follow(self, document: DocumentFile) -> DocumentFile:
        if document.file_format != self.input_format:
            self.raise_exc(f'Unexpected input {document.file_format.name} '
                           f'as input for rdflib-convert '
                           f'(expecting {self.input_format.name})')
        data = self.rdflib_convert(
            self.input_format, self.output_format, document.content, self.options
        )
        return DocumentFile(self.output_format, data)


STEPS = {
    JSONStep.NAME: JSONStep,
    Jinja2Step.NAME: Jinja2Step,
    WkHtmlToPdfStep.NAME: WkHtmlToPdfStep,
    PandocStep.NAME: PandocStep,
    RdfLibConvertStep.NAME: RdfLibConvertStep,
}


def create_step(template, name: str, options: dict) -> Step:
    if name not in STEPS:
        raise KeyError(f'Unknown step name "{name}"')
    step = STEPS[name](template, options)
    return step
