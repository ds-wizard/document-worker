import jinja2

from document_worker.config import DocumentWorkerConfig
from document_worker.consts import TemplateFileField


class TemplateFileLoader(jinja2.BaseLoader):

    def __init__(self, config: DocumentWorkerConfig, template):
        self.config = config
        self.template_id = template.template_id
        self.__templates = {
            f[TemplateFileField.FILENAME]: f[TemplateFileField.CONTENT]
            for f in template.files
        }

    def get_source(self, environment, template):
        source = self.__templates.get(template, None)
        if source is None:
            raise jinja2.loaders.TemplateNotFound(
                f'Template file {template} does not exist in {self.template_id}'
            )
        return source, None, True
