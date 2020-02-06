import jinja2
import json
import logging
import os
import uuid


from document_worker.formats import Format, Formats


class TemplateException(Exception):

    def __init__(self, template_metafile: str, template_uuid: uuid.UUID, message: str):
        self.template_metafile = template_metafile
        self.template_uuid = template_uuid
        self.message = message


class TemplateMetaField:
    UUID = 'uuid'
    NAME = 'name'
    ROOT_FILE = 'rootFile'


class Template:

    META_REQUIRED = [TemplateMetaField.UUID,
                     TemplateMetaField.NAME,
                     TemplateMetaField.ROOT_FILE]

    def __init__(self, meta_file: str):
        self.meta_file = meta_file
        self.uuid = None
        logging.info(f'Loading metadata from {meta_file}')
        self.metadata = self._load_metadata()
        logging.info(f'Verifying metadata from {meta_file}')
        self._verify_metadata()
        self.basedir = os.path.dirname(meta_file)
        self.uuid = uuid.UUID(self.metadata[TemplateMetaField.UUID])
        self.name = self.metadata[TemplateMetaField.NAME]
        self.root_file = os.path.join(self.basedir, self.metadata[TemplateMetaField.ROOT_FILE])
        self.root_filename = os.path.basename(self.metadata[TemplateMetaField.ROOT_FILE])
        logging.info(f'Setting up Jinja2 env for "{self.name}" ({self.uuid})')
        self.j2_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(os.path.dirname(self.root_file)),
            extensions=['jinja2.ext.do'],
        )
        self._add_j2_enhancements()
        self.j2_root_template = self.j2_env.get_template(self.root_filename)

    def _raise_exc(self, message: str):
        raise TemplateException(self.meta_file, self.uuid, message)

    def _load_metadata(self) -> dict:
        try:
            with open(self.meta_file, mode='r') as f:
                return json.load(f)
        except Exception as e:
            self._raise_exc(f'Cannot read template meta file: {e}')

    def _verify_metadata(self):
        for required_field in self.META_REQUIRED:
            if required_field not in self.metadata:
                self._raise_exc(f'Missing required field {required_field}')

    def _create_jinja2_template(self) -> jinja2.Template:
        try:
            with open(self.root_file, mode='r') as f:
                content = f.read()
        except Exception as e:
            self._raise_exc(f'Cannot read template root file: {e}')
        try:
            return jinja2.Template(content)
        except Exception as e:
            self._raise_exc(f'Failed to load Jinja2 template: {e}')

    @property
    def output_format(self) -> Format:
        # Now there are only HTML templates
        return Formats.HTML

    def render(self, context: dict) -> str:
        return self.j2_root_template.render(ctx=context)

    def _add_j2_enhancements(self):
        from document_worker.templates.filters import filters
        from document_worker.templates.tests import tests
        self.j2_env.filters.update(filters)
        self.j2_env.tests.update(tests)


class TemplateRegistry:

    META_EXT = '.json'

    def __init__(self, templates_dir: str):
        self.templates_dir = templates_dir
        self.templates = dict()

    def load_templates(self):
        logging.info(f'Loading document templates from {self.templates_dir}')
        for filename in os.listdir(self.templates_dir):
            if filename.endswith(self.META_EXT):
                self._load_template(os.path.join(self.templates_dir, filename))

    def _load_template(self, meta_file: str):
        try:
            logging.info(f'Loading template from {meta_file}')
            template = Template(meta_file)
            if template.uuid in self.templates:
                logging.warning(f'Duplicate template UUID {template.uuid}'
                                f' - ignoring {meta_file}')
            else:
                self.templates[template.uuid] = template
                logging.info(f'Template from {meta_file} loaded ({template.uuid})')
        except TemplateException as e:
            logging.error(f'Template from {meta_file} ({e.template_uuid})'
                          f'cannot be loaded - {e.message}')
        except Exception as e:
            logging.error(f'Template from {meta_file} '
                          f'cannot be loaded - {type(e).__name__}: {e}')

    def has_template(self, template_uuid: uuid.UUID) -> bool:
        self.templates.keys()
        return template_uuid in self.templates.keys()

    def __getitem__(self, template_uuid: uuid.UUID) -> Template:
        return self.templates[template_uuid]

    def render(self, template_uuid: uuid.UUID, context: dict) -> str:
        return self.templates[template_uuid].render(context)
