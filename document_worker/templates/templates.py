import base64
import pathlib
import shutil

from typing import List, Optional, Dict

from document_worker.connection.database import DBTemplate, DBTemplateFile, DBTemplateAsset
from document_worker.consts import FormatField
from document_worker.context import Context
from document_worker.documents import DocumentFile
from document_worker.templates.formats import Format


class TemplateException(Exception):

    def __init__(self, template_id: str, message: str):
        self.template_id = template_id
        self.message = message


class Asset:

    def __init__(self, asset_uuid: str, filename: str, content_type: str,
                 data: bytes):
        self.asset_uuid = asset_uuid
        self.filename = filename
        self.content_type = content_type
        self.data = data

    @property
    def data_base64(self) -> str:
        return base64.b64encode(self.data).decode('ascii')

    @property
    def src_value(self):
        return f'data:{self.content_type};base64,{self.data_base64}'


class TemplateComposite:

    def __init__(self, db_template, db_files, db_assets):
        self.template = db_template  # type: DBTemplate
        self.files = db_files  # type: List[DBTemplateFile]
        self.assets = db_assets  # type: List[DBTemplateAsset]


class Template:

    def __init__(self, template_dir: pathlib.Path, db_template: TemplateComposite):
        self.template_dir = template_dir
        self.db_template = db_template
        self.template_id = self.db_template.template.template_id
        self.formats = dict()  # type: Dict[str, Format]
        self.prepare_template_files()
        self.prepare_template_assets()

    def raise_exc(self, message: str):
        raise TemplateException(self.template_id, message)

    def fetch_asset(self, file_name: str) -> Optional[Asset]:
        Context.logger.info(f'Fetching asset "{file_name}"')
        file_path = self.template_dir / file_name
        asset = None
        for a in self.db_template.assets:
            if a.file_name == file_name:
                asset = a
                break
        if asset is None or not file_path.exists():
            Context.logger.error(f'Asset "{file_name}" not found')
            return None
        return Asset(
            asset_uuid=asset.uuid,
            filename=file_name,
            content_type=asset.content_type,
            data=file_path.read_bytes()
        )

    def asset_path(self, filename: str) -> str:
        return str(self.template_dir / filename)

    def prepare_template_files(self):
        Context.logger.info(f'Storing files of template {self.template_id} locally')
        for template_file in self.db_template.files:
            full_path = self.template_dir / template_file.file_name
            full_path.parent.mkdir(parents=True, exist_ok=True)
            full_path.write_text(
                data=template_file.content,
                encoding='utf-8',
            )

    def prepare_template_assets(self):
        Context.logger.info(f'Storing assets of template {self.template_id} locally')
        for asset in self.db_template.assets:
            remote_path = f'templates/{self.db_template.template.id}/{asset.uuid}'
            local_path = self.template_dir / asset.file_name
            result = Context.get().app.s3.download_file(remote_path, local_path)
            if not result:
                Context.logger.error(f'Asset "{asset.file_name}" cannot be retrieved')

    def prepare_format(self, format_uuid: str) -> bool:
        for format_meta in self.db_template.template.formats:
            if format_uuid == format_meta.get(FormatField.UUID, None):
                try:
                    self.formats[format_uuid] = Format(self, format_meta)
                except Exception as e:
                    Context.logger.error(f'Format {format_uuid} of template '
                                         f'{self.template_id} cannot be loaded - {e}')
                return True
        return False

    def has_format(self, format_uuid: str) -> bool:
        return any(map(
            lambda f: f[FormatField.UUID] == format_uuid,
            self.db_template.template.formats
        ))

    def __getitem__(self, format_uuid: str) -> Format:
        return self.formats[format_uuid]

    def render(self, format_uuid: str, context: dict) -> DocumentFile:
        return self[format_uuid].execute(context)


def prepare_template(template: DBTemplate, files: List[DBTemplateFile],
                     assets: List[DBTemplateAsset]) -> Template:
    workdir = Context.get().app.workdir
    template_id = template.id
    template_dir = workdir / template_id.replace(':', '_')
    if template_dir.exists():
        shutil.rmtree(template_dir)
    template_dir.mkdir()

    return Template(
        template_dir=template_dir,
        db_template=TemplateComposite(
            db_template=template,
            db_files=files,
            db_assets=assets,
        ),
    )
