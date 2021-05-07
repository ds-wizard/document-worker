from config import DocumentWorkerConfig

from document_worker.connection.database import Database
from document_worker.documents import DocumentNameGiver
from document_worker.templates import TemplateRegistry


class AppContext:

    def __init__(self, cfg: DocumentWorkerConfig, template_registry: TemplateRegistry,
                 name_giver: DocumentNameGiver):
        self.db = Database(cfg=cfg.db)
        self.s3 = ...
        self.cfg = cfg
        self.template_registry = template_registry
        self.name_giver = name_giver


class JobContext:

    def __init__(self, trace_id: str):
        self.trace_id = trace_id


class Context:

    def __init__(self, app: AppContext, job: JobContext):
        self.app = app
        self.job = job
