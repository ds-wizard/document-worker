import click
from typing import IO

from document_worker.config import DocumentWorkerConfig, MissingConfigurationError
from document_worker.worker import DocumentWorker


def validate_config(ctx, param, value: IO):
    config = DocumentWorkerConfig()
    config.read_file(value)
    try:
        config.validate()
        return config
    except MissingConfigurationError as e:
        click.echo('Error: Missing configuration', err=True)
        for section, option in e.missing:
            click.echo(f' - {section}: {option}')
        exit(1)


@click.command()
@click.argument('config', type=click.File('r'), callback=validate_config)
@click.argument('templates_dir', type=click.Path(exists=True, file_okay=False, dir_okay=True))
def main(config, templates_dir):
    worker = DocumentWorker(config, templates_dir)
    try:
        worker.run()
    except Exception as e:
        click.echo(f'Ended with error: {e}')
        exit(2)
