# Data Stewardship Wizard Document Worker

[![User Guide](https://img.shields.io/badge/docs-User%20Guide-informational)](https://guide.ds-wizard.org)
[![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/ds-wizard/document-worker)](https://github.com/ds-wizard/document-worker/releases)
[![Docker Pulls](https://img.shields.io/docker/pulls/datastewardshipwizard/document-worker)](https://hub.docker.com/r/datastewardshipwizard/document-worker)
[![Document Worker CI](https://github.com/ds-wizard/document-worker/workflows/Document%20Worker%20CI/badge.svg?branch=main)](https://github.com/ds-wizard/document-worker/actions)
[![GitHub](https://img.shields.io/github/license/ds-wizard/document-worker)](LICENSE)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/4975/badge)](https://bestpractices.coreinfrastructure.org/projects/4975)

:bangbang: **This project has been moved to monorepo [ds-wizard/engine-tools](https://github.com/ds-wizard/engine-tools)** :bangbang:

ds-wizard/mailer

## Dependencies

-  PostgreSQL
-  S3 storage (e.g. [Minio](https://min.io))
-  [wkhtmltopdf](https://github.com/wkhtmltopdf/wkhtmltopdf)
-  [pandoc](https://github.com/jgm/pandoc)

For more information, see [deployment example](https://github.com/ds-wizard/dsw-deployment-example).

## Documentation

For general information, please visit our [User Guide](https://guide.ds-wizard.org).

DSW Document Worker technical documentation for template development:

* [Document Context](./support/DocumentContext.md)
* [Jinja Filters](./support/JinjaFilters.md)
* [Jinja Tests](./support/JinjaTests.md)

## Docker

Docker image is prepared with basic dependencies and worker installed. It is available though Docker Hub: [datastewardshipwizard/document-worker](https://hub.docker.com/r/datastewardshipwizard/document-worker).

### Build image

You can easily build the image yourself:

```bash
$ docker build . -t docworker:local
```

### Mount points

-  `/app/config.yml` = configuration file (see [example](config.yml))
-  `/usr/share/fonts/<type>/<name>` = fonts according to [Debian wiki](https://wiki.debian.org/Fonts/PackagingPolicy) (for wkhtmltopdf)

### Fonts

We bundle Docker image with default fonts (for PDF generation, see `fonts` folder):

- [Noto Fonts](https://github.com/googlefonts/noto-fonts) (some variants)
- [Symbola](https://fontlibrary.org/en/font/symbola)

## License

This project is licensed under the Apache License v2.0 - see the
[LICENSE](LICENSE) file for more details.
