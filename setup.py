from setuptools import setup, find_packages


with open('README.md') as f:
    long_description = ''.join(f.readlines())

setup(
    name='document_worker',
    version='3.13.0',
    description='Worker for assembling and transforming documents',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Marek Suchánek',
    keywords='documents worker jinja2 pandoc pdf-generation',
    license='Apache License 2.0',
    url='https://github.com/ds-wizard/document-worker',
    packages=find_packages(exclude=["addons", "fonts"]),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Topic :: Text Processing',
    ],
    zip_safe=False,
    python_requires='>=3.8, <4',
    install_requires=[
        'click',
        'jinja2',
        'Markdown',
        'MarkupSafe',
        'mdx-breakless-lists',
        'minio',
        'pathvalidate',
        'pdfrw',
        'psycopg2',
        'PyYAML',
        'rdflib',
        'rdflib-jsonld',
        'requests',
        'python-slugify',
        'python-dateutil',
        'sentry-sdk',
        'tenacity',
    ],
    entry_points={
        'console_scripts': [
            'docworker=document_worker:main',
        ],
    },
)
