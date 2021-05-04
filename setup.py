from setuptools import setup, find_packages


with open('README.md') as f:
    long_description = ''.join(f.readlines())

setup(
    name='document_worker',
    version='2.14.0',
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
        'Programming Language :: Python',
        'Topic :: Text Processing',
        ],
    zip_safe=False,
    python_requires='>=3.8, <4',
    install_requires=[
        'click',
        'jinja2',
        'markdown2',
        'pathvalidate',
        'pika',
        'pymongo',
        'PyYAML',
        'rdflib',
        'rdflib-jsonld',
        'python-slugify',
        'tenacity',
    ],
    entry_points={
        'console_scripts': [
            'docworker=document_worker:main',
        ],
    },
)
