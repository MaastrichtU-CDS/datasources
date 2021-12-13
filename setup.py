from setuptools import setup

setup(
    name='datasources',
    version='0.0.1',
    description='A set of datasources to use in data engineering tasks',
    url='https://github.com/jaspersnel/datasources',
    author='Jasper Snel',
    author_email='pypi@jaspersnel.me',
    license='Mozilla Public License 2.0 (MPL-2.0)',
    packages=['datasources'],
    python_requires='>=3.6',
    install_requires=[
        'rdflib'
    ]
)