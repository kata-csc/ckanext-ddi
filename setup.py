from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(
    name='ckanext-ddi',
    version=version,
    description="DDI Importing tools for CKAN",
    long_description="""\
    """,
    classifiers=[],
    # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    keywords='',
    author='CSC',
    author_email='kata-project@postit.csc.fi',
    url='',
    license='',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['ckanext', 'ckanext.ddi'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        # -*- Extra requirements: -*-
        'beautifulsoup4',
        'ckanclient',
        'unicodecsv>=0.9.0',
        'python-dateutil',
    ],
    tests_require=[
        'nose',
        'mock'
    ],
    setup_requires=[
        'nose',
        'coverage'
    ],
    entry_points="""
    [ckan.plugins]
    # Add plugins here, eg
    ddi_harvester=ckanext.ddi.harvester:DDIHarvester
    # ddi3_harvester=ckanext.ddi.harvester:DDI3Harvester
    [paste.paster_command]
    ddi_import = ckanext.ddi.commands.ddi_import:DDIImporter
    """,
)
