from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(
	name='ckanext-ddi',
	version=version,
	description="DDI Importing tools for CKAN",
	long_description="""\
	""",
	classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
	keywords='',
	author='Aleksi Suomalainen',
	author_email='aleksi.suomalainen@nomovok.com',
	url='http://not.there.yet',
	license='AGPL',
	packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
	namespace_packages=['ckanext', 'ckanext.ddi'],
	include_package_data=True,
	zip_safe=False,
	install_requires=[
		# -*- Extra requirements: -*-
		'lxml'
	],
	entry_points=\
	"""
        [ckan.plugins]
	# Add plugins here, eg
	# myplugin=ckanext.ddi:PluginClass
	[paste.paster_command]
	ddi_import = ckanext.ddi.commands.ddi_import:DDIImporter
	""",
)
