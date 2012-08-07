============================================
DDI2/3 parser command and harvester for CKAN
============================================

This extension for ckanext-harvester enables the parsing of a DDI2/3 metadata
source to having them as datasets for CKAN.

Paster parsing command
----------------------

The parser is quite simple to use, it has 2 modes, first single file/url or second
a URL/file to a list of urls. 

Mode 1:

 paster ddi_import fetch http://www.fsd.uta.fi/fi/aineistot/luettelo/FSD1008/FSD1008.xml --config=../ckan/development.ini

Mode 2:
 
 paster ddi_import multifile http://www.fsd.uta.fi/fi/aineistot/luettelo/fsd-ddi-records-uris-fi.txt --config=../ckan/development.ini

Harvester configuration
-----------------------

Please make sure you have ckanext-harvester installed. In the harvester source 
addition/edition UI, please add the URL in which the XML files for DDI2/3 reside.

Configuration options:
