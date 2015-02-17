DDI2 harvester for CKAN. This extends CKAN harvester to parse DDI2 metadata sources and import datasets.

Harvester configuration
=======================

Please make sure you have ckanext-harvest installed. You can add a harvest source from CKAN UI and add the URL in which the XML files for DDI2 reside.

Configuration options:
 *  limit: Import only first 'limit' number of XML files.

Here is an example of a configuration object (the one that must be entered in
the configuration field):
    {
     "limit": 10,
    }
