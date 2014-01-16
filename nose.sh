echo "Running tests for DDI Harvester. (You should run this from src/)";
nosetests --ckan --with-pylons=ckanext-ddi/test-core.ini ckanext-ddi/ckanext/ddi/tests --logging-level=CRITICAL
