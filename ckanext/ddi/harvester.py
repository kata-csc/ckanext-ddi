'''
Harvester for DDI2 formats
'''

#pylint: disable-msg=E1101,E0611,F0401
import logging
import json
import unicodedata
import string
import pprint
import urllib2
from lxml import etree
import xmltodict

from ckan.model import Session, Package, Resource, Group, Member
from ckan.plugins.core import SingletonPlugin, implements
from ckan.lib.navl.dictization_functions import flatten_dict
from ckan import model

from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject, HarvestJob

log = logging.getLogger(__name__)


class DDIHarvester(SingletonPlugin):
    '''
    DDI Harvester for ckanext-harvester.
    '''
    implements(IHarvester)

    config = None

    def _set_config(self, config_str):
        if config_str:
            self.config = json.loads(config_str)
        else:
            self.config = {}

    def info(self):
        return {
                'name':'DDI',
                'title':'DDI import',
                'description':'Mass importing harvester for DDI2',
                }

    def validate_config(self, config):
        return config

    def gather_stage(self, harvest_job):
        self._set_config(self.config)
        gather_url = harvest_job.source.url
        urls = urllib2.urlopen(gather_url)
        harvest_objs = []
        for url in urls.readlines():
            harvest_obj = HarvestObject()
            harvest_obj.content = url
            harvest_obj.job = harvest_job
            harvest_obj.save()
            harvest_objs.append(harvest_obj.id)
        return harvest_objs

    def fetch_stage(self, harvest_object):
        xml = urllib2.urlopen(harvest_object.content).read()
        try:
            harvest_object.content = json.dumps(
                                            xmltodict.parse(
                                                            etree.tostring(
                                                                  etree.fromstring(xml).xpath('/codeBook')[0]
                                                                  )
                                                            )
                                                )
        except Exception, e:
            print e
            return False
        return True

    def import_stage(self, harvest_object):
        model.repo.new_revision()
        code_dict = json.loads(harvest_object.content)
        pkg = Package()
        data_dict = code_dict['codeBook']
        citation = data_dict["stdyDscr"]["citation"]
        study_info = data_dict["stdyDscr"]["stdyInfo"]
        title = citation['titlStmt']['titl']
        pkg.name = title
        producer = citation['prodStmt']['producer']
        author = producer[0] if isinstance(producer,list) else producer
        pkg.author = author
        pkg.author_email = author
        for kw in study_info['subject']['keyword']:
            pkg.add_tag_by_name(kw['#text'])
        for kw in study_info['subject']['topcClas']:
            pkg.add_tag_by_name(kw['#text'])
        descr = citation['serStmt']['serInfo']['p'] 
        description_arr = descr if isinstance(descr, list) else [descr] 
        pkg.notes = '<br />'.join(description_arr)
        pkg.extras = flatten_dict(dict(citation, **study_info))
        pkg.url = unicodedata.normalize('NFKD', unicode(title))\
                                  .encode('ASCII', 'ignore')\
                                  .lower().replace(' ','_')
        pkg.save()
        producer = producer if isinstance(producer,list) else [producer] 
        for producer in producer:
            log.debug(producer)
            prod_text = producer
            group = Group.by_name(prod_text)
            if not group:
                group = Group(name=prod_text, description=prod_text)
            group.add_package_by_name(pkg.name)
            group.save()
        res_url = code_dict['codeBook']['docDscr']['citation']['holdings']['@URI'] if '@URI' in code_dict['codeBook']['docDscr']['citation']['holdings'] else ''
        pkg.add_resource(res_url, description=''.join(description_arr), name=title)
        log.debug(pprint.pprint(pkg.as_dict()))
        return True