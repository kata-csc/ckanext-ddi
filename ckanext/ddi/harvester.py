# coding: utf-8
'''
Harvester for DDI2 formats
'''

#pylint: disable-msg=E1101,E0611,F0401
import logging
import json
import urllib2
from lxml import etree
import xmltodict

from ckan.model import Package, Group

from ckan import model
from ckan.model.authz import setup_default_user_roles
from ckan.lib.munge import munge_tag
from ckanext.harvest.harvesters.base import HarvesterBase
from ckanext.harvest.model import HarvestObject

from bs4 import BeautifulSoup, Tag

log = logging.getLogger(__name__)


class DDIHarvester(HarvesterBase):
    '''
    DDI Harvester for ckanext-harvester.
    '''
    config = None

    def _set_config(self, config_str):
        '''Set the configuration string.
        '''
        if config_str:
            self.config = json.loads(config_str)

    def info(self):
        '''Return information about this harvester.
        '''
        return {
                'name': 'DDI',
                'title': 'DDI import',
                'description': 'Mass importing harvester for DDI2',
                }

    def validate_config(self, config):
        '''Validate the config, returns it since we don't have any configuration
        parameters
        '''
        return config

    def gather_stage(self, harvest_job):
        '''Gather the URLs to fetch from a URL which has a list of links to XML
        documents containing the DDI documents.
        '''
        self._set_config(self.config)
        gather_url = harvest_job.source.url
        try:
            urls = urllib2.urlopen(gather_url)
            harvest_objs = []
            for url in urls.readlines():
                harvest_obj = HarvestObject()
                harvest_obj.content = json.dumps({'url': url})
                harvest_obj.job = harvest_job
                harvest_obj.save()
                harvest_objs.append(harvest_obj.id)
        except urllib2.URLError:
            self._save_gather_error('Could not gather XML files from URL!', 
                                    harvest_job)
            return None
        return harvest_objs

    def fetch_stage(self, harvest_object):
        '''Fetch and parse the DDI XML document.
        '''
        return True

    def _collect_attribs(self, el):
        '''Collect attributes to a string with (k,v) value where k is attribute
        name and v is the attribute value.
        '''
        astr = ""
        if el.attrs:
            for k, v in el.attrs.items():
                astr += "(%s,%s)" % (k, v)
        return astr

    def import_stage(self, harvest_object):
        '''Import the metadata received in the fetch stage to a dataset and
        create groups if ones are defined. Fill in metadata from study and
        document description.
        '''
        try:
            xml_dict = {}
            xml_dict['source'] = harvest_object.content
            udict = json.loads(harvest_object.content)
            if 'url' in udict:
                ddi_xml = BeautifulSoup(urllib2.urlopen(udict['url']).read(),
                                        'xml')
            else:
                print "No url"
                log.debug("No url in content")
                self._save_object_error('No url in content!', harvest_object)
                return False
        except urllib2.URLError:
            print "Fetch"
            log.debug("Could not fetch %s" % udict['url'])
            self._save_object_error('Could not fetch from url %s!' % udict['url'], 
                                    harvest_object)
            return False
        except etree.XMLSyntaxError:
            print "Parse"
            log.debug("Unable to parse!")
            self._save_object_error('Unable to parse XML!', harvest_object)
            return False
        model.repo.new_revision()
        study_descr = ddi_xml.codeBook.stdyDscr
        document_info = ddi_xml.codeBook.docDscr.citation
        title = study_descr.citation.titlStmt.titl.string
        if not title:
            title = document_info.titlStmt.titl.string
        name = self._gen_new_name(self._check_name(title[:100]))
        pkg = Package.get(name)
        if not pkg:
            pkg = Package(name=name)
        producer = study_descr.citation.prodStmt.producer
        if not producer:
            producer = document_info.prodStmt.producer
        pkg.author = producer.string
        keywords = study_descr.stdyInfo.subject('keyword')
        for kw in keywords:
            kw = kw.string
            if kw:
                pkg.add_tag_by_name(munge_tag(kw))
        keywords = study_descr.stdyInfo.subject('topcClas')
        for kw in keywords:
            kw = kw.string
            if kw:
                pkg.add_tag_by_name(munge_tag(kw))
        description_array = study_descr.stdyInfo.abstract('p')
        if not len(description_array):
            description_array = study_descr.citation.sertStmt('p')
        pkg.notes = '<br />'.join([description.string
                                   for description in description_array])
        pkg.title = title[:100]
        pkg.url = udict['url']
        pkg.add_resource(url=document_info.holdings['URI']\
                         if 'URI' in document_info.holdings else '')
        metas = {}
        for docextra in document_info.descendants:
            if isinstance(docextra, Tag):
                if docextra:
                    metas[docextra.name] = docextra.string\
                                    if docextra.string\
                                    else self._collect_attribs(docextra)
        for stdyextra in study_descr.stdyInfo.descendants:
            if isinstance(docextra, Tag):
                if docextra:
                    metas[docextra.name] = docextra.string\
                                    if docextra.string\
                                    else self._collect_attribs(docextra)
        vars = {}
        if ddi_xml.codeBook.dataDscr:
            for var in ddi_xml.codeBook.dataDscr('var'):
                if var.sumStat:
                    if var('sumStat', type='mean'):
                        if var.qstn:
                            vars[var['name']] = var.qstn.qstnLit.string
        pkg.extras = dict(metas, **vars)
        pkg.save()
        producers = study_descr.citation.prodStmt.find_all('producer')
        for producer in producers:
            producer = producer.string
            if producer:
                group = Group.by_name(producer)
                if not group:
                    group = Group(name=producer, description=producer)
                group.add_package_by_name(pkg.name)
                group.save()
                setup_default_user_roles(group)
        log.debug("Saved pkg %s" % (pkg.url))
        setup_default_user_roles(pkg)
        return True
