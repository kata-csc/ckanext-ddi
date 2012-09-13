# coding: utf-8
'''
Harvester for DDI2 formats
'''

#pylint: disable-msg=E1101,E0611,F0401
import logging
import json
import urllib2
import StringIO
import re
import csv
import datetime

from pylons import config

from lxml import etree

from ckan.model import Package, Group
from ckan.lib.base import h
from ckan.controllers.storage import BUCKET, get_ofs

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

    def _check_has_element(self, var, head):
        origHead = head
        attr = None
        if head in ['preQtxt', 'qstnLit', 'postQTxt', 'ivuInstr']:
            var = var.qstn
        if head.startswith('sumStat'):
            types = head.split(' ')[-1]
            varstr = var(type=types)
            if varstr:
                attr = varstr[0]
        else:
            attr = getattr(var, head)
        if attr:
            if hasattr(attr, 'string'):
                attr = attr.string
        if head == 'sumStat':
            head = origHead
        return (head, attr)

    def _construct_csv(self, var, heads):
        retdict = {}
        for head in heads:
            has_elems = self._check_has_element(var, head)
            k, v = has_elems
            if v:
                retdict[k] = v.encode('utf-8')
            else:
                retdict[k] = None
        return retdict

    def _get_headers(self, vars):
        longest_els = []
        for var in vars:
            els = var(re.compile('^((?!catgry).)'), recursive=False)
            tmpels = []
            for el in els:
                if el.name == 'qstn':
                    tmpels.append('preQTxt')
                    tmpels.append('qstnLit')
                    tmpels.append('postQTxt')
                    tmpels.append('ivuInstr')
                if el.name == 'sumStat':
                    tmpels.append('sumStat ' + el['type'])
                if el.name not in ['qstn', 'sumStat']:
                    tmpels.append(el.name)
            if len(tmpels) > len(longest_els):
                longest_els = tmpels
        return longest_els

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
                f = urllib2.urlopen(udict['url']).read()
                ddi_xml = BeautifulSoup(f,
                                        'xml')
            else:
                self._save_object_error('No url in content!', harvest_object)
                return False
        except urllib2.URLError:
            self._save_object_error('Could not fetch from url %s!' % udict['url'], 
                                    harvest_object)
            return False
        except etree.XMLSyntaxError:
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
        pkg.author_email = producer.string
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
        if study_descr.stdyInfo.abstract:
            description_array = study_descr.stdyInfo.abstract('p')
        else:
            description_array = study_descr.citation.serStmt.serInfo('p')
        pkg.notes = '<br />'.join([description.string
                                   for description in description_array])
        pkg.title = title[:100]
        pkg.url = udict['url']
        ofs = get_ofs()
        nowstr = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')
        idno = study_descr.citation.titlStmt.IDNo
        agencyxml = (idno['agency'] if 'agency' in idno.attrs else '') + idno.string
        label = "%s/%s.xml" % (\
                    nowstr,
                    agencyxml)
        ofs.put_stream(BUCKET, label, f, {})
        fileurl = config.get('ckan.site_url') + h.url_for('storage_file', label=label)
        pkg.add_resource(url=fileurl, description="Original file",
                         format="xml")
        pkg.add_resource(url=document_info.holdings['URI']\
                         if 'URI' in document_info.holdings else '',
                         description=title)
        metas = {}
        descendants = [desc for desc in document_info.descendants] +\
                      [sdesc for sdesc in study_descr.stdyInfo.descendants]
        for docextra in descendants:
            if isinstance(docextra, Tag):
                if docextra:
                    if docextra.name == 'p':
                        docextra.name = docextra.parent.name
                    if not docextra.name in metas:
                        metas[docextra.name] = docextra.string\
                                    if docextra.string\
                                    else self._collect_attribs(docextra)
                    else:
                        metas[docextra.name] += " " + docextra.string\
                                        if docextra.string\
                                        else self._collect_attribs(docextra)
        csvs = ""
        if ddi_xml.codeBook.dataDscr:
            vars = ddi_xml.codeBook.dataDscr('var')
            heads = self._get_headers(vars)
            f = StringIO.StringIO()
            writer = csv.DictWriter(f, heads)
            for var in vars:
                writer.writerow(self._construct_csv(var, heads))
            f.flush()
            label = "%s/%s.csv" % (\
                    nowstr,
                    name)
            ofs.put_stream(BUCKET, label, f, {})
            fileurl = config.get('ckan.site_url') + h.url_for('storage_file', label=label)
            pkg.add_resource(url=fileurl, description="Variable metadata",
                             format="csv")
        pkg.extras = metas
        pkg.save()
        producers = study_descr.citation.prodStmt.find_all('producer')
        for producer in producers:
            producer = producer.string
            if producer:
                group = Group.by_name(producer)
                if not group:
                    group = Group(name=producer, description=producer,
                                  title=producer)
                group.add_package_by_name(pkg.name)
                group.save()
                setup_default_user_roles(group)
        log.debug("Saved pkg %s" % (pkg.url))
        setup_default_user_roles(pkg)
        return True
