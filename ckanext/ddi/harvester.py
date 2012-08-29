# coding: utf-8
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
import re

from ckan.model import Package, Group, User
from ckan.plugins.core import SingletonPlugin, implements
from ckan.lib.navl.dictization_functions import flatten_dict
from ckan import model
from ckan.model.authz import setup_default_user_roles

from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.harvesters.base import HarvesterBase, munge_tag
from ckanext.harvest.model import HarvestObject, HarvestJob

log = logging.getLogger(__name__)


class DDIHarvester(HarvesterBase):
    '''
    DDI Harvester for ckanext-harvester.
    '''
    config = None

    def _set_config(self, config_str):
        if config_str:
            self.config = json.loads(config_str)

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
        try:
            urls = urllib2.urlopen(gather_url)
            harvest_objs = []
            for url in urls.readlines():
                harvest_obj = HarvestObject()
                harvest_obj.content = url
                harvest_obj.job = harvest_job
                harvest_obj.save()
                harvest_objs.append(harvest_obj.id)
        except urllib2.URLError:
            self._save_gather_error('Could not gather XML files from URL!', harvest_job)
            return None
        return harvest_objs

    def fetch_stage(self, harvest_object):
        try:
            xml = urllib2.urlopen(harvest_object.content).read()
            retdict = {}
            retdict['xml'] = xmltodict.parse(
                                                etree.tostring(
                                                      etree.fromstring(xml).xpath('/codeBook')[0]
                                                      )
                                                )
            retdict['xmlstr'] = etree.tostring(etree.fromstring(xml).xpath('/codeBook')[0])
            retdict['source'] = harvest_object.content
            harvest_object.content = json.dumps(retdict)
        except urllib2.URLError:
            self._save_object_error('Could not fetch from url!', harvest_object)
            return False
        except etree.XMLSyntaxError:
            self._save_object_error('Unable to parse XML!', harvest_object)
            return False
        return True

    def _collect_attribs(self, el):
        str = ""
        for k,v in el.attrib.items():
            str += "(%s,%s)" % (k, v)
        return str

    def _combine_and_flatten(self, xml_dict):
        res = {}
        tree = etree.fromstring(xml_dict).xpath('//stdyDscr//*[not(child::*)]|//docDscr//*[not(child::*)]')
        for els in tree:
            if els.tag == 'p':
                els.tag = els.getparent().tag
            if not els.tag in res:
                res[els.tag] = els.text if els.text else self._collect_attribs(els)
            else:
                res[els.tag] += " " + els.text if els.text else self._collect_attribs(els)
        return res

    def _collect_vars(self, xml_dict):
        res = {}
        tree = etree.fromstring(xml_dict).xpath('//dataDscr//var')
        for var in tree:
            stats = var.xpath(".//sumStat[(@type='min' or @type='max' or @type='stdev' or @type='mean') and ..//sumStat[@type='stdev']]")
            question = var.xpath('./qstn/qstnLit')[0]
            for stat in stats:
                statstr = "%s:%s" % (stat.attrib['type'], stat.text)
                if not var.attrib['ID'] in res:
                    res[var.attrib['ID']] = "%s %s" % (question.text, statstr)
                else:
                    res[var.attrib['ID']] += " " + statstr
        return res

    def import_stage(self, harvest_object):
        model.repo.new_revision()
        xml_dict = json.loads(harvest_object.content)
        code_dict = xml_dict['xml']
        pkg = Package()
        data_dict = code_dict['codeBook']
        citation = data_dict["stdyDscr"]["citation"]
        study_info = data_dict["stdyDscr"]["stdyInfo"]
        title = citation['titlStmt']['titl']
        producer = citation['prodStmt']['producer']
        author = producer[0] if isinstance(producer,list) else producer
        author = author if not isinstance(author, dict) else author['#text']
        pkg.author = author
        pkg.author_email = author

        keywords = study_info['subject']['keyword'] \
            if isinstance(study_info['subject']['keyword'], list) else \
            [study_info['subject']['keyword']]
        for kw in keywords:
            pkg.add_tag_by_name(munge_tag(kw['#text']) if '#text' in kw else munge_tag(kw))
        keywords = study_info['subject']['topcClas'] \
            if isinstance(study_info['subject']['topcClas'], list) else \
            [study_info['subject']['topcClas']]
        for kw in keywords:
            pkg.add_tag_by_name(munge_tag(kw['#text']) if '#text' in kw else munge_tag(kw))

        descr = citation['serStmt']['serInfo']['p'] 
        description_arr = descr if isinstance(descr, list) else [descr] 
        pkg.notes = '<br />'.join(description_arr)
        pkg.extras = dict(self._combine_and_flatten(xml_dict['xmlstr']),**self._collect_vars(xml_dict['xmlstr']))
        pkg.title = title[:100]
        pkg.name = self._gen_new_name(self._check_name(title))
        pkg.url = xml_dict['source']
        pkg.save()

        producer = producer if isinstance(producer,list) else [producer] 
        for producer in producer:
            prod_text = producer if not isinstance(producer, dict) else producer['#text']
            group = Group.by_name(prod_text)
            if not group:
                group = Group(name=prod_text, description=prod_text)
            group.add_package_by_name(pkg.name)
            group.save()
            setup_default_user_roles(group)
        res_url = code_dict['codeBook']['docDscr']['citation']['holdings']['@URI'] \
            if '@URI' in code_dict['codeBook']['docDscr']['citation']['holdings'] \
            else ''
        pkg.add_resource(res_url, description=''.join(description_arr), name=title)
        log.debug("Saved pkg %s" % (pkg.url))
        setup_default_user_roles(pkg)
        return True