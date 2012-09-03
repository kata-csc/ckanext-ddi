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

from ckanext.harvest.harvesters.base import HarvesterBase, munge_tag
from ckanext.harvest.model import HarvestObject

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
        try:
            udict = json.loads(harvest_object.content)
            if 'url' in udict:
                xml = urllib2.urlopen(udict['url']).read()
                retdict = {}
                retdict['xml'] = xmltodict.parse(
                                            etree.tostring(
                                                  etree.fromstring(xml).\
                                                  xpath('/codeBook')[0]
                                                          )
                                                )
                retdict['xmlstr'] = etree.tostring(etree.fromstring(xml).\
                                                   xpath('/codeBook')[0])
                retdict['source'] = harvest_object.content
                harvest_object.content = json.dumps(retdict)
            else:
                self._save_object_error('No url in content!', harvest_object)
                return False
        except urllib2.URLError:
            self._save_object_error('Could not fetch from url!', 
                                    harvest_object)
            return False
        except etree.XMLSyntaxError:
            self._save_object_error('Unable to parse XML!', harvest_object)
            return False
        return True

    def _collect_attribs(self, el):
        '''Collect attributes to a string with (k,v) value where k is attribute
        name and v is the attribute value.
        '''
        astr = ""
        for k, v in el.attrib.items():
            astr += "(%s,%s)" % (k, v)
        return astr

    def _get_metadata_for_document(self, xml_dict):
        '''Get metadata leaf elements from stdyDscr and docDscr elements from
        DDI document. Get the text value of the leaf element, if it has one, if
        it doesn't, get the attribute key and value pairs.
        '''
        res = {}
        tree = etree.fromstring(xml_dict).\
            xpath('//stdyDscr//*[not(child::*)]|//docDscr//*[not(child::*)]')
        for els in tree:
            if els.tag == 'p':
                els.tag = els.getparent().tag
            if not els.tag in res:
                res[els.tag] = els.text if els.text else\
                                        self._collect_attribs(els)
            else:
                res[els.tag] += " " + els.text if els.text else\
                                                self._collect_attribs(els)
        return res

    def _collect_vars(self, xml_dict):
        '''Collect all variables from the DDI documents, which have a meaningful
        standard deviation and mean values. These have the most significance to
        the metadata.
        '''
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
        '''Import the metadata received in the fetch stage to a dataset and
        create groups if ones are defined. Fill in metadata from study and
        document description.
        '''
        model.repo.new_revision()
        xml_dict = json.loads(harvest_object.content)
        code_dict = xml_dict['xml']
        pkg = Package()
        data_dict = code_dict['codeBook']
        citation = data_dict["stdyDscr"]["citation"]
        study_info = data_dict["stdyDscr"]["stdyInfo"]
        title = citation['titlStmt']['titl']
        producer = citation['prodStmt']['producer']
        author = producer[0] if isinstance(producer, list) else producer
        author = author if not isinstance(author, dict) else author['#text']
        pkg.author = author
        pkg.author_email = author

        keywords = study_info['subject']['keyword'] \
            if isinstance(study_info['subject']['keyword'], list) else \
            [study_info['subject']['keyword']]
        for kw in keywords:
            pkg.add_tag_by_name(munge_tag(kw['#text']) if '#text' in kw \
                                                        else munge_tag(kw))
        keywords = study_info['subject']['topcClas'] \
            if isinstance(study_info['subject']['topcClas'], list) else \
            [study_info['subject']['topcClas']]
        for kw in keywords:
            pkg.add_tag_by_name(munge_tag(kw['#text']) if '#text' in kw \
                                            else munge_tag(kw))

        descr = citation['serStmt']['serInfo']['p']
        description_arr = descr if isinstance(descr, list) else [descr]
        pkg.notes = '<br />'.join(description_arr)
        pkg.extras = dict(self._get_metadata_for_document(xml_dict['xmlstr']), \
                            **self._collect_vars(xml_dict['xmlstr']))
        pkg.title = title[:100]
        pkg.name = self._gen_new_name(self._check_name(title[:100]))
        pkg.url = json.loads(xml_dict['source'])['url']
        pkg.save()

        producer = producer if isinstance(producer, list) else [producer]
        for producer in producer:
            prod_text = producer if not isinstance(producer, dict) else \
                                            producer['#text']
            group = Group.by_name(prod_text)
            if not group:
                group = Group(name=prod_text, description=prod_text)
            group.add_package_by_name(pkg.name)
            group.save()
            setup_default_user_roles(group)
        res_url = \
            code_dict['codeBook']['docDscr']['citation']['holdings']['@URI'] \
            if '@URI' in\
            code_dict['codeBook']['docDscr']['citation']['holdings'] \
            else ''
        pkg.add_resource(res_url, description=''.join(description_arr),
                         name=title)
        log.debug("Saved pkg %s" % (pkg.url))
        setup_default_user_roles(pkg)
        return True
