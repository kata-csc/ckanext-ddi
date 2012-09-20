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
import unicodecsv as csv
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

    def _construct_csv(self, var, heads):
        retdict = {}
        els = var(text=False)
        varcnt = 0
        retdict['ID'] = var['ID'] if 'ID' in var.attrs else var['name']
        for var in els:
            if var.name in ('catValu', 'catStat', 'qstn', 'catgry'):
                continue
            if var.name == 'qstn':
                valstr = var.preQTxt.string.strip() if var.preQTxt.string else None
                retdict['preQTxt'] = valstr
                valstr = var.qstnLit.string.strip() if var.qstnLit.string else None
                retdict['qstnLit'] = valstr
                valstr = var.postQTxt.string.strip() if var.postQTxt.string else None
                retdict['postQTxt'] = valstr
                valstr = var.ivuInstr.string.strip() if var.ivuInstr.string else None
                retdict['ivuInstr'] = valstr
            elif var.name.startswith('sumStat'):
                var.name = "sumStat_%s" % var['type']
                retdict[var.name] = var.string.strip()
            elif var.name == 'valrng':
                retdict['range'] = [("%s,%s" % (k, v) for k, v in var.range.attrs.iteritems())]
            elif var.name == 'invalrng':
                if var.item:
                    retdict['item'] = [("%s,%s" % (k, v) for k, v in var.item.attrs.iteritems())]
            else:
                if var.name == 'labl' and 'level' in var.attrs:
                    if var['level'] == 'variable' and var.string:
                        retdict['labl'] = var.string.strip()
                else:
                    retdict[var.name] = var.string.strip() if var.string else None

        return retdict

    def _create_code_rows(self, var):
        rows = []
        for cat in var('catgry', text=False, recursive=False):
            catdict = {}
            catdict['ID'] = var['ID'] if 'ID' in var else var['name']
            catdict['catValu'] = cat.catValu.string if cat.catValu else None
            catdict['labl'] = cat.labl.string if cat.labl else None
            catdict['catStat'] = cat.catStat.string if cat.catStat else None
            rows.append(catdict)
        return rows

    def _get_headers(self):
        longest_els = []
        longest_els.append('ID')
        longest_els.append('labl')
        longest_els.append('preQTxt')
        longest_els.append('qstnLit')
        longest_els.append('postQTxt')
        longest_els.append('ivuInstr')
        longest_els.append('varFormat')
        longest_els.append('TotlResp')
        longest_els.append('range')
        longest_els.append('item')
        longest_els.append('sumStat_vald')
        longest_els.append('sumStat_min')
        longest_els.append('sumStat_max')
        longest_els.append('sumStat_mean')
        longest_els.append('sumStat_stdev')
        longest_els.append('notes')
        longest_els.append('txt')
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
        if ddi_xml.codeBook.dataDscr:
            vars = ddi_xml.codeBook.dataDscr('var')
            heads = self._get_headers()
            c_heads = ['ID', 'catValu', 'labl', 'catStat']
            f_var = StringIO.StringIO()
            c_var = StringIO.StringIO()
            varwriter = csv.DictWriter(f_var, heads)
            codewriter = csv.DictWriter(c_var, c_heads)
            heading_row = {}
            for head in heads:
                heading_row[head] = head
            varwriter.writerow(heading_row)
            for var in vars:
                try:
                    varwriter.writerow(self._construct_csv(var, heads))
                    codewriter.writerows(self._create_code_rows(var))
                except ValueError, e:
                    raise IOError("Failed to import DDI to CSV! %s" % e)
            f_var.flush()
            label = "%s/%s_var.csv" % (nowstr, name)
            ofs.put_stream(BUCKET, label, f_var, {})
            fileurl = config.get('ckan.site_url') + h.url_for('storage_file', label=label)
            pkg.add_resource(url=fileurl, description="Variable metadata",
                             format="csv")
            label = "%s/%s_code.csv" % (nowstr, name)
            ofs.put_stream(BUCKET, label, c_var, {})
            fileurl = config.get('ckan.site_url') + h.url_for('storage_file', label=label)
            pkg.add_resource(url=fileurl, description="Variable code values",
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
