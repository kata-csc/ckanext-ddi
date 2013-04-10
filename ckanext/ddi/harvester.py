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
import pprint
from dateutil import parser

from pylons import config

from lxml import etree

from ckan.model import Package, Group, Vocabulary, Session
from ckan.lib.base import h
from ckan.controllers.storage import BUCKET, get_ofs

from ckan import model
from ckan.model.authz import setup_default_user_roles
from ckan.lib.munge import munge_tag
from ckanext.harvest.harvesters.base import HarvesterBase
from ckanext.harvest.model import HarvestObject, HarvestJob
from ckanext.harvest.harvesters.retry import HarvesterRetry
from dataconverter import ddi2ckan

from bs4 import BeautifulSoup, Tag

log = logging.getLogger(__name__)


import socket
socket.setdefaulttimeout(30)

import traceback


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

    def _datetime_from_str(self, s):
        # Used to get date from settings file when testing harvesting with
        # (semi-open) date interval.
        if s == None:
            return s
        try:
            t = datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%S')
            return t
        except ValueError:
            pass
        try:
            t = datetime.datetime.strptime(s, '%Y-%m-%d')
            return t
        except ValueError:
            log.debug('Bad date for %s: %s' % (key, s,))
        return None

    def _str_from_datetime(self, dt):
        return dt.strftime('%Y-%m-%dT%H:%M:%S')

    def _add_retry(self, harvest_object):
        HarvesterRetry.mark_for_retry(harvest_object)

    def _scan_retries(self, harvest_job):
        self._retry = HarvesterRetry()
        urls = []
        for harvest_object in self._retry.find_all_retries(harvest_job):
            data = json.loads(harvest_object.content)
            urls.append(data['url'])
        return urls

    def _clear_retries(self):
        self._retry.clear_retry_marks()


    def gather_stage(self, harvest_job):
        '''Gather the URLs to fetch from a URL which has a list of links to XML
        documents containing the DDI documents.
        '''
        self._set_config(harvest_job.source.config)
        def date_from_config(key):
            return self._datetime_from_str(config.get(key, None))
        from_ = date_from_config('ckanext.harvest.test.from')
        until = date_from_config('ckanext.harvest.test.until')
        previous_job = Session.query(HarvestJob) \
            .filter(HarvestJob.source==harvest_job.source) \
            .filter(HarvestJob.gather_finished!=None) \
            .filter(HarvestJob.id!=harvest_job.id) \
            .order_by(HarvestJob.gather_finished.desc()) \
            .limit(1).first()
        if previous_job and not until and not from_:
            from_ = previous_job.gather_finished
            until = None
        def add_harvest_object(harvest_job, url):
            harvest_obj = HarvestObject(job=harvest_job)
            harvest_obj.content = url
            harvest_obj.save()
            return harvest_obj
        harvest_objs = []
        # Add retries.
        for url in self._scan_retries(harvest_job):
            obj = add_harvest_object(harvest_job, url)
            harvest_objs.append(obj.id)
            log.debug('Retrying record: %s' % url)
        try:
            urls = urllib2.urlopen(harvest_job.source.url)
            for url in urls.readlines():
                if from_ or until:
                    # This should not fail the whole gather.
                    try:
                        request = urllib2.Request(url)
                        request.get_method = lambda: 'HEAD'
                        doc_url = urllib2.urlopen(request)
                        lastmod = parser.parse(doc_url.headers['last-modified'],
                            ignoretz=True)
                    except urllib2.URLError:
                        # Actually we do not know if it fits the time limits.
                        # Rather get it twice than lose it.
                        self._add_retry(add_harvest_object(harvest_job, url))
                        continue
                    if from_ and lastmod < from_:
                        continue
                    if until and until < lastmod:
                        continue
                obj = add_harvest_object(harvest_job, url)
                harvest_objs.append(obj.id)
        except urllib2.URLError:
            self._save_gather_error('Could not gather XML files from URL!', 
                                    harvest_job)
            return None
        except exception as e:
            log.debug(traceback.format_exc(e))
            return None
        self._clear_retries()
        log.info('Gathered %i records from %s.' % (
            len(harvest_objs), harvest_job.source.url,))
        return harvest_objs

    def fetch_stage(self, harvest_object):
        '''Fetch and parse the DDI XML document.
        '''
        url = harvest_object.content
        try:
            f = urllib2.urlopen(url).read()
        except urllib2.URLError:
            self._save_object_error('Could not fetch from url %s!' % url, 
                                    harvest_object)
            self._add_retry(harvest_object)
            return False
        except etree.XMLSyntaxError:
            self._save_object_error('Unable to parse XML!', harvest_object)
            # I presume source sent wrong data but it arrived correctly.
            # This could result in a case where incorrect source is tried
            # over and over again without success.
            self._add_retry(harvest_object)
            return False
        harvest_object.content = json.dumps({ 'url':url, 'xml':f })
        return True

    def import_stage(self, harvest_object):
        '''Import the metadata received in the fetch stage to a dataset and
        create groups if ones are defined. Fill in metadata from study and
        document description.
        '''
        info = json.loads(harvest_object.content)
        ddi_xml = BeautifulSoup(info['xml'], 'xml')
        return ddi2ckan(ddi_xml, info['url'], harvest_object)


class DDI3Harvester(HarvesterBase):
    '''
    DDI Harvester for ckanext-harvester.
    '''
    config = None
    incremental = False

    def _set_config(self, config_str):
        '''Set the configuration string.
        '''
        if config_str:
            self.config = json.loads(config_str)

    def info(self):
        '''Return information about this harvester.
        '''
        return {
                'name': 'DDI3',
                'title': 'DDI3 import (EXPERIMENTAL)',
                'description': 'Mass importing harvester for DDI3',
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
        previous_job = Session.query(HarvestJob) \
            .filter(HarvestJob.source==harvest_job.source) \
            .filter(HarvestJob.gather_finished!=None) \
            .filter(HarvestJob.id!=harvest_job.id) \
            .order_by(HarvestJob.gather_finished.desc()) \
            .limit(1).first()
        if previous_job:
            self.incremental = True
        gather_url = harvest_job.source.url
        try:
            urls = urllib2.urlopen(gather_url)
            harvest_objs = []
            for url in urls.readlines():
                gather = True
                if self.incremental:
                    request = urllib2.Request(url)
                    request.get_method = lambda: 'HEAD'
                    doc_url = urllib2.urlopen(request)
                    lastmod = parser.parse(doc_url.headers['last-modified'], ignoretz=True)
                    if previous_job.gather_finished < lastmod:
                        log.debug("Gather false")
                        gather = False
                if gather and not self.incremental:
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
        return True

    def import_stage(self, harvest_object):
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
        model.repo.new_revision()
        ddiroot = ddi_xml.DDIInstance
        main_cit = ddiroot.Citation
        study_info = ddiroot('StudyUnit')[-1]
        idx = 0
        authorgs = []
        pkg = Package.get(study_info.attrs['id'])
        if not pkg:
            pkg = Package(name=study_info.attrs['id'])
        pkg.id = ddiroot.attrs['id']
        pkg.version = main_cit.PublicationDate.SimpleDate.string
        for title in main_cit('Title'):
            pkg.extras['title_%d' % idx] = title.string
            pkg.extras['lang_title_%d' % idx] = title.attrs['xml:lang']
            idx += 1
        for title in study_info.Citation('Title'):
            pkg.extras['title_%d' % idx] = title.string
            pkg.extras['lang_title_%d' % idx] = title.attrs['xml:lang']
            idx += 1
        for value in study_info.Citation('Creator'):
            org = ""
            if value.attrs.get('affiliation', None):
                org = value.attrs['affiliation']
            author = value.string
            authorgs.append((author, org))
        pkg.author = authorgs[0][0]
        pkg.maintainer = study_info.Citation.Publisher.string
        lastidx = 0
        for auth, org in authorgs:
            pkg.extras['author_%s' % lastidx] = auth
            pkg.extras['organization_%s' % lastidx] = org
            lastidx = lastidx + 1
        pkg.extras["licenseURL"] = study_info.Citation.Copyright.string
        pkg.notes = "".join([unicode(repr(chi).replace('\n', '<br />'), 'utf8')\
                             for chi in study_info.Abstract.Content.children])
        for kw in study_info.Coverage.TopicalCoverage('Keyword'):
            pkg.add_tag_by_name(kw.string)
        pkg.extras['contributor'] = study_info.Citation.Contributor.string
        pkg.extras['publisher'] = study_info.Citation.Publisher.string
        pkg.save()
        harvest_object.package_id = pkg.id
        harvest_object.current = True
        harvest_object.save()
        return True
