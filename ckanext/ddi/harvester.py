# coding: utf-8
'''
Harvester for DDI2 formats
'''

#pylint: disable-msg=E1101,E0611,F0401
import datetime
import httplib
import json
import logging
import lxml.etree as etree
import pickle
import pprint
import re
import socket
import StringIO
import urllib2

from bs4 import BeautifulSoup, Tag
from dateutil import parser
from pylons import config
import unicodecsv as csv

from ckan.controllers.storage import BUCKET, get_ofs
from ckan.lib.base import h
from ckan.lib.munge import munge_tag
from ckan.lib.navl.validators import ignore_missing
from ckan.logic import ValidationError
from ckan.logic.converters import convert_to_extras
import ckan.model as model
from ckan.model.authz import setup_default_user_roles
from ckanext.harvest.harvesters.base import HarvesterBase
#from ckanext.harvest.harvesters.retry import HarvesterRetry
from ckanext.harvest.model import HarvestObject, HarvestJob, HarvestObjectError
from ckanext.kata.plugin import KataPlugin
from dataconverter import DataConverter

import traceback

log = logging.getLogger(__name__)

socket.setdefaulttimeout(30)


class DDIHarvester(HarvesterBase):
    '''
    DDI Harvester for ckanext-harvester.
    '''
    config = None

    def __init__(self):
        self.ddi_converter = DataConverter()

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

    def get_original_url(self, harvest_object_id):
        '''Return the URL to the original remote document, given a Harvest
         Object id.
         '''
        obj = model.Session.query(HarvestObject). \
            filter(HarvestObject.id == harvest_object_id).first()
        if obj:
            return obj.source.url
        return None

    def _datetime_from_str(self, key, s):
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

    #    def _add_retry(self, harvest_object):
    #        HarvesterRetry.mark_for_retry(harvest_object)

    #    def _scan_retries(self, harvest_job):
    #        self._retry = HarvesterRetry()
    #        urls = []
    #        for harvest_object in self._retry.find_all_retries(harvest_job):
    #            data = json.loads(harvest_object.content)
    #            urls.append(data['url'])
    #        return urls

    #    def _clear_retries(self):
    #        self._retry.clear_retry_marks()


    def gather_stage(self, harvest_job):
        '''Gather the URLs to fetch from a URL which has a list of links to XML
        documents containing the DDI documents.
        '''
        self._set_config(harvest_job.source.config)

        def date_from_config(key):
            return self._datetime_from_str(key, config.get(key, None))

        def add_harvest_object(harvest_job, url):
            harvest_obj = HarvestObject(job=harvest_job)
            harvest_obj.content = url
            harvest_obj.save()
            return harvest_obj

        from_ = date_from_config('ckanext.harvest.test.from')
        until = date_from_config('ckanext.harvest.test.until')
        previous_job = model.Session.query(HarvestJob) \
            .filter(HarvestJob.source == harvest_job.source) \
            .filter(HarvestJob.gather_finished != None) \
            .filter(HarvestJob.id != harvest_job.id) \
            .order_by(HarvestJob.gather_finished.desc()) \
            .limit(1).first()
        if previous_job and not until and not from_:
            from_ = previous_job.gather_finished
            until = None

        harvest_objs = []
        # Add retries.
        #        for url in self._scan_retries(harvest_job):
        #            obj = add_harvest_object(harvest_job, url)
        #            harvest_objs.append(obj.id)
        #            log.debug('Retrying record: %s' % url)
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
                    except (urllib2.URLError, urllib2.HTTPError,):
                    # Actually we do not know if it fits the time limits.
                    # Rather get it twice than lose it.
                    # self._add_retry(add_harvest_object(harvest_job, url))
                        continue
                    if from_ and lastmod < from_:
                        continue
                    if until and until < lastmod:
                        continue
                obj = add_harvest_object(harvest_job, url)
                harvest_objs.append(obj.id)
        except urllib2.HTTPError, err:
            self._save_gather_error(
                'HTTPError: Could not gather XML files from URL! ' +
                'Error: {er}'.format(er=err.code), harvest_job)
            return None
        except urllib2.URLError, err:
            self._save_gather_error(
                'URLError: Could not gather XML files from URL! ' +
                'Error: {er}, urls: {ur}'.format(er=err.reason, ur=harvest_job.source.url),
                harvest_job)
            return None
        except Exception as e:
            log.debug(traceback.format_exc(e))
            return None
        #        self._clear_retries()
        log.info('Gathered %i records from %s.' % (
            len(harvest_objs), harvest_job.source.url,))
        return harvest_objs

    def fetch_stage(self, harvest_object):
        '''Fetch and parse the DDI XML document.
        '''
        url = harvest_object.content
        try:
            f = urllib2.urlopen(url).read()
        except (urllib2.URLError, urllib2.HTTPError,):
        #            self._add_retry(harvest_object)
            self._save_object_error('Could not fetch from url %s!' % url,
                                    harvest_object)
            return False
        except httplib.BadStatusLine:
        #            self._add_retry(harvest_object)
            self._save_object_error('Bad HTTP response status line.',
                                    harvest_object, stage='Fetch')
            return False
        # Need to pickle the XML so that the data type remains the same.
        harvest_object.content = pickle.dumps({'url': url, 'xml': f})
        return True

    def import_stage(self, harvest_object):
        '''Import the metadata received in the fetch stage to a dataset.

        DDI document is parsed to a BeautifulSoup object for metadata
        extraction. Study (stdyDscr) and document (docDscr) descriptions are
        used. File (fileDscr) and data (dataDscr) description parts of a ddi
        file are saved as csv files (unfinished).
        Also create groups if ones are defined (unfinished).
        '''
        # TODO: cPickle might be faster
        info = pickle.loads(harvest_object.content)
        log.info("Harvest object url: {ur}".format(ur=info['url'].strip()))
        try:
            ddi_xml = BeautifulSoup(info['xml'], 'xml')
        except etree.XMLSyntaxError, err:
            self._save_object_error('Unable to parse XML! {er}'
                                    .format(er=err.msg), harvest_object,
                                    'Import')
            # I presume source sent wrong data but it arrived correctly.
            # This could result in a case where incorrect source is tried
            # over and over again without success.
            del info['xml']
            harvest_object.content = info['url']
            #            self._add_retry(harvest_object)
            return False

        package_dict = self.ddi_converter.ddi2ckan(ddi_xml, info['url'],
                                                   info['xml'], harvest_object)
        errors = self.ddi_converter.get_errors()
        if errors:
            for err in errors:
                self._save_object_error('Missing minimum metadata in {ur}.\n'
                                        'AttributeError: {er}'
                                        .format(ur=info['url'], er=err),
                                        harvest_object,
                                        'Import')
        if not package_dict:
            return False
        schema = KataPlugin.create_package_schema_ddi()
        result = self._create_or_update_package(package_dict, harvest_object,
                                                schema)
        log.debug("Exiting import_stage()")
        return result  # returns True

    def import_xml(self, source, xml):
        try:
            ddi_xml = BeautifulSoup(xml, 'xml')
        except etree.XMLSyntaxError:
            log.debug('Unable to parse XML!')
            return False
        return self.ddi_converter.ddi2ckan(ddi_xml, None, xml)

#
#class DDI3Harvester(HarvesterBase):
#    '''
#    DDI Harvester for ckanext-harvester.
#    '''
#    config = None
#
#    def _set_config(self, config_str):
#        '''Set the configuration string.
#        '''
#        if config_str:
#            self.config = json.loads(config_str)
#
#    def info(self):
#        '''Return information about this harvester.
#        '''
#        return {
#            'name': 'DDI3',
#            'title': 'DDI3 import (EXPERIMENTAL)',
#            'description': 'Mass importing harvester for DDI3',
#        }
#
#    def validate_config(self, config):
#        '''Validate the config, returns it since we don't have any configuration
#        parameters
#        '''
#        return config
#
#    # These have been copy-pasted all around now so a common base class would
#    # have been a good idea. _scan_retries seems to be the only one that
#    # changes.
#
#    def _datetime_from_str(self, s):
#        # Used to get date from settings file when testing harvesting with
#        # (semi-open) date interval.
#        if s == None:
#            return s
#        try:
#            t = datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%S')
#            return t
#        except ValueError:
#            pass
#        try:
#            t = datetime.datetime.strptime(s, '%Y-%m-%d')
#            return t
#        except ValueError:
#            log.debug('Bad date for %s: %s' % (key, s,))
#        return None
#
#    def _str_from_datetime(self, dt):
#        return dt.strftime('%Y-%m-%dT%H:%M:%S')
#
#    def _add_retry(self, harvest_object):
#        HarvesterRetry.mark_for_retry(harvest_object)
#
#    def _scan_retries(self, harvest_job):
#        self._retry = HarvesterRetry()
#        urls = []
#        for harvest_object in self._retry.find_all_retries(harvest_job):
#            data = json.loads(harvest_object.content)
#            urls.append(data['url'])
#        return urls
#
#    def _clear_retries(self):
#        self._retry.clear_retry_marks()
#
#
#    def gather_stage(self, harvest_job):
#        '''Gather the URLs to fetch from a URL which has a list of links to XML
#        documents containing the DDI documents.
#        '''
#        self._set_config(harvest_job.source.config)
#
#        def date_from_config(key):
#            return self._datetime_from_str(config.get(key, None))
#
#        from_ = date_from_config('ckanext.harvest.test.from')
#        until = date_from_config('ckanext.harvest.test.until')
#        previous_job = model.Session.query(HarvestJob) \
#            .filter(HarvestJob.source == harvest_job.source) \
#            .filter(HarvestJob.gather_finished != None) \
#            .filter(HarvestJob.id != harvest_job.id) \
#            .order_by(HarvestJob.gather_finished.desc()) \
#            .limit(1).first()
#        if previous_job and not until and not from_:
#            from_ = previous_job.gather_finished
#            until = None
#
#        def add_harvest_object(harvest_job, url):
#            harvest_obj = HarvestObject(job=harvest_job)
#            harvest_obj.content = url
#            harvest_obj.save()
#            return harvest_obj
#
#        harvest_objs = []
#        # Add retries.
#        for url in self._scan_retries(harvest_job):
#            obj = add_harvest_object(harvest_job, url)
#            harvest_objs.append(obj.id)
#            log.debug('Retrying record: %s' % url)
#        try:
#            urls = urllib2.urlopen(harvest_job.source.url)
#            for url in urls.readlines():
#                if from_ or until:
#                    # This should not fail the whole gather.
#                    try:
#                        request = urllib2.Request(url)
#                        request.get_method = lambda: 'HEAD'
#                        doc_url = urllib2.urlopen(request)
#                        lastmod = parser.parse(doc_url.headers['last-modified'],
#                                               ignoretz=True)
#                    except (urllib2.URLError, urllib2.HTTPError,):
#                        # Actually we do not know if it fits the time limits.
#                        # Rather get it twice than lose it.
#                        self._add_retry(add_harvest_object(harvest_job, url))
#                        continue
#                    if from_ and lastmod < from_:
#                        continue
#                    if until and until < lastmod:
#                        continue
#                obj = add_harvest_object(harvest_job, url)
#                harvest_objs.append(obj.id)
#        except (urllib2.URLError, urllib2.HTTPError,):
#            self._save_gather_error(
#                'DDI3: Could not gather XML files from URL!',
#                harvest_job)
#            return None
#        except Exception as e:
#            log.debug(traceback.format_exc(e))
#            return None
#        self._clear_retries()
#        log.info('Gathered %i records from %s.' % (
#            len(harvest_objs), harvest_job.source.url,))
#        return harvest_objs
#
#    def fetch_stage(self, harvest_object):
#        '''Fetch and parse the DDI XML document.
#        '''
#        url = harvest_object.content
#        try:
#            f = urllib2.urlopen(url).read()
#        except (urllib2.URLErrori, urllib2.HTTPError,):
#            self._add_retry(harvest_object)
#            self._save_object_error('Could not fetch from url %s!' % url,
#                                    harvest_object)
#            return False
#        except httplib.BadStatusLine:
#            self._add_retry(harvest_object)
#            self._save_object_error('Bad HTTP response status line.',
#                                    harvest_object, stage='Fetch')
#            return False
#        except Exception as e:
#            # Guard against miscellaneous stuff. Probably plain bugs.
#            # Also very rare exceptions we haven't seen yet.
#            self._add_retry(harvest_object)
#            log.debug(traceback.format_exc(e))
#            return False
#            # Need to pickle the XML so that the data type remains the same.
#        harvest_object.content = pickle.dumps({'url': url, 'xml': f})
#        return True
#
#    def import_stage(self, harvest_object):
#        '''Import the metadata received in the fetch stage to a dataset and
#        create groups if ones are defined. Fill in metadata from study and
#        document description.
#        '''
#        info = pickle.loads(harvest_object.content)
#        try:
#            ddi_xml = BeautifulSoup(info['xml'], 'xml')
#        except etree.XMLSyntaxError:
#            self._save_object_error('Unable to parse XML!', harvest_object)
#            # I presume source sent wrong data but it arrived correctly.
#            # This could result in a case where incorrect source is tried
#            # over and over again without success.
#            del info['xml']
#            harvest_object.content = info['url']
#            self._add_retry(harvest_object)
#            return False
#        return ddi32ckan(ddi_xml, info['xml'], info['url'], harvest_object)
#
#    def import_xml(self, source, xml):
#        try:
#            ddi_xml = BeautifulSoup(xml, 'xml')
#        except etree.XMLSyntaxError:
#            log.debug('Unable to parse XML!')
#            return False
#        return ddi32ckan(ddi_xml, xml)
#

#if __name__ == '__main__':
#    import sys
#
#    if len(sys.argv) > 3:
#        header, metadata, about = test_fetch(sys.argv[1],
#                                             sys.argv[2], sys.argv[3])
#        #for item in metadata.getMap().items():
#        #    print item
#        print header
#        print metadata.dc.subject
#    else:
#        for item in test_list(sys.argv[1]):
#            print item
#
