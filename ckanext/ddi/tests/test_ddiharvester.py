# coding: utf-8
'''
Tests for DDI harvester
'''
# pylint: disable=E1101,C1101,C0111
# import logging
from unittest import TestCase
# import urllib2
# from StringIO import StringIO
# import json
# import uuid
# import pprint
# from datetime import datetime, timedelta
#
# from nose.exc import SkipTest
#
# import testdata
#
# from lxml import etree

import ckan.model

# from ckan.model import Session, Package, User
# from ckan.lib.helpers import url_for
# from ckan.tests.functional.base import FunctionalTestCase
# from ckan.tests import CreateTestData
# from ckan.logic.auth.get import package_show, group_show
# from ckan import model


# from ckanext.ddi.harvester import DDIHarvester
# from ckanext.harvest.model import HarvestJob, HarvestSource, HarvestObject, \
#                                   HarvestObjectError, HarvestGatherError, setup
# from sqlalchemy.ext.associationproxy import _AssociationDict
#
from ckanext.ddi.harvester import DDIHarvester
import ckanext.harvest.model as harvest_model
from ckanext.kata import model as kata_model
import ckanext.ddi.dataconverter as dconverter

# log = logging.getLogger(__file__)
# realopen = urllib2.urlopen


class TestDataConverter(TestCase):

    @classmethod
    def setup_class(self):
        '''
        Setup database and variables
        '''
        harvest_model.setup()
        kata_model.setup()

        self.ddi_converter = dconverter.DataConverter()

        # username = u'testlogin2'
        # password = u'letmein'
        # CreateTestData.create_user(name=username,
        #                            password=password)
        # # do the login
        # offset = url_for(controller='user', action='login')
        # res = self.app.get(offset)
        # fv = res.forms['login']
        # fv['login'] = str(username)
        # fv['password'] = str(password)
        # fv['remember'] = True
        # res = fv.submit()
        # setup()

    def test_dummy(self):
        assert 1

    @classmethod
    def teardown_class(self):
        #Session.remove()
        ckan.model.repo.rebuild_db()


class TestDDIHarvester(TestCase):

    @classmethod
    def setup_class(self):
        '''
        Setup database and variables
        '''
        harvest_model.setup()
        kata_model.setup()

        self.ddi_harvester = DDIHarvester()

        # username = u'testlogin2'
        # password = u'letmein'
        # CreateTestData.create_user(name=username,
        #                            password=password)
        # # do the login
        # offset = url_for(controller='user', action='login')
        # res = self.app.get(offset)
        # fv = res.forms['login']
        # fv['login'] = str(username)
        # fv['password'] = str(password)
        # fv['remember'] = True
        # res = fv.submit()
        # setup()

    def test_dummy(self):
        assert 1

    @classmethod
    def teardown_class(self):
        #Session.remove()
        ckan.model.repo.rebuild_db()


    # def _create_harvester(self, config=True):
    #     harv = DDIHarvester()
    #     harv.config = "{}"
    #     harvest_job = HarvestJob()
    #     harvest_job.source = HarvestSource()
    #     harvest_job.source.title = "Test"
    #     harvest_job.source.url = "http://foo"
    #     if config:
    #         harvest_job.source.config = ''
    #     else:
    #         harvest_job.source.config = None
    #     harvest_job.source.type = "DDI"
    #     Session.add(harvest_job)
    #     return harv, harvest_job
    #
    # def test_harvester_info(self):
    #     harv, _ = self._create_harvester()
    #     self.assert_(isinstance(harv.info(), dict))
    #     self.assert_(harv.validate_config(harv.config))
    #     harv, _ = self._create_harvester(config=False)
    #
    # def test_harvester_create(self):
    #     harv, job = self._create_harvester()
    #     self.assert_(harv)
    #     self.assert_(job)
    #     self.assert_(job.source)
    #     self.assert_(job.source.title == "Test")
    #
    # def test_harvester_urlerror(self):
    #     harv, job = self._create_harvester()
    #     urllib2.urlopen = realopen
    #     self.assert_(harv.gather_stage(job) == None)
    #     errs = Session.query(HarvestGatherError).all()
    #     self.assert_(len(errs) == 1)
    #     harv_obj = HarvestObject()
    #     harv_obj.job = job
    #     harv_obj.content = json.dumps({'url': "http://foo"})
    #     # XML error and URL error, also the lack of url in content
    #     self.assert_(harv.import_stage(harv_obj) == False)
    #     errs = Session.query(HarvestObjectError).all()
    #     print errs
    #     self.assert_(len(errs) == 1)
    #
    # def test_harvester_gather(self):
    #     harv, job = self._create_harvester()
    #     res = """
    #     http://www.fsd.uta.fi/fi/aineistot/luettelo/FSD0115/FSD0115.xml
    #     """
    #     urllib2.urlopen = mock.Mock(return_value=StringIO(res))
    #     gathered = harv.gather_stage(job)
    #     self.assert_(len(gathered) != 0)
    #     uid = uuid.UUID(gathered[0])
    #     self.assert_(str(uid))
    #     harv, job = self._create_harvester(config=False)
    #     harv.gather_stage(job)
    #
    # def test_harvester_import(self):
    #     harv, job = self._create_harvester()
    #     res = "http://www.fsd.uta.fi/fi/aineistot/luettelo/FSD0115/FSD0115.xml"
    #     urllib2.urlopen = mock.Mock(return_value=StringIO(res))
    #     gathered = harv.gather_stage(job)
    #     urllib2.urlopen = mock.Mock(return_value=StringIO(testdata.nr1))
    #     harvest_obj = HarvestObject.get(gathered[0])
    #     self.assert_(harv.fetch_stage(harvest_obj))
    #     self.assert_(isinstance(json.loads(harvest_obj.content), dict))
    #     self.assert_(harv.import_stage(harvest_obj))
    #     self.assert_(len(Session.query(Package).all()) == 1)
    #
    #     # Lets see if the package is ok, according to test data
    #     pkg = Session.query(Package).filter(Package.title == "Puolueiden ajankohtaistutkimus 1981").one()
    #     self.assert_(pkg.title == "Puolueiden ajankohtaistutkimus 1981")
    #     log.debug(pkg.extras)
    #     self.assert_(len(pkg.get_groups()) == 2)
    #     self.assert_(len(pkg.resources) == 4)
    #     self.assert_(len(pkg.get_tags()) == 9)
    #     self.assert_(pkg.url == "http://www.fsd.uta.fi/fi/aineistot/luettelo/FSD0115/FSD0115.xml")
    #     self.assert_(isinstance(pkg.extras, _AssociationDict))
    #     self.assert_(len(pkg.extras.items()) > 1)
    #
    #     urllib2.urlopen = mock.Mock(return_value=StringIO(testdata.nr2))
    #     harvest_obj = HarvestObject.get(gathered[0])
    #     harvest_obj.content = json.dumps({'url': 'http://foo'})
    #     self.assert_(harv.fetch_stage(harvest_obj))
    #     self.assert_(isinstance(json.loads(harvest_obj.content), dict))
    #     self.assert_(harv.import_stage(harvest_obj))
    #     self.assert_(len(Session.query(Package).all()) == 2)
    #
    #     # Test user access
    #     user = User.get('testlogin2')
    #     grp = pkg.get_groups()[0]
    #     context = {'user': user.name, 'model': model}
    #     data_dict = {'id': pkg.id}
    #     auth_dict = package_show(context, data_dict)
    #     self.assert_(auth_dict['success'])
    #     data_dict = {'id': grp.id}
    #     context = {'user': '', 'model': model}
    #     auth_dict = group_show(context, data_dict)
    #     self.assert_(auth_dict['success'])
    #
    # def test_zfaulty_xml_1088(self):
    #     harv, job = self._create_harvester()
    #     res = "http://www.fsd.uta.fi/fi/aineistot/luettelo/FSD0115/FSD0115.xml"
    #     urllib2.urlopen = mock.Mock(return_value=StringIO(res))
    #     gathered = harv.gather_stage(job)
    #     urllib2.urlopen = mock.Mock(return_value=open("FSD1088.xml"))
    #     harvest_obj = HarvestObject.get(gathered[0])
    #     self.assert_(harv.fetch_stage(harvest_obj))
    #     self.assert_(harv.import_stage(harvest_obj))
    #
    # def test_zfaulty_xml_1050(self):
    #     harv, job = self._create_harvester()
    #     res = "http://www.fsd.uta.fi/fi/aineistot/luettelo/FSD0115/FSD0115.xml"
    #     urllib2.urlopen = mock.Mock(return_value=StringIO(res))
    #     gathered = harv.gather_stage(job)
    #     urllib2.urlopen = mock.Mock(return_value=open("FSD1050.xml"))
    #     harvest_obj = HarvestObject.get(gathered[0])
    #     self.assert_(harv.fetch_stage(harvest_obj))
    #     self.assert_(harv.import_stage(harvest_obj))
    #
    # def test_zfaulty_xml_1216(self):
    #     harv, job = self._create_harvester()
    #     res = "http://www.fsd.uta.fi/fi/aineistot/luettelo/FSD0115/FSD0115.xml"
    #     urllib2.urlopen = mock.Mock(return_value=StringIO(res))
    #     gathered = harv.gather_stage(job)
    #     urllib2.urlopen = mock.Mock(return_value=open("FSD1174.xml"))
    #     harvest_obj = HarvestObject.get(gathered[0])
    #     self.assert_(harv.fetch_stage(harvest_obj))
    #     self.assert_(harv.import_stage(harvest_obj))
    #
    # def test_zfaulty_xml_unknown_errors(self):
    #     harv, job = self._create_harvester()
    #     res = "http://www.fsd.uta.fi/fi/aineistot/luettelo/FSD0115/FSD0115.xml"
    #     urllib2.urlopen = mock.Mock(return_value=StringIO(res))
    #     gathered = harv.gather_stage(job)
    #
    #     urllib2.urlopen = mock.Mock(return_value=open("FSD2355.xml"))
    #     harvest_obj = HarvestObject.get(gathered[0])
    #     self.assert_(harv.fetch_stage(harvest_obj))
    #     self.assert_(harv.import_stage(harvest_obj))
    #     print Package.text_search(\
    #                         Session.query(Package),
    #                         'Kansalaiskeskustelu ydinvoimasta 2006').all()
    #     self.assert_(len(Package.text_search(\
    #                         Session.query(Package),
    #                         'Kansalaiskeskustelu ydinvoimasta 2006').all()) >= 1)
    #
    #     res = "http://www.fsd.uta.fi/fi/aineistot/luettelo/FSD0115/FSD0115.xml"
    #     urllib2.urlopen = mock.Mock(return_value=StringIO(res))
    #     gathered = harv.gather_stage(job)
    #     urllib2.urlopen = mock.Mock(return_value=open("FSD2362.xml"))
    #     harvest_obj = HarvestObject.get(gathered[0])
    #     self.assert_(harv.fetch_stage(harvest_obj))
    #     self.assert_(harv.import_stage(harvest_obj))
    #     self.assert_(len(Package.text_search(\
    #                             Session.query(Package),
    #                             'Energia-asennetutkimus 2004').all()) >= 1)
    #
    # def test_zzcomplete(self):
    #     raise SkipTest('Takes ages, do not run')
    #     urllib2.urlopen = realopen
    #     harv = DDIHarvester()
    #     harv.config = "{}"
    #     harvest_job = HarvestJob()
    #     harvest_job.source = HarvestSource()
    #     harvest_job.source.title = "Test"
    #     harvest_job.source.url = "http://www.fsd.uta.fi/fi/aineistot/luettelo/fsd-ddi-records-uris-fi.txt"
    #     harvest_job.source.config = ''
    #     harvest_job.source.type = "DDI"
    #     Session.add(harvest_job)
    #     gathered = harv.gather_stage(harvest_job)
    #     diffs = []
    #     for gath in gathered:
    #         harvest_object = HarvestObject.get(gath)
    #         print json.loads(harvest_object.content)['url']
    #         before = datetime.now()
    #         harv.fetch_stage(harvest_object)
    #         harv.import_stage(harvest_object)
    #         diff = datetime.now() - before
    #         print diff
    #         diffs.append(diff)
    #     print sum(diffs, timedelta)
    #

#class TestDDI3Harvester(unittest.TestCase, FunctionalTestCase):
#    @classmethod
#    def setup_class(self):
#        setup()
#
#    @classmethod
#    def teardown_class(self):
#        Session.remove()
#
#    def _create_harvester(self):
#        harv = DDI3Harvester()
#        harv.config = '{"ddi3":"yes"}'
#        harvest_job = HarvestJob()
#        harvest_job.source = HarvestSource()
#        harvest_job.source.title = "Test"
#        harvest_job.source.url = "http://foo"
#        harvest_job.source.type = "DDI"
#        harvest_job.source.config = '{"ddi3":"yes"}'
#        Session.add(harvest_job)
#        return harv, harvest_job
#
#    def test_ddi3_harvesting(self):
#        harv, job = self._create_harvester()
#        res = "http://www.fsd.uta.fi/fi/aineistot/luettelo/FSD0115/FSD0115.xml"
#        urllib2.urlopen = mock.Mock(return_value=StringIO(res))
#        gathered = harv.gather_stage(job)
#        urllib2.urlopen = mock.Mock(return_value=open("ddi3.xml"))
#        harvest_obj = HarvestObject.get(gathered[0])
#        harv.fetch_stage(harvest_obj)
#        harv.import_stage(harvest_obj)
