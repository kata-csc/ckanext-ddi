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

from ckan.model import Package, Group, Vocabulary, Session
from ckan.lib.base import h
from ckan.controllers.storage import BUCKET, get_ofs

from ckan import model
from ckan.model.authz import setup_default_user_roles
from ckan.lib.munge import munge_tag
from ckanext.harvest.harvesters.base import HarvesterBase
from ckanext.harvest.model import HarvestObject, HarvestJob

from bs4 import BeautifulSoup, Tag

log = logging.getLogger(__name__)

import socket
socket.setdefaulttimeout(30)

import traceback
import pprint


def ddi2ckan(data, original_url=None, original_xml=None, harvest_object=None):
    try:
        return _ddi2ckan(data, original_url, original_xml, harvest_object)
    except Exception as e:
        log.debug(traceback.format_exc(e))
    return False


def _collect_attribs(el):
    '''Collect attributes to a string with (k,v) value where k is attribute
    name and v is the attribute value.
    '''
    astr = ""
    if el.attrs:
        for k, v in el.attrs.items():
            astr += "(%s,%s)" % (k, v)
    return astr

def _construct_csv(var, heads):
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

def _create_code_rows(var):
    rows = []
    for cat in var('catgry', text=False, recursive=False):
        catdict = {}
        catdict['ID'] = var['ID'] if 'ID' in var else var['name']
        catdict['catValu'] = cat.catValu.string if cat.catValu else None
        catdict['labl'] = cat.labl.string if cat.labl else None
        catdict['catStat'] = cat.catStat.string if cat.catStat else None
        rows.append(catdict)
    return rows

def _get_headers():
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

def _ddi2ckan(ddi_xml, original_url, original_xml, harvest_object):
    model.repo.new_revision()
    study_descr = ddi_xml.codeBook.stdyDscr
    document_info = ddi_xml.codeBook.docDscr.citation
    title = study_descr.citation.titlStmt.titl.string
    if not title:
        title = document_info.titlStmt.titl.string
    name = study_descr.citation.titlStmt.IDNo.string
    update = True
    pkg = Package.get(name)
    if not pkg:
        if document_info.titlStmt.IDNo:
            # Is this guaranteed to be unique?
            pkg = Package(name=name, id=document_info.titlStmt.IDNo.string)
        else:
            pkg = Package(name=name)
        setup_default_user_roles(pkg)
        pkg.save()
        update = False
    producer = study_descr.citation.prodStmt.producer
    if not producer:
        producer = study_descr.citation.rspStmt.AuthEnty
    if not producer:
        producer = study_descr.citation.rspStmt.othId
    pkg.author = producer.string
    pkg.maintainer = producer.string
    if study_descr.citation.distStmt.contact:
        pkg.maintainer = study_descr.citation.distStmt.contact.string
    keywords = study_descr.stdyInfo.subject(re.compile('keyword|topcClas'))
    keywords = list(set(keywords))
    for kw in keywords:
        if kw:
            vocab = None
            kw_str = ""
            if kw.string:
                kw_str = kw.string
            #if 'vocab' in kw.attrs:
            #    vocab = kw.attrs.get("vocab", None)
            #if vocab and kw.string:
            #    kw_str = vocab + ' ' + kw.string
            if kw_str:
                pkg.add_tag_by_name(munge_tag(kw_str))
    if study_descr.stdyInfo.abstract:
        description_array = study_descr.stdyInfo.abstract('p')
    else:
        description_array = study_descr.citation.serStmt.serInfo('p')
    pkg.notes = '<br />'.join([description.string
                               for description in description_array])
    pkg.title = title
    pkg.url = original_url
    if not update:
        # This presumes that resources have not changed. Wrong? If something
        # has changed then technically the XML has chnaged and hence this may
        # have to "delete" old resources and then add new ones.
        ofs = get_ofs()
        nowstr = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')
        idno = study_descr.citation.titlStmt.IDNo
        agencyxml = (idno['agency'] if 'agency' in idno.attrs else '') + idno.string
        label = "%s/%s.xml" % (nowstr, agencyxml,)
        ofs.put_stream(BUCKET, label, original_xml, {})
        fileurl = config.get('ckan.site_url') + h.url_for('storage_file',
            label=label)
        pkg.add_resource(url=fileurl, description="Original metadata record",
            format="xml", size=len(original_xml))
        pkg.add_resource(url=document_info.holdings['URI']\
                         if 'URI' in document_info.holdings else '',
                         description=title)
    metas = []
    descendants = [desc for desc in document_info.descendants] +\
                  [sdesc for sdesc in study_descr.descendants]
    for docextra in descendants:
        if isinstance(docextra, Tag):
            if docextra:
                if docextra.name == 'p':
                    docextra.name = docextra.parent.name
                if not docextra.name in metas and docextra.string:
                    metas.append(docextra.string\
                                if docextra.string\
                                else self._collect_attribs(docextra))
                else:
                    if docextra.string:
                        metas.append(docextra.string\
                                    if docextra.string\
                                    else self._collect_attribs(docextra))
    # Assumes that dataDscr has not changed. Valid?
    if ddi_xml.codeBook.dataDscr and not update:
        vars = ddi_xml.codeBook.dataDscr('var')
        heads = _get_headers()
        c_heads = ['ID', 'catValu', 'labl', 'catStat']
        f_var = StringIO.StringIO()
        c_var = StringIO.StringIO()
        varwriter = csv.DictWriter(f_var, heads)
        codewriter = csv.DictWriter(c_var, c_heads)
        heading_row = {}
        for head in heads:
            heading_row[head] = head
        c_heading_row = {}
        for head in c_heads:
            c_heading_row[head] = head
        varwriter.writerow(heading_row)
        codewriter.writerow(c_heading_row)
        for var in vars:
            try:
                varwriter.writerow(_construct_csv(var, heads))
                codewriter.writerows(_create_code_rows(var))
            except ValueError, e:
                # Assumes that the process failed. Room for retry?
                raise IOError("Failed to import DDI to CSV! %s" % e)
        f_var.flush()
        label = "%s/%s_var.csv" % (nowstr, name)
        ofs.put_stream(BUCKET, label, f_var, {})
        fileurl = config.get('ckan.site_url') + h.url_for('storage_file', label=label)
        pkg.add_resource(url=fileurl, description="Variable metadata",
                         format="csv", size=f_var.len)
        label = "%s/%s_code.csv" % (nowstr, name)
        ofs.put_stream(BUCKET, label, c_var, {})
        fileurl = config.get('ckan.site_url') + h.url_for('storage_file', label=label)
        pkg.add_resource(url=fileurl, description="Variable code values",
                         format="csv", size=c_var.len)
        f_var.seek(0)
        reader = csv.DictReader(f_var)
        for var in reader:
            metas.append(var['labl'] if 'labl' in var else var['qstnLit'])
    pkg.extras['ddi_extras'] = " ".join(metas)
    if study_descr.citation.distStmt.distrbtr:
        pkg.extras['publisher'] = study_descr.citation.distStmt.distrbtr.string
    if study_descr.citation.prodStmt.prodDate:
        if 'date' in study_descr.citation.prodStmt.prodDate.attrs:
            pkg.version = study_descr.citation.prodStmt.prodDate.attrs['date']
    if study_descr.citation.titlStmt.parTitl:
        for (idx, title) in enumerate(study_descr.citation.titlStmt('parTitl')):
            pkg.extras['title_%d' % idx] = title.string
            pkg.extras['lang_title_%d' % idx] = title.attrs['xml:lang']
    authorgs = []
    for value in study_descr.citation.prodStmt('producer'):
        pkg.extras["producer"] = value.string
    for value in study_descr.citation.rspStmt('AuthEnty'):
        org = ""
        if value.attrs.get('affiliation', None):
            org = value.attrs['affiliation']
        author = value.string
        authorgs.append((author, org))
    for value in study_descr.citation.rspStmt('othId'):
        pkg.extras["contributor"] = value.string
    lastidx = 1
    for auth, org in authorgs:
        pkg.extras['author_%s' % lastidx] = auth
        pkg.extras['organization_%s' % lastidx] = org
        lastidx = lastidx + 1
    producers = study_descr.citation.prodStmt.find_all('producer')
    for producer in producers:
        producer = producer.string
        if producer:
            group = Group.by_name(producer)
            if not group:
                group = Group(name=producer, description=producer,
                              title=producer)
                group.save()
            group.add_package_by_name(pkg.name)
            setup_default_user_roles(group)
    if harvest_object != None:
        harvest_object.package_id = pkg.id
        harvest_object.content = None
        harvest_object.current = True
    model.repo.commit()
    return True


def ddi32ckan(ddi_xml, original_xml, original_url=None, harvest_object=None):
    try:
        return _ddi32ckan(ddi_xml, original_xml, original_url, harvest_object)
    except Exception as e:
        log.debug(traceback.format_exc(e))
    return False

def _ddi32ckan(ddi_xml, original_xml, original_url, harvest_object):
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
        # This presumes that resources have not changed. Wrong? If something
        # has changed then technically the XML has chnaged and hence this may
        # have to "delete" old resources and then add new ones.
        ofs = get_ofs()
        nowstr = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')
        label = "%s/%s.xml" % (nowstr, study_info.attrs['id'],)
        ofs.put_stream(BUCKET, label, original_xml, {})
        fileurl = config.get('ckan.site_url') + h.url_for('storage_file',
            label=label)
        pkg.add_resource(url=fileurl, description="Original metadata record",
            format="xml", size=len(original_xml))
        # What the URI should be?
        #pkg.add_resource(url=document_info.holdings['URI']\
        #                 if 'URI' in document_info.holdings else '',
        #                 description=title)
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
    if harvest_object:
        harvest_object.package_id = pkg.id
        harvest_object.content = None
        harvest_object.current = True
        harvest_object.save()
    model.repo.commit()
    return True

