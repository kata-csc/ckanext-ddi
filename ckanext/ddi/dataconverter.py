# coding: utf-8
'''
Harvester for DDI2 formats
'''

#pylint: disable-msg=E1101,E0611,F0401
import datetime
import json
import logging
import lxml.etree as etree
import re
import socket
import StringIO
import urllib2

import bs4
from pylons import config
import unicodecsv as csv

from ckan.controllers.storage import BUCKET, get_ofs
from ckan.lib.base import h
import ckan.model as model
import ckan.model.authz as authz
#from ckan.lib.munge import munge_tag
from ckanext.harvest.harvesters.base import HarvesterBase
import ckanext.harvest.model as hmodel
import ckanext.kata.utils as utils

import traceback
import pprint

log = logging.getLogger(__name__)
socket.setdefaulttimeout(30)

AVAILABILITY_DEFAULT = 'contact_owner'
AVAILABILITY_FSD = 'access_request'
ACCESS_REQUEST_URL_FSD = 'http://www.fsd.uta.fi/fi/aineistot/jatkokaytto/tilaus.html'
LICENCE_ID_FSD = 'other_closed'

def ddi2ckan(data, original_url=None, original_xml=None, harvest_object=None):
    try:
        return _ddi2ckan(data, original_url, original_xml, harvest_object)
    except hmodel.HarvestObjectError:
        raise
    except AttributeError:
        raise
    except Exception as e:
        log.debug(traceback.format_exc(e))
    return False


def _collect_attribs(el):
    '''Collect attributes of a tag 'el' to a string with (k,v) value where k is
    the attribute name and v is the attribute value.
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
    retdict['ID'] = var.get('ID', var['name'])
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
    longest_els = ['ID',
                   'labl',
                   'preQTxt',
                   'qstnLit',
                   'postQTxt',
                   'ivuInstr',
                   'varFormat',
                   'TotlResp',
                   'range',
                   'item',
                   'sumStat_vald',
                   'sumStat_min',
                   'sumStat_max',
                   'sumStat_mean',
                   'sumStat_stdev',
                   'notes',
                   'txt']
    return longest_els

def _is_fsd(url):
    log.debug('url: {ur}'.format(ur=url))
    return True

def _access_request_URL_is_found():
    return False

def ExceptReturn(exception, returns):
    '''
    Example:
    @ExceptReturn(exception=AttributeError, returns='False_text')
def dc_metadata_reader(xml):
        print xml
        raise AttributeError('virhe')

    Jos "exception" tapahtuu dc_metadata_reader():ssa niin paluuarvo on "returns"

    >>> dc_metadata_reader('test')
    False_text
    '''
    def decorator(f):
        def call(*args, **kwargs):
            try:
                log.debug('call()')
                return f(*args, **kwargs)
            except exception as e:
                log.error('Exception occurred: %s' % e)
                return returns
        log.debug('decorator()')
        return call
    log.debug('ExceptReturn()')
    return decorator

#@ExceptReturn(exception=(AttributeError, ), returns=False)
def _ddi2ckan(ddi_xml, original_url, original_xml, harvest_object):
    # Create new revision with VDM
    model.repo.new_revision()

    # JuhoL: Extract package values from bs4 object 'ddi_xml' parsed from xml
    # JuhoL: Language
    language = ddi_xml.codeBook.get('xml:lang')

    # JuhoL: Extract bibliographic information part
    # JuhoL: http://www.ddialliance.org/Specification/DDI-Codebook/2.1/DTD/Documentation/section1.html
    document_info = ddi_xml.codeBook.docDscr.citation

    ## JuhoL: Extract Study Description section
    # JuhoL: http://www.ddialliance.org/Specification/DDI-Codebook/2.1/DTD/Documentation/section2.html
    study_descr = ddi_xml.codeBook.stdyDscr

    # JuhoL: Title
    title = study_descr.citation.titlStmt.titl.string
    if not title:
        log.debug("Title not found in 'codeBook.stdyDscr.citation.titlStmt.titl'"
                  " 'trying codeBook.docDscr.titlStmt.titl'...")
        title = document_info.titlStmt.titl.string

    # JuhoL: ID number to pkg.name
    name = study_descr.citation.titlStmt.IDNo.string
    # JuhoL: WARN: this may mess up things...
    if not name:
        log.debug("Name not found in 'codeBook.stdyDscr.citation.titlStmt.IDNo'"
                  " 'trying codeBook.docDscr.titlStmt.IDNo'...")
        name = document_info.titlStmt.IDNo.string
        if not name:
            raise hmodel.HarvestObjectError(message='No name found!',
                                            object=harvest_object,
                                            stage='Import')
            # JuhoL: if we generate pkg.name we cannot reharvest + end up adding
            # same harvest object at each reharvest
            # name = utils.generate_pid()
        # TODO: JuhoL: Should we try last study_descr.citation.titlStmt.titl?
        # It's one of few mandatory ddi fields.
    update = True
    # JuhoL: Try to get existing package
    # TODO: JuhoL: FSD's IDs for ddi are too simple ('1049'). May collide. They
    # TODO: are about to change though. Just to notice.
    # JuhoL: Use .by_name instead of .get. Otherwise if harvest object had its
    # id (maliciously) set same as some existing packages id, it could overwrite
    # that package.
    pkg = model.Package.by_name(name)
    if not pkg:  # JuhoL: Create a new package
        update = False
        #if document_info.titlStmt.IDNo:  # JuhoL: Why this is taken from different section compared to 'name'?
            # Is this guaranteed to be unique?
        #    pkg = model.Package(name=name, id=document_info.titlStmt.IDNo.string)
        #else:
        pkg = model.Package(name=name)
        authz.setup_default_user_roles(pkg)
        pkg.save()

    # JuhoL: Extract producer element
    # TODO: extract list to author: [ rspStmt.AuthEnty, prodStmt.producer, rspStmt.othId ]
    producer = study_descr.citation.prodStmt.producer or \
               study_descr.citation.rspStmt.AuthEnty or \
               study_descr.citation.rspStmt.othId

   # JuhoL: Assign some metadata to package
    pkg.language = language
    # TODO: author: list of dicts here more likely
    auth_entys = study_descr.citation.rspStmt('AuthEnty')
    authors = []
    organizations = []
    for a in auth_entys:
        authors.append({'value': a.text})
        organizations.append({'value': a.get('affiliation')})

    pkg.author = producer.string  #{'value': study_descr.citation.rspStmt.AuthEnty.string}  # etc ...

    # JuhoL: Assign and reassign the maintainer
    maintainer = study_descr.citation.distStmt.contact or \
                 study_descr.citation.distStmt.distrbtr or \
                 study_descr.citation.prodStmt.producer.get('affiliation')
    if maintainer:
        pkg.maintainer = maintainer.string
    contact = study_descr.citation.distStmt.contact
    try:
        pkg.maintainer_email = contact.get('email')
    except AttributeError, err:
        log.debug('Note: not all fields were found...')
        pass

    # JuhoL: Availability and license
    # JuhoL: FSD specific hack. Automate later in ddi/harvester.py?
    # JuhoL: Make sure pkg.availability is implemented: schema, ...
    pkg.availability = AVAILABILITY_DEFAULT
    if _access_request_URL_is_found:
        pkg.availability = 'direct_download'
    if _is_fsd(original_url):
        pkg.availability = AVAILABILITY_FSD
        pkg.access_request_URL = ACCESS_REQUEST_URL_FSD
        pkg.license_id = LICENCE_ID_FSD
    # TODO: Extract prettier output. Should we check that element contains something?
    try:
        use_stmt = study_descr.dataAccs.useStmt
    except AttributeError:
        raise
    if use_stmt:
        pkg.license_URL = use_stmt.get_text(separator=u' ')

    # JuhoL: extract, process and save keywords
    # JuhoL: keywords, match elements <keyword> <topClass>
    keywords = study_descr.stdyInfo.subject(re.compile('keyword|topcClas'))
    keywords = list(set(keywords))  # JuhoL: For what? Transforming, filtering?
    idx = 0
    for kw in keywords:
        if not kw:
            continue
        #vocab = None
        #if 'vocab' in kw.attrs:
        #    vocab = kw.attrs.get("vocab", None)
        if not kw.string:
            continue
        tag = kw.string.strip()
        if tag.startswith('http://www.yso.fi'):
            tags = utils.label_list_yso(tag)
            pkg.extras['tag_source_%i' % idx] = tag
            idx += 1
        elif tag.startswith('http://') or tag.startswith('https://'):
            pkg.extras['tag_source_%i' % idx] = tag
            idx += 1
            tags = [] # URL tags break links in UI.
        else:
            tags = [tag]
        for tagi in tags:
            #pkg.add_tag_by_name(t[:100])
            tagi = tagi[:100]  # 100 char limit in DB.
            tag_obj = model.Tag.by_name(tagi)
            if not tag_obj:
                tag_obj = model.Tag(name=tagi)
                tag_obj.save()
            pkgtag = model.Session.query(model.PackageTag).filter(
                model.PackageTag.package_id==pkg.id).filter(
                model.PackageTag.tag_id==tag_obj.id).limit(1).first()
            if not pkgtag:
                pkgtag = model.PackageTag(tag=tag_obj, package=pkg)
                pkgtag.save()  # Avoids duplicates if tags has duplicates.

    # JuhoL: Description
    if study_descr.stdyInfo.abstract:
        description_array = study_descr.stdyInfo.abstract('p')
    else:
        description_array = study_descr.citation.serStmt.serInfo('p')
    pkg.notes = '<br />'.join([description.string for
                               description in description_array])
    # JuhoL: Title
    pkg.title = title

    # JuhoL: URL of ddi-xml
    pkg.url = original_url

    if not update:
        # JuhoL: Here is created a ofs storage ie. local pairtree storage for
        # objects/blobs (eg. /opt/data/ckan/data_tree). The original xml is
        # saved to this local storage (eg.
        # /opt/data/ckan/data_tree/pairtree_root//de/fa/ul/t/obj/2013-11-05T18\:10\:19.686858/FSD1049.xml).
        # The original xml is accessible at:
        # <ckan_url>/storage/f/2013-11-05T18%3A10%3A19.686858/FSD1049.xml

        # This presumes that resources have not changed. Wrong? If something
        # has changed then technically the XML has changed and hence this may
        # have to "delete" old resources and then add new ones.
        # JuhoL: Yes, existing resources should be overwritten?
        ofs = get_ofs()
        nowstr = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')
        idno = study_descr.citation.titlStmt.IDNo
        agencyxml = idno.get('agency', '') + idno.string
        label = "%s/%s.xml" % (nowstr, agencyxml,)
        ofs.put_stream(BUCKET, label, original_xml, {})
        fileurl = config.get('ckan.site_url') + h.url_for('storage_file',
                                                          label=label)
        pkg.add_resource(url=fileurl,
                         description="Original metadata record",
                         format="xml",
                         size=len(original_xml))
        # JuhoL: for FSD 'URI' leads to summary web page of data, hence format='html'
        pkg.add_resource(url=document_info.holdings.get('URI', ''),
                         description=title,
                         format='html')

    # JuhoL: Extra ddi metadata
    # in codeBook.docDscr and .stdyDscr handled. This should be rewritten to
    # extras = { xpath/as/key: val, ... }
    metas = []
    # JuhoL: Check if this multiplies some elements because recursive .descendants
    descendants = [desc for desc in document_info.descendants] + \
                  [sdesc for sdesc in study_descr.descendants]
    for docextra in descendants:
        if isinstance(docextra, bs4.Tag):
            if docextra:
                if docextra.name == 'p':
                    docextra.name = docextra.parent.name
                if not docextra.name in metas and docextra.string:
                    metas.append(docextra.string
                                 if docextra.string
                                 else _collect_attribs(docextra))
                else:  # JuhoL: Why this is same as above?
                    if docextra.string:
                        metas.append(docextra.string
                                     if docextra.string
                                     else _collect_attribs(docextra))

    # JuhoL: Handle codeBook.dataDscr parts, extract data (eg. questionnaire)
    # variables etc.
    # Saves <var>...</var> elements to a csv file accessible at:
    # <ckan_url>/storage/f/2013-11-05T18%3A10%3A19.686858/1049_var.csv
    # And separately saves <catgry> elements inside <var> to a csv as a resource
    # for package.

    # Assumes that dataDscr has not changed. Valid?
    if ddi_xml.codeBook.dataDscr and not update:
        vars = ddi_xml.codeBook.dataDscr('var')  # Find all <var> elements
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
        fileurl = config.get('ckan.site_url') + h.url_for('storage_file',
                                                          label=label)
        pkg.add_resource(url=fileurl,
                         description="Variable metadata",
                         format="csv",
                         size=f_var.len)
        label = "%s/%s_code.csv" % (nowstr, name)
        ofs.put_stream(BUCKET, label, c_var, {})
        fileurl = config.get('ckan.site_url') + h.url_for('storage_file',
                                                          label=label)
        pkg.add_resource(url=fileurl,
                         description="Variable code values",
                         format="csv",
                         size=c_var.len)
        # JuhoL: Append labels of variables ('questions') also to metas
        f_var.seek(0)  # JuhoL: Set 'read cursor' to row 0
        reader = csv.DictReader(f_var)
        for var in reader:
            metas.append(var['labl'] if 'labl' in var else var['qstnLit'])

    pkg.extras['ddi_extras'] = " ".join(metas)
    # JuhoL: Add also some basic fields to pkg.extras. Why?
    if study_descr.citation.distStmt.distrbtr:
        pkg.extras['publisher'] = study_descr.citation.distStmt.distrbtr.string
    # JuhoL: Modified date
    if study_descr.citation.prodStmt.prodDate:
        if 'date' in study_descr.citation.prodStmt.prodDate.attrs:
            pkg.version = study_descr.citation.prodStmt.prodDate.attrs['date']

    # Store title in extras as well.
    pkg.extras['title_0'] = pkg.title
    pkg.extras['lang_title_0'] = pkg.language  # Guess. Good, I hope.
    if study_descr.citation.titlStmt.parTitl:
        for (idx, title) in enumerate(study_descr.citation.titlStmt('parTitl')):
            pkg.extras['title_%d' % (idx + 1)] = title.string
            # JuhoL: Should missing xml:lang return KeyError as in this?
            pkg.extras['lang_title_%d' % (idx + 1)] = title.attrs['xml:lang']

    # JuhoL: Authors & organizations to extras
    authorgs = []
    for value in study_descr.citation.prodStmt('producer'):
        pkg.extras["producer"] = value.string
    for value in study_descr.citation.rspStmt('AuthEnty'):
        org = ""
        if value.attrs.get('affiliation', None):
            org = value.attrs['affiliation']
        author = value.string
        authorgs.append((author, org))

    # JuhoL: Other contributors
    for value in study_descr.citation.rspStmt('othId'):
        pkg.extras["contributor"] = value.string

    lastidx = 1
    for auth, org in authorgs:
        pkg.extras['author_%s' % lastidx] = auth
        pkg.extras['organization_%s' % lastidx] = org
        lastidx = lastidx + 1

    # JuhoL: Create groups
    # for organizations extracted. Is this wanted? Check
    # how groups should be used currently. For group
    # stdyDscr.citation.distStmt.distrbtr or
    # docDscr.citation.prodStmt.producer or
    # stdyDscr.citation.prodStmt.producer.get('affiliation') could be more
    # appropriate.
    producers = study_descr.citation.prodStmt('producer')  # this is .find_all()
    for producer in producers:
        producer = producer.string
        if producer:
            group = model.Group.by_name(producer)
            if not group:
                # JuhoL: Gives UnicodeEncodeError if contains scandics, see
                # ckanext-shibboleth plugin.py for similar fix
                group = model.Group(name=producer, description=producer,
                                    title=producer)
                group.save()
            group.add_package_by_name(pkg.name)
            authz.setup_default_user_roles(group)

    # JuhoL: Set harvest object to some end state and commit
    if harvest_object != None:
        harvest_object.package_id = pkg.id
        harvest_object.content = None
        harvest_object.current = True
    model.repo.commit()
    return pkg.id


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
    pkg = model.Package.get(study_info.attrs['id'])
    if not pkg:
        pkg = model.Package(name=study_info.attrs['id'])
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
    return pkg.id

