# coding: utf-8
'''
Harvester for DDI2 formats
'''

#pylint: disable-msg=E1101,E0611,F0401
import inspect
import logging
import re
import socket
import StringIO
import traceback
import warnings
import json


import lxml.etree as etree
from iso639 import languages
from pylons import config
import unicodecsv as csv
from ckanext.kata.utils import generate_pid

import ckan.controllers.storage as storage
from ckan.lib.base import h
import ckan.model as model
import ckan.model.authz as authz
import ckanext.kata.utils as utils
import ckanext.oaipmh.importcore as importcore

from ckanext.kata.utils import generate_pid

log = logging.getLogger(__name__)
socket.setdefaulttimeout(30)

AVAILABILITY_ENUM = [u'direct_download',
                     u'access_application',
                     u'access_request',
                     u'contact_owner',
                     u'through_provider']
AVAILABILITY_DEFAULT = AVAILABILITY_ENUM[3]
LICENCE_ID_DEFAULT = 'notspecified'
AVAILABILITY_FSD = AVAILABILITY_ENUM[2]
ACCESS_REQUEST_URL_FSD = 'https://services.fsd.uta.fi/'
LICENCE_ID_FSD = 'other-closed'
CONTACT_EMAIL_FSD = 'fsd@uta.fi'
CONTACT_URL_FSD = 'http://www.fsd.uta.fi'
KW_VOCAB_REGEX = re.compile(r'^(?!FSD$)')
DATE_REGEX = re.compile(
    r"""
    ([0-9]{4})
    -?
    (0[1-9]|1[0-2])?
    -?
    (0[1-9]|[12][0-9]|3[01])?
    """,
    re.VERBOSE)


# TODO Nice to have: decorator for this decorator to separate mandatory and not
def ExceptReturn(exceptions, returns=u'', mandatory_field=False):
    '''Decorator to handle exceptions in the import stage in controlled manner.

    Prevents the whole import to fail with flawed harvest objects or in the case
    of optional metadata. Collects all deficiencies of harvest objects to
    self.errors to be showed in WUI.

    :param exceptions: Exceptions to catch.
    :type exceptions: single exception or tuple of exceptions
    '''
    def decorator(f):
        def call(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except exceptions as e:
                self_ = args[0]  # Decorator intercepts method args, 1st is self
                # If inspecting is too slow remove it and use
                # 'carg=args[2] or args[1]' in format()'s below.
                frame = inspect.currentframe()  # To show caller argument
                caller_record = inspect.getouterframes(frame)[1]
                line_num = caller_record[2]
                call_line = caller_record[4][0]
                call_arg = call_line.strip().split(')', 1)[0]
                del frame  # To remove possible reference cycles
                if mandatory_field:
                    log.error('Unable to read mandatory value: {etype}: {ex} at'
                              ' {carg} (line {li})'.format(
                        etype=e.__class__.__name__, ex=e, li=line_num,
                        carg=call_arg))
                    self_.errors.append(('{etype}: {ex} at {carg}'.format(
                        etype=e.__class__.__name__, ex=e, carg=call_arg),
                                         line_num))
                else:
                    log.info('Unable to read optional value: {carg} (line {li})'
                       .format(li=line_num, carg=call_arg))
                return returns
        return call
    return decorator


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
    if url and 'fsd.uta.fi' in url:
        return True
    return False


def _access_request_URL_is_found():
    return False


def _future_keywords_to_labels_urls_implementation():
    ''' This old code is kept here for now if needed in future
    '''
    # JuhoL: extract, process and save keywords
    # JuhoL: keywords, match elements <keyword> <topClass>
    keywords = stdy_dscr.stdyInfo.subject(re.compile('keyword|topcClas'))
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


def _create_group_based_on_organizations():
    # JuhoL: Create groups
    # for organizations extracted. Is this wanted? Check
    # how groups should be used currently. For group
    # stdyDscr.citation.distStmt.distrbtr or
    # docDscr.citation.prodStmt.producer or
    # stdyDscr.citation.prodStmt.producer.get('affiliation') could be more
    # appropriate.
    producers = stdy_dscr.citation.prodStmt('producer')  # this is .find_all()
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


def _last_statements_to_rewrite():
    # JuhoL: Add also some basic fields to pkg.extras. Why?
    # MikkoK: Parsing to extras not needed with _create_or_update_package().

    if stdy_dscr.citation.distStmt.distrbtr:
        pkg.extras['publisher'] = stdy_dscr.citation.distStmt.distrbtr.string

    # JuhoL: This was old language for first title
    # Peter: Disabled, since not needed for JSON translations
    # pkg.extras['lang_title_0'] = pkg.language  # Guess. Good, I hope.


class DataConverter:

    def __init__(self):
        self.ddi_xml = None
        self.context = None
        self.strict = True
        self.errors = []

    def ddi2ckan(self, data, original_url=None, original_xml=None,
                 harvest_object=None, context=None, strict=True):
        '''Read DDI2 data and convert it to CKAN format.
        '''
        self.ddi_xml = data
        self.context = context
        self.strict = strict
        try:
            return self._ddi2ckan(original_url, original_xml, harvest_object)
        except Exception as e:
            log.debug(traceback.format_exc(e))
        return False

    def _read_value(self, bs_eval_string, default=u'', mandatory_field=False):
        '''
        Evaluate values from Beautiful Soup objects.
        Returns default if evaluation failed, else return the evaluated output.
        '''
        # Make sure we are using class variables
        eval_string = bs_eval_string if bs_eval_string.startswith('self.') \
            else 'self.' + bs_eval_string
        try:
            output = eval(eval_string)
            return output
        except (AttributeError, TypeError):
            if mandatory_field and self.strict:
                log.debug('Unable to read mandatory value: {path}'
                          .format(path=bs_eval_string))
                self.errors.append('Unable to read mandatory value: {path}'
                                   .format(path=bs_eval_string))
            else:
                log.debug('Unable to read optional value: {path}'
                          .format(path=bs_eval_string))
            return default


    def empty_errors(self):
        '''Remove errors of the instance.
        '''
        self.errors = []

    def get_errors(self):
        '''
        Return errors found in instance's data parsing.
        '''
        return self.errors

    @ExceptReturn(AttributeError)
    def get_clean_date(self, bs4_element):
        raw_date = DATE_REGEX.search(bs4_element.get('date'))
        return raw_date.group(0).rstrip('-') if raw_date and \
                                                raw_date.group(0) else ''

    @ExceptReturn((AttributeError, TypeError, UserWarning, IndexError),
                  mandatory_field=True)
    def get_attrdate_mandatory(self, start_bs4tag, *args, **kwargs):
        # TODO: this is obsolete, more general see: get_attr_mandatory()
        ''''Search BeautifulSoup object for a tag and return its date attribute.

        Search beginning from start_bs4tag with *args and **kwargs. Remove found
        tags from ddi xml with extract(). Assure that no empty tags fail. Remove
        in-keyword-commas.

        :param start_bs4tag: bs4 tag to start search
        :type start_bs4tag: bs4.element.Tag instance
        :returns: a string of comma separated keywords
        :rtype: a string
        '''
        result_set = start_bs4tag(args, kwargs)
        if len(result_set) > 1:
            warnings.simplefilter('error', UserWarning)  # raises warning
            warnings.warn('Ambiguous tag found: {tag}'.format(
                tag=result_set[0].name))
        return self.get_clean_date(result_set[0])

    @ExceptReturn((AttributeError, TypeError, UserWarning))
    def get_attrdate_optional(self, start_bs4tag, *args, **kwargs):
        '''Search BeautifulSoup object for a tag and return its date attribute.

        Optional version. see. get_attrdate_mandatory
        '''
        result_set = start_bs4tag(args, kwargs)
        if len(result_set) > 1:
            warnings.simplefilter('error', UserWarning)  # raises warning
            warnings.warn('Ambiguous tag found: {tag}'.format(
                tag=result_set[0].name))
        return self.get_clean_date(result_set[0]) if result_set else ''

    @ExceptReturn((AttributeError, TypeError, KeyError, UserWarning), mandatory_field=True)
    def get_attr_mandatory(self, start_bs4tag, search_tag, attr):
        '''Return the value of an attribute of a BeautifulSoup tag.
        '''
        result_set = start_bs4tag(search_tag)
        if len(result_set) > 1:
            warnings.simplefilter('error', UserWarning)  # To raise as exception
            warnings.warn('Ambiguous tag found: {tag}'.format(
                tag=result_set[0].name))
        # Strange 'else' statement is to trigger exception
        return result_set[0][attr] if result_set else result_set[attr]

    @ExceptReturn((AttributeError, TypeError, KeyError))
    def get_attr_optional(self, start_bs4tag, search_tag, attr):
        result_set = start_bs4tag(search_tag)
        if len(result_set) > 1:
            warnings.simplefilter('error', UserWarning)  # To raise as exception
            warnings.warn('Ambiguous tag found: {tag}'.format(
                tag=result_set[0].name))
        # Strange 'else' statement is to trigger exception
        return result_set[0][attr] if result_set else result_set[attr]

    # Authors & organizations
    @ExceptReturn((AttributeError, TypeError), mandatory_field=True)
    def get_authors(self, start_bs4tag, search_tag='AuthEnty'):
        result_set = start_bs4tag(search_tag)
        # TODO Prevent / filter duplicate authors.
        authors = []
        for tag in result_set:
            authors.append({'role': 'author',
                            # TODO: use extract() to remove tag
                            'name': tag.text.strip(),
                            'organisation': tag.get('affiliation', '')})
        return authors

    @ExceptReturn((AttributeError, TypeError))
    def get_contributors(self, start_bs4tag, search_tag='othId'):
        result_set = start_bs4tag(search_tag)
        contributors = []
        for tag in result_set:
            contributors.append({'role': 'contributor',
                            # TODO: use extract() to remove tag
                            'name': tag.text.strip(),
                            'organisation': tag.get('affiliation', '')})
        return contributors

    @ExceptReturn((AttributeError, TypeError), mandatory_field=True)
    def get_keywords(self, start_bs4tag):
        return self.search_tag_content(start_bs4tag, vocab=KW_VOCAB_REGEX)

    @ExceptReturn((AttributeError, TypeError))
    def get_discipline(self, start_bs4tag):
        return self.search_tag_content(start_bs4tag, 'topcClas', vocab='FSD')

    def search_tag_content(self, start_bs4tag, *args, **kwargs):
        '''
        Search BeautifulSoup object for keywords or alike and return comma
        separated string of results.

        Search beginning from start_bs4tag with `args` and `kwargs`. Remove found
        tags from ddi xml with extract(). Assure that no empty tags fail.

        :param start_bs4tag: bs4 tag to start search
        :type start_bs4tag: bs4.element.Tag instance
        :param args: searched tag (only one supported) or none
        :type args: one string or None
        :param kwargs: searched attributes of a ddi tag
        :type kwargs: zero or more key-value pairs
        :returns: a string of comma separated keywords
        :rtype: a string
        '''
        result_set = start_bs4tag(args, kwargs)
        strings = [ tag.extract().string for tag in result_set ]
        kw_string = ','.join([ s for s in strings if s ])
        return kw_string

    def _get_events(self, stdy_dscr, authors):
        '''
        Parse data into events from DDI fields
        '''
        events = []

        # Event: Collection
        ev_type_collect = self._read_value(stdy_dscr + ".stdyInfo.sumDscr('collDate', event='start')")
        data_collector = self._read_value(stdy_dscr + ".method.dataColl('dataCollector')")
        data_coll_string = u''
        for d in data_collector:
            if d.text:
                data_coll_string += '; ' + (d.text)
            elif d['affiliation']:  # This is ok because d is BeautifulSoup object
                data_coll_string += '; ' + (d['affiliation'])
        data_coll_string = data_coll_string[2:]
        for collection in ev_type_collect:
            events.append({'descr': u'Event automatically created at import.',
                           'type': u'collection',
                           'when': self.get_clean_date(collection),
                           'who': data_coll_string})

        # Event: Creation (eg. Published in publication)
        ev_type_create = self._read_value(stdy_dscr + ".citation.prodStmt('prodDate')")
        if ev_type_create:
            data_creators = [ a.get('name') or a.get('organisation') for a in authors ]
            data_creator_string = '; '.join(data_creators)
            events.append({'descr': u'Event automatically created at import.',
                           'type': u'creation',
                           'when': self.get_clean_date(ev_type_create[0]),
                           'who': data_creator_string})
        # TODO: Event: Published (eg. Deposited to some public access archive)

        return events

    @ExceptReturn((AttributeError, TypeError))
    def get_geo_coverage(self, start_bs4tag):
        '''Return a string of comma separated locations.

        Removes matched tags from ddi xml with extract().

        >>> self.get_geo_coverage(self.ddi_xml)
            u'Espoo,Keilaniemi'
        '''
        geog_lcs = start_bs4tag('geogCover')
        geog_string = ','.join([ loc.extract().string for loc in geog_lcs ])
        return geog_string

    @ExceptReturn((AttributeError, TypeError))
    def get_temporal_coverage(self, start_bs4tag):
        '''Return the beginning and ending date of a time period covered by
        dataset.

        Removes matched tags from ddi xml with extract().
        '''
        t_begin = t_end = u''
        time_prds = start_bs4tag('timePrd')
        for t in time_prds:
            clean_date = self.get_clean_date(t.extract())
            if t.attrs['event'] == 'single':
                t_begin = t_end = clean_date
            if t.attrs['event'] == 'start':
                t_begin = clean_date
            if t.attrs['event'] == 'end':
                t_end = clean_date
        return t_begin, t_end

    def convert_language(self, lang):
        '''
        Convert alpha2 language (eg. 'en') to terminology language (eg. 'eng')
        '''
        try:
            lang_object = languages.get(part1=lang)
            return lang_object.terminology
        except KeyError as ke:
            # TODO: Parse ISO 639-2 B/T ?
            log.debug('Invalid language: {ke}'.format(ke=ke))
            return ''

    @ExceptReturn(UnicodeEncodeError, mandatory_field=True)
    def _save_original_xml(self, original_xml, name, harvest_object=None):
        ''' Here is created a ofs storage ie. local pairtree storage for
        objects/blobs. The original xml is saved to this storage in
        <harvest_source_id> (or <c.user>) named folder. NOTE: The content of
        this folder is overwritten at reharvest. We assume that if metadata is
        re-parsed also xml is changed. So old xml can be overwritten.

        Example::

        pairtree storage: /opt/data/ckan/data_tree
        xml: <pairtree storage>/pairtree_root//de/fa/ul/t/obj/<harvest_source_id>/FSD1049.xml
        url:<ckan_url>/storage/f/<harvest_source_id>/FSD1049.xml
        '''
        if harvest_object:
            dir = harvest_object.harvest_source_id
        else:
            dir = self.context['user']
        label = '{directory}/{filename}.xml'.format(directory=dir,
                                                    filename=name)
        try:
            ofs = storage.get_ofs()
            ofs.put_stream(storage.BUCKET, label, original_xml, {})
            fileurl = config.get('ckan.site_url') + h.url_for('storage_file',
                                                              label=label)
        except IOError, ioe:
            log.debug('Unable to save original xml to: {sto}, {io}'.format(
                sto=storage.BUCKET, io=ioe))
            self.errors.append('Unable to save original xml: {io}'.format(
                io=ioe))
            return u''
        return fileurl

    def _save_ddi_variables_to_csv(self, name, pkg, harvest_object):
        # JuhoL: Handle codeBook.dataDscr parts, extract data (eg. questionnaire)
        # variables etc.
        # Saves <var>...</var> elements to a csv file accessible at:
        # <ckan_url>/storage/f/2013-11-05T18%3A10%3A19.686858/1049_var.csv
        # And separately saves <catgry> elements inside <var> to a csv as a resource
        # for package.
        # Assumes that dataDscr has not changed. Valid?
        data_dscr = "ddi_xml.codeBook.dataDscr"
        try:
            ofs = storage.get_ofs()
        except IOError, ioe:
            log.debug('Unable to save xml variables: {io}'.format(io=ioe))
            self.errors.append('Unable to save xml variables: {io}'.format(io=ioe))
            return u''

        ddi_vars = self._read_value(data_dscr + "('var')")  # Find all <var> elements
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
        for var in ddi_vars:
            try:
                varwriter.writerow(_construct_csv(var, heads))
                codewriter.writerows(_create_code_rows(var))
            except ValueError, e:
                # Assumes that the process failed. Room for retry?
                raise IOError("Failed to import DDI to CSV! %s" % e)
        f_var.flush()
        label = '{dir}/{filename}_var.csv'.format(
            dir=harvest_object.harvest_source_id, filename=name)
        ofs.put_stream(storage.BUCKET, label, f_var, {})
        fileurl_var = config.get('ckan.site_url') + h.url_for('storage_file',
                                                          label=label)
        #pkg.add_resource(url=fileurl,
        #                 description="Variable metadata",
        #                 format="csv",
        #                 size=f_var.len)

        label = '{dir}/{filename}_code.csv'.format(
            dir=harvest_object.harvest_source_id, filename=name)
        ofs.put_stream(storage.BUCKET, label, c_var, {})
        fileurl_code = config.get('ckan.site_url') + h.url_for('storage_file',
                                                          label=label)
        #pkg.add_resource(url=fileurl,
        #                 description="Variable code values",
        #                 format="csv",
        #                 size=c_var.len)
        # JuhoL: Append labels of variables ('questions') also to metas
        # TODO: change to return XPath dict of labels
        flattened_var_labels = {}
        f_var.seek(0)  # JuhoL: Set 'read cursor' to row 0
        reader = csv.DictReader(f_var)
        #for var in reader:
        #    metas.append(var['labl'] if 'labl' in var else var['qstnLit'])
        # TODO: return flattened_var_labels
        return fileurl_var, fileurl_code

    def _ddi2ckan(self, original_url, original_xml, harvest_object):
        '''Extract package values from bs4 object 'ddi_xml' parsed from xml
        '''
        # TODO: Use .extract() and .string.extract() function so handled elements are removed from ddi_xml.
        doc_citation = "ddi_xml.codeBook.docDscr.citation"
        stdy_dscr = "ddi_xml.codeBook.stdyDscr"

        ####################################################################
        #      Read mandatory metadata fields:                             #
        ####################################################################
        # Authors & organizations
        authors = self.get_authors(self.ddi_xml.stdyDscr.citation, 'AuthEnty')
        agent = authors[:]
        agent.extend(self.get_contributors(self.ddi_xml.stdyDscr.citation))

        # Availability
        availability = AVAILABILITY_DEFAULT
        if _access_request_URL_is_found():
            availability = 'direct_download'
        if _is_fsd(original_url):
            availability = AVAILABILITY_FSD

        # Keywords
        keywords = self.get_keywords(self.ddi_xml.stdyDscr.stdyInfo.subject)

        # Language
        # TODO: Where/how to extract multiple languages: 'language': u'eng, fin, swe' ?
        language = self.convert_language(
            self._read_value("ddi_xml.codeBook.get('xml:lang')"))

        # Titles
        titles = self._read_value(stdy_dscr + ".citation.titlStmt(['titl', 'parTitl'])") or \
            self._read_value(doc_citation + ".titlStmt(['titl', 'parTitl'])", mandatory_field=True)

        # langtitle=[dict(lang=self.convert_language(a.get('xml:lang', '')), value=a.text) for a in titles]
        # [{"lang":"fin", "value":"otsikko"}, {"lang:"en", "value":"title"}]

        # convert the titles to a JSON string of type {"fin":"otsikko", "eng","title"}
        transl_json = {}
        first_title = ""

        # default to finnish, since first title has no lang value, which causes the validator to whine
        # we might want to update the DDI harvester to accept a language configuration parameter, if
        # we decide to harvest DDI resources from other sources.
        default_lang = "fin"
        for title in titles:
            transl_json[self.convert_language(title.get('xml:lang', default_lang))] = title.text

            # we want to get save the first title for use lateron
            if not first_title:
                first_title = title.text

        title = json.dumps(transl_json)

        # License
        # TODO: Extract prettier output. Should we check that element contains something?
        # Should this be in optional section if not mandatory_field?
        license_url = self._read_value(stdy_dscr + ".dataAccs.useStmt.get_text(separator=u' ')", mandatory_field=False)
        if _is_fsd(original_url):
            license_id = LICENCE_ID_FSD
        else:
            license_id = LICENCE_ID_DEFAULT

        # Contact (package_extra.key: contact_[k]_name in database, contact in WUI)
        contact_name = self._read_value(stdy_dscr + ".citation.distStmt('contact')") or \
                     self._read_value(stdy_dscr + ".citation.distStmt('distrbtr')") or \
                     self._read_value(doc_citation + ".prodStmt('producer')", mandatory_field=True)
        # TODO: clean out (or ask FSD to clean) mid text newlines (eg. in FSD2482)
        if contact_name and contact_name[0].text:
            contact_name = contact_name[0].text
        else:
            contact_name = self._read_value(stdy_dscr + ".citation.prodStmt.producer.get('affiliation')", mandatory_field=True)
        if _is_fsd(original_url):
            contact_email = CONTACT_EMAIL_FSD
            # TODO: Allow trying other email also in FSD metadata
        else:
            contact_email = self._read_value(stdy_dscr + ".citation.distStmt.contact.get('email')", mandatory_field=True)

        # Modified date
        version = self.get_attr_optional(self.ddi_xml.stdyDscr.citation,
                                         'prodDate', 'date') or \
                  self.get_attr_mandatory(self.ddi_xml.stdyDscr.citation,
                                          'version', 'date')

        # Name
        name_prefix = self._read_value(stdy_dscr + ".citation.titlStmt.IDNo.get('agency')", mandatory_field=False)
        name_id = self._read_value(stdy_dscr + ".citation.titlStmt.IDNo.text", mandatory_field=False)
        if not name_prefix:
            name_prefix = self._read_value(doc_citation + ".titlStmt.IDNo['agency']", mandatory_field=True)
        if not name_id:
            name_id = self._read_value(doc_citation + ".titlStmt.IDNo.text", mandatory_field=True)
        name = utils.datapid_to_name(name_prefix + name_id)

        pids = list()
        pids.append({'id': name, 'type': 'data', 'primary': 'True', 'provider': name_prefix})

        # Should we generate a version PID?
        # vpid = utils.generate_pid()
        # pids.append({'id': vpid, 'type': 'version', 'provider': 'kata'})

        # Original xml and web page as resource
        orig_xml_storage_url = self._save_original_xml(original_xml, name, harvest_object)
        # For FSD 'URI' leads to summary web page of data, hence format='html'
        orig_web_page = self._read_value(doc_citation + ".holdings.get('URI', '')")
        if orig_web_page:
            orig_web_page_resource = {'description': first_title,
                                      'format': u'html',
                                      'resource_type': 'documentation',
                                      'url': orig_web_page}
        else:
            orig_web_page_resource = {}

        # Owner
        owner = self._read_value(stdy_dscr + ".citation.prodStmt.producer.text") or \
                self._read_value(stdy_dscr + ".citation.rspStmt.AuthEnty.text") or \
                self._read_value(doc_citation + ".prodStmt.producer.string", mandatory_field=True)
        agent.append({'role': 'owner',
                      'name': owner})

        # Owner organisation
        if harvest_object:
            hsid = harvest_object.harvest_source_id
            hsooid = model.Session.query(model.Package).filter(model.Package.id==hsid).one().owner_org
            owner_org = model.Session.query(model.Group).filter(model.Group.id==hsooid).one().name
        else:
            owner_org = u''

        # Distributor (Agent: distributor, the same is used as contact)
        agent.append({
            'role': 'distributor',
            'name': contact_name})

        ####################################################################
        #      Read optional metadata fields:                              #
        ####################################################################
        # Availability
        if _is_fsd(original_url):
            access_request_url = ACCESS_REQUEST_URL_FSD
        else:
            access_request_url = u''

        # Contact
        contact_phone = self._read_value(doc_citation + ".holdings.get('callno')") or \
                        self._read_value(stdy_dscr + ".citation.holdings.get('callno')")

        contact_URL = self._read_value( stdy_dscr + ".dataAccs.setAvail.accsPlac.get('URI')") or \
                      self._read_value( stdy_dscr + ".citation.distStmt.contact.get('URI')") or \
                      self._read_value( stdy_dscr + ".citation.distStmt.distrbtr.get('URI')") or \
                      CONTACT_URL_FSD if _is_fsd(original_url) else None

        # Description
        description_array = self._read_value(stdy_dscr + ".stdyInfo.abstract('p')")
        if not description_array:
            description_array = self._read_value(stdy_dscr + ".citation.serStmt.serInfo('p')")

        notes = '\r\n\r\n'.join([description.string for
                                 description in description_array])

        # Discipline
        discipline = self.get_discipline(self.ddi_xml.stdyDscr.stdyInfo.subject)

        # Dataset lifetime events
        events = self._get_events(stdy_dscr, authors)

        # Geographic coverage
        geo_cover = self.get_geo_coverage(self.ddi_xml)

        # Temporal coverage
        temp_start, temp_end = self.get_temporal_coverage(self.ddi_xml)


        ####################################################################
        #      Flatten rest to 'XPath/path/to/element': 'value' pairs      #
        ####################################################################
        etree_xml = etree.fromstring(str(self.ddi_xml))
        flattened_ddi = importcore.generic_xml_metadata_reader(etree_xml.find('.//{*}docDscr'))
        xpath_dict = flattened_ddi.getMap()
        flattened_ddi = importcore.generic_xml_metadata_reader(etree_xml.find('.//{*}stdyDscr'))
        xpath_dict.update(flattened_ddi.getMap())


        package_dict = dict(
            access_application_URL=u'',
            access_request_URL=unicode(access_request_url),
            agent=agent,
            algorithm=u'',   # To be implemented straight in 'resources'
            availability=unicode(availability),
            contact=[{'name': contact_name,
                      'email': contact_email,
                      'URL': contact_URL,
                      'phone': contact_phone}],
            direct_download_URL=u'',  # To be implemented straight in 'resources
            discipline=discipline,
            event=events,
            geographic_coverage=geo_cover,
            groups=[],
            id=generate_pid(),
            # langtitle=langtitle,
            langdis=u'True',  # HUOMAA!
            language=language,
            license_URL=license_url,
            license_id=license_id,
            mimetype=u'',  # To be implemented straight in 'resources
            name=name,
            notes=notes or u'',
            pids=pids,
            owner_org=owner_org,
            resources=[{'algorithm': u'',
                        'description': u'Original metadata record',
                        'format': u'xml',
                        'hash': u'',
                        'resource_type': 'file.harvest',
                        'size': len(original_xml),
                        'url': orig_xml_storage_url},
                       orig_web_page_resource],
            tag_string=keywords,
            temporal_coverage_begin=temp_start,
            temporal_coverage_end=temp_end,
            # title=langtitle[0].get('value'),   # Must exist in package dict
            title=title,
            type='dataset',
            version=version,
            version_PID='',
        )
        package_dict['xpaths'] = xpath_dict
        # Above line creates:
        # package_dict = {
        #     'access_request_url': 'some_url',
        #     # ...
        #     'xpaths': {'stdyDscr/othrStdyMat.0/relPubl.34':
        #                'Uskon asia: nuorisobarometri 2006 (2006).'},
        #               {'stdyD...': 'Some value'}]
        # }
        #package_dict['extras'].update(_save_ddi_variables_to_csv(ddi_xml, somepkg))


        # Vanhojen koodien järjestys:
        #_save_original_xml_and_link_as_resources()
        #_save_ddi_variables_to_csv()
        #_create_group_based_on_organizations()
        #_last_statements_to_rewrite()

        # JuhoL: Set harvest object to some end state and commit
        if harvest_object is not None:
            harvest_object.content = None
            # Should this be flushed? model.Session.flush()
        #model.repo.commit()

        return package_dict


#def ddi32ckan(ddi_xml, original_xml, original_url=None, harvest_object=None):
#    try:
#        return _ddi32ckan(ddi_xml, original_xml, original_url, harvest_object)
#    except Exception as e:
#        log.debug(traceback.format_exc(e))
#    return False
#
#def _ddi32ckan(ddi_xml, original_xml, original_url, harvest_object):
#    model.repo.new_revision()
#    ddiroot = ddi_xml.DDIInstance
#    main_cit = ddiroot.Citation
#    study_info = ddiroot('StudyUnit')[-1]
#    idx = 0
#    authorgs = []
#    pkg = model.Package.get(study_info.attrs['id'])
#    if not pkg:
#        pkg = model.Package(name=study_info.attrs['id'])
#        pkg.id = ddiroot.attrs['id']
#        # This presumes that resources have not changed. Wrong? If something
#        # has changed then technically the XML has chnaged and hence this may
#        # have to "delete" old resources and then add new ones.
#        ofs = storage.get_ofs()
#        nowstr = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')
#        label = "%s/%s.xml" % (nowstr, study_info.attrs['id'],)
#        ofs.put_stream(storage.BUCKET, label, original_xml, {})
#        fileurl = config.get('ckan.site_url') + h.url_for('storage_file',
#            label=label)
#        pkg.add_resource(url=fileurl, description="Original metadata record",
#            format="xml", size=len(original_xml))
#        # What the URI should be?
#        #pkg.add_resource(url=doc_citation.holdings['URI']\
#        #                 if 'URI' in doc_citation.holdings else '',
#        #                 description=title)
#    pkg.version = main_cit.PublicationDate.SimpleDate.string
#    for title in main_cit('Title'):
#        pkg.extras['title_%d' % idx] = title.string
#        pkg.extras['lang_title_%d' % idx] = title.attrs['xml:lang']
#        idx += 1
#    for title in study_info.Citation('Title'):
#        pkg.extras['title_%d' % idx] = title.string
#        pkg.extras['lang_title_%d' % idx] = title.attrs['xml:lang']
#        idx += 1
#    for value in study_info.Citation('Creator'):
#        org = ""
#        if value.attrs.get('affiliation', None):
#            org = value.attrs['affiliation']
#        author = value.string
#        authorgs.append((author, org))
#    pkg.author = authorgs[0][0]
#    pkg.maintainer = study_info.Citation.Publisher.string
#    lastidx = 0
#    for auth, org in authorgs:
#        pkg.extras['author_%s' % lastidx] = auth
#        pkg.extras['organization_%s' % lastidx] = org
#        lastidx = lastidx + 1
#    pkg.extras["licenseURL"] = study_info.Citation.Copyright.string
#    pkg.notes = "".join([unicode(repr(chi).replace('\n', '<br />'), 'utf8')\
#                         for chi in study_info.Abstract.Content.children])
#    for kw in study_info.Coverage.TopicalCoverage('Keyword'):
#        pkg.add_tag_by_name(kw.string)
#    pkg.extras['contributor'] = study_info.Citation.Contributor.string
#    pkg.extras['publisher'] = study_info.Citation.Publisher.string
#    pkg.save()
#    if harvest_object:
#        harvest_object.package_id = pkg.id
#        harvest_object.content = None
#        harvest_object.current = True
#        harvest_object.save()
#    model.repo.commit()
#    return pkg.id
#
