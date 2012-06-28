import re
import sys
import logging
import urllib2
import unicodedata
import string

from lxml import etree

from pprint import pprint

from ckan import model
from ckan.logic import get_action, ValidationError

from ckan.lib.cli import CkanCommand

log = logging.getLogger(__name__)

class DDIImporter(CkanCommand):
    '''Remotely fetches a DDI XML file and parses it. This parsing information
    is used to create datasets and possible resources as well. Takes a URL to
    the XML or the file which the DDI data is. If the URL ends into ".xml", it
    opens it directly, otherwise it will attempt to list the URL as a list of
    XML files and parse each separately. If the parameter is recognized to be a
    XML file, it is opened directly as such.

    The multifile directive uses only an URL, which lists links to other URLs.

    Usage:

      ddi_import fetch <URL/file>

      ddi_import multifile <URL>
    '''

    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 2
    min_args = 2

    def command(self):
        self._load_config()
        # We'll need a sysadmin user to perform most of the actions
        # We will use the sysadmin site user (named as the site_id)
        context = {'model':model,'session':model.Session,'ignore_auth':True}
        self.admin_user = get_action('get_site_user')(context,{})
        print ''
        if len(self.args) == 0:
            self.parser.print_usage()
            sys.exit(1)
        cmd = self.args[0]
        if cmd == 'fetch':
            self.ddi_import()
        elif cmd == 'multifile':
            self.multi_file_import()
        else:
            self.parser.print_usage()
            sys.exit(1)

    def ddi_import(self):
        data_dict = self._fetch_and_parse_xml(self.args[1])
        if not data_dict:
            print 'There was an unexpected problem parsing the XML.'
        else:
            self._import_to_dataset(data_dict)

    def multi_file_import(self):
        try:
            urls = urllib2.urlopen(self.args[1])
            for url in urls.readlines():
                if not url == '':
                    self._import_to_dataset(self._fetch_and_parse_xml(url.strip()))
        except urllib2.URLError:
            print 'Could not open filelist at %s' % self.args[1]
        

    def _import_to_dataset(self, data_dict):
        if data_dict:
            try: 
                context = {'model':model,'session':model.Session,'ignore_auth':True,
                           'user': self.admin_user['name']}
                data_dict['url'] = 'http://not.found.com/'
                package = model.Package.get(data_dict['name'])
                if not package:
                    action = 'package_create'
                else:
                    action = 'package_update'
                pkg = get_action(action)(context,data_dict)
                print "Package: %s was created successfully" % (pkg['title'])
            except ValidationError, e:
                print e.error_summary
                print e.error_dict

    def _fetch_and_parse_xml(self, url_or_file):
        tree = None
        try:
            with open(url_or_file) as f:
                tree = etree.parse(f)
        except IOError as e:
            try:
                if url_or_file.endswith('.xml'):
                    f = urllib2.urlopen(url_or_file)
                    tree = etree.parse(f)
                else:
                    print 'Cannot determine filetype!'
            except urllib2.URLError as e:
                return False
        if tree:
            data_dict = {}
            title = tree.xpath('//stdyDscr//titl')[0]
            creator = tree.xpath('//AuthEnty')[0]
            keywords = tree.xpath('//keyword') + tree.xpath('//topcClas')
            description = tree.xpath('//abstract//p')
            hold = tree.xpath('//holdings')[0]
            name = unicodedata.normalize('NFKD', unicode(title.text))\
                                  .encode('ASCII', 'ignore')\
                                  .lower().replace(' ','_')
            data_dict['name'] = re.sub('\W+', '', name)
            print data_dict['name']
            data_dict['title'] = title.text
            data_dict['author'] = creator.text
            data_dict['notes'] = '<br/>'.join([descr.text for descr in description])
            data_dict['maintainer'] = ''
            data_dict['author_email'] = ''
            data_dict['tag_string'] = ','.join([kw.text if kw.text else '' for kw in keywords])
            data_dict['maintainer_email'] = ''
            #data_dict['url'] = ''
            data_dict['version'] = ''
            data_dict['license_id'] = ''
            data_dict['log_message'] = ''
            data_dict['extras'] = [{}]
            data_dict['resources'] = [{
                                       'name': title.text,
                                       'description': '\n'.join([descr.text for descr in description]),
                                       'url': hold.attrib['URI'] if 'URI' in hold.attrib else ''
                                       }]
            return data_dict
        else:
            return None
        

