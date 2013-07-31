# -*- coding: utf-8 -*-

"""
Some useful utilities.
"""

__author__ = "Hui Zheng"
__copyright__ = "Copyright 2011-2012 Hui Zheng"
__credits__ = ["Hui Zheng"]
__license__ = "MIT <http://www.opensource.org/licenses/mit-license.php>"
__version__ = "0.1"
__email__ = "xyzdll[AT]gmail[DOT]com"

import os
import sys

import ConfigParser
import cookielib
import hashlib
import logging
import logging.config
import mechanize
import xml.etree.ElementTree
try:
    import xml.etree.cElementTree as etree
except ImportError:
    import xml.etree.ElementTree as etree

ENCODING = sys.stdin.encoding # typically "UTF-8"

LOGGER_CONF_FILE = os.path.join(
        os.getenv('LOG_CONF_DIR') or ".", "box-logging.conf")
LOGGER_NAME = "box"


def is_posix():
    return os.name == 'posix'


def get_sha1(file_obj):
    """Get SHA1 for a file"""
    with open(file_obj) as f:
        sha = hashlib.sha1()
        sha.update(f.read())
        return sha.hexdigest()


def encode(unicode_str):
    """Encode the given unicode as stdin's encoding"""
    return unicode_str.encode(ENCODING)


def print_unicode(unicode_str):
    """Print the given unicode string as stdin's encoding"""
    print encode(unicode_str)


def get_logger():
    """Return a logger with the given name from the given configuration file"""
    if not os.path.exists(LOGGER_CONF_FILE):
        sys.stderr.write("log configuration file {} does NOT exist\n"
                .format(LOGGER_CONF_FILE))
        sys.exit(1)

    try:
        logging.config.fileConfig(LOGGER_CONF_FILE)
        return logging.getLogger(LOGGER_NAME)
    except ConfigParser.Error as e:
        sys.stderr.write("logger configuration error - {}\n".format(e))
        sys.exit(1)


def parse_xml(source):
    """Parse an XML source into an element tree"""
    return etree.parse(source).getroot()


def stringify(obj):
    """Turn an object to a readable string(in default encoding)"""
    if xml.etree.ElementTree.iselement(obj):
        return etree.tostring(obj, ENCODING)

    if hasattr(obj, '__iter__'):
        return encode(repr(obj).decode('raw_unicode_escape'))

    try:
        return str(obj)
    except UnicodeError:
        return encode(obj)


def map_element(element):
    """Convert an XML element to a map"""
    #if sys.version_info >= (2, 7):
        #return {e.tag: e.text.strip() for e in list(element)}
    #return dict((e.tag, e.text and e.text.strip() or "")
            #for e in list(element))
    return dict((e.tag, e.text) for e in list(element))


def get_browser(debug=False):
    """Gets a browser for automating interaction"""
    browser = mechanize.Browser()

    # Cookie Jar
    browser.set_cookiejar(cookielib.LWPCookieJar())

    # Browser options
    browser.set_handle_equiv(True)
    #browser.set_handle_gzip(True)
    browser.set_handle_redirect(True)
    browser.set_handle_referer(True)
    # avoid to be rejected by robots.txt
    browser.set_handle_robots(False)

    # Follows refresh 0 but not hangs on refresh > 0
    browser.set_handle_refresh(
            mechanize._http.HTTPRefreshProcessor(), max_time=1)

    browser.set_debug_http(debug)
    browser.set_debug_redirects(debug)
    browser.set_debug_responses(debug)

    browser.addheaders = [('User-agent',
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:7.0.1)"
        " Gecko/20100101 Firefox/7.0.1")]
    return browser


def decode_args(args, options):
    """Convert args and options to unicode string"""
    for attr, value in options.__dict__.iteritems():
        if isinstance(value, str):
            setattr(options, attr, value.decode(ENCODING))
    return [arg.decode(ENCODING) for arg in args]
