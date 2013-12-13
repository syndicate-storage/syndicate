# -*- coding: utf-8 -*-

"""
Python API for manipulating files on box.com(a.k.a box.net).
"""

__author__ = "Hui Zheng"
__copyright__ = "Copyright 2011-2012 Hui Zheng"
__credits__ = ["Hui Zheng"]
__license__ = "MIT <http://www.opensource.org/licenses/mit-license.php>"
__version__ = "0.1"
__email__ = "xyzdll[AT]gmail[DOT]com"

import base64
import ConfigParser
import errno
import os
import re
import urllib
import urllib2
from zipfile import ZipFile
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

import httplib2
from poster.encode import multipart_encode
from poster.streaminghttp import register_openers

from pybox.utils import encode, get_browser, get_logger, get_sha1, is_posix, \
        map_element, parse_xml, stringify


def is_dotfile(f):
    """Test dot file(box.com is unhappy with dot files)"""
    return os.path.basename(f)[0] == '.'

logger = get_logger()


class ConfigError(Exception):
    """Configuration error"""
    pass


class StatusError(Exception):
    """Status error"""
    pass


class DiffResult(object):
    """Wrap diff results"""

    class _DiffResultItem(object):
        """Diff result for a context directory"""

        def __init__(self, container, context_node, ignore_common=True):
            self.container = container
            self.context_node = context_node
            self._client_uniques = ([], [])
            self._server_uniques = ([], [])
            self._compares = ([], [])
            self._ignore_common = ignore_common

        def get_client_unique(self, is_file):
            return self._client_uniques[0 if is_file else 1]

        def add_client_unique(self, is_file, path):
            self.get_client_unique(is_file).append(
                    path[self.container.local_prelen:])

        def get_server_unique(self, is_file):
            return self._server_uniques[0 if is_file else 1]

        def add_server_unique(self, is_file, mapping):
            uniques = self.get_server_unique(is_file)
            for name, node in mapping.iteritems():
                context = "/".join(self.container.context)
                path = (context + "/" + name)[self.container.remote_prelen:]
                uniques.append((path, node))

        def get_compare(self, is_diff):
            return self._compares[0 if is_diff else 1]

        def add_compare(self, is_diff, localpath, remotenode):
            if is_diff or not self._ignore_common:
                self.get_compare(is_diff).append(
                        (localpath[self.container.local_prelen:], remotenode))

    def __init__(self, localdir, remotedir, ignore_common=True):
        self.localdir = localdir
        self.local_prelen = len(localdir) + 1
        self.remotedir = remotedir
        self.remotename = remotedir.attrib['name']
        self.remote_prelen = len(self.remotename) + 1
        self.items = []
        self.context = []
        self._ignore_common = ignore_common

    def start_add(self, context_node):
        item = DiffResult._DiffResultItem(
                self, context_node, self._ignore_common)
        self.context.append(context_node.attrib['name'])
        self.items.append(item)
        return item

    def end_add(self):
        self.context.pop()

    def get_client_unique(self, is_file):
        for item in self.items:
            for path in item.get_client_unique(is_file):
                yield (path, item.context_node)

    def get_server_unique(self, is_file):
        for item in self.items:
            #yield iter(item.get_server_unique(is_file)).next()
            for i in item.get_server_unique(is_file):
                yield i

    def get_compare(self, is_file):
        for item in self.items:
            for localpath, remotenode in item.get_compare(is_file):
                yield (localpath, remotenode, item.context_node)

    def report(self):
        result = ([], [], [], [], [], [])
        for item in self.items:
            result[0].extend(item.get_client_unique(True))
            result[1].extend(item.get_client_unique(False))
            result[2].extend([l for l, _ in item.get_server_unique(True)])
            result[3].extend([l for l, _ in item.get_server_unique(False)])
            result[4].extend([l for l, _ in item.get_compare(True)])
            if not self._ignore_common:
                result[5].extend([l for l, _ in item.get_compare(False)])
        return result

    def __unicode__(self):
        result = self.report()
        return u"diff between client path({}) and server path({}):\n" \
                "[client only files]:\n{}\n"  \
                "[client only folders]:\n{}\n" \
                "[server only files]:\n{}\n" \
                "[server only folders]:\n{}\n" \
                "[diff files]:\n{}\n" \
                "[common files]:\n{}\n".format(
                        self.localdir, self.remotename,
                        ", ".join(result[0]),
                        ", ".join(result[1]),
                        ", ".join(result[2]),
                        ", ".join(result[3]),
                        ", ".join(result[4]),
                        "***ignored***" if self._ignore_common
                        else ", ".join(result[5]),
                        )

    def __str__(self):
        return encode(unicode(self))


class BoxApi(object):
    """Box API"""
    BOX_URL = "box.com/api/1.0/"
    BASE_URL = "https://www." + BOX_URL
    REST_URL = BASE_URL + "rest"
    AUTH_URL = BASE_URL + "auth/{0}"
    DOWNLOAD_URL = BASE_URL + "download/{0}/{1}"
    UPLOAD_URL = "https://upload." + BOX_URL + "upload/{0}/{1}"
    ROOT_ID = "0"
    ONELEVEL = "onelevel"
    NOZIP = "nozip"
    SIMPLE = "simple"
    NOFILES = "nofiles"

    # patterns
    REQUEST_TOKEN_PATTERN = re.compile(".*var request_token.*'(.+)'")
    FILENAME_PATTERN = re.compile('(.*filename=")(.+)(".*)')

    def __init__(self):
        conf_file = os.path.expanduser(
                "~/.boxrc" if is_posix() else "~/_boxrc")
        if not os.path.exists(conf_file):
            raise ConfigError(
                    "Configuration file {} not found".format(conf_file))

        try:
            conf_parser = ConfigParser.ConfigParser()
            conf_parser.read(conf_file)
            self._conf_parser = conf_parser
            self._api_key = conf_parser.get("app", "api_key")
        except ConfigParser.NoSectionError as e:
            raise ConfigError("{} (in configuration file {})"
                    .format(e, conf_file))

        self._ticket = None
        self._auth_token = None

    @staticmethod
    def _log_response(response):
        """Log response"""
        logger.debug("response: {}".format(stringify(response)))

    @staticmethod
    def _parse_response(response):
        try:
            tree = parse_xml(response)
            status = tree.findtext("status")
        except Exception:
            logger.exception("bad response")
            raise StatusError("response parse failed")

        if status.endswith('_ok') or status.startswith('s_'):
            logger.debug("GOOD status: {}".format(status))
        else:
            logger.error("BAD status: {}".format(status))
            raise StatusError(status)
        return tree

    def _check(self):
        assert self._auth_token, "auth token no found"

    @classmethod
    def _get_filename(cls, response):
        disposition = response['content-disposition']
        logger.debug("disposition: {}".format(disposition))
        return cls.FILENAME_PATTERN.search(disposition).groups()[1]

    def get_ticket(self):
        """Get the ticket used to generate an authentication page.

        Refer: http://developers.box.net/w/page/12923936/ApiFunction_get_ticket
        """
        if not self._ticket:
            params = urllib.urlencode({
                       'action': "get_ticket",
                       'api_key': self._api_key})
            logger.debug("get_ticket params: {}".format(params))
            response = urllib.urlopen(self.REST_URL, params)
            tree = self._parse_response(response)
            self._ticket = tree.findtext('ticket')
        return self._ticket

    @classmethod
    def _get_request_token(cls, content):
        return cls.REQUEST_TOKEN_PATTERN.search(content).groups()[0]

    def _automate1(self, url, login, password):
        browser = get_browser(True)
        response = browser.open(url)
        request_token = self._get_request_token(response.read())

        browser.select_form(name='login_form1')
        browser['login'] = login
        browser['password'] = password

        # add an field which is supposed to be added by javascript
        browser.form.new_control('text', 'request_token', {'value': ''})
        browser.form.fixup()
        browser['request_token'] = request_token

        response = browser.submit()
        if not browser.viewing_html():
            raise StatusError("something is wrong when browsing HTML")

    def _automate2(self, url, login, password):
        http = httplib2.Http()
        httplib2.debuglevel = 1
        response, content = http.request(url, 'GET')
        logger.debug("response: {}".format(response))

        request_token = self._get_request_token(content)
        headers = {'Content-type': "application/x-www-form-urlencoded"}
        body = {
                'login': login, 'password': password,
                'request_token': request_token,
                'dologin': "1", # important
               }
        # setting "is_human" is important
        headers['Cookie'] = response['set-cookie'] + "; is_human=true"
        response, content = http.request(
                url, 'POST', headers=headers, body=urllib.urlencode(body))
        logger.debug("response 2: {}".format(response))

    def authorize(self, login, password):
        """Automates authorization process.

        Refer: http://developers.box.net/w/page/12923915/ApiAuthentication
        """
        url = self.AUTH_URL.format(self.get_ticket())
        logger.debug("browsing auth url: {}".format(url))
        if True: # both will do
            arg = "1"
        else:
            arg = "2"
        getattr(self, "_automate" + arg)(url, login, password)

    def get_auth_token(self, login, password=None):
        """Get an auth token.
        This method MUST be called before any account-relative action.

        If auth token has not been set before, read from configuration file.
        If not found, initiate authorization.
        Refer:
        http://developers.box.net/w/page/12923930/ApiFunction_get_auth_token
        """
        if self._auth_token:
            return self._auth_token

        try:
            auth_token = self._conf_parser.get(
                    "account-" + login, "auth_token")
            if auth_token:
                self._auth_token = auth_token
                return auth_token
        except ConfigParser.NoSectionError:
            logger.warn("no account set for {}".format(login))
        else:
            logger.warn("empty auth_token")

        # authorize first
        assert password, "Password must be provided"
        self.authorize(login, password)
        # get auth token
        params = urllib.urlencode({
                   'action': "get_auth_token",
                   'api_key': self._api_key,
                   'ticket': self._ticket})
        logger.debug("get_auth_token params: {}".format(params))
        response = urllib.urlopen(self.REST_URL, params)
        tree = self._parse_response(response)
        self._auth_token = tree.findtext('auth_token')
        return self._auth_token

    def get_account_info(self):
        """Get account information"""
        self._check()

        params = urllib.urlencode({
                   'action': "get_account_info",
                   'api_key': self._api_key,
                   'auth_token': self._auth_token})
        logger.debug("get_account_info params: {}".format(params))
        response = urllib.urlopen(self.REST_URL, params)
        tree = self._parse_response(response)
        info = map_element(tree.find('user'))
        self._log_response(info)
        return info

    @staticmethod
    def _unzip_node(zipped):
        unzipped = base64.b64decode(zipped)
        sio = StringIO()
        sio.write(unzipped)
        zipfile = ZipFile(sio)
        stream = zipfile.open(zipfile.namelist()[0])
        return parse_xml(stream)

    def list(self, folder_id=None, extra_params=None, by_name=False):
        """List files under the given folder"""
        self._check()

        if not extra_params:
            extra_params = [self.ONELEVEL, self.NOZIP, self.SIMPLE]
        if not folder_id:
            folder_id = self.ROOT_ID
        elif by_name:
            folder_id = self._convert_to_id(folder_id, False)
        params = urllib.urlencode({
                   'action': "get_account_tree",
                   'api_key': self._api_key,
                   'auth_token': self._auth_token,
                   'folder_id': encode(folder_id)})
        params += "&"
        extra_param_list = [('params[]', param) for param in extra_params]
        params += urllib.urlencode(extra_param_list)
        logger.debug("list params: {}".format(params))
        response = urllib.urlopen(self.REST_URL, params)
        tree = self._parse_response(response)
        if self.NOZIP in extra_params:
            folder = tree.find('tree/folder')
        else:
            zipped = tree.findtext("tree")
            folder = self._unzip_node(zipped)
        self._log_response(folder)
        return folder

    @staticmethod
    def _get_file_id(files, name, is_file):
        if is_file:
            type_ = "file"
            attr_name = "file_name"
            files = files.find('files')
        else:
            type_ = "folder"
            attr_name = "name"
            files = files.find('folders')
        logger.debug(u"checking {} {}".format(type_, name))
        for f in (files and files.findall(type_) or []):
            if f.attrib[attr_name] == name:
                f_id = f.attrib['id']
                logger.debug(u"found name '{}' with id {}".format(name, f_id))
                return f_id

    def get_file_id(self, path, is_file=None):
        """Return the file's id for the given server path.
        If is_file is True, check only file type,
        if is_file is False, check only folder type,
        if is_file is None, check both file and folder type.
        Return id and type(whether file or not).
        """
        if not path or path == "/":
            return self.ROOT_ID, False

        path = os.path.normpath(path)
        paths = [p for p in path.split(os.sep) if p]
        folder_id = self.ROOT_ID
        for name in paths[:-1]:
            logger.debug(u"look up folder '{}' in {}".format(name, folder_id))
            files = self.list(folder_id)
            folder_id = self._get_file_id(files, name, False)
            if not folder_id:
                logger.debug(u"no found {} under folder {}".
                        format(name, folder_id))
                return None, None
        # time to check name
        name = paths[-1]
        logger.debug(u"checking name: {}".format(name))
        files = self.list(folder_id)
        if not is_file:
            id_ = self._get_file_id(files, name, False)
            if id_:
                return id_, False

        if is_file is None or is_file:
            return self._get_file_id(files, name, True), True

        return None, None

    def _convert_to_id(self, name, is_file):
        file_id, is_file = self.get_file_id(name, is_file)
        if not file_id:
            logger.error(u"cannot find id for {}".format(name))
            raise ValueError("wrong file name")
        return file_id

    def get_file_info(self, file_id, by_name=False):
        """Get file detailed information"""
        self._check()

        if by_name:
            file_id = self._convert_to_id(file_id, True)
        params = urllib.urlencode({
                   'action': "get_file_info",
                   'api_key': self._api_key,
                   'auth_token': self._auth_token,
                   'file_id': encode(file_id)})
        logger.debug("get_file_info params: {}".format(params))
        response = urllib.urlopen(self.REST_URL, params)
        tree = self._parse_response(response)
        info = map_element(tree.find('info'))
        self._log_response(info)
        return info

    def mkdir(self, name, parent=None, by_name=False, share=0):
        """Create a directory if it does not exists and return its id
        """
        self._check()

        if not parent:
            parent = self.ROOT_ID
        elif by_name:
            parent = self._convert_to_id(parent, False)
        params = urllib.urlencode({
                   'action': "create_folder",
                   'api_key': self._api_key,
                   'auth_token': self._auth_token,
                   'parent_id': encode(parent),
                   'name': encode(name),
                   'share': share})
        logger.debug("mkdir params: {}".format(params))
        response = urllib.urlopen(self.REST_URL, params)

        tree = self._parse_response(response)
        folder = map_element(tree.find('folder'))
        self._log_response(folder)
        return folder

    def rmdir(self, id_, by_name=False):
        """Remove the given directory"""
        self._remove(False, id_, by_name)

    def remove(self, id_, by_name=False):
        """Remove the given file"""
        self._remove(True, id_, by_name)

    def _remove(self, is_file, id_, by_name):
        self._check()

        if by_name:
            id_ = self._convert_to_id(id_, is_file)
        params = urllib.urlencode({
                   'action': "delete",
                   'api_key': self._api_key,
                   'auth_token': self._auth_token,
                   'target': "file" if is_file else "folder",
                   'target_id': encode(id_)})
        logger.debug("delete params: {}".format(params))
        response = urllib.urlopen(self.REST_URL, params)
        tree = self._parse_response(response)
        self._log_response(tree)

    def rename_file(self, id_, new_name, by_name=False):
        """Rename a file"""
        self._rename(True, id_, new_name, by_name)

    def rename_dir(self, id_, new_name, by_name=False):
        """Rename a directory"""
        self._rename(False, id_, new_name, by_name)

    def _rename(self, is_file, id_, new_name, by_name):
        self._check()

        if by_name:
            id_ = self._convert_to_id(id_, is_file)
        params = urllib.urlencode({
                   'action': "rename",
                   'api_key': self._api_key,
                   'auth_token': self._auth_token,
                   'target': "file" if is_file else "folder",
                   'target_id': encode(id_),
                   'new_name': encode(new_name)})
        logger.debug("rename params: {}".format(params))
        response = urllib.urlopen(self.REST_URL, params)
        tree = self._parse_response(response)
        self._log_response(tree)

    def move_file(self, file_, new_folder, by_name=False):
        """Move a file to another folder"""
        self._move(True, file_, new_folder, by_name)

    def move_dir(self, folder, new_folder, by_name=False):
        """Move a directory to another folder"""
        self._move(False, folder, new_folder, by_name)

    def _move(self, is_file, target, new_folder, by_name):
        self._check()

        if by_name:
            target = self._convert_to_id(target, is_file)
            new_folder = self._convert_to_id(new_folder, False)
        params = urllib.urlencode({
                   'action': "move",
                   'api_key': self._api_key,
                   'auth_token': self._auth_token,
                   'target': "file" if is_file else "folder",
                   'target_id': encode(target),
                   'destination_id': encode(new_folder)})
        logger.debug("move params: {}".format(params))
        response = urllib.urlopen(self.REST_URL, params)
        tree = self._parse_response(response)
        self._log_response(tree)

    def download_dir(self, folder_id, localdir=None, by_name=False):
        """Download the directory with the given id to a local directory"""
        self._check()

        if by_name:
            folder_id = self._convert_to_id(folder_id, False)

        tree = self.list(folder_id)
        folder_name = tree.attrib['name']
        localdir = os.path.join(localdir or ".", folder_name)
        try:
            os.makedirs(localdir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

        files = tree.find('files')
        for f in (files and files.findall('file') or []):
            file_name = f.attrib['file_name']
            file_id = f.attrib['id']
            localfile = os.path.join(localdir, file_name)
            if os.path.exists(localfile):
                # check
                sha1 = f.attrib['sha1']
                if get_sha1(localfile) == sha1:
                    logger.debug("same sha1")
                    continue
            # download
            self.download_file(file_id, localdir)

        folders = tree.find('folders')
        for f in (folders and folders.findall('folder') or []):
            folder_id = f.attrib['id']
            self.download_dir(folder_id, localdir)

    def download_file(self, file_id, localdir=None, by_name=False):
        """Download the file with the given id to a local directory"""
        self._check()

        if by_name:
            file_id = self._convert_to_id(file_id, True)
        url = self.DOWNLOAD_URL.format(self._auth_token, encode(file_id))
        logger.debug("download url: {}".format(url))
        http = httplib2.Http()
        response, content = http.request(url, 'GET')
        logger.debug("response: {}".format(response))
        status = response.status
        if status / 100 != 2:
            logger.error("bad status: {}".format(status))
            raise StatusError(status)

        name = self._get_filename(response)
        logger.debug("filename: {}".format(name))
        localdir = encode(localdir)
        file(os.path.join(localdir or ".", name), 'wb').write(content)

    def upload(self, uploaded, parent=None, by_name=False, precheck=True):
        """Upload the given file/directory to a remote directory"""
        self._check()

        if not parent:
            parent = self.ROOT_ID
        elif by_name:
            parent = self._convert_to_id(parent, False)
        uploaded = os.path.normpath(uploaded)
        if os.path.isfile(uploaded):
            self._upload_file(uploaded, parent, precheck)
        elif os.path.isdir(uploaded):
            self._upload_dir(uploaded, parent, precheck)
        else:
            logger.debug("ignore to upload {}".format(uploaded))

    def _upload_dir(self, upload_dir, parent, precheck):
        # create a directory in box.net, it's ok if it already exists
        newdir = self.mkdir(os.path.basename(upload_dir), parent)
        dirname = newdir['folder_id']
        for filename in os.listdir(upload_dir):
            path = os.path.join(upload_dir, filename)
            self.upload(path, dirname, False, precheck)

    def _check_file_on_server(self, filepath, parent):
        """Check if the file with the same SHA in the server"""
        filename = os.path.basename(filepath)
        tree = self.list(parent)
        files = tree.find('files')
        for f in (files and files.findall('file') or []):
            attrs = f.attrib
            name = attrs['file_name']
            if name == filename:
                logger.debug(u"found same filename: {}".format(name))
                sha1 = attrs['sha1']
                if get_sha1(filepath) == sha1:
                    logger.debug("same sha1")
                    return True
                logger.debug("diff sha1")
                return #attrs['id']
        logger.debug(u"file {} not found under the directory {}"
                .format(filename, parent))

    def _upload_file(self, upload_file, parent, precheck):
        if precheck and self._check_file_on_server(upload_file, parent):
            logger.debug(u"skip uploading file: {}".format(upload_file))
            return

        logger.debug(u"uploading {} to {}".format(upload_file, parent))
        url = self.UPLOAD_URL.format(self._auth_token, parent)
        logger.debug(u"upload url: {}".format(url))

        # Register the streaming http handlers with urllib2
        register_openers()
        upload_file = encode(upload_file)
        datagen, headers = multipart_encode({'file': open(upload_file)})

        class DataWrapper(object):
            """Fix filename encoding problem"""

            def __init__(self, filename, datagen, headers):
                data = datagen.next()
                length = int(headers['Content-Length']) - len(data)
                filename = os.path.basename(filename)
                data = BoxApi.FILENAME_PATTERN.sub(
                        "\g<1>" + filename + "\\3", data)
                headers['Content-Length'] = str(length + len(data))
                self.datagen = datagen
                self.header_data = data

            def __iter__(self):
                return self

            def next(self):
                if self.header_data:
                    data = self.header_data
                    self.header_data = None
                    return data
                else:
                    return self.datagen.next()

        datagen = DataWrapper(upload_file, datagen, headers)
        request = urllib2.Request(url, datagen, headers)
        response = urllib2.urlopen(request)
        tree = self._parse_response(response)
        self._log_response(tree)

    def compare_file(self, localfile, remotefile, by_name=False):
        """Compare files between server and client"""
        sha1 = get_sha1(localfile)
        info = self.get_file_info(remotefile, by_name)
        return sha1 == info['sha1']

    def compare_dir(self, localdir, remotedir,
            by_name=False, ignore_common=True):
        """Compare directories between server and client"""
        tree = self.list(remotedir, [self.SIMPLE], by_name)
        localdir = os.path.normpath(localdir)
        return self._compare_dir(localdir, tree,
                DiffResult(localdir, tree, ignore_common))

    def _compare_dir(self, localdir, dir_tree, result):
        files = dir_tree.find('files')
        server_file_nodes = files and files.findall('file') or []
        server_file_map = dict((f.attrib['file_name'], f) \
                            for f in server_file_nodes)

        folders = dir_tree.find('folders')
        server_folder_nodes = folders and folders.findall('folder') or []
        server_folder_map = dict((f.attrib['name'], f) \
                            for f in server_folder_nodes)

        result_item = result.start_add(dir_tree)
        dir_subtrees = []
        for filename in os.listdir(localdir):
            path = os.path.join(localdir, filename)
            if os.path.isfile(path):
                node = server_file_map.pop(filename, None)
                if node is None:
                    result_item.add_client_unique(True, path)
                else:
                    sha1 = node.attrib['sha1']
                    result_item.add_compare(get_sha1(path) != sha1, path, node)
            elif os.path.isdir(path):
                folder_node = server_folder_map.pop(filename, None)
                if folder_node is None:
                    result_item.add_client_unique(False, path)
                else:
                    dir_subtrees.append(folder_node)
        result_item.add_server_unique(True, server_file_map)
        result_item.add_server_unique(False, server_folder_map)
        # compare recursively
        for subtree in dir_subtrees:
            path = os.path.join(localdir, subtree.attrib['name'])
            self._compare_dir(path, subtree, result)
        result.end_add()
        return result

    def sync(self, localdir, remotedir, dry_run=False, by_name=False,
            ignore=is_dotfile):
        """Sync directories between client and server"""
        if dry_run:
            logger.info("dry run...")
        result = self.compare_dir(localdir, remotedir, by_name)
        client_unique_files = result.get_client_unique(True)
        for path, node in client_unique_files:
            f = os.path.join(localdir, path)
            id_ = node.attrib['id']
            if ignore(f):
                logger.info(u"ignoring file: {}".format(f))
            else:
                logger.info(u"uploading file: {} to node {}".format(f, id_))
                if not dry_run:
                    self.upload(f, id_, False, False)
        client_unique_folders = result.get_client_unique(False)
        for path, node in client_unique_folders:
            f = os.path.join(localdir, path)
            id_ = node.attrib['id']
            logger.info(u"uploading folder: {} to node {}".format(f, id_))
            if not dry_run:
                self.upload(f, id_, False, False)

        server_unique_files = result.get_server_unique(True)
        for path, node in server_unique_files:
            id_ = node.attrib['id']
            logger.info(u"removing file {} with id = {}".format(path, id_))
            if not dry_run:
                self.remove(id_)
        server_unique_folders = result.get_server_unique(False)
        for path, node in server_unique_folders:
            id_ = node.attrib['id']
            logger.info(u"removing folder {} with id = {}".format(path, id_))
            if not dry_run:
                self.rmdir(id_)

        diff_files = result.get_compare(True)
        for localpath, remote_node, context_node in diff_files:
            localfile = os.path.join(localdir, localpath)
            remote_id = remote_node.attrib['id']
            remotedir_id = context_node.attrib['id']
            logger.info(u"uploading diff file {} with remote id = {} under {}"
                    .format(localfile, remote_id, remotedir_id))
            if not dry_run:
                self.upload(localfile, remotedir_id, False, False)

        #diff_files = result.get_compare(False)
        #for localpath, remote_node, context_node in diff_files:
            #localfile = os.path.join(localdir, localpath)
            #remote_id = remote_node.attrib['id']
            #remotedir_id = context_node.attrib['id']
            #print u"same file {} with remote id = {} under {}".format(
                    #localfile, remote_id, remotedir_id)
