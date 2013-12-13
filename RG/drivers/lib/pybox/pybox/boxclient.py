#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Python client for manipulating files on box.com(a.k.a box.net).
"""

__author__ = "Hui Zheng"
__copyright__ = "Copyright 2011-2012 Hui Zheng"
__credits__ = ["Hui Zheng"]
__license__ = "MIT <http://www.opensource.org/licenses/mit-license.php>"
__version__ = "0.1"
__email__ = "xyzdll[AT]gmail[DOT]com"

import sys
import getpass
from optparse import OptionParser

from pybox.boxapi import BoxApi, ConfigError, StatusError
from pybox.utils import decode_args, get_logger, print_unicode, stringify

logger = get_logger()


def parse_args(argv):
    usage = "usage: %prog [options] [args]"
    parser = OptionParser(usage)
    parser.add_option("-U", "--username", dest="username",
            help="username(email)")
    parser.add_option("-p", "--password", action="store_true", dest="password",
            help="prompt password")
    parser.add_option("-a", "--auth-token", action="store_true",
            dest="auth_token", help="print auth token")
    parser.add_option("-I", "--account-info", action="store_true",
            dest="account_info", help="get account info")
    parser.add_option("-t", "--target", dest="target",
            help="target(f for file<default>, d for directory)")
    parser.add_option("-l", "--list", action="store_true", dest="list",
            help="list directory")
    parser.add_option("-w", "--what-id", dest="what_id",
            help="get a path(server-side)'s id")
    parser.add_option("-i", "--info", action="store_true", dest="info",
            help="get file info")
    parser.add_option("-M", "--mkdir", action="store_true", dest="mkdir",
            help="make a directory")
    parser.add_option("-R", "--remove", action="store_true", dest="remove",
            help="remove a file or directory")
    parser.add_option("-m", "--move", action="store_true", dest="move",
            help="move a file or directory")
    parser.add_option("-r", "--rename", action="store_true", dest="rename",
            help="rename a file or directory")
    parser.add_option("-1", "--onelevel", action="store_true", dest="onelevel",
            help="list one level files")
    parser.add_option("-z", "--zip", action="store_true", dest="zip",
            help="list file tree in zip format")
    parser.add_option("-N", "--nofiles", action="store_true", dest="nofiles",
            help="only list directory")
    parser.add_option("-s", "--simple", action="store_true", dest="simple",
            help="simple info")
    parser.add_option("-c", "--chdir", dest="chdir",
            help="change directory")
    parser.add_option("-d", "--download", action="store_true", dest="download",
            help="download file")
    parser.add_option("-u", "--upload", action="store_true", dest="upload",
            help="upload file")
    parser.add_option("-P", "--plain-name", action="store_true", dest="plain",
            help="use plain name instead of id")
    parser.add_option("-C", "--compare", action="store_true", dest="compare",
            help="compare local and remote directories")
    parser.add_option("-S", "--sync", action="store_true", dest="sync",
            help="sync local and remote files or directories")
    parser.add_option("-n", "--dry-run", action="store_true", dest="dry_run",
            help="show what would have been transferred when sync")
    parser.add_option("-f", "--from-file", dest="from_file",
            help="read arguments(separated by line break) from file")
    (options, args) = parser.parse_args(argv)
    if options.from_file:
        with open(options.from_file) as f:
            args = [arg.strip() for arg in f.readlines()]
    return (parser, options, decode_args(args, options))


def init_client(options):
    username = options.username
    if not username:
        username = raw_input("username/email: ").strip()
    password = getpass.getpass("password: ") if options.password else None

    try:
        client = BoxApi()
        auth_token = client.get_auth_token(username, password)
    except ConfigError as e:
        sys.stderr.write("box configuration error - {}\n".format(e))
        sys.exit(1)
    except (StatusError, AssertionError) as e:
        sys.stderr.write("{}\n".format(e))
        sys.exit(1)

    if options.auth_token:
        print_unicode(u"auth_token: {}".format(auth_token))
        sys.exit()

    if options.account_info:
        print_unicode(u"account_info: {}".format(client.get_account_info()))
        sys.exit()

    what_id = options.what_id
    if what_id:
        target = options.target
        if target == 'd':
            target_type = False
        elif target == 'f':
            target_type = True
        else:
            target_type = None
        id_, is_file = client.get_file_id(what_id, target_type)
        if not id_:
            print_unicode(u"no id found for {}(type: {})".format(
                    what_id, "unspecified" if target is None else target))
        else:
            print_unicode(u"{} {}'s id is {}".format(
                    "file" if is_file else "folder", what_id, id_))
        sys.exit()

    return client


def get_action(client, parser, options, args):
    target = options.target
    extra_args = []
    if options.rename:
        if len(args) % 2:
            parser.error("rename's arguments must be even numbers")
        action = 'rename_dir' if target == "d" else 'rename_file'
        # pair the arguments
        args = zip(args[::2], args[1::2])
    elif options.move:
        if len(args) % 2:
            parser.error("move's arguments must be even numbers")
        action = 'move_dir' if target == "d" else 'move_file'
        # pair the arguments
        args = zip(args[::2], args[1::2])
    elif options.list:
        action = 'list'
        params = []
        if options.onelevel:
            params.append(client.ONELEVEL)
        if not options.zip:
            params.append(client.NOZIP)
        if options.nofiles:
            params.append(client.NOFILES)
        if options.simple:
            params.append(client.SIMPLE)
        extra_args.append(params)
    elif options.info:
        action = 'get_file_info'
    elif options.remove:
        action = 'rmdir' if target == "d" else 'remove'
    elif options.mkdir:
        action = 'mkdir'
        extra_args.append(options.chdir)
    elif options.download:
        action = 'download_dir' if target == "d" else 'download_file'
        extra_args.append(options.chdir)
    elif options.upload:
        action = 'upload'
        extra_args.append(options.chdir)
    elif options.compare:
        if len(args) % 2:
            parser.error("compare's arguments must be even numbers")
        action = 'compare_dir' if target == "d" else 'compare_file'
        # pair the arguments
        args = zip(args[::2], args[1::2])
    elif options.sync:
        if len(args) % 2:
            parser.error("sync's arguments must be even numbers")
        action = 'sync'
        # pair the arguments
        args = zip(args[::2], args[1::2])
        extra_args.append(options.dry_run)
    else:
        parser.error("too few options")
    extra_args.append(options.plain)

    return (action, args, extra_args)


def main(argv=None):
    # parse the command line
    (parser, options, args) = parse_args(argv)

    # initialize client(may exit early for those no-arg commands)
    client = init_client(options)

    # prepare operations
    if len(args) == 0:
        parser.error("no arguments for the given option")
    action, args, extra_args = get_action(client, parser, options, args)

    # begin operations
    operate = getattr(client, action)
    errors = 0
    for arg in args:
        try:
            if isinstance(arg, basestring):
                result = operate(arg, *extra_args)
            #elif all(isinstance(i, basestring) for i in arg):
            else:
                result = operate(*(list(arg) + extra_args))
            if result is not None:
                print stringify(result)
            print "action {} on {} succeeded".format(action, stringify(arg))
        except Exception as e:
            errors += 1
            print "action {} on {} failed".format(action, stringify(arg))
            sys.stderr.write("error: {}\n".format(e))
            logger.exception(e)
    if errors > 0:
        sys.stderr.write("encountered {} error(s)\n".format(errors))
        return 1
    return 0

if __name__ == '__main__':
    sys.exit(main())
