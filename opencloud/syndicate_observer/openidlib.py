#!/usr/bin/python

# this file is for import into steps/sync_slices.py, and maybe other
# components later

import traceback
import json
import urllib2
import httplib
import hashlib

# it is good practice in Python to define globals like this?  Are the names
# automatically encapsulated by the import filename, meaning that I could
# discard the prefix to the names?  (I'm guessing that they are, but I'm not
# knowledgable enough about Python yet to know for sure.)  -- Russ

OPENID_LIB_SERVERNAME="171.67.92.189:8001"
OPENID_LIB_DIR="users"
OPENID_ADDRESS_BASE="slc.onlab.us"

def generate_password(username):
    return hashlib.sha1(username).hexdigest()

def createOrUpdate_user(username, password):
    if query_user(username):
        retval = update_user(username, password)
    else:
        retval = create_user(username, password)

    # confirm that the form of the json object is what we expect
    if not ('username' in retval and 'password' in retval and 'url' in retval and 'id' in retval):
        print retval
        raise Exception("OpenID CreateOrUpdate User failed: invalid retval")

    return retval

def create_user(username, password):
    try:
        connection = httplib.HTTPConnection(OPENID_LIB_SERVERNAME)
        connection.connect()
        connection.request('POST', "/%s/" % OPENID_LIB_DIR,
                           json.dumps({"username": username, "password": password}),
                           {"Content-Type": "application/json"})
        return json.loads(connection.getresponse().read())
    except Exception, e:
        traceback.print_exc()
        raise Exception("OpenID: Failed to create new user")

def update_user(username, password):
    try:
        userid = get_id_by_username(username)

        connection = httplib.HTTPConnection(OPENID_LIB_SERVERNAME)
        connection.connect()
        connection.request('PATCH', "/%s/%d/" % (OPENID_LIB_DIR, userid),
                           json.dumps({"password": password}),
                           {"Content-Type": "application/json"})
        return json.loads(connection.getresponse().read())
    except Exception, e:
        traceback.print_exc()
        raise Exception("OpenID: Failed to create new user")

def get_entire_openID_table():
    url = "http://%s/%s" % (OPENID_LIB_SERVERNAME, OPENID_LIB_DIR)
    return json.load(urllib2.urlopen(url))

def get_user_list():
    return map(lambda x: x['username'], get_entire_openID_table())

def query_user(username):
    return username in get_user_list()

def get_id_by_username(username):
    table   = get_entire_openID_table()
    one_row = filter(lambda x: x['username'] == username, table)[0]
    return one_row['id']

def build_full_id(username):
    return "%s@%s" % (username, OPENID_ADDRESS_BASE)

