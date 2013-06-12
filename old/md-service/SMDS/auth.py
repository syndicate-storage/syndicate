#
# PLCAPI authentication parameters
#
# Mark Huang <mlhuang@cs.princeton.edu>
# Copyright (C) 2006 The Trustees of Princeton University
#
# $Id: Auth.py 18344 2010-06-22 18:56:38Z caglar $
# $URL: http://svn.planet-lab.org/svn/PLCAPI/trunk/PLC/Auth.py $
#

new_sha1 = None

def new_sha1_hashlib():
   return hashlib.sha1()

def new_sha1_sha():
   return sha.new()

import crypt
try:
    import hashlib
    from hashlib import sha1 as sha
    
    new_sha1 = new_sha1_hashlib
except ImportError:
    import sha
   
    new_sha1 = new_sha1_sha
    
import hmac
import time

from SMDS.faults import *
from SMDS.parameter import Parameter, Mixed
from SMDS.user import Users

def map_auth(auth):
    if auth['AuthMethod'] == "password" or \
         auth['AuthMethod'] == "capability":
        expected = PasswordAuth()
    elif auth['AuthMethod'] == "anonymous":
        expected = AnonymousAuth()
    else:
        raise MDInvalidArgument("must be 'password', or 'anonymous'", "AuthMethod")
    return expected

class Auth(Parameter):
    """
    Base class for all API authentication methods, as well as a class
    that can be used to represent all supported API authentication
    methods.
    """

    def __init__(self, auth = None):
        if auth is None:
            auth = {'AuthMethod': Parameter(str, "Authentication method to use", optional = False)}
        Parameter.__init__(self, auth, "API authentication structure")

    def check(self, method, auth, *args):
        # Method.type_check() should have checked that all of the
        # mandatory fields were present.
        assert 'AuthMethod' in auth

        expected = map_auth(auth)

        # Re-check using the specified authentication method
        method.type_check("auth", auth, expected, (auth,) + args)


class AnonymousAuth(Auth):
    """
    PlanetLab version 3.x anonymous authentication structure.
    """

    def __init__(self):
        Auth.__init__(self, {
            'AuthMethod': Parameter(str, "Authentication method to use, always 'anonymous'", False),
            })

    def check(self, method, auth, *args):
        if 'anonymous' not in method.roles:
            raise MDAuthenticationFailure, "Not allowed to call method anonymously"

        method.caller = None



def auth_user_from_username( api, username ):
   users = Users( api, {'username': username, 'enabled': True} )
   if len(users) != 1:
      raise MDAuthenticationFailure, "No such account"
   
   return users[0]


def auth_user_from_email( api, email ):
   users = Users( api, {'email': email, 'enabled': True} )
   if len(users) != 1:
      raise MDAuthenticationFailure, "No such account"
   
   return users[0]


def auth_password_check( api, auth, user, method ):
   # Method.type_check() should have checked that all of the
   # mandatory fields were present.
   assert auth.has_key('Username')

   if method != None and auth['Username'] == api.config.MD_API_MAINTENANCE_USER:
      # "Capability" authentication, whatever the hell that was
      # supposed to mean. It really means, login as the special
      # "maintenance user" using password authentication. Can
      # only be used on particular machines (those in a list).
      sources = api.config.MD_API_MAINTENANCE_SOURCES.split()
      if method.source is not None and method.source[0] not in sources:
         raise MDAuthenticationFailure, "Not allowed to login to maintenance account"

      # Not sure why this is not stored in the DB
      password = api.config.MD_API_MAINTENANCE_PASSWORD

      if auth['AuthString'] != password:
         raise MDAuthenticationFailure, "Maintenance account password verification failed"
      
   else:
      # compare SHA1 hashes of the database's password and the given password
      m = new_sha1()
      m.update( auth['AuthString'] )
      auth_password_hash = m.hexdigest().lower()
      
      plaintext = auth['AuthString'].encode(api.encoding)
      password = user['password'].lower()

      if password is None or auth_password_hash != password:
         raise MDAuthenticationFailure, "Password verification failed for '%s'" % (auth["Username"])

   return True
   


class PasswordAuth(Auth):
    """
    PlanetLab version 3.x password authentication structure.
    """

    def __init__(self):
        Auth.__init__(self, {
            'AuthMethod': Parameter(str, "Authentication method to use, always 'password' or 'capability'", optional = False),
            'Username': Parameter(str, "PlanetLab username, typically an e-mail address", optional = False),
            'AuthString': Parameter(str, "Authentication string, typically a password", optional = False),
            })

    def check(self, method, auth, *args):
        # Method.type_check() should have checked that all of the
        # mandatory fields were present.
        user = None
        try:
           user = auth_user_from_username( method.api, auth['Username'] )
        except Exception, e:
           raise MDAuthenticationFailure, "Authentication failed (exception = '%s')" % e
           
        rc = auth_password_check( method.api, auth, user, method )
        if not rc:
           raise MDAuthenticationFailure, "Authentication failed"
        
        if not set(user['roles']).intersection(method.roles):
            raise MDAuthenticationFailure, "Not allowed to call method"

        method.caller = user
