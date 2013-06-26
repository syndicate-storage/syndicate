# copied from https://github.com/dound/gae-sessions/blob/master/demo/appengine_config.py

from openid.gaesession import SessionMiddleware, SESSION_COOKIE_KEY, delete_expired_sessions

# suggestion: generate your own random key using os.urandom(64)
# WARNING: Make sure you run os.urandom(64) OFFLINE and copy/paste the output to
# this file.  If you use os.urandom() to *dynamically* generate your key at
# runtime then any existing sessions will become junk every time you start,
# deploy, or update your app!
import os

appstats_CALC_RPC_COSTS = True

def webapp_add_wsgi_middleware(app):

#Does this get called too often?
#  while delete_expired_sessions() is False:
#    pass

  from google.appengine.ext.appstats import recording
  app = SessionMiddleware(app, cookie_key=SESSION_COOKIE_KEY)
  app = recording.appstats_wsgi_middleware(app)
  return app
