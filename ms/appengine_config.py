# copied from https://github.com/dound/gae-sessions/blob/master/demo/appengine_config.py

from openid.gaesession import SessionMiddleware, SESSION_COOKIE_KEY, delete_expired_sessions
import os

appstats_MAX_STACK = 20
appstats_CALC_RPC_COSTS = True

def webapp_add_wsgi_middleware(app):

  #Does this get called too often?
  #while delete_expired_sessions() is False:
    #pass

  from google.appengine.ext.appstats import recording
  app = SessionMiddleware(app, cookie_key=SESSION_COOKIE_KEY)
  app = recording.appstats_wsgi_middleware(app)
  return app
