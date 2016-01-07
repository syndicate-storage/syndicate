"""
   Copyright 2013 The Trustees of Princeton University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

appstats_MAX_STACK = 20
appstats_CALC_RPC_COSTS = True

# copied from https://github.com/dound/gae-sessions/blob/master/demo/appengine_config.py
"""
from openid.gaesession import SessionMiddleware, SESSION_COOKIE_KEY, delete_expired_sessions
import os

def webapp_add_wsgi_middleware(app):

  #Does this get called too often?
  #while delete_expired_sessions() is False:
    #pass

  from google.appengine.ext.appstats import recording
  app = SessionMiddleware(app, cookie_key=SESSION_COOKIE_KEY)
  app = recording.appstats_wsgi_middleware(app)
  return app
"""