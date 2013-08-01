#!/usr/bin/env python

# Google App Engine OpenID Relay Party code.
# Copied from https://github.com/openid/python-openid
# Modifications by Jude Nelson

import webapp2 
import webob
import logging

import cgi
import urlparse
import sys, os
import errno

import traceback 

def quoteattr(s):
    qs = cgi.escape(s, 1)
    return '"%s"' % (qs,)

import openid
from openid.store import gaestore
from openid.consumer import consumer
from openid.oidutil import appendArgs
from openid.cryptutil import randomString
from openid.fetchers import setDefaultFetcher, Urllib2Fetcher
from openid.extensions import pape, sreg

import gaesession


class GAEOpenIDRequestHandler(webapp2.RequestHandler):
    """Request handler that knows how to verify an OpenID identity."""

    TITLE = "OpenID"
    USE_SREG = "use_sreg"
    USE_PAPE = "use_pape"
    IMMEDIATE_MODE = "immediate"
    USE_STATELESS = "use_stateless"

    if os.environ.get('SERVER_SOFTWARE','').startswith('Development'):
      TRUST_ROOT_HOST = "localhost:8080"
      HOST_URL = "http://" + TRUST_ROOT_HOST
    else:
      TRUST_ROOT_HOST = "syndicate-metadata.appspot.com"
      HOST_URL = "https://" + TRUST_ROOT_HOST

    OPENID_PROVIDER_NAME = "VICCI"

    OPENID_PROVIDER_URL = "https://www.vicci.org/id/"

    OPENID_PROVIDER_AUTH_HANDLER = "https://www.vicci.org/id-allow"


    def render_redirect( self, request, trust_root, return_to, immediate ):
       """
       Redirect response.
       """
       self.response.write(request.htmlMarkup(
                              trust_root, return_to,
                              form_tag_attrs={'id':'openid_message'},
                              immediate=immediate)
                           )

       return


    def render_success( self, info, sreg_resp=None, pape_resp=None ):
      """
      Render successful authentication
      """
      css_class = 'alert'
      display_identifier = info.getDisplayIdentifier()
      
      fmt = "Successfully verified %s as identity."
      message = fmt % (cgi.escape(display_identifier),)

      if info.endpoint.canonicalID:
            # You should authorize i-name users by their canonicalID,
            # rather than their more human-friendly identifiers.  That
            # way their account with you is not compromised if their
            # i-name registration expires and is bought by someone else.
            message += ("  This is an i-name, and its persistent ID is %s"
                        % (cgi.escape(info.endpoint.canonicalID),))


      self.render(message, css_class, display_identifier,
                  sreg_data=sreg_resp, pape_data=pape_resp)

      return


    def render_failure( self, info, sreg_resp=None, pape_resp=None ):
      """
      Render failed authentication.
      """
      display_identifier = info.getDisplayIdentifier()

      css_class = 'error'
      message = None

      if display_identifier:
         fmt = "Verification of %s failed: %s"
         message = fmt % (cgi.escape(display_identifier),
                           info.message)

      else:
         message = "Verification failed"
         display_identifier = ""
         
      logging.error( message )

      self.render(message, css_class, display_identifier,
                  sreg_data=sreg_resp, pape_data=pape_resp)

      return


    def render_error( self, info, sreg_resp=None, pape_resp=None ):
      """
      Render general error message.
      """
      
      display_identifier = info.getDisplayIdentifier()
      message = "Verification failed"

      logging.error(message)

      self.render(message, "error", display_identifier,
            sreg_data=sreg_resp, pape_data=pape_resp)

      return



    def render_cancel( self, info, sreg_resp=None, pape_resp=None ):
      """
      Render cancelled authentication
      """

      display_identifier = info.getDisplayIdentifier()
      message = "Verification canceled"

      logging.error(message)
      
      self.render(message, "error", display_identifier,
            sreg_data=sreg_resp, pape_data=pape_resp)

      return


    def render_setup_needed( self, info, sreg_resp=None, pape_resp=None ):
      """
      Render 'setup-needed' message.
      """
      css_class = 'error'
      message = None
      display_identifier = info.getDisplayIdentifier()
      
      if info.setup_url:
            message = '<a href=%s>Setup needed</a>' % (quoteattr(info.setup_url),)
      else:
            # This means auth didn't succeed, but you're welcome to try
            # non-immediate mode.
            message = 'Setup needed'

      logging.error(message)

      self.render(message, "error", display_identifier,
            sreg_data=sreg_resp, pape_data=pape_resp)

      return



    def getConsumer(self, stateless=False):
        if stateless:
            store = None
        else:
            store = gaestore.GAEStore()
        return consumer.Consumer(self.getSession(), store)

        
    def getSession(self):
        """Return the existing session or a new session"""
        if not hasattr(self, "session"):
            session = gaesession.get_current_session()

            if not session.has_key( 'id' ):
               # load from datastore
               session = gaesession.Session( cookie_key = gaesession.SESSION_COOKIE_KEY )

               if not session.has_key( 'id' ):
                     # new session
                     session.start( ssl_only=True )
                     sid = randomString(16, '0123456789abcdef')
                     session['id'] = sid

            self.session = session
        return self.session

        
    def setSessionCookie(self, session):
        cookie_headers = session.make_cookie_headers()
        for ch in cookie_headers:
           self.response.headers.add( "Set-Cookie", ch )
        
        """
        sid = session['id']
        session_cookie = '%s=%s;' % (gaesession.SESSION_COOKIE_KEY, sid)
        self.response.headers['Set-Cookie'] = session_cookie
        """


    def setRedirect(self, url):
        self.response.status = 302
        self.response.headers['Location'] = url

    def load_query( self ):
       if not hasattr(self, "query"):
          self.query = {}
          if self.request.method == "GET":
            self.parsed_uri = urlparse.urlparse(self.request.path_qs)
            for k, v in cgi.parse_qsl(self.parsed_uri[4]):
               self.query[k] = v.decode('utf-8')
               
          elif self.request.method == "POST":
            post_data = self.request.body
            for k, v in cgi.parse_qsl(post_data):
                self.query[k] = v


          logging.info("query = %s" % str(self.query) )
       
        
    def get(self):
        """Dispatching logic. There are two paths defined:

          / - Display an empty form asking for an identity URL to
              verify
          /verify - Handle form submission, initiating OpenID verification
          /process - Handle a redirect from an OpenID server

        Any other path gets a 404 response. This function also parses
        the query parameters.

        If an exception occurs in this function, a traceback is
        written to the requesting browser.
        """
        try:
            self.load_query()
            
            path = self.parsed_uri[2]
            if path == '/':
               self.render()
            elif path == '/verify':
               self.doVerify()
            elif path == '/process':
               self.doProcess()
            elif path == '/affiliate':
               self.doAffiliate()
            else:
               self.notFound()

        except Exception, e:
            logging.info("exception: %s" % traceback.format_exc() )
            self.response.status = 500
            self.response.headers['Content-type'] = 'text/plain'
            self.setSessionCookie( self.getSession() )
            self.response.write("Internal Server Error")
            logging.exception(e)
            

    def begin_openid_auth( self ):
      
      """
      Begin OpenID authentication.
      Return:
         request on success
         throw consumer.DiscoveryFailure on discovery error
         (None, -EINVAL) for no openid identifier
         (None, -ENOTCONN) for no services
      """

      self.load_query()

      # First, make sure that the user entered something
      openid_url = urlparse.urljoin( self.OPENID_PROVIDER_URL, self.query.get('openid_username') )
      if not openid_url:
         logging.error("No OpenID URL given")
         return (None, -errno.EINVAL)
         
      use_sreg = self.USE_SREG in self.query
      use_pape = self.USE_PAPE in self.query
      use_stateless = self.USE_STATELESS in self.query

      oidconsumer = self.getConsumer(stateless = use_stateless)
      try:
         request = oidconsumer.begin(openid_url)
      except consumer.DiscoveryFailure, exc:
         logging.error("Error in discovery on %s: %s" % (cgi.escape(openid_url), cgi.escape(str(exc[0]))) )
         raise exc

      else:
         if request is None:
            logging.error("Error in discovery on %s: %s" % (cgi.escape(openid_url), cgi.escape(str(exc[0]))) )
            return (None, -errno.ENOTCONN)
            
         else:
            # Then, ask the library to begin the authorization.
            # Here we find out the identity server that will verify the
            # user's identity, and get a token that allows us to
            # communicate securely with the identity server.
            if use_sreg:
               self.requestRegistrationData(request)

            if use_pape:
               self.requestPAPEDetails(request)

            return (request, 0)
            
            
    def doVerify(self):
        """
        Initiating OpenID verification.
        """
        self.load_query()
      
        openid_url = urlparse.urljoin( self.OPENID_PROVIDER_URL, self.query.get('openid_username') )
        rc = 0
        
        try:
            request, rc = self.begin_openid_auth()
        except consumer.DiscoveryFailure, exc:
           
            fetch_error_string = 'Error in discovery: %s' % (cgi.escape(str(exc[0])))
            
            self.render(fetch_error_string,
                        css_class='error',
                        form_contents=openid_url)

            return rc
            
        if rc == -errno.EINVAL:
           # bad input
           self.render('Enter your OpenID Identifier', css_class='error')
           return rc

        elif rc == -errno.ENOTCONN:
           # no service
           msg = 'No OpenID services found for <code>%s</code>' % (cgi.escape(openid_url),)
           self.render(msg, css_class='error', form_contents=openid_url)
           return rc

        else:
           # success!
           trust_root = "http://" + self.TRUST_ROOT_HOST
           return_to = self.buildURL( "process" )
           immediate = self.IMMEDIATE_MODE in self.query

           redirect_url = request.redirectURL( trust_root, return_to, immediate=immediate )

           logging.info("redirect to %s" % redirect_url )
           
           if request.shouldSendRedirect():
              self.setRedirect(redirect_url)   
              
           else:
              self.render_redirect( request, trust_root, return_to, immediate )

           return rc

                   
    def requestRegistrationData(self, request):
        sreg_request = sreg.SRegRequest(
            required=['nickname'], optional=['fullname', 'email'])
        request.addExtension(sreg_request)

        
    def requestPAPEDetails(self, request):
        pape_request = pape.Request([pape.AUTH_PHISHING_RESISTANT])
        request.addExtension(pape_request)


    def doProcess(self):
       self.load_query()
      
       info, sreg_resp, pape_resp  = self.complete_openid_auth()

       if info.status == consumer.FAILURE:
          # In the case of failure, if info is non-None, it is the
          # URL that we were verifying. We include it in the error
          # message to help the user figure out what happened.
          self.render_failure( info, sreg_resp, pape_resp )

       elif info.status == consumer.SUCCESS:
          # Success means that the transaction completed without
          # error. If info is None, it means that the user cancelled
          # the verification.

          # This is a successful verification attempt. If this
          # was a real application, we would do our login,
          # comment posting, etc. here.
          self.render_success( info, sreg_resp, pape_resp )

       elif info.status == consumer.CANCEL:
          # canceled
          self.render_cancel( info, sreg_resp, pape_resp )

       elif info.status == consumer.SETUP_NEEDED:
          # This means auth didn't succeed, but you're welcome to try
          # non-immediate mode.
          self.render_setup_needed( info, sreg_resp, pape_resp )

       else:
          # Either we don't understand the code or there is no
          # openid_url included with the error. Give a generic
          # failure message. The library should supply debug
          # information in a log.
          self.render_error( info, sreg_resp, pape_resp )

       return info, sreg_resp, pape_resp
          
       
    def complete_openid_auth(self):
        self.load_query()
      
        """
        Handle the redirect from the OpenID server.
        return (consumer info, sreg response, pape response)
        """
        oidconsumer = self.getConsumer()

        # Ask the library to check the response that the server sent
        # us.  Status is a code indicating the response type. info is
        # either None or a string containing more information about
        # the return type.
        url = "http://" + self.request.headers.get("Host") + self.request.path
        info = oidconsumer.complete(self.query, url)
        
        display_identifier = info.getDisplayIdentifier()
        pape_resp = None
        sreg_resp = None

        if info.status == consumer.SUCCESS:
            session = self.getSession()
            session['openid_url']=cgi.escape(display_identifier)
            session['authenticated'] = True

            pape_resp = pape.Response.fromSuccessResponse(info)
            sreg_resp = sreg.SRegResponse.fromSuccessResponse(info)

            if sreg_resp != None:
               sreg_list = sreg_resp.items()
               for k, v in sreg_list:
                  field_name = str(sreg.data_fields.get(k, k))
                  value = cgi.escape(v.encode('UTF-8'))
                  session[field_name] = value

            session.regenerate_id()
            
        return (info, sreg_resp, pape_resp)


    def doAffiliate(self):
        """Direct the user sign up with an affiliate OpenID provider."""
        sreg_req = sreg.SRegRequest(['nickname'], ['fullname', 'email'])
        href = sreg_req.toMessage().toURL(self.OPENID_PROVIDER_URL)

        message = """Get an OpenID at <a href=%s>%s</a>""" % (
            quoteattr(href), self.OPENID_PROVIDER_NAME)
        self.render(message)


    def buildURL(self, action, **query):
        """Build a URL relative to the server base url, with the given
        query parameters added."""
        base = urlparse.urljoin(self.HOST_URL, action)
        return appendArgs(base, query)

    def notFound(self):
        self.load_query()
      
        """Render a page with a 404 return code and a message."""
        fmt = 'The path <q>%s</q> was not understood by this server.'
        msg = fmt % (self.path,)
        openid_url = urlparse.urljoin( self.OPENID_PROVIDER_URL, self.query.get('openid_username') )
        self.render(msg, 'error', openid_url, status=404)

    def renderSREG(self, sreg_data):
        if not sreg_data:
            self.response.write(
                '<div class="alert">No registration data was returned</div>')
        else:
            sreg_list = sreg_data.items()
            sreg_list.sort()
            self.response.write(
                '<h2>Registration Data</h2>'
                '<table class="sreg">'
                '<thead><tr><th>Field</th><th>Value</th></tr></thead>'
                '<tbody>')

            odd = ' class="odd"'
            for k, v in sreg_list:
                field_name = sreg.data_fields.get(k, k)
                value = cgi.escape(v.encode('UTF-8'))
                self.response.write(
                    '<tr%s><td>%s</td><td>%s</td></tr>' % (odd, field_name, value))
                if odd:
                    odd = ''
                else:
                    odd = ' class="odd"'

            self.response.write('</tbody></table>')

    def renderPAPE(self, pape_data):
        if not pape_data:
            self.response.write(
                '<div class="alert">No PAPE data was returned</div>')
        else:
            self.response.write('<div class="alert">Effective Auth Policies<ul>')

            for policy_uri in pape_data.auth_policies:
                self.response.write('<li><tt>%s</tt></li>' % (cgi.escape(policy_uri),))

            if not pape_data.auth_policies:
                self.response.write('<li>No policies were applied.</li>')

            self.response.write('</ul></div>')


    def render(self, message=None, css_class='alert', form_contents=None,
               status=200, title=TITLE,
               sreg_data=None, pape_data=None):
        """Render a page."""
        self.response.status = status
        self.pageHeader(title)
        if message:
            self.response.write("<div class='%s'>" % (css_class,))
            self.response.write(message)
            self.response.write("</div>")

        if sreg_data is not None:
            self.renderSREG(sreg_data)

        if pape_data is not None:
            self.renderPAPE(pape_data)

        self.pageFooter(form_contents)

    def pageHeader(self, title):
        """Render the page header"""
        self.setSessionCookie( self.getSession() )
        self.response.headers['Content-type'] = 'text/html; charset=UTF-8'
        self.response.write('''\
<html>
  <head><title>%s</title></head>
  <style type="text/css">
      * {
        font-family: verdana,sans-serif;
      }
      body {
        width: 50em;
        margin: 1em;
      }
      div {
        padding: .5em;
      }
      tr.odd td {
        background-color: #dddddd;
      }
      table.sreg {
        border: 1px solid black;
        border-collapse: collapse;
      }
      table.sreg th {
        border-bottom: 1px solid black;
      }
      table.sreg td, table.sreg th {
        padding: 0.5em;
        text-align: left;
      }
      table {
        margin: 0;
        padding: 0;
      }
      .alert {
        border: 1px solid #e7dc2b;
        background: #fff888;
      }
      .error {
        border: 1px solid #ff0000;
        background: #ffaaaa;
      }
      #verify-form {
        border: 1px solid #777777;
        background: #dddddd;
        margin-top: 1em;
        padding-bottom: 0em;
      }
  </style>
  <body>
    <h1>%s</h1>
    <p>
      This is the OpenID front-end to Syndicate.
    </p>
''' % (title, title))

    def pageFooter(self, form_contents):
        """Render the page footer"""
        if not form_contents:
            form_contents = ''

        if form_contents.startswith( self.OPENID_PROVIDER_URL ):
           form_contents = form_contents[ len(self.OPENID_PROVIDER_URL): ]

        self.response.write('''\
    <div id="verify-form">
      <form method="get" accept-charset="UTF-8" action=%s>
        VICCI e-mail address:
        <input type="text" name="openid_username" value=%s />
        <input type="submit" value="Verify" /><br />
        <input type="checkbox" name="immediate" id="immediate" /><label for="immediate">Use immediate mode</label>
        <input type="checkbox" name="use_sreg" id="use_sreg" checked /><label for="use_sreg">Request registration data</label>
        <input type="checkbox" name="use_pape" id="use_pape" /><label for="use_pape">Request phishing-resistent auth policy (PAPE)</label>
        <input type="checkbox" name="use_stateless" id="use_stateless" /><label for="use_stateless">Use stateless mode</label>
      </form>
    </div>
  </body>
</html>
''' % (quoteattr(self.buildURL('verify')), quoteattr(form_contents)))

            


