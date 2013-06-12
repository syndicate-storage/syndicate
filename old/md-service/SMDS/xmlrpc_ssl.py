#!/usr/bin/python


"""
XMLRPC server implementation with SSL support
Based on this article: http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/81549
"""

import SocketServer
import BaseHTTPServer
import SimpleHTTPServer
import SimpleXMLRPCServer

import socket, os, traceback
from OpenSSL import SSL

class MD_XMLRPC_SSL_Server(BaseHTTPServer.HTTPServer,SimpleXMLRPCServer.SimpleXMLRPCDispatcher):
   def verify_cb(connection, x509, errnum, errdepth, ok):
      if not ok:
         return False
      else:
         return ok

   def __init__(self, server_address, HandlerClass, server_keyfile_path, server_certfile_path, logfile=None, logRequests=False, cacerts=None, client_cert=False):
      """
      Secure XML-RPC server.

      It it very similar to SimpleXMLRPCServer but it uses HTTPS for transporting XML data.
      """
      self.logRequests = logRequests
      if logfile:
        self.logfile = logfile
        self.logRequests = True
      
      
      SimpleXMLRPCServer.SimpleXMLRPCDispatcher.__init__(self, allow_none=True, encoding="utf-8")
      
      SocketServer.BaseServer.__init__(self, server_address, HandlerClass)
      ctx = SSL.Context(SSL.SSLv3_METHOD)
      ctx.use_privatekey_file ( server_keyfile_path )
      ctx.use_certificate_file( server_certfile_path )

      if cacerts != None:
         ctx.load_verify_locations( cacerts )

      if client_cert:
         ctx.set_verify( SSL.VERIFY_PEER | SSL.VERIFY_FAIL_IF_NO_PEER_CERT, self.verify_cb )
      else:
         ctx.set_verify( SSL.VERIFY_PEER, verify_cb )

      self.socket = SSL.Connection(ctx, socket.socket(self.address_family,
                                                      self.socket_type))
      self.server_bind()
      self.server_activate()


class MD_XMLRPC_RequestHandler(SimpleXMLRPCServer.SimpleXMLRPCRequestHandler):
   """
   XMLRPC handler to forward calls to a server's API
   """
   def setup(self):
      self.connection = self.request
      self.rfile = socket._fileobject(self.request, "rb", self.rbufsize)
      self.wfile = socket._fileobject(self.request, "wb", self.wbufsize)
      
   
   def do_POST(self):
      """
      Handles the HTTPS POST request.

      It was copied out from SimpleXMLRPCServer.py and modified to shutdown the socket cleanly.
      """

      try:
         # get arguments
         data = self.rfile.read(int(self.headers["content-length"]))
         
         response = self.server._marshaled_dispatch(
                    data, getattr(self, '_dispatch', None)
                )
                
      except Exception, e: # This should only happen if the module is buggy
         # internal error, report as HTTP server error
         self.send_response(500)
         exc_str = str(e) + "\n" + traceback.format_exc() + "\n"
         self.send_header("Content-length", str(len(exc_str)) )
         self.send_header("Content-type", "text/plain")
         self.end_headers()
         
         self.wfile.write( exc_str )
         self.wfile.flush()
         self.connection.shutdown()
         
         self.server.logfile.write( exc_str )
         self.server.logfile.flush()
         
         print exc_str
         
      else:
         # got a valid XML RPC response
         self.send_response(200)
         self.send_header("Content-type", "text/xml")
         self.send_header("Content-length", str(len(response)))
         self.end_headers()
         self.wfile.write(response)

         # shut down the connection
         self.wfile.flush()
         self.connection.shutdown() # Modified here!
         
