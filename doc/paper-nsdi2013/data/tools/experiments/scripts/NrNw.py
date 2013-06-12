#!/usr/bin/python

import urllib
import urllib2
import time
import random
import socket
import base64
import traceback
import sys

WRITER_PORT = 44444
WRITER_FILE = "/file-%s" % socket.gethostname()
AUTH = "jude:sniff"
MODE = '0666'
BLOCK_SIZE = 61440
CMD_URL = "http://vcoblitz-cmi.cs.princeton.edu/NrNw-command"

ret = {}

peer_files = []

EXPERIMENT_SERVER_URL = "http://vcoblitz-cmi.cs.princeton.edu:40000/"
HOSTNAME = socket.gethostname()

def post_message( loglevel, msg_txt ):
   global HOSTNAME
   global EXPERIMENT_SERVER_URL
   global VERBOSE

   values = { "host": HOSTNAME, "date": time.ctime(), "loglevel": loglevel, "msg": msg_txt }
   data = urllib.urlencode(values)

   req = urllib2.Request( EXPERIMENT_SERVER_URL, data )

   print "%s: %s" % (loglevel, msg_txt)

   try:
      resp = urllib2.urlopen( req )
      resp.close()
   except Exception, e:
      print( "EXCEPTION", "post_message: %s" % e )



class SyndicateHTTPRedirectHandler( urllib2.HTTPRedirectHandler ):
   def http_error_302( self, req, fp, code, msg, headers ):
      self.code = code
      return urllib2.HTTPRedirectHandler(self, req, fp, code, msg, headers)

   http_error_301 = http_error_303 = http_error_304 = http_error_302


def delete_file( dat ):
   global WRITER_FILE
   global HOSTNAME
   
   dat['delete_file'] = {}

   http_m = ""
   http_m += "DELETE %s HTTP/1.0\r\n" % (WRITER_FILE)
   http_m += "Host: %s\r\n" % (HOSTNAME)
   http_m += "\r\n"

   rc = 0
   
   try:

      dat['delete_file']['start'] = time.time()
      
      s = socket.socket( socket.AF_INET, socket.SOCK_STREAM, 0 )
      s.connect( ("localhost", WRITER_PORT) )

      s.send( http_m )

      buf = s.recv(32768)
      s.close()

      dat['delete_file']['end'] = time.time()
      
      dat['delete_file']['response'] = buf
      
   except Exception, e:
      dat['delete_file']['exception'] = str(e)
      rc = -1

   return rc


   
def create_file( dat ):
   global WRITER_FILE
   global AUTH
   global BLOCK_SIZE
   global HOSTNAME
   global MODE

   dat['create_file'] = {}

   size = BLOCK_SIZE * 100
   http_m = ""
   http_m += "PUT %s HTTP/1.0\r\n" % (WRITER_FILE)
   http_m += "Host: %s\r\n" % (HOSTNAME)
   http_m += "Content-Type: application/octet-stream\r\n"
   http_m += "Content-Length: %s\r\n" % (size)
   http_m += "Authorization: Basic %s\r\n" % (base64.b64encode(AUTH))
   http_m += "X-POSIX-Mode: %s\r\n" % (MODE)
   http_m += "\r\n"

   rc = 0
   
   try:
      dat['create_file']['start'] = time.time()

      s = socket.socket( socket.AF_INET, socket.SOCK_STREAM, 0 )
      s.connect( ("localhost", WRITER_PORT) )

      s.send( http_m )
      sent = 0
      me = HOSTNAME

      while sent < size:
         buf = me + "\n"
         if sent + len(buf) > size:
            buf = buf[:size - sent]

         s.send( buf )
         sent += len(buf)

      buf = s.recv(32768)
      s.close()

      dat['create_file']['end'] = time.time()
      dat['create_file']['size'] = sent
      dat['create_file']['response'] = buf

   except Exception, e:
      dat['create_file']['exception'] = str(e)
      dat['create_file']['start'] = -1
      dat['create_file']['end'] = -1
      dat['create_file']['size'] = -1
      dat['create_file']['response'] = None
      rc = -1

   return rc
   

def read_file( dat, filename, block_size ):
   global WRITER_PORT

   rc = 0
   if dat.get('read_file') == None:
      dat['read_file'] = {}
   if dat['read_file'].get(filename) == None:
      dat['read_file'][filename] = {}
      dat['read_file'][filename]['start_open'] = []
      dat['read_file'][filename]['end_open'] = []
      dat['read_file'][filename]['start_recv'] = []
      dat['read_file'][filename]['end_recv'] = []
      dat['read_file'][filename]['exception'] = []

   run = {}
   try:
      request = urllib2.Request( "http://localhost:" + str(WRITER_PORT) + "/" + filename )
      opener = urllib2.build_opener()

      run['start_open'] = time.time()
      f = opener.open( request )
      run['end_open'] = time.time()

      run['start_recv'] = time.time()

      # check status
      if f.code >= 400:
         f.close()
         raise Exception( "Read status code rc = %s" % f.code )
      
      while True:
         buf = f.read(block_size)
         if len(buf) == 0:
            break

      run['end_recv'] = time.time()
      run['exception'] = None
      
   except Exception, e:
      run['start_open'] = -1
      run['end_open'] = -1
      run['start_recv'] = -1
      run['end_recv'] = -1
      run['exception'] = str(e)
      rc = -1


   for k in run.keys():
      dat['read_file'][filename][k].append( run[k] )

   return rc


def write_file( dat, filename, block_size, percentage, new_data_seed ):
   global AUTH
   global WRITER_PORT

   rc = 0
   if dat.get('write_file') == None:
      dat['write_file'] = {}
   if dat['write_file'].get( filename ) == None:
      dat['write_file'][filename] = {}
      dat['write_file'][filename]['start_write'] = []
      dat['write_file'][filename]['end_write'] = []
      dat['write_file'][filename]['response'] = []
      dat['write_file'][filename]['exception'] = []

   run = {}
   
   try:
      
      upload_len = int(block_size * percentage)
      block_buf = new_data_seed * ((BLOCK_SIZE / len(new_data_seed)) + 1)
      if len(block_buf) > upload_len:
         block_buf = block_buf[:upload_len]

      http_m = ""
      http_m += "POST /%s HTTP/1.0\r\n" % filename
      http_m += "Host: %s\r\n" % (HOSTNAME)
      http_m += "Authorization: Basic %s\r\n" % (base64.b64encode( AUTH ))
      http_m += "Content-Length: %s\r\n" % upload_len
      http_m += "Content-Type: application/octet-stream\r\n"
      http_m += "Content-Range: bytes=0-%s\r\n" % upload_len
      http_m += "\r\n"
      
      s = socket.socket( socket.AF_INET, socket.SOCK_STREAM, 0 )
      s.connect( ("localhost", WRITER_PORT) )
      
      # begin upload
      run['start_write'] = time.time()
      
      s.send( http_m )
      s.send( block_buf )

      # get response
      resp = s.recv( 32768 )

      run['end_write'] = time.time()
      run['response'] = resp
      run['exception'] = None

      s.close()


   except Exception, e:
      run['start_write'] = -1
      run['end_write'] = -1
      run['exception'] = str(e)
      run['response'] = None
      rc = -1

   for k in run.keys():
      dat['write_file'][filename][k].append( run[k] )

   return rc
      
      
      
def choose_peers( dat, size ):
   global peer_files
   global HOSTNAME
   
   dat['choose_peers'] = {
      'peers': None,
      'exception': None
   }
      
   
   try:
      request = urllib2.Request( "http://localhost:" + str(WRITER_PORT) + "/" )
      opener = urllib2.build_opener()

      f = opener.open( request )

      filebuf = ""
      while True:
         buf = f.read(32768)
         if len(buf) == 0:
            break

         filebuf += buf

      file_metadata = filebuf.split('\n')[:-1]

      # eliminate the . and ..
      file_metadata = file_metadata[2:]
      
      me = HOSTNAME
      valid_metadata = []
      for fm in file_metadata:
         if me not in fm.split()[1]:
            valid_metadata.append( fm )

      if len(valid_metadata) < size:
         raise Exception( "Too few files to choose %s peers" % size)

      random.shuffle( valid_metadata )

      chosen_files = valid_metadata[:size]

      for chosen in chosen_files:
         peer_files.append( chosen.split()[0] )

      dat['choose_peers']['peers'] = peer_files

   except Exception, e:
      dat['choose_peers']['exception'] = str(e)

   return dat['choose_peers']['peers']


      
def download_peer_files( dat, files, count ):
   global BLOCK_SIZE
   global WRITER_PORT
   
   for f in files:
      for i in xrange( 0, count ):
         read_file( dat, f, BLOCK_SIZE )


         

def wait_command( ret, cmd_url, value ):
   if ret.get('command') == None:
      ret['command'] = {}

   post_message( "PRAGMA", "wait for %s" % value )
   rc = []
   waiting = True
   while waiting:
      try:
         request = urllib2.Request( cmd_url )
         opener = urllib2.build_opener()

         f = opener.open( request )

         cmd = f.read(32768)
         f.close()

         cmds = cmd.split()
         if cmds[0] == "exit":
            print ret
            sys.exit(0)
            
         if cmds[0] == value:
            waiting = False
            if len(cmds) > 0:
               rc = cmds[1:]

         else:
            time.sleep(60)

      except Exception, e:
         print e
         ret['command'][value] = str(e)
         rc = None

   return rc

"""

def wait_command( ret, cmd_url, value ):
   if value == "peers":
      return ['2']
   if value == "read":
      return ['1']
   if value == "write":
      return ['0.1', "hello world"]
"""      
   
         
if __name__ == "__main__":
   ret = {}

   time.sleep( random.random() * 180 )

   # clean up from a previous execution
   post_message( "PRAGMA", "delete file")
   rc = delete_file( ret )
   if rc != 0:
      print ret
      sys.exit(1)
   
   time.sleep( random.random() * 180 )

   # make our file
   post_message( "PRAGMA", "create file")
   rc = create_file( ret )
   if rc != 0:
      print ret
      sys.exit(1)

   
   params = wait_command( ret, CMD_URL, "peers" )
   if params == None:
      print ret
      sys.exit(1)
   
   num_peers = int(params[0])
   
   # pick our peers
   peers = choose_peers( ret, num_peers )
   if peers == None:
      print ret
      sys.exit(1)

   post_message( "PRAGMA", "peers = %s" % peers )
   running = True
   while running:
      # read the files X times
      params = wait_command( ret, CMD_URL, "read" )
      if params == None:
         print ret
         sys.exit(1)

      post_message( "PRAGMA", "read %s times" % params[0] )
      
      read_count = int(params[0])
      rc = 0
      for peer_file in peers:
         for i in xrange(0,read_count):
            rc = read_file( ret, peer_file, BLOCK_SIZE )
            if rc != 0:
               post_message( "PRAGMA", "read %s rc = %s" % (peer_file, rc) )
               print ret
               sys.exit(1)

      post_message( "PRAGMA", "finished reading, begin writing" )
      
      # write our file, changing a given percentile
      params = wait_command( ret, CMD_URL, "write" )
      if params == None:
         print ret
         sys.exit(1)

      
      percentile = float(params[0])
      data_seed = params[1]

      # pick a peer's file and write a certain percent of its blocks
      peer_file = random.choice( peers )

      post_message( "PRAGMA", "write %s percent of %s" % (percentile * 100, peer_file) )
      
      rc = write_file( ret, peer_file, BLOCK_SIZE, percentile, HOSTNAME )

      post_message( "PRAGMA", "write rc = %s" % rc)

