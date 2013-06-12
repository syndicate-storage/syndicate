#!/usr/bin/python

"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

import urllib
import urllib2
import os
import sys
import socket
import select
import time
import BaseHTTPServer
import SocketServer
import urlparse
import subprocess
import cgi
import threading
import signal
import getopt
import urllib

import style

CONFIG="~/.experiment.conf"

# configuration parameters
TIMEOUT = 300
SLICE = "princeton_syndicate"
HTTPPORT = 40000
IDENTITY = None

# global status of experiments
SUCCESS = []
NOCONNECT = []
FAILED = []
KILLED = []
UPLOAD_FAILED = []
RUNNING = []

PROCS = {}

# runtime information 
RESOLVED = {}     # host: IP
START_TIME = time.time()

# command templates
SCP_CMD_LIST = ["scp", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=%s" % TIMEOUT]
SSH_CMD_LIST = ["ssh", "-tt", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=%s" % TIMEOUT]

def scp_experiment( host, fpath ):
   global IDENTITY
   ret = SCP_CMD_LIST[:]
   if IDENTITY != None:
      ret.append("-i")
      ret.append(IDENTITY)
      
   ret.append(fpath)
   expname = os.path.basename(fpath)
   ret.append("%s@%s:~/%s" % (SLICE, host, expname))
   return ret

def ssh_experiment( host, fpath ):
   global IDENTITY
   ret = SSH_CMD_LIST[:]
   if IDENTITY != None:
      ret.append("-i")
      ret.append(IDENTITY)
      
   ret.append("%s@%s" % (SLICE, host))
   expname = os.path.basename( fpath )
   ret.append("chmod +x ~/%s; echo ---------- BEGIN %s ----------; sudo ~/%s; echo ---------- END %s ----------" % (expname, expname, expname, expname))
   return ret

def ssh_command( host, cmd ):
   global IDENTITY
   ret = SSH_CMD_LIST[:]
   if IDENTITY != None:
      ret.append("-i")
      ret.append(IDENTITY)
      
   ret.append("%s@%s" % (SLICE, host))
   ret.append("echo ---------- BEGIN cmd-%s ----------; %s; echo ---------- END cmd-%s ----------" % (START_TIME, cmd, START_TIME))
   return ret
   

class ExperimentHandler( BaseHTTPServer.BaseHTTPRequestHandler ):

   HEADER = """
   <!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
   <html dir="ltr"><head>
   <meta content="text/html; charset=UTF-8" http-equiv="content-type">
   <title>Experiment Status</title>
   """

   END_HEADER = "</head><body><div class=\"yui-t1\" id=\"doc3\"><div style=\"margin-left: 40px;\" id=\"ft\">"

   END_DOC = "</div></div></body></html>"

   def get_column_data(self):
      column_metadata = self.server.columns
      ret = {}
      for (name, value) in column_metadata.items():
         ret[name] = globals()[value]

      return ret

   def do_GET(self):
      HOSTINFO = self.server.HOSTINFO
      columns = self.get_column_data()
      
      if self.path == "/":
         # main page
         self.send_response(200)
         self.end_headers()

         self.wfile.write( self.HEADER )
         self.wfile.write( style.STYLESHEET )
         self.wfile.write( self.END_HEADER )

         my_hostname = socket.gethostname()
         my_portnum = self.server.server_address[1]

         column_names = columns.keys()
         column_names.sort()
         
         # convert to list
         dd = [columns[cn] for cn in column_names]
         mm = max( [len(s) for s in dd] )
         
         self.wfile.write("<table border=\"3\">")
         self.wfile.write("<tr><div style=\"margin-left: 10%%; width: %s%%; background-color: rgb(102,102,102)\">" % int(100.0 / len(dd)))

         for cn in column_names:
            data_url = "http://%s:%s/%s" % (my_hostname, my_portnum, urllib.quote(cn))
            self.wfile.write("<td><h1>%s (<a href=\"%s\">%s</a>)</h1></td>" % (cn, data_url, len(columns[cn])) )
         
         self.wfile.write("</div></tr>")

         
         for i in xrange(0, mm):
            html = "<tr><div style=\"margin-left: 10%%; width: %s%%; background-color: rgb(102,102,102)\">" % int(100.0 / len(dd))
            for j in xrange(0,len(dd)):
               if i >= len(dd[j]):
                  html += "<td style=\"background-color: rgb(54,54,54)\"></td>"
               else:
                  host_url = "http://%s:%s/%s" % (my_hostname, my_portnum, dd[j][i])
                  html += "<td style=\"width: %s%%; background-color: rgb(102,102,102)\"><a href=\"%s\">%s</a></td>" % (int(100.0 / len(dd)), host_url, dd[j][i])

            html += "</div></tr>"
            self.wfile.write( html )

         
         self.wfile.write("</table><br><br>")
         self.wfile.write( self.END_DOC )
         return

      else:
         path = urllib.unquote( self.path )
         path = os.path.basename( path )
        
         # request for a column?
         if path in columns.keys():
            # give back a list
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()

            for name in columns[path]:
               self.wfile.write("%s\n" % name)

            return
         
         hostname = path
         hostdata = hostname + ".txt"
         if not os.path.exists( os.path.join(HOSTINFO,hostdata)):
            self.send_response(404)
            self.end_headers()
            self.wfile.write( self.HEADER )
            self.wfile.write( style.STYLESHEET )
            self.wfile.write( self.END_HEADER )
            self.wfile.write("<body><h1>Host '%s' has not reported data</h1>" % hostname)
            self.wfile.write( self.END_DOC )
            return

         else:
            fd = open( os.path.join(HOSTINFO, hostdata), "r" )
            self.send_response(200)
            self.end_headers()
            self.wfile.write( self.HEADER )
            self.wfile.write( style.STYLESHEET )
            self.wfile.write( self.END_HEADER )
            self.wfile.write("<h1><br><br>Tail of '%s'</h1><br>" % hostname)
            
            last_100_lines_subproc = subprocess.Popen("tail -n 100 %s" % (os.path.join(HOSTINFO, hostdata)), shell=True, stdout=subprocess.PIPE)
            last_100_lines = last_100_lines_subproc.stdout.read()
            last_100_lines_subproc.wait()

            lines = last_100_lines.split("\n")
            for line in lines:
               self.wfile.write(line + "<br>")

            self.wfile.write("<br><br>")
            self.wfile.write( self.END_DOC )
            return

      return
      

class ResultsServer( SocketServer.ThreadingMixIn, BaseHTTPServer.HTTPServer ):
   def __init__( self, addr, handler, outputs, columns ):
      BaseHTTPServer.HTTPServer.__init__(self, addr, handler)
      
      self.HOSTINFO = outputs
      self.columns = columns

      
class HTTPServerThread( threading.Thread ):
   def __init__(self, outputs, columns, port):
      threading.Thread.__init__(self)
      self.outputs = outputs
      self.columns = columns
      self.port = port

   def run(self):
      httpd = ResultsServer( ('', self.port), ExperimentHandler, self.outputs, self.columns )
      while True:
         httpd.handle_request()



def open_cmd( node, cmd_list, output ):
   # run the command
   outfd = None
   errfd = None

   inputfd = open("/dev/null", "r")
   
   if output != None:
      out_path = os.path.join( output, node + ".txt" )
      outfd = open( out_path, "a" )
      errfd = outfd

   p = subprocess.Popen( cmd_list, stdin=inputfd, stdout=outfd, stderr=outfd, close_fds=True )
   return p

   

def close_cmd( proc ):
   if proc.stdin != None:
      proc.stdin.close()

   if proc.stdout != None:
      proc.stout.close()

   if proc.stderr != None:
      proc.stderr.close()

      

def get_ssh_host( node ):
   global RESOLVED
   ip = RESOLVED.get( node )
   if ip == None:
      ip = node

   return ip

   
      
def upload_experiment( node, fpath, output ):
   cmd_list = scp_experiment( get_ssh_host( node ), fpath )
   p = open_cmd( node, cmd_list, output )
   return p

   
   
def run_experiment( node, fpath, output ):
   cmd_list = ssh_experiment( get_ssh_host( node ), fpath )
   p = open_cmd( node, cmd_list, output )
   return p


   
def run_command( node, command, output ):
   cmd_list = ssh_command( get_ssh_host( node ), command )
   p = open_cmd( node, cmd_list, output )
   return p


   
def do_experiment( nodelist, fpath, output, uploader=upload_experiment, runner=run_experiment ):
   global RUNNING
   global SUCCESS
   global FAILED
   global KILLED
   global NOCONNECT
   global UPLOAD_FAILED

   global PROCS
   
   PROCS = {}    # node : proc
   
   RUNNING = []
   runnable = []
   SUCCESS = []
   FAILED = []
   KILLED = []
   UPLOAD_FAILED = []

   if uploader != None:
      # upload and run the experiment
      for node in nodelist:
         print "Upload to %s" % node
         upload_proc = uploader( node, fpath, output )
         PROCS[ node ] = upload_proc

      # wait for uploads
      print "Wait %s seconds for uploads to complete" % (TIMEOUT)
      time.sleep( TIMEOUT )

      # kill all uploads
      for (node, proc) in PROCS.items():
         status = proc.poll()
         if status == None:
            # dead; kill it
            proc.kill()
            print "Killed upload to %s" % node
            KILLED.append( node )
         else:
            if status != 0:
               if status != 255:
                  # failed
                  print "Failed upload to %s (rc = %s)" % (node, str(int(status)))
                  UPLOAD_FAILED.append( node )
               else:
                  print "Connection timeout to %s (rc = %s)" % (node, str(int(status)))
                  NOCONNECT.append( node )
            else:
               runnable.append( node )

         close_cmd( proc )
         
   else:
      runnable = nodelist

   if runner != None:

      # run the experiment
      for node in runnable:
         print "Run experiment on %s" % node
         run_proc = runner( node, fpath, output )
         if run_proc == None:
            NOCONNECT.append( node )
         else:
            PROCS[node] = run_proc
            RUNNING.append( node )

      print "Wait for experiments to finish"

      while True:
         time.sleep(1)

         # poll everyone
         for (node, proc) in PROCS.items():
            status = proc.poll()
            if status != None:
               
               del PROCS[node]

               try:
                  RUNNING.remove( node )
               except:
                  pass

               if status == 0:
                  SUCCESS.append(node)
               elif status != 255:
                  FAILED.append(node)
               else:
                  NOCONNECT.append(node)

               print "(%s,%s) Experiment on %s rc = %s" % (len(SUCCESS), len(FAILED), node, str(int(status)))

               close_cmd( proc )
               
         if len(PROCS) == 0:
            break
          
   return len(SUCCESS)

   
def do_command( nodelist, command, output ):
   do_experiment( nodelist, command, output, uploader=None, runner=run_command )


def print_results( signal, frame ):
   global SUCCESS
   global FAILED
   global PROCS
   global KILLED
   global UPLOAD_FAILED

   # killall our processes
   for (node, proc) in PROCS.items():
      proc.kill()
      close_cmd( proc )
      KILLED.append( node )

   print ""
   print "Successful Experiments (%d)" % len(SUCCESS)
   SUCCESS.sort()
   for suc in SUCCESS:
      print "  " + str(suc)

   print ""

   print "Killed Experiments (%s) " % len(KILLED)
   KILLED.sort()
   for k in KILLED:
      print "  " + str(k)

   print ""

   print "Failed Experiments (%s)" % len(FAILED)
   FAILED.sort()
   for fail in FAILED:
      print "  " + str(fail)

   print ""

   sys.exit(0)
   

def resolve_host( node ):
   ip = None

   for i in xrange(0,3):
      try:
         ip = socket.gethostbyname( node )
         break
      except:
         time.sleep(1)
         pass

   return ip

def resolve_hosts( nodelist ):
   ressed = {}

   for node in nodelist:
      ip = resolve_host( node )
      if ip != None:
         ressed[node] = ip

   return ressed
         
def usage():
   opts = "[-r|--resolve] [-R|--resolve-cache] [-s|--slice=SLICE] [-t|--timeout=TIMEOUT] [-p|--port=PORT] [-i|--identity=SSH_IDENTITY]"
   print "Usage: %s %s NODELIST OUTPUT_DIR EXPERIMENT" % (sys.argv[0], opts)
   print "       %s %s NODELIST OUTPUT_DIR COMMAND" % (sys.argv[0], opts)
   
if __name__ == "__main__":
   args = None
   opts = None
   
   try:
      opts, args = getopt.getopt( sys.argv[1:], "rRs:t:p:i:", ["resolve", "--resolve-cache", "slice=", "timeout=", "port=", "identity="] )
   except getopt.GetoptErr, e:
      print str(e)
      usage()
      sys.exit(1)
   
   try:
      nodefile = args[0]
      output = args[1]
      command_str = " ".join(args[2:])
      experiment_file = args[2]
   except:
      usage()
      sys.exit(1)


   # parse options
   resolve = False
   cache = True

   for (o, a) in opts:
      if o in ("-r", "--resolve"):
         resolve = True

      elif o in ("-R", "--resolve-cache"):
         resolve = True
         cache = True

      elif o in ("-s", "--slice"):
         SLICE = a

      elif o in ("-t", "--timeout"):
         try:
            TIMEOUT = int(a)
         except:
            print "TIMEOUT must be an integer"
            usage()
            sys.exit(1)

      elif o in ("-p", "--port"):
         try:
            HTTPPORT = int(a)
         except:
            print "PORT must be an integer"
            usage()
            sys.exit(1)

      elif o in ("-i", "--identity"):
         IDENTITY = a
         

   # get the list of nodes
   nodes_fd = open(nodefile, "r")
   nodelist = nodes_fd.readlines()
   nodes_fd.close()

   nodelist = [nl.strip() for nl in nodelist]

   # kill scp, ssh processes on shutdown
   signal.signal( signal.SIGINT, print_results )
   signal.signal( signal.SIGQUIT, print_results )
   signal.signal( signal.SIGTERM, print_results )
   signal.signal( signal.SIGPIPE, signal.SIG_IGN )

   # start up results server
   columns = {
      "Successful":        "SUCCESS",
      "Failed":            "FAILED",
      "No connection":     "NOCONNECT",
      "Killed":            "KILLED",
      "Upload Failed":     "UPLOAD_FAILED",
      "Running":           "RUNNING"
   }
   
   http = HTTPServerThread( output, columns, HTTPPORT )
   http.setDaemon(True)
   http.start()

   # get host IP addresses first, if needed
   if resolve:
      print "Resolving hosts..."

      loaded = False

      # check to see if there is a cached version
      cache_filename = nodefile + ".resolved"
      
      if cache:
         fd = None
         try:
            fd = open( cache_filename, "r" )
         except:
            pass

         if fd != None:
            resolved_lines = fd.readlines()
            for line in resolved_lines:
               hostname, ip = line.split()
               RESOLVED[hostname] = ip
         
            loaded = True

            fd.close()

      if not loaded:
         RESOLVED = resolve_hosts( nodelist )

         if cache:
            fd = open( cache_filename, "w" )
            for (host, ip) in RESOLVED.items():
               fd.write("%s %s\n" % (host, ip))

            fd.close()
         


   print "Running..."
   
   processed = 0
   
   # is this a file or a shell command?
   if os.path.exists( experiment_file ):
      processed = do_experiment( nodelist, experiment_file, output )

   else:
      processed = do_command( nodelist, command_str, output )
      
   print "Ran %s experiments" % processed
   print "Press ^C to exit"
   while True:
      time.sleep(1)

   os._exit(0)
